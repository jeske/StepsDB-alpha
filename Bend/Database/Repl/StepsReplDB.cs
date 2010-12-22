// Copyright (C) 2008-2011 by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.IO; 
using System.Linq;

using System.Threading;

using Bend;



// TODO: need to generate a new LOG server-guid EVERYTIME we restart a log, to be sure we don't
//        collide with our old log.

// TODO: make sure non-active servers will never respond to getServerLogStatus() requests

// TODO: fix repl-full-copy so it doesn't jsut happen to work because data happens before logs in the keyspace! 

/*
 * TODO: 
 * 
 * - don't let us touch keys unless the server is "active"
 * - make a simulated "server connect" so we can also have "disconnect"
 * - tail-log command (where we supply log pointers, error if the pointer is too old)
 * - fallback to key-copy if the tail-log didn't work
 * - make sure which server-log we process first doesn't change final records 
 *      (timestamp order log apply? per-record timestamp?)
 *      
 * TODO: consider having "pending" log entries and "real" (how do we flip? two logs? flag?)
 * TODO: fix write-ordering problems (add record timestamp resolver attribute)
 * 
 * Cases to Handle
 * 
 * read/write keys in our keyspace
 * push new writes to other clients, manage 'quorum agreed' pointer
 * check/pull logs from other servers when we join/connect
 * 
 * 
 * our keyspace schema
 * 
 *  _config/DATA-INSTANCE-ID = <guid of the dataset>
 *  _config/MY-SERVER-ID = <server guid>
 *  
 *  _config/quorum_write_requirement = <number of servers before we advance the repl tail>
 *  _config/log_max_age = <oldest loglines we want to keep.. 
 *                             note tombstones have to be forced to live this long>
 *  _config/log_max_size = <max log size in approximate bytes>
 * 
 *  _seeds/<SERVER GUID>
 *  
 *  _logs/<SERVER GUID>/<logid> -> [update info]
 *  
 * 
 * not really using this yet...
 *  _log_commit_heads/<SERVER_GUID> -> the newest log entry that is known to be committed for this server_guid
 * 
 */

namespace Bend.Repl {

   
    internal class MyReplConnection : IReplConnection {
        ReplHandler hndl;
        internal MyReplConnection(ReplHandler hndl) {
            this.hndl = hndl;
        }

        public string getDataInstanceId() {
            return hndl.getDataInstanceId();
        }
        public string getServerGuid() {
            return hndl.getServerGuid();
        }
        public IEnumerable<LogStatus> getStatusForLogs() {
            return hndl.getStatusForLogs();
        }
        public ReplState getState() {
            return hndl.State;
        }
        public JoinInfo requestToJoin(string server_guid) {
            return hndl.requestToJoin(server_guid);
        }
        public int getEstimatedRemainingLogData(string server_guid, RecordKeyType log_start_key) {
            return hndl.getEstimatedRemainingLogData(server_guid, log_start_key);
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordData>> fetchLogEntries(
                    string log_server_guid,
                    RecordKeyType log_start_key) {
            return hndl.fetchLogEntries(log_server_guid, log_start_key);
        }
        public IStepsKVDB getSnapshot() {
            return hndl.getSnapshot();
        }

    }

    public enum ReplState {
        init,        // bringup
        rebuild,     // it's safe to just copy everything from someone else
        resolve,     // we have newer entries than someone else
        active,      // ready to serve queries
        error,       // catastrophic error, so just stop
        do_shutdown,
        shutdown     // shutdown down
    }

    // ----------------------------------------------

   
    public class ReplHandler {
        IStepsSnapshotKVDB next_stage;        
        internal ServerContext ctx;

        IReplConnection my_repl_interface;
        Dictionary<string, ReplLogFetcher> fetcher_for_logserverguid = new Dictionary<string, ReplLogFetcher>();

        Random rnd;
        ReplPusher pusher;
        string data_instance_id;

        private static FastUniqueIds id_gen = new FastUniqueIds();

        public static Random myrnd = new Random();

        Thread worker;
        bool should_shutdown = false;

       
        private volatile ReplState state = ReplState.init;
        public ReplState State { get { return state; } }


        public override string ToString() {
            return String.Format("ReplHandler({0}:{1})", this.ctx.server_guid, this.state.ToString());
        }

       

        #region Constructors

        public ReplHandler(IStepsSnapshotKVDB db, ServerContext ctx) {
            this.next_stage = db;
            this.my_repl_interface = new MyReplConnection(this);
            this.rnd = new Random();
            this.pusher = new ReplPusher(this);
            this.ctx = ctx;

            try {

                var di_rk = new RecordKey()
                    .appendKeyPart("_config")
                    .appendKeyPart("DATA-INSTANCE-ID");
                var rec = db.FindNext(di_rk, true);
                if (di_rk.CompareTo(rec.Key) != 0) {
                    throw new Exception(
                        String.Format("ReplHandler {0} , not able to fetch DATA-INSTANCE-ID", ctx.server_guid));
                }
                this.data_instance_id = rec.Value.ToString();
                Console.WriteLine("ReplHandler - {0}: data_instance_id {1}",
                    ctx.server_guid, data_instance_id);
            } catch (KeyNotFoundException) {
                throw new Exception("no data instance ID, try InitResume or InitJoin");
            }

            // check server_guid matches?

            // register ourself
            ctx.connector.registerServer(ctx.server_guid, this.my_repl_interface);

            // startup our background task
            worker = new Thread(delegate() {
                this.workerThread();
            });
            worker.Start();
        }

        public static ReplHandler InitResume(IStepsSnapshotKVDB db, ServerContext ctx) {
            ReplHandler repl = new ReplHandler(db, ctx);
            return repl;
        }

        public static ReplHandler InitFresh(IStepsSnapshotKVDB db, ServerContext ctx) {
            // init fresh

            // record our instance ID
            db.setValue(new RecordKey()
                .appendKeyPart("_config")
                .appendKeyPart("MY-SERVER-ID"),
                RecordUpdate.WithPayload(ctx.server_guid));

            // create and record a new instance ID
            db.setValue(new RecordKey()
                .appendKeyPart("_config")
                .appendKeyPart("DATA-INSTANCE-ID"),
                RecordUpdate.WithPayload(Lsd.numberToLsd(ReplHandler.myrnd.Next(), 15)));

            // record the "start of fresh log" record
                        
            db.setValue(new RecordKey()
                .appendKeyPart("_logs")
                .appendKeyPart(ctx.server_guid)
                .appendKeyPart(new RecordKeyType_Long(0)),
                RecordUpdate.WithPayload(new byte[0]));

            // record ourself as a seed/log 
            db.setValue(new RecordKey()
                .appendKeyPart("_config").appendKeyPart("seeds").appendKeyPart(ctx.server_guid),
                RecordUpdate.WithPayload(""));


            ReplHandler repl = new ReplHandler(db, ctx);

            repl.state = ReplState.active; // TODO: is this the right way to become active?            
            return repl;

        }

        public static ReplHandler InitJoin(IStepsSnapshotKVDB db, ServerContext ctx, string seed_name) {

            // connect to the other server, get his instance id, exchange seeds
            IReplConnection seed = ctx.connector.getServerHandle(seed_name);
            JoinInfo join_info = seed.requestToJoin(ctx.server_guid);

            // record the join result
            db.setValue(new RecordKey()
                .appendKeyPart("_config")
                .appendKeyPart("DATA-INSTANCE-ID"),
                RecordUpdate.WithPayload(join_info.data_instance_id));

            // init a clean log 
            db.setValue(new RecordKey()
                .appendKeyPart("_logs")
                .appendKeyPart(ctx.server_guid)
                .appendKeyPart(new RecordKeyType_Long(0)),
                RecordUpdate.WithPayload(new byte[0]));


            foreach (var seed_server in join_info.seed_servers) {
                db.setValue(new RecordKey()
                    .appendKeyPart("_config")
                    .appendKeyPart("seeds")
                    .appendKeyPart(seed_server),
                    RecordUpdate.WithPayload(""));
            }

            Console.WriteLine("InitJoin: server ({0}) joining seeds ({1})",
                ctx.server_guid, String.Join(",", join_info.seed_servers));

            ReplHandler repl = new ReplHandler(db, ctx);
            return repl;
        }

        #endregion

        // ---------------------------
        public string getServerGuid() {
            return this.ctx.server_guid;
        }

        public void Shutdown() {
            // remove us from the connector
            ctx.connector.unregisterServer(ctx.server_guid);
            this.should_shutdown = true;            
        }
       

        private LogEntry _decodeLogEntry(RecordKey key, RecordData data) {
            var le = new LogEntry();
            if (!((RecordKeyType_String)key.key_parts[0]).GetString().Equals("_logs")) {
                throw new Exception("_decodeLogEntry: handed non-log entry: " + key.ToString());
            }
            le.server_guid = ((RecordKeyType_String)key.key_parts[1]).GetString();
            le.logstamp = ((RecordKeyType_Long)key.key_parts[2]).GetLong();
            le.data = data.data;
            return le;
        }

        public LogStatus getStatusForLog(string server_guid) {
            var log_status = new LogStatus();
            log_status.server_guid = server_guid;

            var log_prefix_key = new RecordKey()                
                .appendKeyPart("_logs")
                .appendKeyPart(server_guid);
            // first log entry for this log
            
            try {
                var oldestlogrow = next_stage.FindNext(log_prefix_key, false);
                if (oldestlogrow.Key.isSubkeyOf(log_prefix_key)) {
                    log_status.oldest_entry_pointer =
                        ((RecordKeyType_Long)oldestlogrow.Key.key_parts[oldestlogrow.Key.key_parts.Count - 1]);
                } else {
                    log_status.oldest_entry_pointer = new RecordKeyType_Long(0);
                }
            } catch (KeyNotFoundException) {
                log_status.oldest_entry_pointer = new RecordKeyType_Long(0);
            }

            // newest log entry for this log
            try {
                var newestlogrow = next_stage.FindPrev(RecordKey.AfterPrefix(log_prefix_key), false);
                if (newestlogrow.Key.isSubkeyOf(log_prefix_key)) {
                    log_status.log_commit_head =
                        ((RecordKeyType_Long)newestlogrow.Key.key_parts[newestlogrow.Key.key_parts.Count - 1]);
                } else {
                    log_status.log_commit_head = new RecordKeyType_Long(0);
                }
            } catch (KeyNotFoundException) {
                log_status.log_commit_head = new RecordKeyType_Long(0);
            }


            // Console.WriteLine("_statusForLog returning: " + log_status);

            return log_status;
        }

       

        internal IEnumerable<LogStatus> getStatusForLogs() {
            var seeds_prefix = new RecordKey()                
                .appendParsedKey("_config/seeds");

            var scanrange = new ScanRange<RecordKey>(seeds_prefix,
                                            RecordKey.AfterPrefix(seeds_prefix), null);

            yield return getStatusForLog(ctx.server_guid); // be sure to include myself

            foreach (var seed_row in next_stage.scanForward(scanrange)) {
                RecordKeyType last_keypart = seed_row.Key.key_parts[seed_row.Key.key_parts.Count - 1];

                string server_guid = ((RecordKeyType_String)last_keypart).GetString();

                if (server_guid.Equals(ctx.server_guid)) { continue; } // skip ourselves

                yield return getStatusForLog(server_guid);
            }
        }

        public string getDataInstanceId() {
            return this.data_instance_id;
        }

        private void worker_fullRebuild() {
            IReplConnection srvr;
            // TODO: make sure servers are only listed as seeds when they are "active". 
            try {
                srvr = pusher.getRandomSeed();
                // double check that we didn't get ourself, because that won't work
                if (srvr.getServerGuid().CompareTo(this.getServerGuid()) == 0) {
                    Console.WriteLine("******************* ERROR: getRandomSeed() returned US when we're trying to full rebuild");
                    this.state = ReplState.error;
                    return;
                }
            } catch (ReplPusher.NoServersAvailableException) {
                Console.WriteLine("Repl({0}): fullRebuild - no servers available... return to init",
                    ctx.server_guid);
                this.state = ReplState.init;
                return;
            }

            // (1) clear our keyspace

            // TODO: How do we trigger a reinit on a new prefix, so we don't
            //   have to actually delete everything??  Maybe we can make SubsetStage
            //   capable of clearing by dropping the old prefix-id?  Or maybe we will 
            //   use built-in support for a range-deletion-tombstone? 

            // TODO: how do we verify that this isn't going to lose important information?

            // TODO: probably should just delete/copy _data and _logs
            //     so we don't lose our seeds and config info
            Console.WriteLine("Rebuild({0}): deleting our keys", ctx.server_guid);
            foreach (var row in this.next_stage.scanForward(ScanRange<RecordKey>.All())) {
                Console.WriteLine("   Rebuild({0}): deleting {1}", ctx.server_guid, row);
                this.next_stage.setValue(row.Key, RecordUpdate.DeletionTombstone());                
            }

            // (2) re-record our data-instance id, so we don't get confused
            next_stage.setValue(new RecordKey()
            .appendKeyPart("_config")
            .appendKeyPart("DATA-INSTANCE-ID"),
            RecordUpdate.WithPayload(this.data_instance_id));

            // (3) ask for a snapshot

            IStepsKVDB snapshot = srvr.getSnapshot();

            // (4) then copy the snapshot

            // TODO: make sure we get data keys before logs, or that we do something to
            //      be sure to know whether this copy completes successfully or not.
            // TODO: probably want to record our copy-progress, so we can continue copy after
            //      restart without having to do it from scratch!! 
            
            // TODO: need to be able to see tombstones in this scan!! 
            foreach (var row in snapshot.scanForward(ScanRange<RecordKey>.All())) {
                Console.WriteLine("    + Rebuild({0}) setValue: {1}", ctx.server_guid, row);
                this.next_stage.setValue(row.Key,RecordUpdate.WithPayload(row.Value.data));
            }

            // (5) make sure to record our server-id correctly
            next_stage.setValue(new RecordKey()
            .appendKeyPart("_config")
            .appendKeyPart("MY-SERVER-ID"),
            RecordUpdate.WithPayload(ctx.server_guid));

            // (6) if our log is empty, write our log-start
            LogStatus status = this.getStatusForLog(ctx.server_guid);
            if (status.log_commit_head.GetLong().CompareTo(0) == 0) {
                next_stage.setValue(new RecordKey()
                    .appendKeyPart("_logs")
                    .appendKeyPart(ctx.server_guid)
                    .appendKeyPart(new RecordKeyType_Long(0)),
                    RecordUpdate.WithPayload(new byte[0]));
            }

            // (7) make sure there is a seed/log entry for ourselves
            next_stage.setValue(new RecordKey()
                .appendKeyPart("_config").appendKeyPart("seeds").appendKeyPart(ctx.server_guid),
                RecordUpdate.WithPayload(""));

            Console.WriteLine("Rebuild({0}): finished, sending to init", ctx.server_guid);
            this.state = ReplState.init; // now we should be able to log resume!! 
        }

        public IStepsKVDB getSnapshot() {
            Console.WriteLine("Repl({0}): getSnapshot() returning new snapshot", ctx.server_guid);
            return this.next_stage.getSnapshot();
        }

        public void truncateLogs_Hack() {
            // we want to erase all log entries except the last to cause a gap
            // force others to full rebuild

            Console.WriteLine("*** ReplHandler({0}): truncateLogs_Hack!!", this.ctx.server_guid);

            foreach (var ls in this.getStatusForLogs()) {
                var scan_old_log_entries = new ScanRange<RecordKey>(
                    new RecordKey().appendKeyPart("_logs").appendKeyPart(ls.server_guid),
                    new RecordKey().appendKeyPart("_logs").appendKeyPart(ls.server_guid).appendKeyPart(ls.log_commit_head),
                    null);

                foreach (var row in this.next_stage.scanForward(scan_old_log_entries)) {
                    // make sure we stop before we delete the last entry
                    LogEntry le = _decodeLogEntry(row.Key, row.Value);
                    if (le.logstamp.Equals(ls.log_commit_head)) {
                        // we reached the head... 
                        break;
                    }
                    this.next_stage.setValue(row.Key, RecordUpdate.DeletionTombstone());
                    Console.WriteLine("   truncateLogs({0}): deleting {1}", ctx.server_guid, row);
                }
            }
        }

        private void _stopFetchers() {
            lock (this.fetcher_for_logserverguid) {
                foreach (var fetcher in this.fetcher_for_logserverguid.Values) {
                    fetcher.Stop();
                }
                this.fetcher_for_logserverguid = new Dictionary<string, ReplLogFetcher>();
            }
        }

        private void worker_logResume() {
            bool can_log_replay = true;
            bool need_resolve = false;
            IReplConnection srvr;
            List<string> rebuild_reasons = new List<string>();
            try {
                srvr = pusher.getRandomSeed();
            } catch (ReplPusher.NoServersAvailableException) {
                // TODO: How will the first resume decide he is current enough to go active if there is
                //   no Seed?
                Console.WriteLine("Repl({0}): no servers available for log resume...",
                    ctx.server_guid);
                return;
            }
            // (0) check that our data-instance-ids match

            if (srvr.getDataInstanceId().CompareTo(this.data_instance_id) != 0) {
                this.state = ReplState.error;
                return;
            }


            // (1) see if we can resume from our commit_head pointers                    
            var our_log_status_dict = new Dictionary<string, LogStatus>();            
            List<LogStatus> client_log_status = this.getStatusForLogs().ToList();

            Console.WriteLine("worker_logResume({0}) - ourlogs: {1}",
                ctx.server_guid,String.Join(",",client_log_status));
               
            foreach (var ls in client_log_status) {
                our_log_status_dict[ls.server_guid] = ls;                
            }

            
            List<LogStatus> srvr_log_status = srvr.getStatusForLogs().ToList();
            Console.WriteLine("worker_logResume({0}) - serverlogs({1}): {2}",
                ctx.server_guid, srvr.getServerGuid(),
                String.Join(",",srvr_log_status));
            foreach (var ls in srvr_log_status) {                
                if (!our_log_status_dict.ContainsKey(ls.server_guid)) {
                    // we are missing an entire log...
                    if (ls.oldest_entry_pointer.GetLong().Equals(0)) {
                        // it's the magic start of log pointer, so we can resume from it
                    } else {
                        // otherwise, we need a full rebuild!
                        rebuild_reasons.Add(String.Format("we are entirely missing log data: {0}", ls));
                        can_log_replay = false;
                    }
                } else {
                    // if our log_head is before their oldest_entry, we need a full rebuild! 
                    var our_ls = our_log_status_dict[ls.server_guid];                    
                    if (our_ls.log_commit_head.CompareTo(ls.oldest_entry_pointer) < 0) {
                        rebuild_reasons.Add(String.Format("log:{0}, our log_head:{1} < their oldest:{2}", 
                            ls.server_guid,our_ls.log_commit_head,ls.oldest_entry_pointer));
                        can_log_replay = false;
                    }
                    if (our_ls.log_commit_head.CompareTo(ls.log_commit_head) > 0) {
                        // we have newer log entries than they do for at least one log!!
                        rebuild_reasons.Add(String.Format("log:{0}, our log_head:{1} > their head:{2} need resolve",
                            ls.server_guid, our_ls.log_commit_head, ls.log_commit_head));

                        need_resolve = true;
                    }

                }

            }

            if (!can_log_replay) {

                if (!need_resolve) {                    
                    // stop all the fetchers!
                    this._stopFetchers();

                    // schedule a full rebuild
                    Console.WriteLine("Repl({0}) logs don't match, we need a full rebuild. Reasons: {1}", 
                        ctx.server_guid, String.Join(",",rebuild_reasons));
                    this.state = ReplState.rebuild;
                    return;
                } else {
                    // TODO: do we really need to do anything? 
                    Console.WriteLine("Repl({0}) our log has newer changes than somebody, we expect he'll resolve with us. Reasons: {1}",
                        ctx.server_guid, String.Join(",",rebuild_reasons));
                    // this.state = ReplState.resolve;
                    // return;
                }
            }

            
            
            bool all_caught_up = true;
            // (3) make sure we have a fetcher for every log_server_guid
            // (4) check the to see if we're caught up on all logs

            foreach (var ls in srvr_log_status) {
                if (ls.server_guid.Equals(this.getServerGuid())) {
                    // don't try to fetch entries for our own log. 
                    // TODO: check for agreement about our log entries.
                    continue;
                }

                // make sure we have a fetcher...
                lock (this.fetcher_for_logserverguid) {
                    if (!this.fetcher_for_logserverguid.ContainsKey(ls.server_guid)) {
                        this.fetcher_for_logserverguid[ls.server_guid] = new ReplLogFetcher(this, ls.server_guid);
                    }
                    if (!this.fetcher_for_logserverguid[ls.server_guid].IsCaughtUp) {
                        all_caught_up = false;
                    }
                }
            }

            // if we're all caught up, and we're currently not active, make us active!! 
            if (all_caught_up && (this.state != ReplState.active)) {
                Console.WriteLine("** Server {0} becoming ACTIVE!!", ctx.server_guid);
                state = ReplState.active;  // we are up to date and online!! 
            }

            // TODO: if we're NOT all caught up, we should go back to inactive! 

        }

        private void workerFunc() {
            // Make sure we try to stay connnected to all seeds so we can push writes to them
            // as fast as possible.
            pusher.scanSeeds();
            if (this.should_shutdown) {
                this.state = ReplState.do_shutdown;
            }

            switch (this.state) {
                case ReplState.init:
            
                    // we are initializing.... check our log state and see if we need a full rebuild                    
                    Console.WriteLine("Repl({0}): log resume", ctx.server_guid);
                    worker_logResume();
                    break;
                case ReplState.rebuild:


                    // we need a FULL rebuild
                    Console.WriteLine("Repl({0}): full rebuild started", ctx.server_guid);
                    worker_fullRebuild();
                    Console.WriteLine("Repl({0}): full rebuild finished", ctx.server_guid);
                    break;
                case ReplState.resolve:
                    Console.WriteLine("Repl({0}): resolve needed!!", ctx.server_guid);
                    break;
                case ReplState.active:
                    // we are just running!! 
                    ctx.connector.registerServer(ctx.server_guid, this.my_repl_interface);

                    // pop back to init to check log tails
                    worker_logResume();
                    break;
                case ReplState.error:
            
                    Console.WriteLine("Repl({0}): error, stalled", ctx.server_guid);
                    break;
                case ReplState.do_shutdown:
                    // do whatever cleanup we need
                    _stopFetchers();
                    this.state = ReplState.shutdown;
                    break;
                case ReplState.shutdown:
                    break;
                default:
                    // UNKNOWN ReplState
                    throw new Exception("unknown ReplState: " + this.state.ToString());
                    break;
            }
        }

        public class LogException : Exception {
            public LogException(String info) : base(info) { }
        }

        public int getEstimatedRemainingLogData(string server_guid, RecordKeyType log_start_key) {
            int count = 0;
            // TODO: build a more efficient way to get this by asking the MTree for the size between keys
            foreach (var log_line in this.fetchLogEntries(server_guid,log_start_key)) {
                count++;
            }
            return count;
        }

        internal IEnumerable<KeyValuePair<RecordKey, RecordData>> fetchLogEntries(
                        string log_server_guid,
                        RecordKeyType log_start_key,
                        int limit = -1) {

            var rk_start = new RecordKey()                
                .appendKeyPart("_logs")
                .appendKeyPart(log_server_guid);

            if (!log_start_key.Equals("")) {
                rk_start.appendKeyPart(log_start_key);
            }

            var rk_end = new RecordKey()                
                .appendKeyPart("_logs")
                .appendKeyPart(log_server_guid);           

            var scanrange = new ScanRange<RecordKey>(rk_start, RecordKey.AfterPrefix(rk_end), null);

            Console.WriteLine(" fetchLogEntries for ({0}): start {1}  end {2}", 
                log_server_guid, rk_start, rk_end);

            bool matched_first = false;
            int count = 0;

            foreach (var logrow in next_stage.scanForward(scanrange)) {
                if (!matched_first) {
                    // the first logrow needs to match the log_start_key, or there was a gap in the log!!
                    var logstamp = logrow.Key.key_parts[2];
                    if (logstamp.CompareTo(log_start_key) != 0) {
                        throw new LogException(
                            String.Format("log start gap! guid:{0} log_start_key:{1} logstamp:{2}",
                               log_server_guid,log_start_key,logstamp));
                    }
                    matched_first = true;
                }
                yield return logrow;
                count++;

                // if we're limiting the number of return rows...
                if (limit != -1) {
                    if (count > limit) {
                        yield break;
                    }
                }
            }
            if (!matched_first) {
                throw new LogException("no log entries!");
            }
        }


        private byte[] fetchLogEntries_block(string log_server_guid, string log_start_key, string log_end_key) {
            var rk_start = new RecordKey()                
                .appendKeyPart("_logs")
                .appendKeyPart(log_server_guid)
                .appendKeyPart(log_start_key);
            var rk_end = RecordKey.AfterPrefix(new RecordKey()                
                .appendKeyPart("_logs")
                .appendKeyPart(log_server_guid)
                .appendKeyPart(log_end_key));
            var scanrange = new ScanRange<RecordKey>(rk_start, rk_end, null);

            byte[] packed_log_records;
            {
                MemoryStream writer = new MemoryStream();
                // TODO: this seems like a really inefficient way to write out a key
                ISegmentBlockEncoder encoder = new SegmentBlockBasicEncoder();
                encoder.setStream(writer);
                foreach (var logrow in next_stage.scanForward(scanrange)) {
                    encoder.add(logrow.Key, RecordUpdate.WithPayload(logrow.Value.data));
                }
                encoder.flush();
                packed_log_records = writer.ToArray();
            }

            // IF there are no log entries... BLOCK! 


            return packed_log_records;
        }

        private void workerThread() {
            ReplState last_state = this.state;
            int error_count = 0;
            while (true) {
                try {
                    workerFunc();
                } catch (Exception e) {
                    error_count++;
                    Console.WriteLine("Server ({0}) exception {1}:\n{2}",
                        ctx.server_guid, error_count, e.ToString());
                    if (error_count > 5) {
                        Console.WriteLine("too many exceptions, shutting down");
                        this.ctx.connector.unregisterServer(ctx.server_guid);
                        this.state = ReplState.shutdown;
                        return;
                    }
                }
                if (this.state != last_state) {
                    Console.WriteLine("++ Server ({0}) changed state {1} -> {2}", ctx.server_guid, last_state, this.state);
                    last_state = this.state;
                }
                if (state == ReplState.shutdown) {
                    Console.WriteLine("worker ending..");
                    return;
                }

                Thread.Sleep(1000);
            }
        }




        public JoinInfo requestToJoin(string server_guid) {
            // (1) record his guid
            next_stage.setValue(new RecordKey()
                .appendKeyPart("_config").appendKeyPart("seeds").appendKeyPart(server_guid),
                RecordUpdate.WithPayload(""));

            // (2) send him our instance ID and a list of seeds
            var ji = new JoinInfo();
            ji.data_instance_id = this.data_instance_id;

            ji.seed_servers = new List<string>();
            var seed_key_prefix = new RecordKey()
                .appendKeyPart("_config")
                .appendKeyPart("seeds");
            foreach (var row in next_stage.scanForward(new ScanRange<RecordKey>(seed_key_prefix, RecordKey.AfterPrefix(seed_key_prefix), null))) {
                string sname =
                    ((RecordKeyType_String)row.Key.key_parts[row.Key.key_parts.Count - 1]).GetString();
                ji.seed_servers.Add(sname);
            }
            // add ourself to the seed list! 
            if (!ji.seed_servers.Contains(this.ctx.server_guid)) {
                ji.seed_servers.Add(this.ctx.server_guid);
            }
            return ji;
        }

        

        public class ReplPusher {
            HashSet<string> servers;
            ReplHandler myhandler;
            public ReplPusher(ReplHandler handler) {
                servers = new HashSet<string>();
                myhandler = handler;
            }
            public class NoServersAvailableException : Exception {
                public NoServersAvailableException(string e) : base(e) { }
            };

            public void addServer(string server_guid) {
                if (server_guid.CompareTo(myhandler.ctx.server_guid) == 0) {
                    // we can't add ourself!!
                    return;
                }
                servers.Add(server_guid);
                Console.WriteLine("Server {0} pusher added seed {1}",
                    myhandler.ctx.server_guid, server_guid);
            }
            public IReplConnection getRandomSeed() {
                List<IReplConnection> available_servers = new List<IReplConnection>();
                this.scanSeeds();
                foreach (var server_guid in servers) {
                    try {
                        available_servers.Add(myhandler.ctx.connector.getServerHandle(server_guid));
                    } catch (KeyNotFoundException) {
                        Console.WriteLine("getRandomSeed: server {0} not available", server_guid);
                    }
                }
                if (available_servers.Count == 0) {
                    throw new NoServersAvailableException("getRandomSeed: no servers avaialble");
                }
                IReplConnection[] srvr_array = available_servers.ToArray();
                int pick = myhandler.rnd.Next(available_servers.Count);
                return available_servers[pick];
            }

            public void scanSeeds() {
                // Console.WriteLine("** seed scan {0}", myhandler.ctx.server_guid);
                var seed_key_prefix = new RecordKey()
                    .appendKeyPart("_config")
                    .appendKeyPart("seeds");

                // scan our config list of seeds
                foreach (var row in myhandler.next_stage.scanForward(
                    new ScanRange<RecordKey>(seed_key_prefix,
                        RecordKey.AfterPrefix(seed_key_prefix), null))) {
                    string sname =
                        ((RecordKeyType_String)row.Key.key_parts[row.Key.key_parts.Count - 1]).GetString();

                    if (sname == myhandler.ctx.server_guid) {
                        continue; // ignore our own guid!!
                    }

                    // Console.WriteLine("** seed scan {0} row: {1}", myhandler.ctx.server_guid, row);

                    if (!servers.Contains(sname))
                        try {
                            IReplConnection srvr = myhandler.ctx.connector.getServerHandle(sname);
                            // only add this as a seed if it's active! 
                            if (srvr.getState() == ReplState.active) {
                                this.addServer(sname);
                                Console.WriteLine("** scan seed, server {0} pusher, added seed {1}", myhandler.ctx.server_guid,
                                    sname);
                            }

                        } catch (KeyNotFoundException) {
                            // server handle not found by connector
                        }

                }
            }

            public void pushNewLogEntry(long logstamp, RecordUpdate logdata) {
                // TODO: change this to wakeup any pullers that are blocked
#if false
                foreach (var server_guid in servers) {
                    try {
                        IReplConnection srvr = myhandler.ctx.connector.getServerHandle(server_guid);
                        srvr.applyLogEntry(myhandler.ctx.server_guid, logstamp, logdata);
                    } catch (Exception e) {
                        Console.WriteLine("Server {0}, couldn't push to server {1}",
                            myhandler.ctx.server_guid, server_guid);
                        myhandler.state = ReplState.init; // force us to reinit
                    }
                }
#endif
            }

        }

        internal void _recordLogEntry(string from_server_guid, long logstamp, RecordUpdate logdata) {
            RecordKey logkey = new RecordKey()
            .appendKeyPart("_logs")
            .appendKeyPart(from_server_guid)
            .appendKeyPart(new RecordKeyType_Long(logstamp));

            next_stage.setValue(logkey, logdata);
        }

        internal void applyLogEntry(string from_server_guid, long logstamp, RecordUpdate logdata) {
            // (0) unpack the data
            BlockAccessor ba = new BlockAccessor(logdata.data);
            ISegmentBlockDecoder decoder = new SegmentBlockBasicDecoder(ba);

            // (1) add it to our copy of that server's log

            this._recordLogEntry(from_server_guid, logstamp, logdata);
            // (2) add it to the database

            foreach (var kvp in decoder.sortedWalk()) {
                RecordKey local_data_key = new RecordKey()
                    .appendKeyPart("_data");
                foreach (var part in kvp.Key.key_parts) {
                    local_data_key.appendKeyPart(part);
                }
                next_stage.setValue(local_data_key, kvp.Value);
            }
        }

        private void checkActive() {
            if (this.state != ReplState.active) {
                // TODO: spin for the timeout duration to see if we become active
                throw new Exception(String.Format("Server {0} in state {1}, not ready for setValue",
                    ctx.server_guid, state));
            }
        }
        public void setValue(RecordKey skey, RecordUpdate supdate) {
            checkActive();

            // (1) write our repl log entry

            DateTime now = DateTime.Now;
            long logstamp = id_gen.nextTimestamp();
            
            RecordKey logkey = new RecordKey()
                .appendKeyPart("_logs")
                .appendKeyPart(ctx.server_guid)
                .appendKeyPart(new RecordKeyType_Long(logstamp));

            // (1.1) pack the key/value together into the log entry
            byte[] packed_update;
            {
                MemoryStream writer = new MemoryStream();
                // TODO: this seems like a really inefficient way to write out a key
                ISegmentBlockEncoder encoder = new SegmentBlockBasicEncoder();
                encoder.setStream(writer);
                encoder.add(skey, supdate);
                encoder.flush();
                packed_update = writer.ToArray();
            }
            RecordUpdate logupdate = RecordUpdate.WithPayload(packed_update);


            Console.WriteLine("writing log entry: {0} -> [ {1} = {2} ]",
                logkey, skey, supdate);
            next_stage.setValue(logkey, logupdate);

            // (2) trigger the repl notifier that there is a new entry to push
            pusher.pushNewLogEntry(logstamp, logupdate);

            // (2) write the record key
            Console.WriteLine("writing data entry: {0} = {1}",
                skey, supdate);
            RecordKey private_record_key = new RecordKey()
                .appendKeyPart("_data");
            foreach (var part in skey.key_parts) {
                private_record_key.appendKeyPart(part);
            }
            next_stage.setValue(private_record_key, supdate);
        }

        public void setValueParsed(string skey, string svalue) {
            RecordKey key = new RecordKey();
            key.appendParsedKey(skey);
            RecordUpdate update = RecordUpdate.WithPayload(svalue);

            this.setValue(key, update);
        }
    }
}