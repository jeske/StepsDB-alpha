// Copyright (C) 2008-2011 by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.IO; 
using System.Linq;

using System.Threading;

using Bend;


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

namespace Bend {

    public class ServerConnector {
        Dictionary<string, ReplHandler> server_list = new Dictionary<string, ReplHandler>();
        public ReplHandler getServerHandle(string server_guid) {

            try {
                return server_list[server_guid];
            } catch (KeyNotFoundException) {
                throw new KeyNotFoundException("could not connect to server: " + server_guid);
            }
        }

        public void registerServer(string name, ReplHandler instance) {
            server_list[name] = instance;
        }
        public void unregisterServer(string server_guid) {
            try {
                server_list.Remove(server_guid);
            } catch (KeyNotFoundException) {
                // nothing to remove
            }
        }
    }

    public class ServerContext {
        public string server_guid;
        public ServerConnector connector;        
    }


    // ----------------------------------------------


    public class ReplHandler {
        IStepsSnapshotKVDB next_stage;        
        ServerContext ctx;

        Random rnd;
        ReplPusher pusher;
        string data_instance_id;

        private static FastUniqueIds id_gen = new FastUniqueIds();

        public static Random myrnd = new Random();

        Thread worker;
        public enum ReplState {
            init,        // bringup
            rebuild,     // it's safe to just copy everything from someone else
            resolve,     // we have newer entries than someone else
            active,      // ready to serve queries
            error,       // catastrophic error, so just stop
            shutdown     // shutting down
        };
        private ReplState state = ReplState.init;
        public ReplState State { get { return state; } }


        public override string ToString() {
            return String.Format("ReplHandler({0}:{1})", this.ctx.server_guid, this.state.ToString());
        }

        #region Constructors

        public ReplHandler(IStepsSnapshotKVDB db, ServerContext ctx) {
            this.next_stage = db;
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
            ctx.connector.registerServer(ctx.server_guid, this);

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

            ReplHandler repl = new ReplHandler(db, ctx);

            repl.state = ReplState.active; // TODO: is this the right way to become active?            
            return repl;

        }

        public static ReplHandler InitJoin(IStepsSnapshotKVDB db, ServerContext ctx, string seed_name) {

            // connect to the other server, get his instance id, exchange seeds
            ReplHandler seed = ctx.connector.getServerHandle(seed_name);
            ReplHandler.JoinInfo join_info = seed.requestToJoin(ctx.server_guid);

            // record the join result
            db.setValue(new RecordKey()
                .appendKeyPart("_config")
                .appendKeyPart("DATA-INSTANCE-ID"),
                RecordUpdate.WithPayload(join_info.data_instance_id));

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
        }
        public class LogStatus {
            public string server_guid;
            public RecordKeyType_Long log_commit_head;
            public RecordKeyType_Long oldest_entry_pointer;
            // public string newest_pending_entry_pointer;
            public override string ToString() {
                return String.Format("LogStatus( {0} oldest:{1} head:{2} )", server_guid, oldest_entry_pointer, log_commit_head);
            }
        }

        private LogStatus _statusForLog(string server_guid) {
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

        IEnumerable<LogStatus> getStatusForLogs() {
            var seeds_prefix = new RecordKey()                
                .appendParsedKey("_config/seeds");

            var scanrange = new ScanRange<RecordKey>(seeds_prefix,
                                            RecordKey.AfterPrefix(seeds_prefix), null);

            yield return _statusForLog(ctx.server_guid); // be sure to include myself

            foreach (var seed_row in next_stage.scanForward(scanrange)) {
                RecordKeyType last_keypart = seed_row.Key.key_parts[seed_row.Key.key_parts.Count - 1];

                string server_guid = ((RecordKeyType_String)last_keypart).GetString();

                if (server_guid.Equals(ctx.server_guid)) { continue; } // skip ourselves

                yield return _statusForLog(server_guid);
            }

        }

        public string getDataInstanceId() {
            return this.data_instance_id;
        }

        private void worker_fullRebuild() {

            // (1) clear our keyspace

            // TODO: How do we trigger a reinit on a new prefix, so we don't
            //   have to actually delete everything??  Maybe we can make SubsetStage
            //   capable of clearing by dropping the old prefix-id?  Or maybe we will 
            //   use built-in support for a range-deletion-tombstone? 

            // TODO: how do we verify that this isn't going to lose important information?

            // TODO: probably should just delete/copy _data and _logs
            Console.WriteLine("Rebuild({0}): deleting our keys", ctx.server_guid);
            foreach (var row in this.next_stage.scanForward(ScanRange<RecordKey>.All())) {
                this.next_stage.setValue(row.Key, RecordUpdate.DeletionTombstone());
            }

            // (2) re-record our data-instance id
            next_stage.setValue(new RecordKey()
            .appendKeyPart("_config")
            .appendKeyPart("DATA-INSTANCE-ID"),
            RecordUpdate.WithPayload(this.data_instance_id));

            // (3) ask for a snapshot

            // TODO: make sure servers are only listed as seeds when they are "active". 
            
            ReplHandler srvr = pusher.getRandomSeed();


            // (4) then copy the snapshot

            // TODO: make sure we get data keys before logs, or that we do something to
            //      be sure to know whether this copy completes successfully or not.
            // TODO: probably want to record our copy-progress, so we can continue copy after
            //      restart without having to do it from scratch!! 

        }

        private void worker_logResume() {
            bool can_log_replay = true;
            bool need_resolve = false;
            ReplHandler srvr;

            try {
                srvr = pusher.getRandomSeed();
            } catch (ReplPusher.NoServersAvailableException) {
                // TODO: How will the first resume decide he is current enough to go active if there is
                //   no Seed?
                Console.WriteLine("Repl({0}): no servers available for log resume, waiting...",
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
            Console.WriteLine("worker_logResume({0}) - serverlogs: {1}",
                ctx.server_guid,String.Join(",",srvr_log_status));
            foreach (var ls in srvr_log_status) {                
                if (!our_log_status_dict.ContainsKey(ls.server_guid)) {
                    // we are missing an entire log, we need a full rebuild!
                    can_log_replay = false;
                } else {
                    // if our log_head is before their oldest_entry, we need a full rebuild! 
                    var our_ls = our_log_status_dict[ls.server_guid];                    
                    if (our_ls.log_commit_head.CompareTo(ls.oldest_entry_pointer) < 0) {
                        can_log_replay = false;
                    }
                    if (our_ls.log_commit_head.CompareTo(ls.log_commit_head) > 0) {
                        // we have newer log entries than they do for at least one log!!
                        need_resolve = true;
                    }

                }

            }

            if (!can_log_replay) {
                if (!need_resolve) {
                    // schedule a full rebuild
                    Console.WriteLine("Repl({0}) logs don't match, we need a full rebuild;", 
                        ctx.server_guid);
                    this.state = ReplState.rebuild;
                    return;
                } else {
                    Console.WriteLine("Repl({0}) our log has newer changes than somebody, schedule a resolve!!",
                        ctx.server_guid);
                    this.state = ReplState.resolve;
                    return;
                }
            }

            // (3) replay logs from the commit heads

            foreach (var ls in srvr_log_status) {
                RecordKeyType log_start_key = new RecordKeyType_Long(0);
                if (our_log_status_dict.ContainsKey(ls.server_guid)) {
                    log_start_key = our_log_status_dict[ls.server_guid].log_commit_head;
                }

                foreach (var logrow in srvr.fetchLogEntries(ls.server_guid, log_start_key, ls.log_commit_head)) {
                    RecordKeyType last_keypart = logrow.Key.key_parts[logrow.Key.key_parts.Count - 1];
                    RecordKeyType_Long keypart = (RecordKeyType_Long)last_keypart;

                    long logstamp = keypart.GetLong();
                    this.applyLogEntry(ls.server_guid, logstamp, RecordUpdate.WithPayload(logrow.Value.data));
                }              
            }

            // TODO: double check our log heads
            if (this.state != ReplState.active) {
                Console.WriteLine("** Server {0} becoming ACTIVE!!", ctx.server_guid);
                state = ReplState.active;  // we are up to date and online!! 
            }

        }

        private void workerFunc() {
            // Make sure we try to stay connnected to all seeds so we can push writes to them
            // as fast as possible.
            pusher.scanSeeds();

            if (this.state == ReplState.init) {
                // we are initializing.... check our log state and see if we need a full rebuild                    

                worker_logResume();
            } else if (this.state == ReplState.rebuild) {
                // we need a FULL rebuild
                Console.WriteLine("full rebuild started");

                worker_fullRebuild();
            } else if (this.state == ReplState.resolve) {
                Console.WriteLine("Repl({0}): resolve needed!!", ctx.server_guid);

            } else if (this.state == ReplState.active) {
                // we are just running!! 
                ctx.connector.registerServer(ctx.server_guid, this);

                // pop back to init to check log tails
                worker_logResume();
            } else if (this.state == ReplState.error) {
                Console.WriteLine("Repl({0}): error, stalled", ctx.server_guid);
            } else {
                // UNKNOWN ReplState
                throw new Exception("unknown ReplState: " + this.state.ToString());
            }
        }

        private IEnumerable<KeyValuePair<RecordKey, RecordData>> fetchLogEntries(
                        string log_server_guid,
                        RecordKeyType log_start_key,
                        RecordKeyType log_end_key) {

            var rk_start = new RecordKey()                
                .appendKeyPart("_logs")
                .appendKeyPart(log_server_guid);

            if (!log_start_key.Equals("")) {
                rk_start.appendKeyPart(log_start_key);
            }

            var rk_end = new RecordKey()                
                .appendKeyPart("_logs")
                .appendKeyPart(log_server_guid);
            if (!log_start_key.Equals("")) {
                rk_end.appendKeyPart(log_end_key);
            }

            var scanrange = new ScanRange<RecordKey>(rk_start, RecordKey.AfterPrefix(rk_end), null);

            Console.WriteLine(" fetchLogEntries: start {0}  end {1}", rk_start, rk_end);

            foreach (var logrow in next_stage.scanForward(scanrange)) {
                yield return logrow;
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

        public class JoinInfo {
            public string data_instance_id;
            public List<string> seed_servers;
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
            public ReplHandler getRandomSeed() {
                List<ReplHandler> available_servers = new List<ReplHandler>();
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
                ReplHandler[] srvr_array = available_servers.ToArray();
                int pick = myhandler.rnd.Next(available_servers.Count);
                return available_servers[pick];
            }

            public void scanSeeds() {
                // Console.WriteLine("** seed scan {0}", myhandler.ctx.server_guid);
                var seed_key_prefix = new RecordKey()
                    .appendKeyPart("_config")
                    .appendKeyPart("seeds");
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
                            ReplHandler srvr = myhandler.ctx.connector.getServerHandle(sname);
                            this.addServer(sname);
                            Console.WriteLine("** scan seed, server {0} pusher, added seed {1}", myhandler.ctx.server_guid,
                                sname);
                        } catch (KeyNotFoundException) {
                        }

                }
            }

            public void pushNewLogEntry(long logstamp, RecordUpdate logdata) {
                foreach (var server_guid in servers) {
                    try {
                        ReplHandler srvr = myhandler.ctx.connector.getServerHandle(server_guid);
                        srvr.applyLogEntry(myhandler.ctx.server_guid, logstamp, logdata);
                    } catch (Exception e) {
                        Console.WriteLine("Server {0}, couldn't push to server {1}",
                            myhandler.ctx.server_guid, server_guid);
                        myhandler.state = ReplState.init; // force us to reinit
                    }
                }
            }
        }


        public void applyLogEntry(string from_server_guid, long logstamp, RecordUpdate logdata) {
            // (0) unpack the data
            BlockAccessor ba = new BlockAccessor(logdata.data);
            ISegmentBlockDecoder decoder = new SegmentBlockBasicDecoder(ba);

            // (1) add it to our copy of that server's log
            RecordKey logkey = new RecordKey()
            .appendKeyPart("_logs")
            .appendKeyPart(from_server_guid)
            .appendKeyPart(new RecordKeyType_Long(logstamp));

            next_stage.setValue(logkey, logdata);

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