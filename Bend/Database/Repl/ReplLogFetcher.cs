// Copyright (C) 2008-2011 by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using System.Threading;

using Bend;

namespace Bend.Repl {


    // this is currently a per-thread repl log fetcher, it keeps fetching log entries until it gets close to the
    // end of the log.. all the while publishing the estimated 'distance'. 
    
    // When it gets to the end, it
    // does a blocking fetch so when the next log entry comes in, the server can return immediately.

    public class ReplLogFetcher {
        ReplHandler local_db;

        IReplConnection target_server = null;
        string log_server_guid;          // this might not be the same as the target_server guid

        Thread our_fetcher;
        bool keep_running = true;

        int _distanceToEndOfLog = Int32.MaxValue;  // ? 
        public int DistanceToEndOfLog { get { return _distanceToEndOfLog; } }

        bool _isCaughtUp = false;
        public bool IsCaughtUp { get { return _isCaughtUp; } }

        public ReplLogFetcher(ReplHandler local_db, string log_server_guid) {
            this.local_db = local_db;            
            this.log_server_guid = log_server_guid;
            
            our_fetcher = new Thread(delegate() {
                this.workerThread();
            });
            our_fetcher.Start();
        }

        public void Stop() {
            this.keep_running = false;
            this.our_fetcher.Abort();
        }

        private void _adjustCaughtUpFlag() {
            if (this._distanceToEndOfLog < 5) {
                this._isCaughtUp = true;
            }

            if (this._distanceToEndOfLog > 30) {
                this._isCaughtUp = false;
            }
        }

        private void _connect() {
            this.target_server = local_db.ctx.connector.getServerHandle(this.log_server_guid);
            // TODO: if we can't get the target, we should just get someone who has his logs! 
        }


        public void workerThread() {
            while (keep_running) {
                try {
                    doFetch();
                } catch (ThreadAbortException e) {
                    Console.WriteLine("fetcher stopped (thread abort)..");
                    _isCaughtUp = false;
                    keep_running = false;
                    
                    return;
                } catch (ReplHandler.LogException e) {
                    Console.WriteLine("fetcher aborting - LogException {0}", e);
                    _isCaughtUp = false;
                    keep_running = false;
                    return;
                }
                Thread.Sleep(1000);
            }
        }
        private void doFetch() {         
            if (this.target_server == null) {
                // we need to connect
                this._connect();
            }

            // figure out the latest log status for the server_guid we're handling
            LogStatus local_ls = local_db.getStatusForLog(this.log_server_guid);
            IReplConnection srvr = this.target_server;

            // ask for the estimated distance to the end of the log
            RecordKeyType_Long log_start_key = local_ls.oldest_entry_pointer;
            this._distanceToEndOfLog = srvr.getEstimatedRemainingLogData(this.log_server_guid, log_start_key);

            this._adjustCaughtUpFlag();   
            
            // catch up as much as we can

            // if we're caught up, use the blocking log fetch!! 
                
            foreach (var logrow in srvr.fetchLogEntries(this.log_server_guid, log_start_key)) {
                RecordKeyType last_keypart = logrow.Key.key_parts[logrow.Key.key_parts.Count - 1];
                RecordKeyType_Long keypart = (RecordKeyType_Long)last_keypart;

                long logstamp = keypart.GetLong();

                if (logstamp.CompareTo(log_start_key.GetLong()) == 0) {
                    // if it is the magic "start of log" record, then just record it
                    if (logstamp == 0) {
                        local_db._recordLogEntry(this.log_server_guid, logstamp, RecordUpdate.WithPayload(logrow.Value.data));
                    } else {
                        // check and make sure the first log entry actually matches our recorded entry
                        // otherwise there is a log-sync mismatch! 
                    }
                } else {
                    local_db.applyLogEntry(this.log_server_guid, logstamp, RecordUpdate.WithPayload(logrow.Value.data));
                }
            }
        }         
    }
}
