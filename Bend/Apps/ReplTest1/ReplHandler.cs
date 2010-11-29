
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Bend;

using System.IO;



/*
 * our keysapce schema
 * 
 *  _my/config/DATA-INSTANCE-ID = <guid of the dataset>
 *  
 *  _my/config/MY-SERVER-ID = <server guid>
 *  
 *  _my/config/quorum_requirement = <number of servers before we advance the repl tail>
 *  
 *  _server/<SERVER GUID>/location = host:port
 *  
 *  _logs/<SERVER GUID>/<logid> -> [update info]
 *  
 *  _log_status/<SERVER_GUID>/repl_tail -> the oldest <logid> that may not be replicated for this server-guid
 *  
 * 
 * Cases to Handle
 * 
 * read/write keys in our keyspace
 * push new writes to other clients, manage 'quorum agreed' pointer
 * check/pull logs from other servers when we join/connect
 * 
 * 
 * TODO: 
 * 
 * - don't let us touch keys until there is an instance id (either via INIT_NEW or JOIN_EXISTING)
 * - make a simulated "server connect" so we can also have "disconnect"
 * - tail-log command (where we supply log pointers, error if the pointer is too old)
 * - fallback to key-copy if the tail-log didn't work
 * - make sure log application order doesn't matter (timestamp order log apply? per-record timestamp?)
 * 
 */

namespace Bend {

    public class ReplHandler {
        LayerManager db;
        string my_server_guid;
        Random rnd;
        ReplPusher pusher;
        string prefix_hack;

        public ReplHandler(LayerManager db, string prefix_hack, string server_guid) {
            this.db = db;
            this.rnd = new Random();
            this.pusher = new ReplPusher(this);
            this.prefix_hack = prefix_hack;

            my_server_guid = server_guid;
            db.setValue(new RecordKey().appendParsedKey(prefix_hack)
                .appendKeyPart("_config")
                .appendKeyPart("MY-SERVER-ID"),
                RecordUpdate.WithPayload(my_server_guid));
        }

        public class ReplPusher {
            List<ReplHandler> servers;
            ReplHandler myhandler;
            public ReplPusher(ReplHandler handler) {
                servers = new List<ReplHandler>();
                myhandler = handler;
            }
            public void addServer(ReplHandler server) {
                // make sure the server is up to date first
                servers.Add(server);
            }
            public void removeServer(ReplHandler server) {
                servers.Remove(server);
            }

            public void pushNewLogEntry(byte[] logstamp, RecordUpdate logdata) {
                foreach (var server in servers) {
                    server.applyLogEntry(myhandler.my_server_guid, logstamp, logdata);
                }
            }
        }
    

        public void applyLogEntry(string from_server_guid, byte[] logstamp, RecordUpdate logdata) {
            // (0) unpack the data
            BlockAccessor ba = new BlockAccessor(logdata.data);
            ISegmentBlockDecoder decoder = new SegmentBlockBasicDecoder(ba);
                                               
            // (1) add it to our copy of that server's log
            RecordKey logkey = new RecordKey()
            .appendKeyPart(prefix_hack)
            .appendKeyPart("_logs")
            .appendKeyPart(from_server_guid)
            .appendKeyPart(logstamp);

            db.setValue(logkey, logdata);

            // (2) add it to the database

            foreach (var kvp in decoder.sortedWalk()) {
                RecordKey local_data_key = new RecordKey()
                    .appendKeyPart(prefix_hack)
                    .appendKeyPart("_data");
                foreach (var part in kvp.Key.key_parts) {
                    local_data_key.appendKeyPart(part);
                }
                db.setValue(local_data_key, kvp.Value);
            }

            

        }
        public void addServer(ReplHandler target_server) {
            pusher.addServer(target_server);
        }

        public void setValue(RecordKey skey, RecordUpdate supdate) {
            // (1) write our repl log entry

            DateTime now = DateTime.Now;
            long timestamp = (now.Ticks * 100000) + now.Millisecond + rnd.Next(100);

            byte[] logstamp = Lsd.numberToLsd(timestamp, 35);
            RecordKey logkey = new RecordKey()
                .appendKeyPart(prefix_hack)
                .appendKeyPart("_logs")
                .appendKeyPart(my_server_guid)
                .appendKeyPart(logstamp);

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
            db.setValue(logkey, logupdate);

            // (2) write the record key
            Console.WriteLine("writing data entry: {0} = {1}",
                skey, supdate);
            RecordKey private_record_key = new RecordKey()
                .appendKeyPart(prefix_hack)
                .appendKeyPart("_data");
            foreach (var part in skey.key_parts) {
                private_record_key.appendKeyPart(part);
            }
            db.setValue(private_record_key, supdate);

            // (3) trigger the repl notifier that there is a new entry to push
            pusher.pushNewLogEntry(logstamp, logupdate);
        }
        
        public void setValueParsed(string skey, string svalue) {
            RecordKey key = new RecordKey();
            key.appendParsedKey(skey);
            RecordUpdate update = RecordUpdate.WithPayload(svalue);

            this.setValue(key, update);
        }
    }

}