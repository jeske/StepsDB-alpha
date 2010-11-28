
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Bend;



// our keysapce schema

// _my/config/ID = <server guid>
// _my/config/quorum_requirement = <number of servers before we advance the repl tail>

// _server/<SERVER GUID>/location = host:port

// _logs/<SERVER GUID>/<logid> -> [update info]

// _log_status/<SERVER_GUID>/repl_tail -> the oldest <logid> that may not be replicated for this server-guid

namespace Bend.ReplTest1 {


    public class ReplHandler {
        LayerManager db;
        string my_server_guid;
        Random rnd;

        public ReplHandler(LayerManager db) {
            this.db = db;
            this.rnd = new Random();

            my_server_guid = "TESTGUID";

        }


        public void setValue(RecordKey skey, RecordUpdate supdate) {
            // (1) write our repl log entry

            DateTime now = DateTime.Now;
            long timestamp = (now.Ticks * 100000) + now.Millisecond + rnd.Next(100);

            RecordKey logkey = new RecordKey()
                .appendKeyPart("_logs")
                .appendKeyPart(my_server_guid)
                .appendKeyPart(Lsd.numberToLsd(timestamp,35));
            
            
            // (1.1) pack the key/value together into the log entry

            // okay, this is really hacky to use RecordKey to pack a structure, TODO: fix this!!
            RecordKey packed_update = new RecordKey()
                .appendKeyPart(skey)
                .appendKeyPart(supdate.encode());
            RecordUpdate logupdate = RecordUpdate.WithPayload(packed_update.encode());


            Console.WriteLine("writing log entry: {0} -> [ {1} = {2} ]",
                logkey,skey,supdate);
            db.setValue(logkey, logupdate);

            // (2) write the record key
            Console.WriteLine("writing data entry: {0} = {1}",
                skey, supdate);
            db.setValue(skey, supdate);

            // (3) trigger the repl notifier that there is a new entry to push

        }

        public void setValueParsed(string skey, string svalue) {
            RecordKey key = new RecordKey();
            key.appendParsedKey(skey);
            RecordUpdate update = RecordUpdate.WithPayload(svalue);

            this.setValue(key, update);
        }


    }

}