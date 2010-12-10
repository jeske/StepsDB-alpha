using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Bend;

namespace Bend.DBClientTest {
    class DBClientTest {
        static void Main(string[] args) {

            LayerManager raw_db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\main");
            StepsDatabase db_broker = new StepsDatabase(raw_db);

            IStepsKVDB db = db_broker.getDatabase();

            db.setValue(new RecordKey().appendParsedKey("test/1"),
                RecordUpdate.WithPayload("blah"));


            
            var key = new RecordKey().appendParsedKey("test/1");

            raw_db.debugDump();

            Console.WriteLine("top level readback:");

            foreach (var rec in db.scanForward(new ScanRange<RecordKey>(key,new ScanRange<RecordKey>.maxKey(),null))) {
                Console.WriteLine(rec);
            }
        }
    }
}
