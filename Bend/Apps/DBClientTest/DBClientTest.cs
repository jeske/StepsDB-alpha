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

            IStepsKVDB generic_db = db_broker.getDatabase();

            TimestampSnapshotStage db = (TimestampSnapshotStage)generic_db;

            db.setValue(new RecordKey().appendParsedKey("test/1"),
                RecordUpdate.WithPayload("blah-t0"));

            TimestampSnapshotStage db_snap = db.getSnapshot();

            db.setValue(new RecordKey().appendParsedKey("test/1"),
                RecordUpdate.WithPayload("blah-t1"));


            var key = new RecordKey().appendParsedKey("test/1");

            raw_db.debugDump();

            Console.WriteLine("-------------------[ top level readback ] -------------------");

            foreach (var rec in db.scanForward(new ScanRange<RecordKey>(key,new ScanRange<RecordKey>.maxKey(),null))) {
                Console.WriteLine(rec);
            }

            Console.WriteLine("-------------------[ snapshot readback ] -------------------");

            foreach (var rec in db_snap.scanForward(new ScanRange<RecordKey>(key, new ScanRange<RecordKey>.maxKey(), null))) {
                Console.WriteLine(rec);
            }



        }
    }
}
