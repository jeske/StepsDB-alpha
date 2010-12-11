using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Bend;

using MongoDB.Bson;

namespace Bend.DBClientTest {
    class DBClientTest {

        
        static void Main(string[] args) {

            document_db_test();

            // snapshot_test();
        }


        static void document_db_test() {
            LayerManager raw_db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\main");
            StepsDatabase db_broker = new StepsDatabase(raw_db);

            IStepsDocumentDB doc_db = db_broker.getDocumentDatabase();

            var doc = new BsonDocument {
                { "_id" , "foo" },
                { "name" , "David" }
            };
            doc_db.ensureIndex( new string[] { "name", "_id" } );
            doc_db.Insert(doc);

            raw_db.debugDump();

        }

        static void snapshot_test() {
            LayerManager raw_db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\main");
            StepsDatabase db_broker = new StepsDatabase(raw_db);

            IStepsKVDB generic_db = db_broker.getSnapshotDatabase();

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
