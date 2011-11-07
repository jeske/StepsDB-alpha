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

            System.GC.Collect();

            snapshot_test();
        }


        static void document_db_test() {

            Console.WriteLine("======================= Document DB Test ==============================");

            LayerManager raw_db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\main");
            StepsDatabase db_broker = new StepsDatabase(raw_db);

            IStepsDocumentDB doc_db = db_broker.getDocumentDatabase();

            doc_db.ensureIndex( new string[] { "name" } );
            doc_db.ensureIndex(new string[] { "age"});

            doc_db.Insert(new BsonDocument {
                { "_id" , "user1" },
                { "name" , "David" },
                { "age", 60 }

            });

            doc_db.Insert(new BsonDocument {
                { "_id" , "user2" },
                { "name" , "Tom" },
                { "age", 32 }
            });

            doc_db.Insert(new BsonDocument {
                { "_id" , "user3" },
                { "name" , "Tom" },
                { "age", 32 }
            });

            raw_db.debugDump();

            int count=0;
            foreach (var doc in doc_db.Find(new BsonDocument() )) {
                    Console.WriteLine(" [{0}] = {1}", count++, doc.ToJson());
            }

            var change_spec = new BsonDocument{ 
                { "$inc" , new BsonDocument { { "age", 1 } } }
                };

            Console.WriteLine("change spec = " + change_spec.ToJson());

            doc_db.Update(new BsonDocument(), change_spec);
                

            raw_db.debugDump();

            foreach (var doc in doc_db.Find(new BsonDocument () )) {
                Console.WriteLine(" [{0}] = {1}", count++, doc.ToJson());
            }




        }

        static void snapshot_test() {
            Console.WriteLine("======================= Snapshot DB Test ==============================");

            LayerManager raw_db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\main");
            StepsDatabase db_broker = new StepsDatabase(raw_db);

            IStepsKVDB generic_db = db_broker.getSnapshotDatabase();

            StepsStageSnapshot db = (StepsStageSnapshot)generic_db;

            db.setValue(new RecordKey().appendParsedKey("test/1"),
                RecordUpdate.WithPayload("blah-t0"));

            IStepsKVDB db_snap = db.getSnapshot();

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
