// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;

using NUnit.Framework;

namespace Bend
{

    [TestFixture]
    public class LayerManagerTests : LayerManager
    {
        public LayerManagerTests() {


        }
        [Test]
        public void Test00EmptyLayerInitAndResume() {
            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\3");

            Assert.AreEqual(1, db.segmentlayers.Count);
            Assert.AreEqual(db.segmentlayers[0], db.workingSegment);
            Assert.AreEqual(0, db.workingSegment.RowCount);

            // TEST: log is empty
            // TEST: freespace record established!
        }

        [Test]
        public void Test01LayerTxnLogResume() {
            String[] keys = { "test-1", "test-2", "test-3" };
            String[] values = {"a","b","c" };

            {
                LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\4");

                LayerManager.Txn txn = db.newTxn();
                for (int i=0;i<keys.Length;i++) {
                    txn.setValueParsed(keys[i],values[i]);
                }
                txn.commit();

                // TODO: assure the freespace hasn't been affected

                // assure we have not committed any segments
                Assert.AreEqual(1, db.segmentlayers.Count);
                Assert.AreEqual(db.segmentlayers[0], db.workingSegment);

                // assure the working segment contains the right data
                Assert.AreEqual(3, db.workingSegment.RowCount);
                db.Dispose();
            }

            {
                LayerManager db = new LayerManager(InitMode.RESUME, "c:\\test\\4");
                
                // assure we still have not committed any segments
                Assert.AreEqual(1, db.segmentlayers.Count);
                Assert.AreEqual(db.segmentlayers[0], db.workingSegment);

                // assure the working segment contains the right data
                Assert.AreEqual(3, db.workingSegment.RowCount);
                for (int i = 0; i < keys.Length; i++) {
                    RecordKey key = new RecordKey();
                    key.appendKeyPart(keys[i]);

                    // look directly in the working segment
                    {
                        RecordUpdate update;
                        GetStatus status = db.workingSegment.getRecordUpdate(key, out update);
                        Assert.AreEqual(GetStatus.PRESENT, status);
                        Assert.AreEqual(values[i], update.ToString(), "SegmentBuilder.getRecordUpdate()");
                    }

                    // assure the global query interface finds it
                    {
                        RecordData data;
                        GetStatus status = db.getRecord(key, out data);
                        Assert.AreEqual(GetStatus.PRESENT, status);
                        Assert.AreEqual(values[i], data.ToString(), "LayerManager.getRecord()");
                    }
                }

                // cleanup
                db.Dispose();
            }

        }

        [Test]
        public void Test02LayerSegmentFlushAndFreespaceModification() {
            String[] keys = { "test-1", "test-2", "test-3" };
            String[] values = { "a", "b", "c" };

            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\5");

            LayerManager.Txn txn = db.newTxn();
            for (int i = 0; i < keys.Length; i++) {
                txn.setValueParsed(keys[i], values[i]);
            }
            txn.commit();
            db.flushWorkingSegment();

            // assure we have an extra segment
            Assert.AreEqual(2, db.segmentlayers.Count);

            // assure we allocated a new generation and gen0 range record (walk .ROOT range map)
            // TODO: move this test to RangemapManager, to remove this cross-dependency
            {
                RecordData data;
                Assert.AreEqual(GetStatus.PRESENT,
                    db.getRecord(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"), out data),
                    "missing numgenerations record");
                Assert.AreEqual("1", data.ToString(),"generation count");
            }
            {
                RecordData data;
                Assert.AreEqual(GetStatus.PRESENT,
                    db.getRecord(new RecordKey().appendParsedKey(".ROOT/GEN/000/</>"), out data));
            }

            // TODO: assure we subtracted the new range record from the freespace

            // assure the records we wrote are NOT in the working segment, but ARE in the next layer
            for (int i = 0; i < keys.Length; i++) {
                RecordKey key = new RecordKey();
                key.appendKeyPart(keys[i]);

                // look directly in the working segment, they should be **MISSING*
                {
                    RecordUpdate update;
                    GetStatus status =
                        db.workingSegment.getRecordUpdate(key, out update);
                    Assert.AreEqual(GetStatus.MISSING, status, "working segment should be clear");
                }

                // assure the global query interface finds it
                {
                    RecordData data;
                    GetStatus status = db.getRecord(key, out data);
                    Assert.AreEqual(GetStatus.PRESENT, status, "records should be found in layers");
                    Assert.AreEqual(values[i], data.ToString(), "LayerManager.getRecord()");
                }
            }

            db.Dispose();

        }

        [Test]
        public void Test03SegmentLayerGetRecordApplicationOrder() {

            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\6");

            {
                String[] keys = { "test-1", "test-2", "test-3" };
                String[] values = { "a-first", "b-first", "c-first" };

                LayerManager.Txn txn = db.newTxn();
                for (int i = 0; i < keys.Length; i++) {
                    txn.setValueParsed(keys[i], values[i]);
                }
                txn.commit();
                db.flushWorkingSegment();

                // assure the records we wrote are NOT in the working segment, but ARE in the next layer
                for (int i = 0; i < keys.Length; i++) {
                    RecordKey key = new RecordKey();
                    key.appendKeyPart(keys[i]);

                    // look directly in the working segment, they should be **MISSING*
                    {
                        RecordUpdate update;
                        GetStatus status =
                            db.workingSegment.getRecordUpdate(key, out update);
                        Assert.AreEqual(GetStatus.MISSING, status, "working segment should be clear");
                    }

                    // assure the global query interface finds it
                    {
                        RecordData data;
                        GetStatus status = db.getRecord(key, out data);
                        Assert.AreEqual(GetStatus.PRESENT, status, "records should be found in layers");
                        Assert.AreEqual(values[i], data.ToString(), "LayerManager.getRecord()");
                    }
                }
            }

            // now write the same keys again with different values into the working segment
            {
                String[] keys = { "test-1", "test-2", "test-3" };
                String[] values = { "a-second", "b-second", "c-second" };

                LayerManager.Txn txn = db.newTxn();
                for (int i = 0; i < keys.Length; i++) {
                    txn.setValueParsed(keys[i], values[i]);
                }
                txn.commit();


                // assure that both the workingsegment and layermanager see the NEW VALUES
                for (int i = 0; i < keys.Length; i++) {
                    RecordKey key = new RecordKey();
                    key.appendKeyPart(keys[i]);

                    // look directly in the working segment, they should be the NEW VALUES
                    {
                        RecordUpdate update;
                        GetStatus status =
                            db.workingSegment.getRecordUpdate(key, out update);
                        Assert.AreEqual(GetStatus.PRESENT, status, "working segment should have NEW VALUES");
                        Assert.AreEqual(values[i], update.ToString(), "SegmentBuilder.getRecordUpdate should see NEW VALUES");
                    }

                    // assure the global query interface finds the NEW VALUES
                    {
                        RecordData data;
                        GetStatus status = db.getRecord(key, out data);
                        Assert.AreEqual(GetStatus.PRESENT, status, "LayerManager should see NEW VALUES");
                        Assert.AreEqual(values[i], data.ToString(), "LayerManager.getRecord() should see NEW VALUES");
                    }
                }

                // now flush the working segment (so we have two on-disk layers)
                db.flushWorkingSegment();

                // assure we still see the NEW VALUES, but that the working segment is empty
                for (int i = 0; i < keys.Length; i++) {
                    RecordKey key = new RecordKey();
                    key.appendKeyPart(keys[i]);

                    // look directly in the working segment, they should MISSING
                    {
                        RecordUpdate update;
                        GetStatus status =
                            db.workingSegment.getRecordUpdate(key, out update);
                        Assert.AreEqual(GetStatus.MISSING, status, "working segment should have NO values");
                    }

                    // assure the global query interface finds the NEW VALUES
                    {
                        RecordData data;
                        GetStatus status = db.getRecord(key, out data);
                        Assert.AreEqual(GetStatus.PRESENT, status, "LayerManager should see NEW VALUES");
                        Assert.AreEqual(values[i], data.ToString(), "LayerManager.getRecord() should see NEW VALUES");
                    }
                }
            }
            db.Dispose();
            
        }

        
        [Test]
        public void Test04SingleSegmentRootMetadataLogRecovery() {
            // perform the previous test
            Test03SegmentLayerGetRecordApplicationOrder();

            // ... and then perform a resume
            LayerManager db = new LayerManager(InitMode.RESUME, "c:\\test\\6");

            // verify that it has the same data...
            // now write the same keys again with different values into the working segment
            {
                String[] keys = { "test-1", "test-2", "test-3" };
                String[] values = { "a-second", "b-second", "c-second" };

                // working segment should be empty
                for (int i = 0; i < keys.Length; i++) {
                    RecordKey key = new RecordKey();
                    key.appendKeyPart(keys[i]);

                    // look directly in the working segment, they should be MISSING
                    //   This is testing the checkpoint as well. If log resume didn't
                    //   also checkpoint, then the values will be duplicated in the working segment.
                    {
                        RecordUpdate update;
                        GetStatus status =
                            db.workingSegment.getRecordUpdate(key, out update);
                        Assert.AreEqual(GetStatus.MISSING, status, "working segment should be MISSING");
                    }

                    // assure the global query interface finds the NEW VALUES
                    {
                        RecordData data;
                        GetStatus status = db.getRecord(key, out data);
                        Assert.AreEqual(GetStatus.PRESENT, status, "LayerManager should see NEW VALUES");
                        Assert.AreEqual(values[i], data.ToString(), "LayerManager.getRecord() should see NEW VALUES");
                    }
                }

                // TODO: how can this possibly succeed?!?!?!? We're not walking the rangeroot yet.

                // now we should write some more data, close, restore
            }

            Assert.Fail("test not done");
        }

       // TEST: test multiple segments flushed, and "log resumed"  (walk .ROOT range map)
       // TEST: Tombstones

       // TEST: two stage "checkpoint" -> "drop/finalize", concurrency, atomicity

       // ----------------------------[ TEST MERGING ]-----------------------------

       // TEST: double flush and merge 2 segments into 1
       // TEST: random pattern test (long), lots of merging
       // TEST: Tombstone cleanup
       // TEST: RANGE walk during getRecord()
       //        - assure the initial bootstrap does not reach all segments
       //        - ask for a key in an indirect referenced segment (to assure it uses the metadata to find it)
       // ----------------------------[  TEST ROW ATTRIBUTES ]---------------------

       // TEST: row attributes
       // TEST: segment merge row attribute collapse/cleanup for old TX (before it hits the bottom)

       // ----------------------------[    TEST CONCURRENCY    ]------------------

        // TEST: assure the atomicity of a LogCommitGroup (Txn?)


       // ----------------------------[   TEST MVCC    ]---------------------------
       // TEST: MVCC Row Read Locking
       // TEST: MVCC Row Write Locking
       // TEST: MVCC Row-Range Read Locking
       // TEST: MVCC pending TX past restart

       // ----------------------------[  TWO PHASE COMMIT ]------------------------
       // TEST: two-phase commit prepare past restart
 

    }
}