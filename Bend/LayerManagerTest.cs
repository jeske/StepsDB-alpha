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
            String[] keys = { "test/1", "test/2", "test/3" };
            String[] values = {"a","b","c" };

            {
                LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\4");

                LayerManager.Txn txn = db.newTxn();
                for (int i=0;i<keys.Length;i++) {
                    txn.setValue(keys[i],values[i]);
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
                for (int i=0;i<keys.Length;i++) {
                    RecordKey key = new RecordKey();
                    key.appendKeyPart(keys[i]);
                    RecordUpdate update;
                    db.workingSegment.getRecordUpdate(key,out update);
                    Assert.AreEqual(values[i],update.data);
                }

                // cleanup
                db.Dispose();
            }

        }

        [Test]
        public void Test02LayerSegmentFlushAndLogRecoveryRestore() {
            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\5");

            LayerManager.Txn txn = db.newTxn();
            txn.setValue("test/3", "a");
            txn.setValue("test/2", "b");
            txn.setValue("test/1", "c");
            txn.commit();
            db.flushWorkingSegment();

            // assure we have an extra segment, and that the working segment is empty
            Assert.AreEqual(2, db.segmentlayers.Count);
            Assert.AreEqual(0, db.workingSegment.RowCount);

            // assure we allocated a new range record

            // assure we subtracted the new range record from the freespace

            // assure the records we wrote are in the next layer
            Assert.AreEqual(0, 1, "test not done");
        }

       // TEST: double flush and restore 2 segments  (walk .ROOT range map)
       // TEST: Tombstones

       // ----------------------------[ TEST MERGING ]-----------------------------

       // TEST: double flush and merge 2 segments into 1
       // TEST: random pattern test (long), lots of merging
       // TEST: Tombstone cleanup

       // ----------------------------[  TEST ROW ATTRIBUTES ]---------------------

       // TEST: row attributes
       // TEST: segment merge row attribute collapse/cleanup for old TX (before it hits the bottom)

       // ----------------------------[   TEST MVCC    ]---------------------------
       // TEST: MVCC Row Read Locking
       // TEST: MVCC Row Write Locking
       // TEST: MVCC Row-Range Read Locking
       // TEST: MVCC pending TX past restart

       // ----------------------------[  TWO PHASE COMMIT ]------------------------
       // TEST: two-phase commit prepare past restart
 

    }
}