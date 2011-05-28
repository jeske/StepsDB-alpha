// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using NUnit.Framework;

using Bend;
using System.Threading;

namespace BendTests
{

    [TestFixture]
    public partial class A03_LayerManagerTests 
    {
        [SetUp]
        public void TestSetup() {
            System.GC.Collect(); // cause full collection to try and dispose/free filehandles
        }

        [Test]
        public void T000_EmptyLayerInitAndResume() {
            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\3");

            Assert.AreEqual(1, db.segmentlayers.Count);
            Assert.AreEqual(db.segmentlayers[0], db.workingSegment);
            Assert.AreEqual(1, db.workingSegment.RowCount); // expecting only the boostrap NUMGENERATIONS record

            // TEST: log is empty
            // TEST: freespace record established!
        }


        [Test]
        public void T001_WorkingSegmentReadWrite() {
            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\3");

            var rk = new RecordKey().appendParsedKey(".a");
            db.setValueParsed(".a", "1");
            KeyValuePair<RecordKey, RecordData> record;

            try {
                record = db.FindNext(rk, true);
                Assert.AreEqual(rk, record.Key, "fetched key does not match");
            } catch (KeyNotFoundException) {            
                Assert.Fail("couldn't find 'a' record");
            }

            int found_recs = 0;
            var scan_range = new ScanRange<RecordKey>(rk, RecordKey.AfterPrefix(rk), null);

            foreach (var row in db.scanForward(scan_range)) {                
                found_recs++;
            }
            Assert.AreEqual(1, found_recs, "found the wrong number of records in working segment scan!");

            db.flushWorkingSegment();

            try {
                record = db.FindNext(rk, true);
                Assert.AreEqual(rk, record.Key, "fetched key does not match (after flush)");
            } catch (KeyNotFoundException) {
                Assert.Fail("couldn't find 'a' record");
            }

            found_recs = 0;
            foreach (var row in db.scanForward(
                new ScanRange<RecordKey>(rk, RecordKey.AfterPrefix(rk), null))) {
                found_recs++;
            }
            Assert.AreEqual(1, found_recs, "found the wrong number of records after flush !");


        }

        [Test]
        public void T002_ScanDirections() {
            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\3");

            var rk_a = new RecordKey().appendParsedKey(".a");
            var rk_b = new RecordKey().appendParsedKey(".b");
            string[] keys = { ".a", ".b" };
            foreach (var key in keys) {
                db.setValueParsed(key, "valueof:" + key);
            }

            {
                var rec = db.FindNext(rk_a, false);
                Assert.AreEqual(rk_b, rec.Key, "simple FindNext");
            }

            {
                var rec = db.FindPrev(rk_b, false);
                Assert.AreEqual(rk_a, rec.Key, "simple FindPrev");
            }


            var scan_range = new ScanRange<RecordKey>(rk_a, rk_b, null);

            // scan forward
            int count = 0;
            foreach (var row in db.scanForward(scan_range)) {
                Console.WriteLine("forward scan: " + row);
                if (count == keys.Length) {
                    Assert.Fail("too many keys returned in scan");
                }
                Assert.AreEqual(new RecordKeyType_String(keys[count]), row.Key.key_parts[0], "forward scan mistake");
                count++;
            }
            if (count != keys.Length) {
                Assert.Fail("not enough keys returned in scan");
            }


            // scan backward

            count = keys.Length;
            foreach (var row in db.scanBackward(scan_range)) {
                Console.WriteLine("backward scan: " + row);
                if (count == 0) {
                    Assert.Fail("too many keys returned in scan backward");
                }
                count--;
                Assert.AreEqual(new RecordKeyType_String(keys[count]), row.Key.key_parts[0], "backward scan mistake");
            }
            if (count != 0) {
                Assert.Fail("not enough keys returned in scan");
            }


        }


        [Test]
        public void T01_LayerTxnLogResume() {
            String[] keys = { "test-1", "test-2", "test-3" };
            String[] values = {"a","b","c" };

            {
                LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\4");

                LayerManager.WriteGroup txn = db.newWriteGroup();
                for (int i=0;i<keys.Length;i++) {
                    txn.setValueParsed(keys[i],values[i]);
                }
                txn.finish();

                // TODO: assure the freespace hasn't been affected

                // assure we have not committed any segments
                Assert.AreEqual(1, db.segmentlayers.Count);
                Assert.AreEqual(db.segmentlayers[0], db.workingSegment);

                // assure the working segment contains the right data
                // 3 test records, and the NUMGENERATIONS record 
                // TODO: make a more robust way to do this test (i.e. count non .ROOT records)
                Assert.AreEqual(4, db.workingSegment.RowCount);
                db.Dispose();
            }

            {
                LayerManager db = new LayerManager(InitMode.RESUME, "c:\\BENDtst\\4");
                System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

                // assure we still have not committed any segments
                Assert.AreEqual(1, db.segmentlayers.Count);
                Assert.AreEqual(db.segmentlayers[0], db.workingSegment);

                // assure the working segment contains the right data
                Assert.AreEqual(4, db.workingSegment.RowCount);
                for (int i = 0; i < keys.Length; i++) {
                    RecordKey key = new RecordKey();
                    key.appendKeyPart(new RecordKeyType_String(keys[i]));

                    // look directly in the working segment
                    {

                        RecordUpdate update;
                        GetStatus status = db.workingSegment.getRecordUpdate(key, out update);
                        Assert.AreEqual(GetStatus.PRESENT, status, "SegmentBuilder.getRecordUpdate({0})", key);
                        Assert.AreEqual(values[i], enc.GetString(update.data), "SegmentBuilder.getRecordUpdate({0})",key);
                    }

                    // assure the global query interface finds it
                    {
                        RecordData data;
                        GetStatus status = db.getRecord(key, out data);
                        Assert.AreEqual(GetStatus.PRESENT, status, "LayerManager.getRecord({0})", key);
                        Assert.AreEqual(values[i], enc.GetString(data.data), "LayerManager.getRecord({0})",key);
                    }
                }

                // cleanup
                db.Dispose();
            }

        }

        [Test]
        public void T02_LayerSegmentFlushAndFreespaceModification() {
            String[] keys = { "test-1", "test-2", "test-3" };
            String[] values = { "a", "b", "c" };
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\5");

            LayerManager.WriteGroup txn = db.newWriteGroup();
            for (int i = 0; i < keys.Length; i++) {
                txn.setValueParsed(keys[i], values[i]);
            }
            txn.finish();
            db.flushWorkingSegment();

            // assure that we checkpointed down to a single working segment
            Assert.AreEqual(1, db.segmentlayers.Count, "segment layer count");

            // assure we allocated a new generation and gen0 range record (walk .ROOT range map)
            // TODO: move this test to RangemapManager, to remove this cross-dependency
            {
                RecordData data;
                RecordKey key = new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS");
                Assert.AreEqual(GetStatus.PRESENT,db.getRecord(key, out data),"missing numgenerations record");
                Assert.AreEqual("1", data.ToString(),"generation count");

                RecordUpdate update;
                Assert.AreEqual(GetStatus.PRESENT,
                    db.workingSegment.getRecordUpdate(key, out update), "missing workingsegment numgenerations record");
                    Assert.AreEqual("1", enc.GetString(update.data), "generation count");  
            }


            if (false) {
                RecordData data;
                Assert.AreEqual(
                    GetStatus.PRESENT,
                    db.getRecord(new RecordKey()
                    .appendParsedKey(".ROOT/GEN")
                    .appendKeyPart(new RecordKeyType_Long(0))
                    .appendKeyPart("</>"), out data),
                    ".ROOT/GEN/0/</>  key is missing");
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
                    Assert.AreEqual(GetStatus.PRESENT, status, "records should be found in layers, {0} missing", key);
                    Assert.AreEqual(values[i], data.ToString(), "LayerManager.getRecord()");
                }
            }

            db.Dispose();

        }

        [Test]
        public void T03_SegmentLayerGetRecordApplicationOrder() {
            // Assure that when records are written more than once, the updates are applied in the correct
            // order so we see the proper current data value

            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\6");

            {
                String[] keys = { "test-1", "test-2", "test-3" };
                String[] values = { "a-first", "b-first", "c-first" };

                LayerManager.WriteGroup txn = db.newWriteGroup();
                for (int i = 0; i < keys.Length; i++) {
                    txn.setValueParsed(keys[i], values[i]);
                }
                txn.finish();
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
                System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

                LayerManager.WriteGroup txn = db.newWriteGroup();
                for (int i = 0; i < keys.Length; i++) {
                    txn.setValueParsed(keys[i], values[i]);
                }
                txn.finish();


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
                        Assert.AreEqual(values[i], enc.GetString(update.data), "SegmentBuilder.getRecordUpdate should see NEW VALUES");
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
        public void T04_SingleSegmentRootMetadataLogRecovery() {
            // TEST: test multiple segments flushed, and "log resumed"  (walk .ROOT range map)
            
            // perform the previous test
            T03_SegmentLayerGetRecordApplicationOrder();

            // ... and then perform a resume
            LayerManager db = new LayerManager(InitMode.RESUME, "c:\\BENDtst\\6");

            String[] keys = { "test-1", "test-2", "test-3" };
            String[] values = { "a-second", "b-second", "c-second" };

            // verify that it has the same data as before the RESUME
            {
                // working segment should be empty
                for (int i = 0; i < keys.Length; i++) {
                    RecordKey key = new RecordKey();
                    key.appendKeyPart(keys[i]);

                    // look directly in the working segment, they should be MISSING
                    //   This is testing the checkpoint as well. If log resume didn't
                    //   CHECKPOINT_DROP, then the values will be duplicated in the working segment.
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

                // now generate a BUNCH of new segments...
                {
                    String[] secondkeys = { "second-test-1", "second-test-2", "second-test-3" };
                    String[] secondvalues = { "a-second", "b-second", "c-second" };

                    // put each new record in its OWN segment
                    for (int i = 0; i < secondkeys.Length; i++) {
                        LayerManager.WriteGroup txn = db.newWriteGroup();
                        txn.setValueParsed(secondkeys[i], secondvalues[i]);
                        txn.finish();
                        db.flushWorkingSegment();
                    }

                    db.Dispose();

                    // RESUME
                    db = new LayerManager(InitMode.RESUME, "c:\\BENDtst\\6");

                    // first test records should still be visible
                    for (int i = 0; i < keys.Length; i++) {
                        RecordKey key = new RecordKey();
                        key.appendKeyPart(keys[i]);

                        // look directly in the working segment, they should be MISSING
                        //   This is testing the checkpoint as well. If log resume didn't
                        //   CHECKPOINT_DROP, then the values will be duplicated in the working segment.
                        {
                            RecordUpdate update;
                            GetStatus status =
                                db.workingSegment.getRecordUpdate(key, out update);
                            Assert.AreEqual(GetStatus.MISSING, status, "working segment should be MISSING {0}", key);
                        }

                        // assure the global query interface finds the NEW VALUES
                        {
                            RecordData data;
                            GetStatus status = db.getRecord(key, out data);
                            Assert.AreEqual(GetStatus.PRESENT, status, "LayerManager should see NEW VALUES : {0}", key);
                            Assert.AreEqual(values[i], data.ToString(), "LayerManager.getRecord() should see NEW VALUES : {0}", key);
                        }
                    }
                    db.debugDump();
                    // verify that the secondkeys/values are still in there
                    for (int i = 0; i < secondkeys.Length; i++) {
                        RecordKey key = new RecordKey();
                        key.appendKeyPart(secondkeys[i]);

                        // look directly in the working segment, they should be MISSING
                        //   This is testing the checkpoint as well. If log resume didn't
                        //   CHECKPOINT_DROP, then the values will be duplicated in the working segment.
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
                            Assert.AreEqual(GetStatus.PRESENT, status, "LayerManager should see NEW VALUES, where is: " + key);
                            Assert.AreEqual(secondvalues[i], data.ToString(), "LayerManager.getRecord() should see NEW VALUES");
                        }
                    }
                   
                }
                db.Dispose();
            }
        }


        // TEST: Tombstones

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

        // TEST: region IO concurrency
        
        public class ReadThreadsTest : IDisposable {
            internal LayerManager db; 
            int TEST_RECORD_COUNT = 100;
            int RECORDS_PER_SEGMENT = 30;
            SortedDictionary<string, string> testdata;
            SortedDictionary<RecordKey, RecordUpdate> testrows;
            internal int records_read = 0;
            internal bool had_errors = false;

            internal ReadThreadsTest(int rec_count, int rec_per_segment) {
                this.TEST_RECORD_COUNT = rec_count;
                this.RECORDS_PER_SEGMENT = rec_per_segment;
                System.GC.Collect();
                db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\10");
                testdata = new SortedDictionary<string, string>();
                testrows = new SortedDictionary<RecordKey, RecordUpdate>();

                // generate some data
                for (int i=0;i<TEST_RECORD_COUNT;i++) {
                    string key = "test/" + i.ToString();
                    string value = "data: " + key;
                    testdata[key] = value;

                    RecordKey rkey = new RecordKey().appendParsedKey(key);
                    RecordUpdate rupdate = RecordUpdate.WithPayload(value);

                    testrows[rkey] = rupdate;
                }

                // fill the db with some data.
                int pos = 0;
                foreach (KeyValuePair<RecordKey,RecordUpdate> kvp in testrows) {
                    LayerManager.WriteGroup txn = db.newWriteGroup();
                    txn.setValue(kvp.Key, kvp.Value);
                    txn.finish();
                    pos++;

                    if ((pos % RECORDS_PER_SEGMENT) == 0) {
                       db.flushWorkingSegment();
                    }
                }            
                db.flushWorkingSegment();
            }

            public void Dispose() {
                if (db != null) { db.Dispose(); db = null; }                
            }

            internal void verifyData() {
                // make sure it reads back..
                int pos = 0;
                foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in testrows) {
                    RecordData rdata;
                    RecordKey rkey = kvp.Key;
                    if (db.getRecord(rkey, out rdata) == GetStatus.MISSING) {
                        had_errors = true;
                        Assert.Fail("failed to read: " + kvp.Key.ToString());                        
                    }
                    Interlocked.Increment(ref records_read);
                    
                    pos++;
                    if ((pos % 10) == 0) {
                        // System.Console.WriteLine("at record {0} of {1}", pos, testdata.Count);
                    }
                }
            }
            internal void threadedVerify(int numthreads) {
                this.records_read = 0;
                DateTime start = DateTime.Now;
                List<Thread> threads = new List<Thread>();
                // now do the same thing simultaneously in multiple threads
                for (int threadnum = 0; threadnum < numthreads; threadnum++) {
                    Thread newthread = new Thread(new ThreadStart(this.verifyData));

                    newthread.Start();
                    threads.Add(newthread);
                    Thread.Sleep(1);
                }

                foreach (Thread th in threads) {
                    // rejoin the threads
                    th.Join();
                }

                DateTime end = DateTime.Now;
                TimeSpan duration = end - start;
                double dur_ms = (end - start).TotalMilliseconds;

                double records_per_second = (double)this.records_read * (1000.0 / dur_ms);

                System.Console.WriteLine("ReadThreads ({0} records) in elapsed time: {1} -- {2} rec/sec",
                    this.records_read, duration, records_per_second);

                Assert.AreEqual(false, had_errors, "should have no read errors");
            }
        }

        [Test]
        public void T10_LayerManager_ReadThreads() {
            ReadThreadsTest test = new ReadThreadsTest(100,30);

            test.verifyData();
            test.threadedVerify(10);
            System.Console.WriteLine("----- merge --------");
            test.db.mergeAllSegments();
            test.verifyData();
            test.threadedVerify(10);
            test.Dispose();
        }




        // TEST: that our record-get will see data in ALL in-memory segments
        // TEST: two stage "checkpoint" -> "drop/finalize", concurrency, atomicity

        // TEST: assure the atomicity of a LogCommitGroup (Txn?)

        // ----------------------------[   TEST MVCC    ]---------------------------
        // TEST: MVCC Row Read Locking
        // TEST: MVCC Row Write Locking
        // TEST: MVCC Row-Range Read Locking
        // TEST: MVCC pending TX past restart

        // ----------------------------[  TWO PHASE COMMIT ]------------------------
        // TEST: two-phase commit prepare past restart


    }

    [TestFixture]
    public class ZZ_Todo_LayerManagerTests
    {
        [Test]
        public void T12_LayerManager_AssureTombstones_DeleteRecords() {
            Assert.Fail("TODO: test to assure tombstones kill records in segment merge process");
        }
        
        [Test]
        public void T13_LayerManager_Efficient_RangeKeyScans() {
            // not sure how to even test this...
            Assert.Fail("TODO: test layermanager efficiently uses block pointer range references");
        }

        [Test]
        public void T001_FullScanWithOnlyWorkingSegment() {
            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\31");
            db.setValueParsed("test/1", "a");
            Assert.Fail("test not implemented");
        }

        [Test]
        public void T002_PartialScanWithOnlyWorkingSegment() {
            Assert.Fail("test not implemented");
        }
    }
}

namespace BendPerfTest {
    using BendTests;

    [TestFixture]
    public class A02_LayerManagerPerf {

        [Test]
        public void T01_Small_ReadThreads_Perf() {
            A03_LayerManagerTests.ReadThreadsTest test =                 
                new A03_LayerManagerTests.ReadThreadsTest(100,30);

            test.verifyData();
            test.threadedVerify(50);
            System.Console.WriteLine("----- merge --------");
            test.db.mergeAllSegments();
            test.verifyData();
            test.threadedVerify(50);
            test.Dispose();
          
        }
        
        [Test]
        public void T02_Small_WriteThreads_Perf() {
            A03_LayerManagerTests.WriteThreadsTest test =
                new A03_LayerManagerTests.WriteThreadsTest(20, 800);
            test.runThreadedTest(100);
        }


        [Test]
        public void T10_ReadThreads_Perf() {
            A03_LayerManagerTests.ReadThreadsTest test =                 
                new A03_LayerManagerTests.ReadThreadsTest(500,100);

            test.verifyData();
            test.threadedVerify(50);
            System.Console.WriteLine("----- merge --------");
            test.db.mergeAllSegments();
            test.threadedVerify(50);
            test.Dispose();
        }

        [Test]
        public void T11_WriteThreads_Perf() {
            A03_LayerManagerTests.WriteThreadsTest test = 
                new A03_LayerManagerTests.WriteThreadsTest(500, 7000);
            test.runThreadedTest(60);
            test.Dispose();
        }

    }

}