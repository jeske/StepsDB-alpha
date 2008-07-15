﻿// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using NUnit.Framework;

using Bend;
using System.Threading;

namespace BendTests
{

    [TestFixture]
    public class A03_LayerManagerTests 
    {
        [SetUp]
        public void TestSetup() {
            System.GC.Collect(); // cause full collection to try and dispose/free filehandles
        }

        [Test]
        public void T000_EmptyLayerInitAndResume() {
            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\3");

            Assert.AreEqual(1, db.segmentlayers.Count);
            Assert.AreEqual(db.segmentlayers[0], db.workingSegment);
            Assert.AreEqual(1, db.workingSegment.RowCount); // expecting only the boostrap NUMGENERATIONS record

            // TEST: log is empty
            // TEST: freespace record established!
        }

        [Test]
        public void T01_LayerTxnLogResume() {
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
                // 3 test records, and the NUMGENERATIONS record 
                // TODO: make a more robust way to do this test (i.e. count non .ROOT records)
                Assert.AreEqual(4, db.workingSegment.RowCount);
                db.Dispose();
            }

            {
                LayerManager db = new LayerManager(InitMode.RESUME, "c:\\test\\4");
                
                // assure we still have not committed any segments
                Assert.AreEqual(1, db.segmentlayers.Count);
                Assert.AreEqual(db.segmentlayers[0], db.workingSegment);

                // assure the working segment contains the right data
                Assert.AreEqual(4, db.workingSegment.RowCount);
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
        public void T02_LayerSegmentFlushAndFreespaceModification() {
            String[] keys = { "test-1", "test-2", "test-3" };
            String[] values = { "a", "b", "c" };

            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\5");

            LayerManager.Txn txn = db.newTxn();
            for (int i = 0; i < keys.Length; i++) {
                txn.setValueParsed(keys[i], values[i]);
            }
            txn.commit();
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
                    Assert.AreEqual("1", update.ToString(), "generation count");  
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
        public void T03_SegmentLayerGetRecordApplicationOrder() {
            // Assure that when records are written more than once, the updates are applied in the correct
            // order so we see the proper current data value

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
        public void T04_SingleSegmentRootMetadataLogRecovery() {
            // TEST: test multiple segments flushed, and "log resumed"  (walk .ROOT range map)
            
            // perform the previous test
            T03_SegmentLayerGetRecordApplicationOrder();

            // ... and then perform a resume
            LayerManager db = new LayerManager(InitMode.RESUME, "c:\\test\\6");

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
                        LayerManager.Txn txn = db.newTxn();
                        txn.setValueParsed(secondkeys[i], secondvalues[i]);
                        txn.commit();
                        db.flushWorkingSegment();
                    }

                    db.Dispose();

                    // RESUME
                    db = new LayerManager(InitMode.RESUME, "c:\\test\\6");

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
        
        public class ReadThreadsTest {
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
                db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\10");
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
                    LayerManager.Txn txn = db.newTxn();
                    txn.setValue(kvp.Key, kvp.Value);
                    txn.commit();
                    pos++;

                    if ((pos % RECORDS_PER_SEGMENT) == 0) {
                       db.flushWorkingSegment();
                    }
                }            
                db.flushWorkingSegment();
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
        public void T09_LayerManager_ReadMergeAll() {
            ReadThreadsTest test = new ReadThreadsTest(100, 30);

            test.verifyData();
            System.Console.WriteLine("----- merge --------");
            test.db.mergeAllSegments();
            test.verifyData();

        }

        [Test]
        public void T10_LayerManager_ReadThreads() {
            ReadThreadsTest test = new ReadThreadsTest(100,30);

            test.verifyData();
            test.threadedVerify(10);
            System.Console.WriteLine("----- merge --------");
            test.db.mergeAllSegments();
            test.threadedVerify(10);            
        }

        public class WriteThreadsTest
        {
            internal LayerManager db;

            int[] datavalues;

            int num_additions = 0;
            int num_retrievals = 0;
            int num_removals = 0;

            internal int checkpoint_interval;

            internal WriteThreadsTest(int num_values, int checkpoint_interval_rowcount) {
                System.GC.Collect();
                db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\11");
                this.checkpoint_interval = checkpoint_interval_rowcount;
                
                Random rnd = new Random();
                datavalues = new int[num_values];
                for (int i = 0; i < num_values; i++) {
                    datavalues[i] = rnd.Next(0xfffff);
                }
            
            }

            public class threadLauncher
            {
                WriteThreadsTest parent;
                int thread_num;
                public threadLauncher(WriteThreadsTest parent, int thread_num) {
                    this.parent = parent;
                    this.thread_num = thread_num;
                }
                public void doVerify() {
                    this.parent.doVerify(this.thread_num);
                }
            }
            public void checkpointer() {
                int iteration = 0;
                while (checkpoint_interval != 0) {                    
                    if (db.workingSegment.RowCount > checkpoint_interval) {
                        DateTime start = DateTime.Now;
                        iteration++;
                        System.Console.WriteLine("checkpoint {0} start ", iteration);
                        db.flushWorkingSegment();
                        double duration_ms = (DateTime.Now - start).TotalMilliseconds;
                        System.Console.WriteLine("checkpoint {0} end in {1} ms", iteration, duration_ms);
                        Thread.Sleep(5);
                    } else {
                        Thread.Sleep(5);
                    }
                }

            }
            public void threadedTest(int numthreads) {
                List<Thread> threads = new List<Thread>();

                for (int threadnum = 0; threadnum < numthreads; threadnum++) {
                    threadLauncher launcher = new threadLauncher(this, threadnum);
                    Thread newthread = new Thread(new ThreadStart(launcher.doVerify));
                    threads.Add(newthread);
                }
                Thread checkpointer = new Thread(new ThreadStart(this.checkpointer));
                DateTime start = DateTime.Now;
                try {
                    
                    checkpointer.Start();

                    num_additions = 0; num_removals = 0; num_retrievals = 0;
                    
                    foreach (Thread th in threads) {
                        th.Start();
                    }

                    foreach (Thread th in threads) {
                        // rejoin the threads
                        th.Join(); 
                    }
                } finally {
                    // stop the checkpointer
                    checkpoint_interval = 0;
                }
                checkpointer.Join();

                double duration_ms = (DateTime.Now - start).TotalMilliseconds;
                double ops_per_sec = (num_additions + num_retrievals + num_removals) * (1000.0 / duration_ms);

                System.Console.WriteLine("LayerManager Threading Test, {0} ms elapsed",
                    duration_ms);
                System.Console.WriteLine("  {0} additions, {1} retrievals, {2} removals",
                    num_additions, num_retrievals, num_removals);
                System.Console.WriteLine("  {0} ops/sec", ops_per_sec);

                int expected_count = numthreads * datavalues.Length;
                Assert.AreEqual(expected_count, num_additions, "addition count");
                Assert.AreEqual(expected_count, num_retrievals, "retrieval count");
                Assert.AreEqual(expected_count, num_removals, "removal count");

            }
            public void doVerify(int thread_num) {
                Random rnd = new Random(thread_num);
                Thread.Sleep(rnd.Next(1000));
                System.Console.WriteLine("startwrites.. " + thread_num);
                // add the values
                for (int i = 0; i < datavalues.Length; i++) {
                    string data = datavalues[i].ToString();
                    string value = "v/" + data + ":" + thread_num.ToString();                    
                    db.setValueParsed(value, data);                    
                    Interlocked.Increment(ref num_additions);
                }
                
                System.Console.WriteLine("endwrites, startread " + thread_num);

                // read the values
                for (int i = 0; i < datavalues.Length; i++) {
                    string value = datavalues[i].ToString() + ":" + thread_num.ToString();
                    RecordData data;
                    if (db.getRecord(new RecordKey().appendParsedKey("v/" + value), out data) == GetStatus.PRESENT) {
                        Assert.AreEqual(datavalues[i].ToString(), data.ToString());
                        Interlocked.Increment(ref num_retrievals);
                    }
                }

                System.Console.WriteLine("endreads, startremove " + thread_num);

                // remove the values
                for (int i = 0; i < datavalues.Length; i++) {
                    string value = datavalues[i].ToString() + ":" + thread_num.ToString();
                    db.setValue(new RecordKey().appendParsedKey("v/" + value), RecordUpdate.DeletionTombstone());
                    Interlocked.Increment(ref num_removals);

                }

            }
        }


        [Test]
        public void T11_LayerManager_WriteThreads() {
            WriteThreadsTest test = new WriteThreadsTest(10,50);
            test.threadedTest(100);
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
        public void T001_FullScanWithOnlyWorkingSegment() {
            LayerManager db = new LayerManager(InitMode.NEW_REGION, "c:\\test\\31");
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
        }
        
        [Test]
        public void T02_Small_WriteThreads_Perf() {
            A03_LayerManagerTests.WriteThreadsTest test =
                new A03_LayerManagerTests.WriteThreadsTest(20, 800);
            test.threadedTest(100);
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
        }

        [Test]
        public void T11_WriteThreads_Perf() {
            A03_LayerManagerTests.WriteThreadsTest test = 
                new A03_LayerManagerTests.WriteThreadsTest(100, 1000);
            test.threadedTest(50);
        }

    }

}