// Copyright (C) 2008-2011, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using NUnit.Framework;

using Bend;
using System.Threading;

namespace BendTests {

    public partial class A03_LayerManagerTests {

        public class WriteThreadsTest : IDisposable {
            internal LayerManager db;

            int[] datavalues;

            int num_additions = 0;
            int num_retrievals = 0;
            int num_removals = 0;
            bool withMerge;
            public int exceptions = 0;

            internal int checkpoint_interval;

            internal WriteThreadsTest(int num_values=10, int checkpoint_interval_rowcount=50, bool withMerge=false) {
                System.GC.Collect();
                db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\11");
                // db.startMaintThread();
                this.checkpoint_interval = checkpoint_interval_rowcount;
                this.withMerge = withMerge;

                Random rnd = new Random();
                datavalues = new int[num_values];
                for (int i = 0; i < num_values; i++) {
                    datavalues[i] = rnd.Next(0xfffffff);
                }

            }

            public void Dispose() {
                if (db != null) { db.Dispose(); db = null; }
            }

            public class threadLauncher {
                WriteThreadsTest parent;
                int thread_num;
                public threadLauncher(WriteThreadsTest parent, int thread_num) {
                    this.parent = parent;
                    this.thread_num = thread_num;
                }
                public void testThreadWorker() {
                    try {
                        this.parent.testThreadWorker(this.thread_num);
                    } catch (Exception e) {
                        System.Console.WriteLine("EXCEPTION in thread " + thread_num + ": " + e.ToString());
                        parent.exceptions++;
                    }
                }
            }

            // --------[ Checkpointer Thread ]-----------------------------
            // currently this only checkpoints the working segment, should this also do merges?
            public void checkpointer() {
                int iteration = 0;                
                while (checkpoint_interval != 0) {
                    try {
                        if (db.workingSegment.RowCount > checkpoint_interval) {
                            DateTime start = DateTime.Now;
                            iteration++;
                            System.Console.WriteLine("checkpoint {0} start ", iteration);
                            db.flushWorkingSegment();
                            if (this.withMerge) {
                                db.mergeIfNeeded();
                            }
                            // db.DEBUG_addNewWorkingSegmentWithoutFlush();
                            double duration_ms = (DateTime.Now - start).TotalMilliseconds;
                            System.Console.WriteLine("checkpoint {0} end in {1} ms", iteration, duration_ms);
                            Thread.Sleep(5);
                        } else {
                            Thread.Sleep(5);
                        }
                    } catch (Exception e) {
                        System.Console.WriteLine("EXCEPTION in checkpointer -- " + e.ToString());
                        exceptions++;
                    }
                }

            }

            // -------[ startup code ]------------------------------------
            public void runThreadedTest(int numthreads) {
                List<Thread> threads = new List<Thread>();

                for (int threadnum = 0; threadnum < numthreads; threadnum++) {
                    threadLauncher launcher = new threadLauncher(this, threadnum);
                    Thread newthread = new Thread(new ThreadStart(launcher.testThreadWorker));
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
                System.Console.WriteLine("  {0} exceptions", exceptions);

                int expected_count = numthreads * datavalues.Length;
                Assert.AreEqual(expected_count, num_additions, "addition count");
                Assert.AreEqual(expected_count, num_retrievals, "retrieval count");
                Assert.AreEqual(expected_count, num_removals, "removal count");
                Assert.AreEqual(exceptions, 0, "exceptions");

            }

            private string composeKey(int thread_num, string forData) {
                return "v/" + forData + ":" + thread_num.ToString();
            }


            // 
            public void testThreadWorker(int thread_num) {
                Random rnd = new Random(thread_num);
                Thread.Sleep(rnd.Next(1000)); // sleep a random amount of time


                // add a set of data values
                System.Console.WriteLine("startwrites.. " + thread_num);
                // add the values
                for (int i = 0; i < datavalues.Length; i++) {
                    string data = datavalues[i].ToString();
                    string key = this.composeKey(thread_num, data);
                    db.setValueParsed(key, data);
                    Interlocked.Increment(ref num_additions);
                    if (i % 100 == 0) {
                        Console.WriteLine(" ..thread {0} : {1}", thread_num, i);
                    }
                }

                System.Console.WriteLine("endwrites, startread " + thread_num);

                // read the values
                for (int i = 0; i < datavalues.Length; i++) {
                    RecordData rdata;

                    string data = datavalues[i].ToString();
                    string key = this.composeKey(thread_num, data);

                    if (db.getRecord(new RecordKey().appendParsedKey(key), out rdata) == GetStatus.PRESENT) {
                        if (datavalues[i].ToString() == rdata.ReadDataAsString()) {
                            Interlocked.Increment(ref num_retrievals);
                        } else {
                            System.Console.WriteLine("-- ERR: record data didn't match for key({0}). expected {1} != got {2}",
                                key, datavalues[i].ToString(), rdata.ReadDataAsString());
                        }
                    } else {
                        System.Console.WriteLine("-- ERR: missing record, thread({0}), key({1})",
                            thread_num, key);
                        return;
                    }
                }

                System.Console.WriteLine("endreads, startremove " + thread_num);

                // remove the values
                for (int i = 0; i < datavalues.Length; i++) {
                    string data = datavalues[i].ToString();
                    string key = this.composeKey(thread_num, data);
                    db.setValue(new RecordKey().appendParsedKey(key), RecordUpdate.DeletionTombstone());
                    Interlocked.Increment(ref num_removals);

                }

            }
        }

        [Test]
        public void T11_LayerManager_WriteThreads() {
            WriteThreadsTest test = new WriteThreadsTest(10, 50);
            test.runThreadedTest(100);
            test.Dispose();
        }

        [Test]
        public void T12_LayerManager_WriteThread_WithMerge() {
            WriteThreadsTest test = new WriteThreadsTest(num_values:150, checkpoint_interval_rowcount:50, withMerge:true);            
            test.runThreadedTest(100);
            test.Dispose();
        }

    }
}