// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using System.Threading;

namespace Bend
{
    using BendTests;

    // ---------------[ Main ]---------------------------------------------------------

    /*  try to let the bringup test run in NUnit
    // [TestFixture]
    public class MainTest
    {
        // [Test]
        public void T00_MainTest() {
            MainBend.do_bringup_test();
        }

    }
     */


    public static class MainBend
    {

        [STAThread]
        static void Main(string[] args) {

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            var window = new DbgGUI();
            window.SetDesktopLocation(700, 200);

            Thread newThread = new Thread(delegate() {
                MainBend.do_bringup_test(window);
            });
            newThread.Start();

            Application.Run(window);      
        }


        public static void dumpAllDbRecords(LayerManager db) {
            foreach (var kv in db.scanForward(null)) {
                System.Console.WriteLine("  nfound: {0} -> {1}",kv.Key,kv.Value);
            }
        }

        public static void dumpAllDbRecords_old(LayerManager db) {
            RecordKey next_key = new RecordKey();
            RecordKey fkey = null;
            RecordData fdata = null;
            while (db.getNextRecord(next_key, ref fkey, ref fdata) == GetStatus.PRESENT) {
                next_key = fkey;

                System.Console.WriteLine("  found: {0} -> {1}", fkey.ToString(), fdata.ToString());

            }
        }

        public static void stop() {
            throw new Exception("stop tests");
        }

        public static void dumpMergeCandidates(LayerManager db) {
            MergeManager_Incremental mm = db.rangemapmgr.mergeManager;
            System.Console.WriteLine("-- dumpMergeCandidates");
            foreach (var mc in mm.prioritizedMergeCandidates) {
                System.Console.WriteLine("  " + mc.ToString());
            }
        }

        public static void dumpSegmentList(LayerManager db) {
            if (true) {
                // this is the slow method

                var walk = db.rangemapmgr.mergeManager.segmentInfo.GetEnumerator();

                bool discrepancy = false;

                foreach (var seg in db.listAllSegments()) {

                    // Assert.AreEqual(true, walk.MoveNext(), "mergemanager missing record!");
                    // Assert.AreEqual(0, walk.Current.Key.CompareTo(seg), "mergemanager and db.listAllSegments have different data!");
                    if (walk.MoveNext()) {
                        if (walk.Current.Key.CompareTo(seg) != 0) {
                            discrepancy = true;
                            Console.WriteLine("  mismatch: db{0} mm{1}", seg, walk.Current.Key);
                        }
                    } else { discrepancy = true; }

                    System.Console.WriteLine("db gen{0} start({1}) end({2})", seg.generation, seg.start_key, seg.end_key);
                }

                if (discrepancy) {
                    foreach (var seginfo in db.rangemapmgr.mergeManager.segmentInfo) {
                        var seg = seginfo.Key;
                        System.Console.WriteLine("mm gen{0} start({1}) end({2})", seg.generation, seg.start_key, seg.end_key);
                    }
                    throw new Exception("mergemanager and db.listAllSegments have different data!");
                }


            } else {
                // this is the fast method
                foreach (var seginfo in db.rangemapmgr.mergeManager.segmentInfo) {
                    var seg = seginfo.Key;
                    System.Console.WriteLine("fgen{0} start({1}) end({2})", seg.generation, seg.start_key, seg.end_key);
                }
            }
        }

        /*
        public static void do_bringup_test(DbgGUI win) {
            var db = new LayerManager(InitMode.NEW_REGION, @"c:\EmailTest\DB");
            var eminj = new EmailInjector(db, win);
            eminj.DoEmailTest();
            System.Console.WriteLine("done");
        }
         */
        public static void do_bringup_test3(DbgGUI win) {

            var testclass = new BendTests.A02_MergeManagerTests();
            testclass.T000_TestRangeBoundaries();

            System.Console.WriteLine("done");
        }

        public static void do_bringup_test(DbgGUI win) {


            LayerManager db = new LayerManager(InitMode.NEW_REGION,"c:\\BENDtst\\main");
         
            db.setValueParsed("test/3","a");
            db.setValueParsed("test/2","b");
            db.setValueParsed("test/1","c");
            db.debugDump();

            db.flushWorkingSegment();    // this will flush and read the current segment
            Console.WriteLine("--- after flush");
            db.debugDump();
            dumpMergeCandidates(db);

            Console.WriteLine("--- check record read");
            RecordData data;
            GetStatus status = db.getRecord(new RecordKey().appendParsedKey("test/3"), out data);
            System.Console.WriteLine("getRecord({0}) returned {1}", "test/3", data.ToString());
            
            Console.WriteLine("--- make lots of segments");
            db.setValueParsed("test/4", "d");
            db.flushWorkingSegment();
            db.setValueParsed("test/5", "e");
            db.flushWorkingSegment();
            db.setValueParsed("test/6", "f");
            db.flushWorkingSegment();
            db.debugDump();

            System.Console.WriteLine("-------- dump keys ---------------------");
            dumpAllDbRecords(db);
            dumpMergeCandidates(db);

            System.Console.WriteLine("-------- draw graphical debug ---------------------");
            win.debugDump(db);

            System.Console.WriteLine("-------- PERFORMING A SINGLE MERGE ---------------------");

            
            MergeCandidate mc;
            mc = db.rangemapmgr.mergeManager.getBestCandidate();
            System.Console.WriteLine("MERGE :" + mc);
            db.performMerge(mc);
            dumpMergeCandidates(db);

            db.flushWorkingSegment();
            db.debugDump();
            dumpSegmentList(db);
            win.debugDump(db);
            
            System.Console.WriteLine("-------- SINGLE MERGE DONE, close/dispose ---------------------");
                                   
            dumpSegmentList(db);
            dumpMergeCandidates(db);
            db.debugDump();
            db.Dispose();

            System.Console.WriteLine("-------- NOW RESUME ---------------------------------");
            db = new LayerManager(InitMode.RESUME, "c:\\BENDtst\\main");
            dumpSegmentList(db);
            dumpMergeCandidates(db);
            win.debugDump(db);
            db.debugDump();

            System.Console.WriteLine("-------- NOW FINDNEXT ---------------------------------");
            dumpAllDbRecords(db);
            win.debugDump(db);

            System.Console.WriteLine("-------- NOW MERGE ALL SEGMENTS ---------------------------------");
            dumpSegmentList(db);
            db.mergeAllSegments();
            db.debugDump();
            win.debugDump(db);

            // stop(); // ------------------------- ((  S   T   O   P  )) ---------------------------------

            System.Console.WriteLine("-------- NOW FINDNEXT (after merge) ---------------------------------");
            dumpAllDbRecords(db);

            //System.Console.WriteLine("-------- Now run Readthreads Test ---------------------------------");
            //A03_LayerManagerTests test = new A03_LayerManagerTests();
            //test.T10_LayerManager_ReadThreads();

            dumpMergeCandidates(db);
            win.debugDump(db);
            db.Dispose();
            

            System.Console.WriteLine("-------- Write ALOT of data ---------------------------------");


            int keysize = 2000;
            int keycount = 1000000;
            int flush_period = 10000;
            int commit_period = 1000;
            bool random_order = true;





            DateTime start = DateTime.Now;
            int record_count = 0;

            db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\bigtest");
            String value = "";
            String keybase = "TestValueDataABC";
            for (int x = 0; x < keysize / keybase.Length; x++) { value = value + keybase; }
            int seed = (int)DateTime.Now.Ticks;
            Random rnd = new Random(seed);

            System.Console.WriteLine("*** RANDOM SEED: " + seed);
            var write_group = db.newWriteGroup();

            for (int x = 10000001; x < 10000001 + keycount; x++) {
                if (random_order) {
                    write_group.setValueParsed("test/rnd/" + rnd.Next(), value);
                } else {
                    write_group.setValueParsed("test/ordered/" + x, value);
                }
                record_count++;

                if (x % commit_period == 0) { write_group.finish(); write_group = db.newWriteGroup(); }

                if (x % flush_period == 0) {
                    System.Console.WriteLine("*** RANDOM SEED: " + seed);
                    write_group.finish(); write_group = db.newWriteGroup();
                    System.Console.WriteLine("start % 1000 cycle..");
                    db.flushWorkingSegment();                    
                    
                    win.debugDump(db);
                    dumpMergeCandidates(db);
                    
                    for (int mx = 0; mx < 30; mx++) {
                        
                        mc = db.rangemapmgr.mergeManager.getBestCandidate();
                        if (mc == null) { break; }
                        if (mc.score() > (1.6 + (float)db.rangemapmgr.mergeManager.getMaxGeneration()/12.0f)) {
                            System.Console.WriteLine("** best merge score too high: " + mc);
                            break;
                        }
                        System.Console.WriteLine("merge " + mx + " : " + mc);
                        
                        win.debugDump(db, mc);
                        db.performMerge(mc);
                        System.Console.WriteLine("mergedone " + mx + " : " + mc);

                        dumpSegmentList(db);
                        dumpMergeCandidates(db);
                        win.debugDump(db);                        
                    }

                    double elapsed = (DateTime.Now - start).TotalMilliseconds / 1000.0;
                    System.Console.WriteLine("*** merge cycle done  {0} records so far, in {1} total time, {2} records/second",
                             record_count,elapsed, (double)record_count/elapsed);                
                    
                }
            }


            System.Console.WriteLine("-------- Merge a bunch more ------------------");

            for (int x = 0; x < 30; x++) {
                mc = db.rangemapmgr.mergeManager.getBestCandidate();
                System.Console.WriteLine("merge : " + mc);
                if (mc == null) break;
                win.debugDump(db, mc);   
                db.performMerge(mc);

                dumpSegmentList(db);
                dumpMergeCandidates(db);
                win.debugDump(db,null);                              
            }

            dumpSegmentList(db);
            System.Console.WriteLine("** done.");
        }
    }
}
