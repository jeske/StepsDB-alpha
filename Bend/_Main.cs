// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using NUnit.Framework;

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
            Application.Run(window);      
        }

        public static void dumpAllDbRecords(LayerManager db) {
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
            foreach (var seg in db.listAllSegments()) {
                System.Console.WriteLine("gen{0} start({1}) end({2})", seg.generation, seg.start_key, seg.end_key);
            }            
        }

        public static void do_bringup_test2(DbgGUI win) {

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

            int keysize = 20000;
            int keycount = 10000;
            int flush_period = 1000;
            bool random_order = true;

            db = new LayerManager(InitMode.NEW_REGION, "c:\\BENDtst\\bigtest");
            String value = "";
            String keybase = "TestValueDataABC";
            for (int x = 0; x < keysize / keybase.Length; x++) { value = value + keybase; }
            Random rnd = new Random();

            for (int x = 10000001; x < 10000001 + keycount; x++) {
                if (random_order) {
                    db.setValueParsed("test/rnd/" + rnd.Next(), value);
                } else {
                    db.setValueParsed("test/ordered/" + x, value);
                }
               
                if (x % flush_period == 0) {
                    System.Console.WriteLine("start % 1000 cycle..");
                    db.flushWorkingSegment();                    
                    
                    win.debugDump(db);
                    dumpMergeCandidates(db);
                    
                    for (int mx = 0; mx < 4; mx++) {
                        
                        mc = db.rangemapmgr.mergeManager.getBestCandidate();
                        System.Console.WriteLine("merge " + mx + " : " + mc);
                        if (mc == null) {
                            break;
                        }
                        db.performMerge(mc);
                        System.Console.WriteLine("mergedone " + mx + " : " + mc);

                        dumpSegmentList(db);
                        dumpMergeCandidates(db);
                        win.debugDump(db);                        
                    }

                    System.Console.WriteLine("merge cycle done");                    
                }
            }


            System.Console.WriteLine("-------- Merge a bunch more ------------------");

            for (int x = 0; x < 30; x++) {
                mc = db.rangemapmgr.mergeManager.getBestCandidate();
                System.Console.WriteLine("merge : " + mc);
                if (mc == null) break;
                db.performMerge(mc);

                dumpSegmentList(db);
                dumpMergeCandidates(db);
                win.debugDump(db);                              
            }

            dumpSegmentList(db);
            System.Console.WriteLine("** done.");
        }
    }
}
