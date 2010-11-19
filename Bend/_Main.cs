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

        public static void dumpSegmentList(LayerManager db) {
            foreach (var seg in db.listAllSegments()) {
                System.Console.WriteLine("gen{0} start({1}) end({2})", seg.generation, seg.start_key, seg.end_key);
            }            
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
            System.Console.WriteLine("-------- draw graphical debug ---------------------");
            win.debugDump(db);

            System.Console.WriteLine("-------- PERFORMING A SINGLE MERGE ---------------------");

            // calculate merge ratios
            LayerManager.MergeRatios mr = db.generateMergeRatios();
            mr.DebugDump();

            // use the merge ratios to calculate a single merge
            LayerManager.MergeTask merge_task = mr.generateMergeTask();
            System.Console.WriteLine(merge_task.ToString());
            
            // do the merge
            db.mergeSegments(merge_task); 
            db.flushWorkingSegment();
            db.debugDump();
            dumpSegmentList(db);
            win.debugDump(db);
            System.Console.WriteLine("-------- SINGLE MERGE DONE, merge all and close/dispose ---------------------");

            
            db.mergeAllSegments();
            dumpSegmentList(db);

            mr = db.generateMergeRatios();
            mr.DebugDump();

            db.debugDump();
            db.Dispose();

            System.Console.WriteLine("-------- NOW RESUME ---------------------------------");
            db = new LayerManager(InitMode.RESUME, "c:\\BENDtst\\main");
            dumpSegmentList(db);
            db.debugDump();

            System.Console.WriteLine("-------- NOW FINDNEXT ---------------------------------");
            dumpAllDbRecords(db);

            System.Console.WriteLine("-------- NOW MERGE ALL SEGMENTS ---------------------------------");
            dumpSegmentList(db);
            db.mergeAllSegments();
            db.debugDump();

            System.Console.WriteLine("-------- NOW FINDNEXT (after merge) ---------------------------------");
            dumpAllDbRecords(db);

            //System.Console.WriteLine("-------- Now run Readthreads Test ---------------------------------");
            //A03_LayerManagerTests test = new A03_LayerManagerTests();
            //test.T10_LayerManager_ReadThreads();



            System.Console.WriteLine("-------- Write ALOT of data ---------------------------------");

            String value = "";
            for (int x = 0; x < 1000; x++) { value = value + "TestValueDataABC"; }
            Random rnd = new Random();

            for (int x = 1000000; x < 1000000 + 10000; x++) {
                // db.setValueParsed("test/rnd/" + rnd.Next(), value);
                db.setValueParsed("test/ordered/" + x, value);


                if (x % 1000 == 0) {
                    db.flushWorkingSegment();
                    
                    win.debugDump(db);                    
                    // db.debugDump();

                    for (int mx = 0; mx < 4; mx++) {
                        System.Console.WriteLine("merge " + mx);
                        mr = db.generateMergeRatios();
                        merge_task = mr.generateMergeTask();
                        if (merge_task == null) {
                            System.Console.WriteLine("nothing more to merge");
                            break;
                        }
                        System.Console.WriteLine(merge_task.ToString());
                        db.mergeSegments(merge_task);
                        win.debugDump(db);
                        db.debugDump();
                    }


                    System.Console.WriteLine("windump");

                    dumpSegmentList(db);
                }
            }


            System.Console.WriteLine("-------- Merge a bunch more ------------------");

            for (int x = 0; x < 30; x++) {
                mr = db.generateMergeRatios();
                merge_task = mr.generateMergeTask();
                if (merge_task == null) {
                    break;
                }
                db.mergeSegments(merge_task);
                win.debugDump(db);
                
                
            }

            dumpSegmentList(db);


            System.Console.WriteLine("** done.");
        }
    }
}
