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
            win.debugDump(db);
            // calculate merge ratios
            LayerManager.MergeRatios mr = db.generateMergeRatios();
            mr.DebugDump();

            // use the merge ratios to calculate a single merge
            LayerManager.MergeTask merge_task = mr.generateMergeTask();
            System.Console.WriteLine(merge_task.ToString());

            System.Console.WriteLine("-------- PERFORMING A SINGLE MERGE ---------------------");
            // do the merge
            db.mergeSegments(merge_task); 
            db.flushWorkingSegment();
            db.debugDump();
            win.debugDump(db);
            System.Console.WriteLine("-------- SINGLE MERGE DONE, merge all and close/dispose ---------------------");
            db.mergeAllSegments();
            db.debugDump();
            db.Dispose();

            System.Console.WriteLine("-------- NOW RESUME ---------------------------------");
            db = new LayerManager(InitMode.RESUME, "c:\\BENDtst\\main");
            db.debugDump();

            System.Console.WriteLine("-------- NOW FINDNEXT ---------------------------------");
            {
                RecordKey next_key = new RecordKey();
                RecordKey fkey = null;
                RecordData fdata = null;
                while (db.getNextRecord(next_key, ref fkey, ref fdata) == GetStatus.PRESENT) {
                    next_key = fkey;

                    System.Console.WriteLine("  found: {0} -> {1}", fkey.ToString(), fdata.ToString());

                }
            }

            System.Console.WriteLine("-------- NOW MERGE ---------------------------------");
            db.mergeAllSegments();
            mr = db.generateMergeRatios();
            mr.DebugDump();
            db.debugDump();

            System.Console.WriteLine("-------- NOW FINDNEXT (after merge) ---------------------------------");
            {
                RecordKey next_key = new RecordKey();
                RecordKey fkey = null;
                RecordData fdata = null;
                while (db.getNextRecord(next_key, ref fkey, ref fdata) == GetStatus.PRESENT) {
                    next_key = fkey;

                    System.Console.WriteLine("  found: {0} -> {1}", fkey.ToString(), fdata.ToString());
                }
            }

            //System.Console.WriteLine("-------- Now run Readthreads Test ---------------------------------");
            //A03_LayerManagerTests test = new A03_LayerManagerTests();
            //test.T10_LayerManager_ReadThreads();



            System.Console.WriteLine("-------- Write ALOT of data ---------------------------------");

            String value = "";
            for (int x = 0; x < 10000; x++) { value = value + "dataablaskdjalskdja"; }

            for (int x = 0; x < 100000; x++) {
                db.setValueParsed("test/rnd/" + x, value);

                if (x % 1000 == 0) {
                    win.debugDump(db);
                    db.flushWorkingSegment();
                    merge_task = mr.generateMergeTask();
                    System.Console.WriteLine(merge_task.ToString());
                    db.mergeSegments(merge_task); 

                    win.debugDump(db);
                    System.Console.WriteLine("windump");

                }
            }

            System.Console.WriteLine("** done.");
        }
    }
}
