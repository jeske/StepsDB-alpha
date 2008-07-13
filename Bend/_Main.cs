// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Bend
{
    using BendTests;

    // ---------------[ Main ]---------------------------------------------------------


    // [TestFixture]
    public class MainTest
    {
        // [Test]
        public void T00_MainTest() {
            MainBend.do_bringup_test();
        }

    }


    class MainBend
    {

        static void Main(string[] args) {
            try {
                do_bringup_test();
            }
            catch (Exception exc) {
                System.Console.WriteLine("died to exception: " + exc.ToString());
                Console.WriteLine("press any key...");

            }
            Console.ReadKey();
        }
        

        public static void do_bringup_test() {
        
            LayerManager db = new LayerManager(InitMode.NEW_REGION,"c:\\test\\main");
         
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
            db.Dispose();

            System.Console.WriteLine("-------- NOW RESUME ---------------------------------");
            db = new LayerManager(InitMode.RESUME, "c:\\test\\main");
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


            A03_LayerManagerTests test = new A03_LayerManagerTests();
            test.T10_LayerManager_ReadThreads();


            Console.WriteLine("press any key...");            
            // Console.ReadKey();
        }
    }
}
