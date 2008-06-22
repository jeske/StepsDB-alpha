// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bend
{
    // ---------------[ Main ]---------------------------------------------------------


    class Program
    {
        static void Main(string[] args)
        {

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

            System.Console.WriteLine("-------- NOW MERGE ---------------------------------");
            db.mergeAllSegments();
            db.debugDump();

            Console.WriteLine("press any key...");
            Console.ReadKey();
        }
    }
}
