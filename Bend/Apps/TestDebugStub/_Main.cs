using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using BendTests;
using Bend;


namespace MyTestStub {
    class _Main {
        static void Main(string[] args) {

            // var testclass = new A03_LayerManagerTests();
            // testclass.T000_WorkingSegmentReadWrite();


            // var testclass = new A03_LayerManagerTests();
            // testclass.T002_ScanDirections();


            // var testclass = new A02_RangemapManagerTests();
            // testclass.T001_RangeKey_Bug();
            
            
            var testclass = new A02_RangemapManagerTests();
            testclass.T000_RangeKey_EncodedSort();

            // fetchHitsTest();


        }
        static void dumpAllRows() {
            int count = 0;
            LayerManager db = new LayerManager(InitMode.RESUME, @"c:\EmailTest\DB");
            foreach (var row in db.scanForward(null)) {
                count++;
                Console.WriteLine(row);
            }
            Console.WriteLine("{0} rows", count);

        }
        static void countAllSegments() {
            int count = 0;
            LayerManager db = new LayerManager(InitMode.RESUME, @"c:\EmailTest\DB");
            foreach (var seg in db.listAllSegments()) {
                count++;
                Console.WriteLine(seg);
            }
            Console.WriteLine("{0} segments", count);
        }
        static void fetchHitsTest() {

            

            int count = 0;
            LayerManager db = new LayerManager(InitMode.RESUME, @"c:\EmailTest\DB");

            Console.WriteLine("====================== FETCH HITS TEST =======================");
            Console.WriteLine("====================== FETCH HITS TEST =======================");
            Console.WriteLine("====================== FETCH HITS TEST =======================");
            Console.WriteLine("====================== FETCH HITS TEST =======================");
            Console.WriteLine("====================== FETCH HITS TEST =======================");
            Console.WriteLine("====================== FETCH HITS TEST =======================");
            Console.WriteLine("====================== FETCH HITS TEST =======================");

            var kprefix = new RecordKey().appendParsedKey(".zdata/index/jeske");

            var first_row = db.FindNext(kprefix, true);

            Console.WriteLine("First foudn key: {0}", first_row);

            return;

            foreach (var hit in db.scanForward(new ScanRange<RecordKey>(kprefix, RecordKey.AfterPrefix(kprefix), null))) {
                Console.WriteLine(hit);
                count++;
            }
            Console.WriteLine("scanned {0} hits", count);
            
        }
    }
}
