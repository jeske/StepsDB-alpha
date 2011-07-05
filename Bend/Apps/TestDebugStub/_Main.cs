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
            
            
            // var testclass = new A02_RangemapManagerTests();
            // testclass.T000_RangeKey_EncodedSort();


            // var testclass = new A01_RecordTests();
            // testclass.T02b_RecordKeyNesting();

            // var testclass = new A02_SegmentDescriptorTests();
            // testclass.T02_DescriptorOverlapTests();

            // var testclass = new A03_LayerManagerTests();
            // testclass.T001_WorkingSegmentReadWrite();
            // testclass.T002_ScanDirections();
            // testclass.T04_SingleSegmentRootMetadataLogRecovery();
            // testclass.T01_LayerTxnLogResume();

            // var testclass = new A01_RecordTests();
            // testclass.T00_RecordKeyEquality();
            // testclass.T11_RecordKey_ComposableIComparableTest();

            // var testclass = new A02_RecordKeyType_Field();
            // testclass.T02_RecordKeyTypes_RawBytes();


            // var testclass = new A04_StepsDatabase_StageSnapshot();
            // testclass.T000_TestBasic_SnapshotScanAll();

            // var testclass = new A02_SortedSegmentTests();
            // testclass.T01_SortedSegment_ScanTest();

            var testclass = new A03_LayerManagerTests();
            // testclass.T001_MultiWorkingSegmentReadWrite();
            testclass.T04_SingleSegmentRootMetadataLogRecovery();

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
