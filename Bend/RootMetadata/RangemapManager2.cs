// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;


namespace Bend
{

    // RANGEs are represented with an implicit prefix '='. This allows special endpoint markers:
    // "<" - the key before all keys
    // "=KEYDATA" - keys after and including "KEYDATA"
    // ">" - the key after all keys


    // ROOT/VARS/NUMGENERATIONS -> 1
    // ROOT/GEN/(gen #:3)/(start key)/(end key) -> (segment metadata)
    // ROOT/GEN/000/</> -> addr:length

    //
    // Algorithm example:
    //
    //
    // rangemap.scanForward(scanner(=apple,>apple)
    // - start in workingSegment(numgen=7) (with range <,>)
    //   - set prefix to empty
    //   - resutlsmerge = create a "record updates" merge (left is newer, right is older)
    //   - foreach datarow in new RecordUpdateMerge(curseg.scanForward(scanner),
    //             new RangeMapScan(curseg,prefix=root,scanner(=apple,>apple,gen=6)).scanForward()
    //      yield return datarow;
    //
    //
    //  class RangeMapScan {
    //    RangeMapScan(curseg,prefix,datascanner,child_generations)

    //    scanForward(scanner(=apple,>apple))
    //      if (child_generations > 0):
    //        - foreach rangemaprow in RecordUpdateMerge(curseg.
    //      foreach gen in desc(child_generations):
    //        curprefix = prefix + "ROOT/GEN/$gen"
    //        - foreach rangemaprow in curseg.scanForward(scanner(ROOT/GEN/$gen/=apple))

    //  rangemapScanForward(rangeseg,prefix,scanner(=apple,>apple,gen)
    //   for gen in generations:
    //     - curprefix = prefix + "ROOT/GEN/$gen"
    //     - for curseg.scanForward(curprefix/=apple)
    //   
    //       - open-segment, and add to resultsmerge  (we know there are results, because it ended in =apple)
    //   - for curseg.scanForward(prefix/prefix)
    //     - open-segment, and recurse to 
    //       - rangemapScanForward(newrangeseg,prefix+prefix,scanner(=apple,>apple),scanmerge)

    //
    // how do we work in the generations?

    // if there are two versions of the same record, we need to "apply" updates from left to right
    public class RecordDeltaMergeScan : IScannable<RecordKey,RecordData>{
        IScannable<RecordKey, RecordUpdate> newer, older;
        public RecordDeltaMergeScan(IScannable<RecordKey, RecordUpdate> newer,
            IScannable<RecordKey, RecordUpdate> older) {
            this.newer = newer;
            this.older = older;
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner) {
            throw new Exception("not impl");
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner) {
            throw new Exception("not impl");
        }
        public KeyValuePair<RecordKey, RecordData> FindNext(IComparable<RecordKey> keytest, bool equal_ok) {
            throw new Exception("not impl");
        }
        public KeyValuePair<RecordKey, RecordData> FindPrev(IComparable<RecordKey> keytest, bool equal_ok) {
            throw new Exception("not impl");
        }
        

    }

    public class ScanController
    {
        bool workingSegmentRangemapChanged;
        ScanController() {
            workingSegmentRangemapChanged = true;
        }

        IEnumerator<KeyValuePair<RecordKey,RecordData>> buildRecordMergePipe(IScanner<RecordKey> scanner) {
            // we need to prevent any lock any rangemap activity and find all Segments that are relevant for us
            // then if anything changes about the rangemap, the RangemapManager will be sure to let
            // us know so we can invalidate.
            // TODO: in a future fancy version of this we can simply update the merge-pipe instead of
            //    .. rebuilding it from scratch

            // prevent rangemap changes (safer if we don't cause any writes)



            // allow rangemap changes again


            throw new Exception("not impl");
        }

        IEnumerable<KeyValuePair<RecordKey,RecordData>> scanForward(IScanner<RecordKey> scanner) {
            IComparable<RecordKey> mylaststartkeytest = scanner.genLowestKeyTest();
            IComparable<RecordKey> mykeyendtest = scanner.genHighestKeyTest();
            
            IEnumerator<KeyValuePair<RecordKey, RecordData>> segment_scan_composite = null;

            bool has_more = false;
            do {
                if (this.workingSegmentRangemapChanged || segment_scan_composite == null) {
                    ScanRange<RecordKey> myscanner = new ScanRange<RecordKey>(mylaststartkeytest,mykeyendtest,null); // todo, fix matchtest
                    segment_scan_composite = buildRecordMergePipe(myscanner);
                    has_more = segment_scan_composite.MoveNext();
                }
                KeyValuePair<RecordKey,RecordData> next = segment_scan_composite.Current;

                if (mylaststartkeytest.CompareTo(next.Key) == 0) {
                    // we can't let two duplicate keys come across, because it will destroy our
                    // ability to resume if we have to rebuild the merge pipe
                    throw new Exception("ScanController.scanForward: saw two duplicate keys: " + 
                        mylaststartkeytest.ToString());
                }
                mylaststartkeytest = next.Key;  // remember where we made it to                
                yield return next;

                has_more = segment_scan_composite.MoveNext();
            } while (has_more);
        }
    }

    public class RangemapManager2
    {
        LayerManager store;
        int num_generations;
        private static int GEN_LSD_PAD = 3;

        public RangemapManager2(LayerManager store) {
        }
    }
}
