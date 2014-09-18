

// #define INCLUDE_OLD_CODE




using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;





namespace Bend {

    public partial class RangemapManager {

#if INCLUDE_OLD_CODE



        
        

        private void INTERNAL_segmentWalkCursorSetupForNextKey(
            IComparable<RecordKey> startkeytest,
            bool direction_is_forward,
            ISortedSegment curseg_raw,
            RangeKey curseg_rangekey,
            IScannableDictionary<RecordKey, RecordData> handledIndexRecords,
            int maxgen,
            IScannableDictionary<RangeKey, IScannable<RecordKey, RecordUpdate>> segmentsWithRecords,
            bool equal_ok,
            SegmentWalkStats stats) {

            // TODO: convert all ISortedSegments to be IScannable
            IScannable<RecordKey, RecordUpdate> curseg = (IScannable<RecordKey, RecordUpdate>)curseg_raw;

            stats.segmentWalkInvocations++;


            // first look in this segment for a next-key **IF** it may contain one
            if (curseg_rangekey.directlyContainsKey(startkeytest)) {
                // add the current segment to the list of segments with records. 
                segmentsWithRecords.Add(curseg_rangekey,curseg);                
            }

            // find all generation range references that are relevant for this key
            // .. make a note of which ones are "current"             
            if (curseg_rangekey.directlyContainsKey(GEN_KEY_PREFIX)) {
                BDSkipList<RecordKey, RecordUpdate> todo_list = new BDSkipList<RecordKey, RecordUpdate>();

                if (curseg_rangekey.generation > stats.handlingGeneration) {
                    throw new Exception("cursor segup generation priority inversion");
                }
                stats.handlingGeneration = curseg_rangekey.generation;


                for (int i = maxgen - 1; i >= 0; i--) {
                    stats.segmentRangeRowScansPerformed++;
                    foreach (KeyValuePair<RecordKey, RecordUpdate> rangerow in RangeKey.findAllEligibleRangeRows(curseg, startkeytest, i, stats)) {
                        // see if it is new for our handledIndexRecords dataset
                        RecordData partial_rangedata;
                        stats.segmentAccumulate_TryGet++;
                        if (!handledIndexRecords.TryGetValue(rangerow.Key, out partial_rangedata)) {
                            partial_rangedata = new RecordData(RecordDataState.NOT_PROVIDED, rangerow.Key);
                            handledIndexRecords[rangerow.Key] = partial_rangedata;
                        }
                        if ((partial_rangedata.State == RecordDataState.INCOMPLETE) ||
                            (partial_rangedata.State == RecordDataState.NOT_PROVIDED)) {
                            // we're suppilying new data for this index record
                            partial_rangedata.applyUpdate(rangerow.Value);
                            stats.segmentUpdatesApplied++;
                            // because we're suppilying new data, we should add this to our
                            // private TODO list if it is a FULL update, NOT a tombstone
                            if (rangerow.Value.type == RecordUpdateTypes.FULL) {
#if DEBUG_SEGMENT_RANGE_WALK
                                for (int depth = 10; depth > maxgen; depth--) { Console.Write("  "); }
                                Console.WriteLine("adding SegmentRangeRow: {0}", rangerow);
#endif

                                todo_list.Add(rangerow);
                            }
                        }
                    }
                }


                // now repeat the walk through our todo list:
                foreach (KeyValuePair<RecordKey, RecordUpdate> rangepointer in todo_list.scanBackward(null)) {
                    if (rangepointer.Value.type == RecordUpdateTypes.DELETION_TOMBSTONE) {
                        // skip deletion tombstones
                        stats.segmentDeletionTombstonesSkipped++;
                        continue;
                    }
                    SegmentReader next_seg = segmentReaderFromRow(rangepointer);

                    RangeKey next_seg_rangekey = RangeKey.decodeFromRecordKey(rangepointer.Key);
#if DEBUG_SEGMENT_WALK
                    for (int depth = 10; depth > maxgen; depth--) { Console.Write("  "); }
                    Console.WriteLine("..WalkForNextKey descending to: {0}", rangepointer);
#endif
                    // RECURSE
                    INTERNAL_segmentWalkCursorSetupForNextKey(
                        startkeytest,
                        direction_is_forward,
                        next_seg,
                        next_seg_rangekey,
                        handledIndexRecords,
                        maxgen - 1,
                        segmentsWithRecords,
                        equal_ok,
                        stats);

                }
                // now repeat the walk of range references in this segment, this time actually descending
            }
        }


        public GetStatus getNextRecord_LowLevel_OLD(
            IComparable<RecordKey> lowkey,
            bool direction_is_forward,
            ref RecordKey key,
            ref RecordData record,
            bool equal_ok,
            bool tombstone_ok) {

            SegmentWalkStats stats = new SegmentWalkStats();

            BDSkipList<RecordKey, RecordData> handledIndexRecords = new BDSkipList<RecordKey, RecordData>();
            BDSkipList<RecordKey, RecordData> recordsBeingAssembled = new BDSkipList<RecordKey, RecordData>();

#if DEBUG_SEGMENT_WALK
            Console.WriteLine("getNextRecord_LowLevel({0})", lowkey);
#endif


            SegmentMemoryBuilder[] layers;
            // snapshot the working segment layers
            lock (this.store.segmentlayers) {
                layers = this.store.segmentlayers.ToArray();
            }

            DateTime start = DateTime.Now;
            // TODO: fix this super-hack to do "minKey/maxKey"
            foreach (SegmentMemoryBuilder layer in layers) {
                if (layer.RowCount == 0) {
                    continue;
                }
                // use the first and last records in the segment as the rangekeys
                var segrk = RangeKey.newSegmentRangeKey(
                                layer.FindNext(null, true).Key,
                                layer.FindPrev(null, true).Key,
                                num_generations_persisted);

                INTERNAL_segmentWalkForNextKey(
                    lowkey,
                    direction_is_forward,
                    layer,
                    segrk,
                    handledIndexRecords,
                    num_generations_persisted,
                    recordsBeingAssembled,
                    equal_ok,
                    stats: stats);
            }
            DateTime end = DateTime.Now;

#if DEBUG_SEGMENT_WALK_COUNTERS
            Console.WriteLine("getNextRecord({0}) took {1}ms", lowkey, (((end-start).TotalMilliseconds)));
            Console.WriteLine(stats);
            
#endif

            // now check the assembled records list
            try {
                IEnumerable<KeyValuePair<RecordKey, RecordData>> assembled_candidates;
                if (direction_is_forward) {
                    assembled_candidates = recordsBeingAssembled.scanForward(null);
                } else {
                    assembled_candidates = recordsBeingAssembled.scanBackward(null);
                }

                foreach (var kvp in assembled_candidates) {

                    if (kvp.Value.State == RecordDataState.FULL) {
                        key = kvp.Key;
                        record = kvp.Value;
                        return GetStatus.PRESENT;
                    } else if (kvp.Value.State == RecordDataState.DELETED) {
                        if (tombstone_ok) {
                            key = kvp.Key;
                            record = kvp.Value;
                            return GetStatus.PRESENT;

                        }
                    } else {
                        throw new Exception("invalid record state in getNextRecord, record assembly processing: " +
                            kvp.Value.State + " k:" + kvp.Key + "   v:" + kvp.Value);
                    }

                }
                return GetStatus.MISSING;
            } catch (KeyNotFoundException) {
                return GetStatus.MISSING;
            }

        }



        private void INTERNAL_segmentWalkForNextKey(
            IComparable<RecordKey> startkeytest,
            bool direction_is_forward,
            ISortedSegment curseg_raw,
            RangeKey curseg_rangekey,
            IScannableDictionary<RecordKey, RecordData> handledIndexRecords,
            int maxgen,
            IScannableDictionary<RecordKey, RecordData> recordsBeingAssembled,
            bool equal_ok,
            SegmentWalkStats stats) {

            // TODO: convert all ISortedSegments to be IScannable
            IScannable<RecordKey, RecordUpdate> curseg = (IScannable<RecordKey, RecordUpdate>)curseg_raw;

            stats.segmentWalkInvocations++;


            // first look in this segment for a next-key **IF** it may contain one
            if (curseg_rangekey.directlyContainsKey(startkeytest)) {
                // we need to keep looking until we find a live record, as we need all the deletion tombstones
                // between startkey and the next live record.
                IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> seg_scanner;
                if (direction_is_forward) {
                    seg_scanner = curseg.scanForward(
                        new ScanRange<RecordKey>(
                            startkeytest,
                            new ScanRange<RecordKey>.maxKey(),
                            null));
                } else {
                    seg_scanner = curseg.scanBackward(
                        new ScanRange<RecordKey>(
                            new ScanRange<RecordKey>.minKey(),
                            startkeytest,
                            null));
                }
                foreach (var kvp in seg_scanner) {

                    if (!equal_ok) { // have ">" test vs ">="
                        if (startkeytest.CompareTo(kvp.Key) == 0) {
                            continue;
                        }
                    }

                    RecordData partial_record;
                    stats.rowAccumulate_TryGet++;
                    if (!recordsBeingAssembled.TryGetValue(kvp.Key, out partial_record)) {
                        partial_record = new RecordData(RecordDataState.NOT_PROVIDED, kvp.Key);
                        recordsBeingAssembled[kvp.Key] = partial_record;
                    } else {
                        stats.rowDuplicatesAppeared++;
                    }
                    partial_record.applyUpdate(kvp.Value);
                    stats.rowUpdatesApplied++;

#if DEBUG_SEGMENT_ACCUMULATION
                    for (int depth = 10; depth > maxgen; depth--) { Console.Write("  "); }
                    Console.WriteLine("accumulated update: {0}", kvp);
#endif


                    if (partial_record.State != RecordDataState.DELETED) {
                        // we accumulated to at least one live record, so stop adding potential records
                        break;
                    }
                }
            }


            // find all generation range references that are relevant for this key
            // .. make a note of which ones are "current"             
            if (curseg_rangekey.directlyContainsKey(GEN_KEY_PREFIX)) {
                BDSkipList<RecordKey, RecordUpdate> todo_list = new BDSkipList<RecordKey, RecordUpdate>();


                for (int i = maxgen - 1; i >= 0; i--) {
                    stats.segmentRangeRowScansPerformed++;
                    foreach (KeyValuePair<RecordKey, RecordUpdate> rangerow in RangeKey.findAllEligibleRangeRows(curseg, startkeytest, i, stats)) {
                        // see if it is new for our handledIndexRecords dataset
                        RecordData partial_rangedata;
                        stats.segmentAccumulate_TryGet++;
                        if (!handledIndexRecords.TryGetValue(rangerow.Key, out partial_rangedata)) {
                            partial_rangedata = new RecordData(RecordDataState.NOT_PROVIDED, rangerow.Key);
                            handledIndexRecords[rangerow.Key] = partial_rangedata;
                        }
                        if ((partial_rangedata.State == RecordDataState.INCOMPLETE) ||
                            (partial_rangedata.State == RecordDataState.NOT_PROVIDED)) {
                            // we're suppilying new data for this index record
                            partial_rangedata.applyUpdate(rangerow.Value);
                            stats.segmentUpdatesApplied++;
                            // because we're suppilying new data, we should add this to our
                            // private TODO list if it is a FULL update, NOT a tombstone
                            if (rangerow.Value.type == RecordUpdateTypes.FULL) {
#if DEBUG_SEGMENT_RANGE_WALK
                                for (int depth = 10; depth > maxgen; depth--) { Console.Write("  "); }
                                Console.WriteLine("adding SegmentRangeRow: {0}", rangerow);
#endif

                                todo_list.Add(rangerow);
                            }
                        }
                    }
                }


                // now repeat the walk through our todo list:
                foreach (KeyValuePair<RecordKey, RecordUpdate> rangepointer in todo_list.scanBackward(null)) {
                    if (rangepointer.Value.type == RecordUpdateTypes.DELETION_TOMBSTONE) {
                        // skip deletion tombstones
                        stats.segmentDeletionTombstonesSkipped++;
                        continue;
                    }
                    SegmentReader next_seg = segmentReaderFromRow(rangepointer);

                    RangeKey next_seg_rangekey = RangeKey.decodeFromRecordKey(rangepointer.Key);
#if DEBUG_SEGMENT_WALK
                    for (int depth = 10; depth > maxgen; depth--) { Console.Write("  "); }
                    Console.WriteLine("..WalkForNextKey descending to: {0}", rangepointer);
#endif
                    // RECURSE
                    INTERNAL_segmentWalkForNextKey(
                        startkeytest,
                        direction_is_forward,
                        next_seg,
                        next_seg_rangekey,
                        handledIndexRecords,
                        maxgen - 1,
                        recordsBeingAssembled,
                        equal_ok,
                        stats);

                }
                // now repeat the walk of range references in this segment, this time actually descending
            }
        }


#endif

    } // class RangemapManager

} // namespace
