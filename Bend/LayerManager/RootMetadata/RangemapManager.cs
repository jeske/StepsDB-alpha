﻿// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


// #define DEBUG_SEGMENT_WALK            // full segment walking debug
// #define DEBUG_SEGMENT_WALK_COUNTERS    // prints a set of debug counters at the end of each row fetch
// #define DEBUG_SEGMENT_ACCUMULATION   
// #define DEBUG_SEGMENT_RANGE_WALK

#define DEBUG_USE_NEW_FINDALL


using System;
using System.Collections.Generic;

using System.Diagnostics;

using System.Reflection; // for SegmentWalkStats reflective printing



namespace Bend
{

    // RANGEs are represented with an implicit prefix '='. This allows special endpoint markers:
    // "<" - the key before all keys
    // "=KEYDATA" - keys after and including "KEYDATA"
    // ">" - the key after all keys


    // .ROOT/VARS/NUMGENERATIONS -> 1
    // .ROOT/GEN/(gen #:3)/(start key)/(end key) -> (segment metadata)
    // .ROOT/GEN/000/</> -> addr:length

    public class RangemapManager
    {
        LayerManager store;
        public MergeManager_Incremental mergeManager;

        int num_generations;
        public static int GEN_LSD_PAD = 3;

        public static String altdebug_pad = "                                          --";

        // TODO: FIXME: this is a hacky cache... the segmentreaders sitting inside
        //   use a single FileStream. If you have multiple threads calling them, 
        //   chaos will ensue because of the shared seek pointer. 
        Dictionary<RecordKey, SegmentReader> disk_segment_cache; 

        public RangemapManager(LayerManager store) {
            this.store = store;            
            disk_segment_cache = new Dictionary<RecordKey, SegmentReader>();

            // get the current number of generations
            RecordUpdate update;
            if (store.workingSegment.getRecordUpdate(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                out update) == GetStatus.MISSING) {
                throw new Exception("RangemapManager can't init without NUMGENERATIONS");
            }
            num_generations = (int)Lsd.lsdToNumber(update.data);


            // init the merge manager
            mergeManager = new MergeManager_Incremental(this);            
        }

        public void primeMergeManager() {         
            // because we depend on the merge manager for all kinds of things now, we really
            // MUST do this, or new segments will be allocated in the wrong generations! 

            Console.WriteLine("primeMergeManager(): start");
            int seg_count = 0;
            foreach (var segdesc in store.listAllSegments()) {
                // TODO: make sure these are in increasing generation order! 
                mergeManager.notify_addSegment(segdesc);
                seg_count++;
            }            
            Console.WriteLine("primeMergeManager(): finished. Loaded {0} segments.", seg_count);
        }

        public static void Init(LayerManager store) {
            // setup "zero" initial generations
            store.setValue(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                RecordUpdate.WithPayload(0.ToString())); // TODO: this should be a var-enc number
        }


        internal SegmentReader segmentReaderFromRow(KeyValuePair<RecordKey, RecordUpdate> segmentrow) {
            return segmentReaderFromRow(segmentrow.Key, segmentrow.Value);
        }

        internal SegmentReader segmentReaderFromRow(RecordKey key, RecordData data) {
            return __segmentReaderFromData(key, data.data);
        }
        internal SegmentReader segmentReaderFromRow(RecordKey key, RecordUpdate update) {
            return __segmentReaderFromData(key, update.data);
        }


        internal SegmentReader __segmentReaderFromData(RecordKey key, byte[] data) {
            lock (disk_segment_cache) {
                try {
                    return disk_segment_cache[key];
                } catch (KeyNotFoundException) {
                    SegmentReader next_seg = getSegmentFromMetadataBytes(data);
                    disk_segment_cache[key] = next_seg;
                    return next_seg;
                }
            }
        }
        





        public void mapGenerationToRegion(LayerManager.WriteGroup tx, int gen_number, RecordKey start_key, RecordKey end_key, IRegion region) {

            // TODO: consider putting the address or a GUID into the key so two descriptors can't be mixed up
            SegmentDescriptor sdesc = new SegmentDescriptor((uint)gen_number, start_key, end_key);
            RecordKey key = sdesc.record_key;

            System.Console.WriteLine("mapGenerationToRegion: {0} -> {1}",sdesc,region);

            // TODO: pack the metdata record <addr>:<size>
            // String segmetadata = String.Format("{0}:{1}", region.getStartAddress(), region.getSize());            
            String seg_metadata = "" + region.getStartAddress();
            tx.setValue(key, RecordUpdate.WithPayload(seg_metadata));
            
            // TODO: make this occur only when the txn commits!
            mergeManager.notify_addSegment(sdesc);
        }

        public void clearSegmentCacheHack() {            
            lock (disk_segment_cache) {
                disk_segment_cache = new Dictionary<RecordKey, SegmentReader>();
                GC.Collect();
            }
            System.Console.WriteLine("*** clearSegmentCacheHack() ***");
            // TODO: see unmapGeneration(). This should go away when a transaction apply which
            //  .. touches a rangemap row automagically causes an invalidation of the segment cache            
        }

        public void unmapSegment(LayerManager.WriteGroup tx, SegmentDescriptor segment) {
            this.unmapSegment(tx, segment.record_key, null);
        }

        public void unmapSegment(LayerManager.WriteGroup tx, RecordKey key, RecordData data) {            

            // TODO: how do we assure that existing read operations flush and reload all segments?          
            lock (disk_segment_cache) {
                // clear the entry from the cache
                // TODO: fix this so it works when we fix setValue...
                //   ... technically this only works right now because setValue applies immediately.
                //   ... if it actually applied when the TX commits like it's supposed to, there would
                //   ... be a race condition here
                try {
                    disk_segment_cache.Remove(key);
                }
                catch (KeyNotFoundException) {
                    // not being in ths cache is okay
                }
            }
            tx.setValue(key, RecordUpdate.DeletionTombstone());

            // TODO: assure this happens only if the txn commits
            SegmentDescriptor sdesc = getSegmentDescriptorFromRecordKey(key);
            mergeManager.notify_removeSegment(sdesc);

            System.Console.WriteLine("unmapSegment: " + sdesc);

            // we can't really do this because the file is still open
            // store.regionmgr.disposeRegionAddr(unpackRegionAddr(data.data)); 
                       
        }
       
        public void setGenerationCountToZeroHack() {                       
            int highest_valid_gen = 0;
            if (highest_valid_gen + 1 < num_generations) {
                num_generations = highest_valid_gen + 1;
                store.setValue(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                    RecordUpdate.WithPayload(num_generations.ToString()));
            }
        }
       
        public int allocNewGeneration(LayerManager.WriteGroup tx) {
            // allocate a new generation number
            int newgen = num_generations;
            num_generations++;
    
            // TODO: write the new generation count, and the rangemap entry for the generation

            tx.setValue(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                RecordUpdate.WithPayload(num_generations.ToString()));

            return newgen;
        }

        public void recordMaxGeneration(LayerManager.WriteGroup tx,int num_generations) {
            tx.setValue(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                RecordUpdate.WithPayload(num_generations.ToString()));

        }

        private long unpackRegionAddr(byte[] data) {
            // TODO:unpack the update data when we change it to "<addr>:<length>"
            return Lsd.lsdToNumber(data);
        }

        private SegmentReader getSegmentFromMetadataBytes(byte[] data) {
            // we now have a pointer to a segment addres for GEN<max>
            long region_addr = unpackRegionAddr(data);

            if (region_addr == 0) {                
                throw new Exception("segment bytes unpacked to zero! (" + BitConverter.ToString(data) + ")");
            }
            System.Console.WriteLine(altdebug_pad + "open SegmentReader {0}", region_addr);
            IRegion region = store.regionmgr.readRegionAddrNonExcl(region_addr);
            SegmentReader sr = new SegmentReader(region);
            return sr;

        }


        public SegmentDescriptor getSegmentDescriptorFromRecordKey(RecordKey key) {
            return new SegmentDescriptor(key);
        }

        public ISortedSegment getSegmentFromMetadata(RecordData data) {
            
            return getSegmentFromMetadataBytes(data.data);
        }

        public ISortedSegment getSegmentFromMetadata(RecordUpdate update) {
            return getSegmentFromMetadataBytes(update.data);
        }

        public int genCount() {
            return num_generations;
        }

        // ------------[ public segmentWalkForKey ] --------------

        

        public GetStatus getNextRecord(
            IComparable<RecordKey> lowkey, 
            ref RecordKey key, 
            ref RecordData record,
            bool equal_ok) {
                return getNextRecord_LowLevel_Cursor(
                    lowkey, 
                    true, 
                    ref key, 
                    ref record, 
                    equal_ok:equal_ok, 
                    tombstone_ok:false);
        }

        
        public class SegmentWalkStats {
            public int segmentWalkInvocations;
            public int segmentDeletionTombstonesSkipped;
            public int segmentDeletionTombstonesAccumulated;
            public int segmentUpdatesApplied;
            public int segmentRangeRowScansPerformed;
            public int segmentRangeRow_FindCalls;
            public int segmentRangeRowsConsidered;               
            public int segmentIndirectRangeRowsConsidered;
            public int segmentAccumulate_TryGet;            
            public int rowUpdatesApplied;
            public int rowDuplicatesAppeared;
            public int rowAccumulate_TryGet;
            public int rowDeletionTombstonesSkipped;

            public override String ToString() {
                var string_lines = new List<string>();

                Type otype = this.GetType();
                // MemberInfo[] members = otype.GetMembers();
                FieldInfo[] fields = otype.GetFields();
                foreach (FieldInfo f in fields) {
                    string_lines.Add(String.Format("{1,10} = {0}", f.Name, f.GetValue(this).ToString()));
                }
                return String.Join("\n", string_lines);
            }
        }

        public GetStatus getNextRecord_LowLevel(
            IComparable<RecordKey> lowkey, 
            bool direction_is_forward, 
            ref RecordKey key, 
            ref RecordData record, 
            bool equal_ok, 
            bool tombstone_ok) {

            SegmentWalkStats stats = new SegmentWalkStats();

            BDSkipList<RecordKey, RecordData> handledIndexRecords = new BDSkipList<RecordKey,RecordData>();
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
                                num_generations);
                
                INTERNAL_segmentWalkForNextKey(
                    lowkey,
                    direction_is_forward,
                    layer,
                    segrk,
                    handledIndexRecords,
                    num_generations,
                    recordsBeingAssembled,
                    equal_ok,
                    stats:stats);
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
                            kvp.Value.State  + " k:" + kvp.Key + "   v:" + kvp.Value);
                    }

                }
                return GetStatus.MISSING;
            }
            catch (KeyNotFoundException) {
                return GetStatus.MISSING;
            }

        }

        public GetStatus getNextRecord_LowLevel_Cursor(
            IComparable<RecordKey> lowkey,
            bool direction_is_forward,
            ref RecordKey key,
            ref RecordData record,
            bool equal_ok,
            bool tombstone_ok) {


                IComparable<RecordKey> cur_key = lowkey;
                Console.WriteLine("getNextRecord_LowLevel_Cursor(lowkey={0})", lowkey);                    

                while (true) {

                    SegmentWalkStats stats = new SegmentWalkStats();

                    var handledIndexRecords = new BDSkipList<RecordKey, RecordData>();
                    var recordSegments = new BDSkipList<RangeKey, IScannable<RecordKey, RecordUpdate>>();

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
                                        num_generations);

                        INTERNAL_segmentWalkCursorSetupForNextKey(
                            cur_key,
                            direction_is_forward,
                            layer,
                            segrk,
                            handledIndexRecords,
                            num_generations,
                            recordSegments,
                            equal_ok,
                            stats: stats);
                    }
                    DateTime end = DateTime.Now;

#if DEBUG_SEGMENT_WALK_COUNTERS
                Console.WriteLine("segmentWalkCursorSetup({0}) took {1}ms", lowkey, (((end - start).TotalMilliseconds)));
                Console.WriteLine(stats);

#endif

                    // at this point we should have the "next segment that could contain the target" for
                    // every generation. We only NEED to advance the ones whose rangekeys are lower than the 
                    // "current" lowkey. However, we're going to start by doing the simpler loop of just 
                    // getting the next record from each, and merging. If any of them run out of keys, we have to
                    // throw an exception and re-trigger a fetch of the next segment for that generation. 


                    // now scan the returned segments for records...
                    IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> chain = null;
                    foreach (var curseg_kvp in recordSegments.scanForward(null)) {
                        Console.WriteLine("setup: " + curseg_kvp);
                        IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> seg_scanner;
                        if (direction_is_forward) {
                            seg_scanner = curseg_kvp.Value.scanForward(
                                new ScanRange<RecordKey>(
                                    lowkey,
                                    new ScanRange<RecordKey>.maxKey(),
                                    null));
                        } else {
                            seg_scanner = curseg_kvp.Value.scanBackward(
                                new ScanRange<RecordKey>(
                                    new ScanRange<RecordKey>.minKey(),
                                    lowkey,
                                    null));
                        }

                        // add the exhausted check
                        seg_scanner = SortedExhaustedCheck.CheckExhausted(seg_scanner, "exhausted: " + curseg_kvp.Key);

                        if (chain == null) {
                            chain = seg_scanner;
                        } else {
                            chain = SortedMergeExtension.MergeSort(seg_scanner, chain,
                            dropRightDuplicates: true, direction_is_forward: direction_is_forward);  // merge sort keeps keys on the left
                        }
                    }

                    try {
                        foreach (var out_rec in chain) {
                            cur_key = out_rec.Key;
                            if (out_rec.Value.type == RecordUpdateTypes.FULL) {
                                if (equal_ok || (lowkey.CompareTo(out_rec.Key)) != 0) {
                                    record = new RecordData(RecordDataState.NOT_PROVIDED, out_rec.Key);
                                    record.applyUpdate(out_rec.Value);
                                    key = out_rec.Key;
                                    return GetStatus.PRESENT;
                                }
                            }
                            equal_ok = false;
                        }
                    } catch (SortedSetExhaustedException e) {
                        Console.WriteLine("One ran out: " + e.ToString());
                        // we need to prime the next key properly
                        Console.WriteLine("lowkey: {0}  cur_key:{1}", lowkey, cur_key);
                        throw new Exception(e.ToString());
                        continue;
                    }
                    return GetStatus.MISSING;
                }
        }



        // [DebuggerDisplay("RangeKey( {generation}:{lowkey} -> {highkey} )")]
        public class RangeKey : IComparable<RangeKey>
        {
            public RecordKey lowkey = null;
            public RecordKey highkey = null;
            public int generation;

            private RangeKey() {
            }

            public static RangeKey newSegmentRangeKey(RecordKey lowkey,RecordKey highkey, int generation) {
                RangeKey rk = new RangeKey();
                rk.lowkey = lowkey;
                rk.highkey = highkey;
                rk.generation = generation;
                return rk;

                if (rk.lowkey.CompareTo(rk.highkey) > 0) {
                    throw new Exception(
                        String.Format("RangeKey.newSegmentRangeKey()  inverted endpoints ({0} -> {1})",
                            rk.lowkey, rk.highkey));
                }

            }


            public override String ToString() {
                return String.Format("RangeKey( {0}:{1} -> {2} )", generation, this.lowkey, this.highkey);
            }


            private static void verifyPart(string expected,RecordKeyType part) {
                RecordKeyType_String conv_part = (RecordKeyType_String)part;
                string value = conv_part.GetString();
                if (!expected.Equals(value)) {
                    throw new Exception(String.Format("verify failed on RangeKey decode ({0} != {1})", expected, value));
                }
            }
            public static RangeKey decodeFromRecordKey(RecordKey existingkey) {
                System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

                RangeKey rangekey = new RangeKey();
                // TODO, switch this to use a key PIPE!!
                verifyPart(".ROOT", existingkey.key_parts[0]);
                verifyPart("GEN", existingkey.key_parts[1]);

                rangekey.generation = (int)((RecordKeyType_Long)existingkey.key_parts[2]).GetLong();
                rangekey.lowkey = ((RecordKeyType_RecordKey)existingkey.key_parts[3]).GetRecordKey();
                rangekey.highkey = ((RecordKeyType_RecordKey)existingkey.key_parts[4]).GetRecordKey();

                if (rangekey.lowkey.CompareTo(rangekey.highkey) > 0) {
                    throw new Exception(
                        String.Format("RangeKey.decodeFromRecordKey() decoded inverted endpoints ({0} -> {1})",
                            rangekey.lowkey, rangekey.highkey));
                }

                return rangekey;
            }
            public RecordKey toRecordKey() {
                if (lowkey == null || highkey == null) {
                    throw new Exception("no low/high keys for RangeKey.toRecordKey()");
                }
                RecordKey key = new RecordKey();
                key.appendParsedKey(".ROOT/GEN");
                key.appendKeyPart(new RecordKeyType_Long(generation));
                key.appendKeyPart(new RecordKeyType_RecordKey(lowkey));
                key.appendKeyPart(new RecordKeyType_RecordKey(highkey));
                return key;
            }

            public static bool isRangeKey(RecordKey key) {
                // .ROOT/GEN/X/lk/hk  == 5 parts
                if (key == null) {
                    throw new Exception("isRangeKey() handed a null key");
                }
                if (key.key_parts == null) {
                    throw new Exception("isRangeKey() handed a key with null key_parts");
                }
                if ( (key.key_parts.Count == 5)  &&
                     (key.key_parts[0].Equals(new RecordKeyType_String(".ROOT"))) &&
                     (key.key_parts[1].Equals(new RecordKeyType_String("GEN")))) {
                    return true;
                } else {
                    return false;
                }
            }

            public int CompareTo(RangeKey target) {
                int cmpres = this.generation.CompareTo(target.generation);
                if (cmpres != 0) { return cmpres; }
                cmpres = this.lowkey.CompareTo(target.lowkey);
                if (cmpres != 0) { return cmpres; }
                cmpres = this.highkey.CompareTo(target.highkey);
                return cmpres;

            }

            public bool directlyContainsKey(IComparable<RecordKey> testkey) {
                // return true; 
                if ((testkey.CompareTo(this.lowkey) >= 0) &&
                    (testkey.CompareTo(this.highkey) <= 0)) {
                    return true;
                } else {
                    return false;
                }
            }

            private static bool _eventuallyPastRangeKey(RecordKey top, IComparable<RecordKey> testkey) {
                bool lowrk = RangeKey.isRangeKey(top);
                if (lowrk) {
                    return _eventuallyPastRangeKey(RangeKey.decodeFromRecordKey(top).lowkey, testkey);
                } else {
                    if (testkey.CompareTo(top) >= 0) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }
            private static bool _eventuallyBeforeRangeKey(RecordKey bottom, IComparable<RecordKey> testkey) {
                bool highrk = RangeKey.isRangeKey(bottom);
                if (highrk) {
                    return _eventuallyBeforeRangeKey(RangeKey.decodeFromRecordKey(bottom).highkey, testkey);
                } else {
                    if (testkey.CompareTo(bottom) <= 0) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            private static bool _eventuallyBetweenRangeKeys(RecordKey top, RecordKey bottom, 
                    IComparable<RecordKey> testkey) {
                bool lowrk = RangeKey.isRangeKey(top);
                bool highrk = RangeKey.isRangeKey(bottom);

                // see if the key is between the two keys
                if ((testkey.CompareTo(top) >= 0) &&
                    (testkey.CompareTo(bottom) <= 0)) {
                    return true;
                }
                // see if all range pointers are between them
                RecordKey all_gen_keys = new RecordKey().appendParsedKey(".ROOT/GEN");
                if ((all_gen_keys.CompareTo(top) >= 0) &&
                    (all_gen_keys.CompareTo(bottom) <= 0)) {
                    return true;
                }

                if (lowrk && highrk) {
                    // unpack both and recurse
                    return _eventuallyBetweenRangeKeys(
                        RangeKey.decodeFromRecordKey(top).lowkey,
                        RangeKey.decodeFromRecordKey(bottom).highkey, testkey);
                } 
                if (lowrk && !highrk) {                    
                    return _eventuallyPastRangeKey(RangeKey.decodeFromRecordKey(top).lowkey, testkey);
                } else if (!lowrk && highrk) {
                    return _eventuallyBeforeRangeKey(RangeKey.decodeFromRecordKey(bottom).highkey, testkey);
                } else {
                    return false;
                }        
            }


            public static HashSet<string> unique_discards = new HashSet<string>();
            public bool eventuallyContainsKey(IComparable<RecordKey> testkey) {
                if (this.lowkey.CompareTo(this.highkey) >= 0) {
                    String.Format("eventuallyContainsKey called with inverted endpoints ({0} -> {1})",
                            this.lowkey, this.highkey);
                }
                
                // old converative hack... if we are ever thinking our walking logic is back, it's
                // generally okay (but very slow) just to return true...
                // return true;

                // TODO: fix the datavalues to all use "=" prefix encoding so our <> range tests work
                // todo, recursively unpack the low-key/high-key until we no longer have a .ROOT/GEN formatted key
                // then find out if the supplied testkey is present in the final range
                bool contained = _eventuallyBetweenRangeKeys(this.lowkey,this.highkey,testkey);

                /*
                if (contained == false) {
                    var miss = String.Format("{0} not in {1} -> {2}", testkey, this.lowkey, this.highkey);
                    if (!unique_discards.Contains(miss)) {
                        unique_discards.Add(miss);
                        Console.WriteLine(miss);
                    }
                }
                 */
                 

                return contained;

            }

            public static IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> checkForSegmentKeyAboveAndBelow(
                IScannable<RecordKey, RecordUpdate> in_segment,
                IComparable<RecordKey> for_rangekey,
                IComparable<RecordKey> for_key,
                int for_generation,
                SegmentWalkStats stats) {

                // backward
                {
                    stats.segmentRangeRow_FindCalls++;
                    var seg_scanner = in_segment.scanBackward(
                       new ScanRange<RecordKey>(
                           new ScanRange<RecordKey>.minKey(),
                           for_rangekey,
                           null));
                    foreach (var kvp in seg_scanner) {
                        if (!RangeKey.isRangeKey(kvp.Key)) { break; }
                        RangeKey test_rk = RangeKey.decodeFromRecordKey(kvp.Key);
                        if (test_rk.generation != for_generation) { break; }
                        if (test_rk.eventuallyContainsKey(for_key)) {
                            yield return kvp;
                        }                        
                        if (kvp.Value.type != RecordUpdateTypes.DELETION_TOMBSTONE) {
                            break;
                        } else {
                            stats.segmentDeletionTombstonesAccumulated++;
                        }
                    }
                }
                   // forward
                {
                    stats.segmentRangeRow_FindCalls++;
                    var seg_scanner = in_segment.scanForward(
                        new ScanRange<RecordKey>(
                            for_rangekey, 
                            new ScanRange<RecordKey>.maxKey(),
                            null));
                    foreach (var kvp in seg_scanner) {
                        if (!RangeKey.isRangeKey(kvp.Key)) { break; }
                        RangeKey test_rk = RangeKey.decodeFromRecordKey(kvp.Key);
                        if (test_rk.generation != for_generation) { break; }
                        if (test_rk.eventuallyContainsKey(for_key)) {
                            yield return kvp;
                        }
                        if (kvp.Value.type != RecordUpdateTypes.DELETION_TOMBSTONE) {
                            break;
                        } else {
                            stats.segmentDeletionTombstonesAccumulated++;
                        }
                    }
                }
            }
                
#if DEBUG_USE_NEW_FINDALL
            public static IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> findAllEligibleRangeRows(
                IScannable<RecordKey, RecordUpdate> in_segment,
                IComparable<RecordKey> for_key,
                int for_generation,
                SegmentWalkStats stats) {


                // TODO: fix this to me more efficient.... we would like to:
                // (1) look through anything that could be a direct range-row of "for_key"                        
                {
                    RecordKeyComparator startrk = new RecordKeyComparator()
                        .appendParsedKey(".ROOT/GEN")
                        .appendKeyPart(new RecordKeyType_Long(for_generation))
                        .appendKeyPart(for_key);

                    foreach (var kvp in RangeKey.checkForSegmentKeyAboveAndBelow(in_segment, 
                            startrk, for_key, for_generation, stats)) {
                        stats.segmentRangeRowsConsidered++;
                        if (RangeKey.isRangeKey(kvp.Key)) {
                            RangeKey test_rk = RangeKey.decodeFromRecordKey(kvp.Key);
                            if (test_rk.generation == for_generation && test_rk.eventuallyContainsKey(for_key)) {
                                yield return kvp;
                            }
                        }
                    }
                }


                // (2) look through any "row range of row range" keys
                //   ... HOWEVER, #1 is hard to do when for_key is a comparable, not a key
                //   ... either need to switch it to an actual key, or make a composable
                //       IComparable<RecordKey> so we can shove the .ROOT/GEN prefix before the
                //       supplied IComparable
                {
                    RecordKey startrk = new RecordKey()
                        .appendParsedKey(".ROOT/GEN")
                        .appendKeyPart(new RecordKeyType_Long(for_generation))
                        .appendKeyPart(new RecordKey().appendParsedKey(".ROOT/GEN"));

                    foreach (var kvp in RangeKey.checkForSegmentKeyAboveAndBelow(in_segment, 
                            startrk, for_key, for_generation, stats)) {
                        stats.segmentRangeRowsConsidered++;
                        if (RangeKey.isRangeKey(kvp.Key)) {
                            RangeKey test_rk = RangeKey.decodeFromRecordKey(kvp.Key);
                            if (test_rk.generation == for_generation && test_rk.eventuallyContainsKey(for_key)) {
                                yield return kvp;
                            }
                        }
                    }

                }
                
            }

#else
            public static IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> findAllEligibleRangeRows(
               IScannable<RecordKey, RecordUpdate> in_segment,
               IComparable<RecordKey> for_key,
               int for_generation,
               SegmentWalkStats stats) {

                // TODO: fix this to me more efficient.... we would like to:
                // (1) look through anything that could be a direct range-row of "for_key"            
                // (2) look through any "row range of row range" keys
                //   ... HOWEVER, #1 is hard to do when for_key is a comparable, not a key
                //   ... either need to switch it to an actual key, or make a composable
                //       IComparable<RecordKey> so we can shove the .ROOT/GEN prefix before the
                //       supplied IComparable

                RecordKey startrk = new RecordKey()
                    .appendParsedKey(".ROOT/GEN")
                    .appendKeyPart(new RecordKeyType_Long(for_generation));
                IComparable<RecordKey> endrk = RecordKey.AfterPrefix(startrk);

                foreach (KeyValuePair<RecordKey, RecordUpdate> kvp
                    in in_segment.scanForward(new ScanRange<RecordKey>(startrk, endrk, null))) {

                    stats.segmentRangeRowsConsidered++;
                    if (!RangeKey.isRangeKey(kvp.Key)) {
                        System.Console.WriteLine("INTERNAL error, RangeKey scan found non-range key: "
                            + kvp.Key.ToString() + " claimed to be before " + endrk.ToString());
                        break;
                    }
                    RangeKey test_rk = RangeKey.decodeFromRecordKey(kvp.Key);
                    if (test_rk.eventuallyContainsKey(for_key)) {
                        yield return kvp;
                    }

                }

            }
#endif


        }

        
            
                
        

        private static RecordKey GEN_KEY_PREFIX = new RecordKey().appendParsedKey(".ROOT/GEN");

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
                foreach(var kvp in seg_scanner) {

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


        // ---------------------------------------------------------

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




    }

}