// Copyright (C) 2008-2014 David W. Jeske
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

// #define DEBUG_FINDNEXT

#define DEBUG_CHECKPOINT

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;
using System.Reflection;
using System.Threading;
using System.ComponentModel;

// TODO: eliminate the dual paths for "manual apply" and "log apply"... make it always just do log apply

namespace Bend
{

    public enum InitMode
    {
        NEW_REGION,    // initialize the region fresh, error if it looks like there is a region still there
        RESUME         // standard resume/recover from an existing region
    }

    // ---------------[ LayerManager ]---------------------------------------------------------


    public partial class LayerManager : IStepsKVDB, IDisposable
    {

        // private int SEGMENT_BLOCKSIZE = 4 * 1024 * 1024;  // 4 MB
        internal int SEGMENT_BLOCKSIZE = 512 * 1024; // 512k

        internal List<SegmentMemoryBuilder> segmentlayers;  // newest to oldest list of the in-memory segments
        internal SegmentMemoryBuilder workingSegment;
        public long checkpointNumber = 0;

        internal String dir_path;   // should change this to not assume directories/files
        public  IRegionManager regionmgr;
        internal BDSkipList<long,WeakReference<LayerWriteGroup>> pending_txns;
        public RangemapManager rangemapmgr;
        public FreespaceManager freespacemgr;

        private LayerMaintenanceThread maint_worker;
        
        
        internal bool needCheckpointNow = false;

        internal LogWriter logwriter;
        internal LayerLogReceiver receiver;

        internal FastUniqueIds tsnidgen = new FastUniqueIds();

        // constructors ....

        public LayerManager() {
            pending_txns = new BDSkipList<long,WeakReference<LayerWriteGroup>>();

            segmentlayers = new List<SegmentMemoryBuilder>();   // a list of segment layers, newest to oldest
            workingSegment = new SegmentMemoryBuilder();
            
            segmentlayers.Add(workingSegment);
            receiver = new LayerLogReceiver(this);
        }

        public LayerManager(InitMode mode, String dir_path)
            : this() {
            this.dir_path = dir_path;


            if (mode == InitMode.NEW_REGION) {
                // right now we only have one region type
                regionmgr = new RegionExposedFiles(InitMode.NEW_REGION, dir_path);

                {
                    // get our log online...
                    int system_reserved_space = 0;
                    logwriter = LogWriter.LogWriter_NewRegion(regionmgr, receiver, out system_reserved_space);

                    // TODO: init the freespace! 
                    FreespaceManager.Init(this, system_reserved_space);
                }


                // setup the initial numgenerations record
                RangemapManager.Init(this);

            } else if (mode == InitMode.RESUME) {
                regionmgr = new RegionExposedFiles(dir_path);                                
                logwriter = LogWriter.LogWriter_Resume(regionmgr, receiver);
            } else {
                throw new Exception("unknown init mode");
            }

            // once the rangemap is initialized we can actually READ data!!
            rangemapmgr = new RangemapManager(this);
            freespacemgr = new FreespaceManager(this);

            rangemapmgr.primeMergeManager();
        }


        public long workingSegmentSize() {
            return this.segmentlayers[0].approx_size;

        }

       

        public void startMaintThread() {
            lock (this) {
                if (maint_worker == null) {
                    maint_worker = LayerMaintenanceThread.startMaintThread(this);
                }
            }
        }
        
        // impl ....

        public LayerWriteGroup newWriteGroup(LayerWriteGroup.WriteGroupType type=LayerWriteGroup.DEFAULT_WG_TYPE) {
            LayerWriteGroup newtx = new LayerWriteGroup(this, type: type);                        
            return newtx;
        }
        

        private void _writeSegment(LayerWriteGroup tx, IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> records,int target_generation=-1) {
            // write the checkpoint segment and flush
            // TODO: make this happen in the background!!
            SegmentWriter segmentWriter = new SegmentWriter(records);


            while (segmentWriter.hasMoreData()) {
                DateTime start = DateTime.Now;
                // allocate new segment address from freespace
                NewUnusedSegment cur_segment = freespacemgr.allocateNewSegment(tx, SEGMENT_BLOCKSIZE);
                IRegion writer = cur_segment.getWritableRegion();
                Stream wstream = writer.getNewAccessStream();
                SegmentWriter.WriteInfo wi = segmentWriter.writeToStream(wstream);

                wstream.Flush();  // TODO: flush at the end of all segment writing, not for each one
                wstream.Close();

                double elapsed = (DateTime.Now - start).TotalSeconds;
                Console.WriteLine("segmentWritten with {0} keys in {1} seconds {2} keys/second",
                    wi.key_count, elapsed, (double)wi.key_count / elapsed);
                    

                // reopen the segment for reading
                IRegion reader = regionmgr.readRegionAddr(writer.getStartAddress());


                // record the index pointer (geneneration and rangekey -> block address)
                // rangemapmgr.newGeneration(tx, reader);   // add the checkpoint segment to the rangemap
                int use_gen = target_generation;
                if (target_generation == -1) {
                    use_gen = rangemapmgr.mergeManager.minSafeGenerationForKeyRange(wi.start_key, wi.end_key);
                }
                // rangemapmgr.mapGenerationToRegion(tx, use_gen, wi.start_key, wi.end_key, reader);
                cur_segment.mapSegment(tx, use_gen, wi.start_key, wi.end_key, reader);
            }                
            

        }

      

        Object flushLock = new Object();
        public void flushWorkingSegment() {
            if (workingSegment.RowCount == 0) {
                System.Console.WriteLine("flushWorkingSegment(): nothing to flush()");
                return;
            }

            

            lock (flushLock) {
                Console.WriteLine("=====================================[ Flush Working Segment (Begin) ]=================================");

                // (1) create a new working segment            
                SegmentMemoryBuilder newlayer = new SegmentMemoryBuilder();
                SegmentMemoryBuilder checkpointSegment;
                int checkpoint_segment_size;

#if DEBUG_CHECKPOINT
                System.Console.WriteLine("CHKPT: create new working segment");
#endif


                // (2) grab the current working segment and move it aside (now the checkpoint segment)

                lock (this.segmentlayers) {
                    LayerWriteGroup start_tx = new LayerWriteGroup(this, type: LayerWriteGroup.WriteGroupType.DISK_ATOMIC_FLUSH);
                    this.checkpointNumber++;

                    // get a handle to the current working segment                    
                    checkpointSegment = workingSegment;
                    checkpoint_segment_size = checkpointSegment.RowCount;

                    // mark the place in the log that contains the previous data, and allocate a new working segment
                    // this also sets aside the checkpointSegment

                    start_tx.checkpointStart();

                    // TODO: make sure it's not possible to lose a write in between here
                    //       maybe the log writer should be locked during this..

                    start_tx.finish();
                }


                {
                    LayerWriteGroup tx = new LayerWriteGroup(this, type: LayerWriteGroup.WriteGroupType.DISK_ATOMIC_FLUSH);
                    // (3) write the checkpoint segment to disk, accumulating the rangemap entries into tx
                    

#if DEBUG_CHECKPOINT
                    System.Console.WriteLine("CHKPT: write old working segment to disk segments");
#endif

                    // allocate a new generation number
                    uint new_generation_number = (uint)rangemapmgr.allocNewGeneration(tx);

                    // -----------------------------
                    // _writeSegment() does the heavy lifting, reading from the merge-sort chain and building
                    // the new output segments...

                    this._writeSegment(tx, SortedAscendingCheck.CheckAscending(checkpointSegment.sortedWalk(), "checkpoint segment"));

                    // ------------------------------

                    tx.checkpointDrop(); // drop the memory-checkpoint segment and let go of the log-space
                    
                    // (4) commit the new segment rangemap entries into the dataset, unlink the old memorybuilder copy of the checkpoint segment
                    lock (this.segmentlayers) {
#if DEBUG_CHECKPOINT
                        System.Console.WriteLine("CHKPT: commit new checkpoint");
#endif
                        // commit the new rangemap entries (and eventually the freespace modifications)
                        tx.finish();   
                        rangemapmgr.recordMaxGeneration(tx, rangemapmgr.mergeManager.getMaxGeneration() + 1); // this must come after the finish

                        if (checkpointSegment.RowCount != checkpoint_segment_size) {
                            System.Console.WriteLine("********* checkpointSegment was added to while checkpointing!! lost {0} rows",
                                checkpointSegment.RowCount - checkpoint_segment_size);
                        }
                        
                        // drop the old memory segment out of the segment layers now that's it's checkpointed
#if DEBUG_CHECKPOINT
                        System.Console.WriteLine("CHKPT: drop old working segment from layers");
#endif
                        segmentlayers.Remove(checkpointSegment);
                    }
                }
            }
            Console.WriteLine("=====================================[ Flush Working Segment (End) ]=================================");

        }

        //-------------------------------------------------------

        public void verifySegmentList() {
            
            // this is the slow method

            var walk = this.rangemapmgr.mergeManager.segmentInfo.GetEnumerator();

            bool discrepancy = false;

            foreach (var seg in this.listAllSegments()) {

                // Assert.AreEqual(true, walk.MoveNext(), "mergemanager missing record!");
                // Assert.AreEqual(0, walk.Current.Key.CompareTo(seg), "mergemanager and db.listAllSegments have different data!");
                if (walk.MoveNext()) {
                    if (walk.Current.Key.CompareTo(seg) != 0) {
                        discrepancy = true;
                        Console.WriteLine("  mismatch: db{0} mm{1}", seg, walk.Current.Key);
                    }
                } else { discrepancy = true; }

                System.Console.WriteLine("db gen{0} start({1}) end({2})", seg.generation, seg.start_key, seg.end_key);
            }

            if (discrepancy) {
                foreach (var seginfo in this.rangemapmgr.mergeManager.segmentInfo) {
                    var seg = seginfo.Key;
                    System.Console.WriteLine("mm gen{0} start({1}) end({2})", seg.generation, seg.start_key, seg.end_key);
                }

                System.Console.WriteLine("======================= verifySegmentList: Discrepancy");
                this.debugDump();
                
                throw new Exception("mergemanager and db.listAllSegments have different data!");
                // TODO: it would be nice if we could stop all other threads right now...
                
            }
           
        }



        public bool mergeIfNeeded() {            
            lock (this) {                
                var mc = this.rangemapmgr.mergeManager.getBestCandidate();
                if (mc == null) { return false; }
                System.Console.WriteLine("** LayerManager.mergeIfNeeded() --- start");
                if (mc.score() > (1.6 + (float)this.rangemapmgr.mergeManager.getMaxGeneration() / 12.0f)) {
                    System.Console.WriteLine("** best merge score too high: " + mc);
                    return false;
                }
                System.Console.WriteLine("doMerge " + mc);

                this.performMerge(mc);
                this.checkpointNumber++;
                System.Console.WriteLine("** LayerManager.mergeIfNeeded() --- end");
                return true;
            }
        }


        public void performMerge(MergeCandidate mc) {
            if (mc == null) { return; }

            Console.WriteLine("performMerge({0})", mc.ToString());

            // verify the merge is valid and doesn't have any suplicate segments!
            {
                var segs_to_merge_hash = new HashSet<SegmentDescriptor>();
                foreach (var seg in mc.source_segs) {
                    if (segs_to_merge_hash.Contains(seg)) {
                        throw new Exception("duplicate segment in performMerge!");
                    }
                    segs_to_merge_hash.Add(seg);
                }
                foreach (var seg in mc.target_segs) {
                    if (segs_to_merge_hash.Contains(seg)) {
                        throw new Exception("duplicate segment in performMerge!");
                    }
                    segs_to_merge_hash.Add(seg);
                }
            }


            var segs_to_merge = new List<SegmentDescriptor>();
            segs_to_merge.AddRange(mc.source_segs);
            segs_to_merge.AddRange(mc.target_segs);
            segs_to_merge.Reverse();
            this.mergeSegments(segs_to_merge);
            this.verifySegmentList();
        }

        public List<SegmentDescriptor> listSegmentsForGen(int gen) {
            List<SegmentDescriptor> segs = new List<SegmentDescriptor>();

            RecordKey start_key = new RecordKey()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(new RecordKeyType_Long(gen));
                               
            RecordKey cur_key = start_key;
            RecordKey found_key = new RecordKey();
            RecordData found_record = new RecordData(RecordDataState.NOT_PROVIDED, found_key);
            while (rangemapmgr.getNextRecord(cur_key, true, ref found_key, ref found_record, false) == GetStatus.PRESENT) {
                cur_key = found_key;
                // check that the first two keyparts match
                if (found_key.isSubkeyOf(start_key)) {
                    if (found_record.State == RecordDataState.DELETED) {
                        continue; // ignore the tombstone
                    } else if (found_record.State != RecordDataState.FULL) {
                        throw new Exception("can't handle incomplete segment record");
                    } else {
                        segs.Add(rangemapmgr.getSegmentDescriptorFromRecordKey(found_key));                        
                    }
                } else {
                    // we're done matching the generation records
                    break;
                }
            }
            return segs;
        }

        public IEnumerable<SegmentDescriptor> listAllSegments() {                        
            RecordKey start_key = new RecordKey().appendParsedKey(".ROOT/GEN");
            var end_key = RecordKey.AfterPrefix(start_key);
            int seg_count = 0;
            // int gen_count = rangemapmgr.genCount();            

            DateTime start = DateTime.Now;
            foreach (var kvp in rangemapmgr.getRecord_LowLevel_Cursor(start_key, end_key, true, true, false)) {
                yield return rangemapmgr.getSegmentDescriptorFromRecordKey(kvp.Key);
                seg_count++;
            }
            DateTime end = DateTime.Now;

            Console.WriteLine("listAllSegments: {0} segments listed in {1} seconds",
                seg_count, (end - start).TotalSeconds);
        }

        public static IEnumerable<KeyValuePair<K, V>> RemoveTombstones<K, V>(
            IEnumerable<KeyValuePair<K, V>> input)             
            where K : RecordKey
            where V : RecordUpdate {           

            IEnumerator<KeyValuePair<K, V>> oneenum = input.GetEnumerator();            
            
            bool one_hasmore = oneenum.MoveNext();            
            
            while (one_hasmore) {
                KeyValuePair<K, V> curval = oneenum.Current;
                // output anything that is not a deletion tombstone
                if (curval.Value.type != RecordUpdateTypes.DELETION_TOMBSTONE) {
                    yield return curval; 
                }
                one_hasmore = oneenum.MoveNext();
            }

        }

        public void mergeSegments(IEnumerable<SegmentDescriptor> segs) {

            Console.WriteLine("=====================================[ Merge Segments (Begin) ]=================================");

            // TODO: assure this is a valid merge
            // TODO: change this to map/unmap segments as an atomic operation at the end
            // TODO: assure we don't have the same segment listed twice
            //
            // We write our output in the "minimum" generation number of the merge.
            // This is only valid if our keyranges meet certain constraints. 
            //
            // For example, the merge gen3(a,b) + gen2(c,d) + gen1(e,f) -> gen1(a,f) would be invalid if 
            // there was also a gen2(a,b), because the gen3(a,b) records would "trump" them by becoming gen 1. 
            //
            // For the merge to be valid, the older-generation segments must be supersets of
            // the newer generations involved in the merge. Currently this only occurs because our code 
            // that generates the merge candidates uses a tree to propose merges. 
            //
            // TODO: build validation code that assures this invariant is never violated.
            int count = 0;

            uint target_generation = int.MaxValue; // will contain "minimum generation of the segments"
            int last_generation = int.MinValue;



            // (1) iterate through the generation pointers, building the merge chain
            IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> chain = null;
            {
                foreach (SegmentDescriptor segment in segs) {
                    if (segment.generation < last_generation) {
                        throw new Exception("segment merge generation order invalid: " + String.Join(",", segs));
                    }
                    last_generation = (int)segment.generation;

                    count++;
                    target_generation = Math.Min(target_generation, segment.generation);
                    var seg = segment.getSegment(rangemapmgr);

                    var nextchain = SortedAscendingCheck.CheckAscending(seg.sortedWalk(),
                        String.Format("merge-input: {0}", segment));

                    if (chain == null) {
                        chain = nextchain;
                    } else {
                        chain = SortedMergeExtension.MergeSort(nextchain, chain, true);  // merge sort keeps keys on the left
                    }
                }

                if (count == 1) {
                    System.Console.WriteLine("only one segment, nothing to merge");
                    return;
                }

                // add check-ascending to be sure keys appear in order
                chain = SortedAscendingCheck.CheckAscending(chain, "merge-final");

                // add "remove tombstones" stage if we are at the bottom
                if (target_generation == 0) {
                    chain = LayerManager.RemoveTombstones(chain);
                }
            }

            // (2) now perform the merge!
            {
                LayerWriteGroup tx = new LayerWriteGroup(this, type: LayerWriteGroup.WriteGroupType.DISK_ATOMIC_FLUSH);

                // (2a) delete the old segment mappings

                // HACK: we delete the segment mappings first, so if we write the same mapping that we're removing, 
                // we don't inadvertantly delete the new mapping..

                foreach (SegmentDescriptor segment in segs) {
                    // remove the old segment mappings
                    rangemapmgr.unmapSegment(tx, segment);

                    // ... and free the space from the old segments
                    FreespaceExtent segment_extent = segment.getFreespaceExtent(rangemapmgr);
                    this.freespacemgr.freeSegment(tx, segment_extent);
                }


                // (2b) actually perform the merge, writing out the new segments..
                //      _writSegment is responsible for adding the new segment mappings

                this._writeSegment(tx, chain, (int)target_generation);


                // (2c) finish/commit the transaction                
                lock (this.segmentlayers) {
                    tx.finish();                             // commit the freespace and rangemap transaction
                    this.checkpointNumber++;
                }

                // (2d) cleanup

                // check to see if we can shrink NUMGENERATIONS
                rangemapmgr.setMaxGenCountHack(rangemapmgr.mergeManager.getMaxGeneration() + 1);
                // rangemapmgr.clearSegmentCacheHack();                
            }
            Console.WriteLine("=====================================[ Merge Segments (End) ]=================================");
        }

        public void mergeAllSegments() {
            lock (this) {
                var allsegs = new List<SegmentDescriptor>();
                allsegs.AddRange(listAllSegments());
                allsegs.Sort((a, b) => a.generation.CompareTo(b.generation));

                mergeSegments(allsegs);                
            }
        }


        public List<SegmentDescriptor> _findRelevantSegs(int gen, RecordKey start_key, RecordKey end_key,int max_segs_before_abort) {
            List<SegmentDescriptor> sub_segment_keys = new List<SegmentDescriptor>();
            int current_seg_count = 0;

            RecordKey gen_key = new RecordKey()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(new RecordKeyType_Long(gen));                                
            RecordKey search_key = new RecordKey()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(new RecordKeyType_Long(gen))
                .appendKeyPart(new RecordKeyType_RecordKey(start_key));


            RecordKey cur_key = search_key;
            RecordKey found_key = new RecordKey();
            RecordData found_record = new RecordData(RecordDataState.NOT_PROVIDED, found_key);
            while (rangemapmgr.getNextRecord(cur_key, true, ref found_key, ref found_record, false) == GetStatus.PRESENT) {
                cur_key = found_key;

                // ignore deletion tombstones
                if (found_record.State == RecordDataState.DELETED) {
                    continue;
                }

                // check to see we found a segment pointer
                if (!found_key.isSubkeyOf(gen_key)) {
                    break;
                }
                SegmentDescriptor subsegment = rangemapmgr.getSegmentDescriptorFromRecordKey(found_key);

                // check to see that the segment we found overlaps with the segment in question
                //  i.e. the start key falls between start and end

                // if the subsegment start is greater than the end, or end is less than the start, it's not overlapping, otherwise it is
                if ((subsegment.start_key.CompareTo(end_key) > 0) || (subsegment.end_key.CompareTo(start_key) < 0)) {
                    // it's not, so we're done with this recurse
                    break;
                } else {
                    // yes! it's inside the parent segment
                    sub_segment_keys.Add(subsegment);
                    current_seg_count++;
                    if (current_seg_count > max_segs_before_abort) {
                        return null;
                    }
                }
            }
            return sub_segment_keys;


        }




        // ---------------------------------------------------------------------------------------------


        public GetStatus getNextRecord(RecordKey lowkey, ref RecordKey found_key, ref RecordData found_record) {
            return (rangemapmgr.getNextRecord(lowkey, false, ref found_key, ref found_record,false));
        }
           
        public GetStatus getRecord(RecordKey key, out RecordData record) {
            try {
                KeyValuePair<RecordKey, RecordData> val = this.FindNext(key, equal_ok: true);
                if (val.Key.CompareTo(key) == 0) {
                    record = val.Value;
                    return GetStatus.PRESENT;
                }
            } catch (KeyNotFoundException) {

            }
            record = null;
            return GetStatus.MISSING;
        }

        public KeyValuePair<RecordKey, RecordData> FindNext(IComparable<RecordKey> keytest, bool equal_ok) {
            RecordKey found_key = new RecordKey();
            RecordData record = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());

#if DEBUG_FINDNEXT
            Console.WriteLine("FindNext({0})", keytest);
#endif


            foreach (var rec in this._scan(new ScanRange<RecordKey>(keytest, new ScanRange<RecordKey>.maxKey(), null),
                    direction_is_forward:true, equal_ok:equal_ok)) {
#if DEBUG_FINDNEXT
                Console.WriteLine("FindNext returning: {0} -> {1}", found_key, record);
#endif
                return rec;
            }

            throw new KeyNotFoundException(String.Format("LayerManager.FindNext({0},{1}) found no key", keytest, equal_ok));

        }

        public KeyValuePair<RecordKey, RecordData> FindPrev(IComparable<RecordKey> keytest, bool equal_ok) {
            RecordKey found_key = new RecordKey();
            RecordData record = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());

            foreach (var rec in this._scan(new ScanRange<RecordKey>(new ScanRange<RecordKey>.minKey(), keytest, null),
                    direction_is_forward: false, equal_ok: equal_ok)) {
                        return rec;
            }

            throw new KeyNotFoundException(String.Format("LayerManager.FindNext({0},{1}) found no key", keytest, equal_ok));

        }        

        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner) {
            return _scan(scanner,true, true);
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner) {
            return _scan(scanner, false, true);
        }


        // scan using cursor setup..
        private IEnumerable<KeyValuePair<RecordKey, RecordData>> _scan(IScanner<RecordKey> scanner, 
            bool direction_is_forward, bool equal_ok) {

            IComparable<RecordKey> lowestKeyTest = null;
            IComparable<RecordKey> highestKeyTest = null;
            if (scanner != null) {
                lowestKeyTest = scanner.genLowestKeyTest();
                highestKeyTest = scanner.genHighestKeyTest();
            } else {
                lowestKeyTest = new ScanRange<RecordKey>.minKey();
                highestKeyTest = new ScanRange<RecordKey>.maxKey();
            }

            // TODO: introduce a data-prefix to prevent callers from seeing the ".ROOT" data.

            return rangemapmgr.getRecord_LowLevel_Cursor(
                lowestKeyTest,
                highestKeyTest,
                direction_is_forward : direction_is_forward,
                equal_ok: equal_ok,
                tombstone_ok:false);

        }


        // this scans using the old cursor-free interface
        private IEnumerable<KeyValuePair<RecordKey, RecordData>> scan_nocursor(IScanner<RecordKey> scanner, bool direction_is_forward) {
            IComparable<RecordKey> lowestKeyTest = null;
            IComparable<RecordKey> highestKeyTest = null;
            if (scanner != null) {
                lowestKeyTest = scanner.genLowestKeyTest();
                highestKeyTest = scanner.genHighestKeyTest();
            } else {
                lowestKeyTest = new ScanRange<RecordKey>.minKey();
                highestKeyTest = new ScanRange<RecordKey>.maxKey();
            }

            KeyValuePair<RecordKey, RecordData> cursor;            
            RecordKey found_key = new RecordKey();
            RecordData found_record = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());

            IComparable<RecordKey> cursor_key;
            if (direction_is_forward) {
                cursor_key = lowestKeyTest;
            } else {
                cursor_key = highestKeyTest;
            }

            // get the first key
            if (rangemapmgr.getNextRecord(cursor_key, direction_is_forward, ref found_key, ref found_record, true, false) == GetStatus.MISSING) {
                yield break; // no keys
            }
            while (true) {                
                // System.Console.WriteLine("highkeytest = " + highestKeyTest + " found_key " + found_key + " result: " + highestKeyTest.CompareTo(found_key));
                
                if (
                    (direction_is_forward && (highestKeyTest.CompareTo(found_key) >= 0)) ||   // forward scan end test
                    (!direction_is_forward && (lowestKeyTest.CompareTo(found_key) <= 0))      // backward scan end test
                   ) {
                    if ((scanner == null) || scanner.MatchTo(found_key)) {
                        cursor = new KeyValuePair<RecordKey, RecordData>(found_key, found_record);
                        yield return cursor;
                    }
                } else {
                    yield break;
                }

                cursor_key = found_key;
                found_key = new RecordKey();
                found_record = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());

                if (rangemapmgr.getNextRecord(cursor_key, direction_is_forward,ref found_key, ref found_record, false, false) == GetStatus.MISSING) {
                    yield break; // no keys
                }
            }

        }


        public void setValueParsed(String skey, String svalue)
        {
            LayerWriteGroup implicit_txn = this.newWriteGroup();
            implicit_txn.setValueParsed(skey, svalue);
            implicit_txn.finish();            
        }

        public void setValue(RecordKey key, RecordUpdate value) {
            LayerWriteGroup implicit_txn = this.newWriteGroup();
            implicit_txn.setValue(key, value);
            implicit_txn.finish();            
        }

        public void Dispose() {

            System.Console.WriteLine("*\n*\n*\n* Layermanger: Dispose \n*\n*\n*\n****************");

            // TODO: fix the race with flipping pending freelist entries to freelist after db.Dispose

            GC.Collect();
            DateTime start = DateTime.Now;

            while (pending_txns.Count() > 0 && ((DateTime.Now - start).TotalSeconds < 3)) {
                System.Console.WriteLine("waiting for pending transactions to close {0}", pending_txns.Count());
                Thread.Sleep(1000);
                GC.Collect();

                foreach (var pending_txn in pending_txns) {
                    System.Console.WriteLine("{0} : {1}", pending_txn.Key, pending_txn.Value);
                }
            }
            foreach (var pending_txn in pending_txns) {
                LayerWriteGroup wg = pending_txn.Value.Target;
                if (wg != null) {
                    wg.cancel();
                }
            }

            if (logwriter != null) {
                logwriter.Dispose(); logwriter = null;
            }
            if (maint_worker != null) {
                maint_worker.end(); maint_worker = null;
            }

            foreach (ISortedSegment segment in segmentlayers) {
                segment.Dispose();
            }
        }
    }

}