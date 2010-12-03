// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;
using System.Reflection;

// TODO: eliminate the dual paths for "manual apply" and "log apply"... make it always just do log apply

namespace Bend
{

    public enum InitMode
    {
        NEW_REGION,    // initialize the region fresh, error if it looks like there is a region still there
        RESUME         // standard resume/recover from an existing region
    }

    // ---------------[ LayerManager ]---------------------------------------------------------

    public class LayerManager : IDisposable 
    {

        // private int SEGMENT_BLOCKSIZE = 4 * 1024 * 1024;  // 4 MB
        private int SEGMENT_BLOCKSIZE = 512 * 1025;

        internal List<SegmentMemoryBuilder> segmentlayers;  // newest to oldest list of the in-memory segments
        internal SegmentMemoryBuilder workingSegment;

        internal String dir_path;   // should change this to not assume directories/files
        public  IRegionManager regionmgr;
        internal List<WeakReference<WriteGroup>> pending_txns;
        public RangemapManager rangemapmgr;
        public FreespaceManager freespacemgr;        

        LogWriter logwriter;
        Receiver receiver;

        // constructors ....

        public LayerManager() {
            pending_txns = new List<WeakReference<WriteGroup>>();

            segmentlayers = new List<SegmentMemoryBuilder>();   // a list of segment layers, newest to oldest
            workingSegment = new SegmentMemoryBuilder();
            
            segmentlayers.Add(workingSegment);
            
        }

        public LayerManager(InitMode mode, String dir_path)
            : this() {
            this.dir_path = dir_path;


            if (mode == InitMode.NEW_REGION) {
                // right now we only have one region type
                regionmgr = new RegionExposedFiles(InitMode.NEW_REGION, dir_path);

                // get our log online...
                logwriter = new LogWriter(InitMode.NEW_REGION, regionmgr);

                // setup the initial numgenerations record
                RangemapManager.Init(this);
                // TODO: do something sane with initial freespace setup
            } else if (mode == InitMode.RESUME) {
                regionmgr = new RegionExposedFiles(dir_path);
                receiver = new Receiver(this);
                logwriter = new LogWriter(InitMode.RESUME, regionmgr, receiver);
            } else {
                throw new Exception("unknown init mode");
            }

            // once the rangemap is initialized we can actually READ data!!
            rangemapmgr = new RangemapManager(this);
            freespacemgr = new FreespaceManager(this);

            rangemapmgr.primeMergeManager();
        }

        // inner classes ...
        public struct Receiver : ILogReceiver
        {
            LayerManager mylayer;
            SegmentMemoryBuilder checkpointSegment;
            public Receiver(LayerManager mylayer) {
                this.mylayer = mylayer;
                checkpointSegment = null;
            }
            public void handleCommand(byte cmd, byte[] cmddata) {
                if (cmd == (byte)LogCommands.UPDATE) {
                    // decode basic block key/value writes
                    BlockAccessor ba = new BlockAccessor(cmddata);
                    ISegmentBlockDecoder decoder = new SegmentBlockBasicDecoder(ba);
                    foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in decoder.sortedWalk()) {
                        // populate our working segment
                        lock (mylayer.segmentlayers) {
                            mylayer.workingSegment.setRecord(kvp.Key, kvp.Value);
                        }
                    }
                } else if (cmd == (byte)LogCommands.CHECKPOINT) {
                    // TODO: we need some kind of key/checksum to be sure that we CHECKPOINT and DROP the right data
                    checkpointSegment = mylayer.workingSegment;
                    SegmentMemoryBuilder newsegment = new SegmentMemoryBuilder();
                    lock (mylayer.segmentlayers) {
                        mylayer.workingSegment = newsegment;
                        mylayer.segmentlayers.Insert(0, mylayer.workingSegment);
                    }
                } else if (cmd == (byte)LogCommands.CHECKPOINT_DROP) {
                    // TODO: we need some kind of key/checksum to be sure that we CHECKPOINT and DROP the right data
                    if (checkpointSegment != null) {
                        lock (mylayer.segmentlayers) {
                            mylayer.segmentlayers.Remove(checkpointSegment);
                        }
                    } else {
                        throw new Exception("can't drop, no segment to drop");
                    }
                } else {
                    throw new Exception("unimplemented command");
                }
            }
        }

        enum LogCommands
        {
            UPDATE = 0,
            CHECKPOINT = 1,
            CHECKPOINT_DROP = 2
        }

        public class WriteGroup : IDisposable
        {
            LayerManager mylayer;
            long tsn; // transaction sequence number
            long last_logwaitnumber = 0;
            public bool add_to_log = true;
            enum WriteGroupState
            {
                PENDING,
                PREPARED,
                CLOSED,                
            }
            WriteGroupState state = WriteGroupState.PENDING;
            public WriteGroup(LayerManager _layer) {
                this.mylayer = _layer;
                this.tsn = System.DateTime.Now.ToBinary();
                // TODO: store the stack backtrace of who created this if we're in debug mode
            }

            public void setValue(RecordKey key, RecordUpdate update) {
                // build a byte[] for the updates using the basic block encoder
                if (add_to_log)  {
                    MemoryStream writer = new MemoryStream();
                    // TODO: this seems like a really inefficient way to write out a key
                    ISegmentBlockEncoder encoder = new SegmentBlockBasicEncoder();
                    encoder.setStream(writer);
                    encoder.add(key, update);
                    encoder.flush();
                    mylayer.logwriter.addCommand((byte)LogCommands.UPDATE, writer.ToArray(), 
                        ref this.last_logwaitnumber);
                }
                // TODO: switch our writes to always occur through "handling the log"
                // TODO: make our writes only visible to US?, by creating a "transaction segment"
                lock (mylayer.segmentlayers) {
                    mylayer.workingSegment.setRecord(key, update); // add to working set
                }
            }

            public void setValueParsed(String skey, String svalue) {
                RecordKey key = new RecordKey();
                key.appendParsedKey(skey);
                RecordUpdate update = RecordUpdate.WithPayload(svalue);

                this.setValue(key, update);
            }
            public void addCommand(byte cmd, byte[] cmddata) {
                mylayer.logwriter.addCommand(cmd, cmddata, ref this.last_logwaitnumber);
            }

            public void finish() {
                // TODO: make a higher level TX commit that finalizes pending writes into final writes
                //     and cleans up locks and state

                // TODO: this is a flush not a commmit. When other
                // writers are concurrent, some of their stuff is also written to the
                // log when we flush. Therefore, as soon as you write, your write is
                // "likely to occur" whether you commit or not. We need to layer 
                // an MVCC on top of this

                if (this.state == WriteGroupState.CLOSED) {
                    throw new Exception("flush called on closed WriteGroup"); // TODO: add LSN/info
                }
                if (add_to_log) {
                    // we've been logging
                    if (this.last_logwaitnumber != 0) {
                        mylayer.logwriter.flushPendingCommandsThrough(last_logwaitnumber);
                    }
                } else {
                    // we turned off logging, so the only way to commit is to checkpoint!
                    // TODO: force checkpoint 
                }

                state = WriteGroupState.CLOSED;
            }
           
            public void Dispose() {
                if (state == WriteGroupState.PENDING) {
                    throw new Exception("disposed Txn still pending " + this.tsn);
                }
            }
        }

        // impl ....

        public WriteGroup newWriteGroup() {
            WriteGroup newtx = new WriteGroup(this);
            pending_txns.Add(new WeakReference<WriteGroup>(newtx));  // make sure we don't prevent collection
            return newtx;
        }
        

        private void _writeSegment(WriteGroup tx, IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> records,int target_generation=-1) {
            // write the checkpoint segment and flush
            // TODO: make this happen in the background!!
            SegmentWriter segmentWriter = new SegmentWriter(records);


            while (segmentWriter.hasMoreData()) {
                DateTime start = DateTime.Now;
                // allocate new segment address from freespace
                IRegion writer = freespacemgr.allocateNewSegment(tx, SEGMENT_BLOCKSIZE);
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
                rangemapmgr.mapGenerationToRegion(tx, use_gen, wi.start_key, wi.end_key, reader);
            }                
            

        }

        public void flushWorkingSegment() {
            // create a new working segment            
            SegmentMemoryBuilder newlayer = new SegmentMemoryBuilder();
            SegmentMemoryBuilder checkpointSegment;
            int checkpoint_segment_size;

            // grab the checkpoint segment and move it aside
            lock (this.segmentlayers) {
                checkpointSegment = workingSegment;
                checkpoint_segment_size = checkpointSegment.RowCount;
                workingSegment = newlayer;
                segmentlayers.Insert(0, workingSegment);                
            }

            { // TODO: does this need to be in the lock?
                byte[] emptydata = new byte[0];
                long logWaitNumber = 0;
                this.logwriter.addCommand((byte)LogCommands.CHECKPOINT, emptydata, ref logWaitNumber);
            }

            
            
            {
                WriteGroup tx = new WriteGroup(this);

                // allocate a new generation number
                uint new_generation_number = (uint) rangemapmgr.allocNewGeneration(tx);

                this._writeSegment(tx, checkpointSegment.sortedWalk()); 
    
                {
                    byte[] emptydata = new byte[0];
                    tx.addCommand((byte)LogCommands.CHECKPOINT_DROP, emptydata);
                }
                rangemapmgr.recordMaxGeneration(tx,rangemapmgr.mergeManager.getMaxGeneration()+1);

                tx.finish();                             // commit the freespace and rangemap transaction

            }

            // TODO: make this atomic            
            
            // FIXME: don't do this anymore, because we are now rangemap walking!!
              // re-read the segment and use it to replace the checkpoint segment            
              // SegmentReader sr = new SegmentReader(reader.getStream());
              // segmentlayers.Insert(1, sr); // working segment is zero, so insert this after it
            
            
            // reader.getStream().Close(); // force close the reader

            lock (this.segmentlayers) {
                if (checkpointSegment.RowCount != checkpoint_segment_size) {
                    System.Console.WriteLine("checkpointSegment was added to while checkpointing!! lost {0} rows",
                        checkpointSegment.RowCount - checkpoint_segment_size);
                }
                segmentlayers.Remove(checkpointSegment);
            }
        }

        //-------------------------------------------------------

        public void performMerge(MergeCandidate mc) {
            if (mc == null) { return; }

            Console.WriteLine("performMerge({0})", mc.ToString());

            var segs_to_merge = new List<SegmentDescriptor>();
            segs_to_merge.AddRange(mc.source_segs);
            segs_to_merge.AddRange(mc.target_segs);
            segs_to_merge.Reverse();
            this.mergeSegments(segs_to_merge);
        }

        public List<SegmentDescriptor> listSegmentsForGen(int gen) {
            List<SegmentDescriptor> segs = new List<SegmentDescriptor>();

            RecordKey start_key = new RecordKey()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(new RecordKeyType_Long(gen));
                               
            RecordKey cur_key = start_key;
            RecordKey found_key = new RecordKey();
            RecordData found_record = new RecordData(RecordDataState.NOT_PROVIDED, found_key);
            while (rangemapmgr.getNextRecord(cur_key, ref found_key, ref found_record, false) == GetStatus.PRESENT) {
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
            int gen_count = rangemapmgr.genCount();

            if (gen_count < 1) {
                // nothing to even reprocess
                yield break;
            }


            RecordKey start_key = new RecordKey().appendParsedKey(".ROOT/GEN");
            RecordKey cur_key = start_key;
            RecordKey found_key = new RecordKey();
            RecordData found_record = new RecordData(RecordDataState.NOT_PROVIDED, found_key);
            while (rangemapmgr.getNextRecord(cur_key, ref found_key, ref found_record, false) == GetStatus.PRESENT) {
                cur_key = found_key;
                // check that the first two keyparts match
                if (found_key.isSubkeyOf(start_key)) {
                    // TODO: why is getNextRecord returning deleted records?!?!? Is that correct?
                    if (found_record.State == RecordDataState.DELETED) {
                        continue; // ignore the tombstone
                    } else if (found_record.State != RecordDataState.FULL) {
                        throw new Exception("can't handle incomplete segment record");
                    } else {
                        Console.WriteLine("listAllSegments: {0} => {1}", found_key, found_record);
                        yield return rangemapmgr.getSegmentDescriptorFromRecordKey(found_key);
                    }             
                } else {
                    // we're done matching the generation records
                    break;
                }
            }           
        }

        public void mergeSegments(IEnumerable<SegmentDescriptor> segs) {
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

            foreach (SegmentDescriptor segment in segs) {
                if (segment.generation < last_generation) {
                    throw new Exception("segment merge generation order invalid: " + String.Join(",", segs));
                }
                last_generation = (int)segment.generation;

                count++;
                target_generation = Math.Min(target_generation, segment.generation);
                    var seg = segment.getSegment(rangemapmgr);

                var nextchain = seg.sortedWalk();

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

           // (2) now perform the merge!
            {
                WriteGroup tx = new WriteGroup(this);

                // HACK: we delete the segment mappings first, so if we write the same mapping that we're removing, 
                // we don't inadvertantly delete the new mapping..

                // remove the old segment mappings
                foreach (SegmentDescriptor segment in segs) {
                    // TODO: it's not safe to free this space yet!
                    rangemapmgr.unmapSegment(tx, segment);  
                    
                }

                this._writeSegment(tx, chain, (int)target_generation);

                // free the space from the old segments

                // check to see if we can shrink NUMGENERATIONS

                tx.finish();                             // commit the freespace and rangemap transaction

                rangemapmgr.clearSegmentCacheHack();                
            }
        }

        public void mergeAllSegments() {
            var allsegs = new List<SegmentDescriptor>();
            allsegs.AddRange(listAllSegments());
            allsegs.Sort((a, b) => a.generation.CompareTo(b.generation));
            
            mergeSegments(allsegs);
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
            while (rangemapmgr.getNextRecord(cur_key, ref found_key, ref found_record, false) == GetStatus.PRESENT) {
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



        public GetStatus getRecord(RecordKey key, out RecordData record) {
            RecordKey found_key = new RecordKey();
            record = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());
            if (rangemapmgr.getNextRecord(key, ref found_key, ref record,true) == GetStatus.PRESENT) {
                if (found_key.Equals(key)) {
                    return GetStatus.PRESENT;
                }
            }
            record = null;
            return GetStatus.MISSING;
        }

        public GetStatus getNextRecord(RecordKey lowkey, ref RecordKey found_key, ref RecordData found_record) {
            return (rangemapmgr.getNextRecord(lowkey, ref found_key, ref found_record,false));
        }

        public void debugDump()
        {
                
            foreach (ISortedSegment layer in segmentlayers) {
                Console.WriteLine("--- Memory Layer : " + layer.GetHashCode());
                debugDump(layer, "  ", new HashSet<string>());
            }
        }

        
        private void debugDump(ISortedSegment seg, String indent, HashSet<string> seenGenerations) {
            HashSet<string> nextSeenGenerations = new HashSet<string>(seenGenerations);
            RecordKey genkey = new RecordKey().appendParsedKey(".ROOT/GEN");

            // first, print all our keys
            foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in seg.sortedWalk()) {
                String value_str = kvp.Value.ToString();
                if (value_str.Length < 40) {
                    Console.WriteLine(indent + kvp.Key + " : " + value_str);
                } else {
                    Console.WriteLine(indent + kvp.Key + " : " + value_str.Substring(0,10) + "..[" + (value_str.Length-40) + " more bytes]");
                }
                
                if (kvp.Key.isSubkeyOf(genkey)) {
                    nextSeenGenerations.Add(kvp.Key.ToString());
                }
                
            }

            // second, walk the rangemap
            foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in seg.sortedWalk()) {
                // see if this is a range key (i.e.   .ROOT/GEN/###/</>   )
                // .. if so, recurse

                if (kvp.Key.isSubkeyOf(genkey) && kvp.Value.type == RecordUpdateTypes.FULL) {
                    if (seenGenerations.Contains(kvp.Key.ToString())) {
                        Console.WriteLine("--- Skipping Tombstoned layer for Key " + kvp.Key.ToString());
                    } else {
                        Console.WriteLine("--- Layer for Keys: " + kvp.Key.ToString());
                        ISortedSegment newseg = rangemapmgr.getSegmentFromMetadata(kvp.Value);
                        debugDump(newseg, indent + " ",nextSeenGenerations);
                    }
                }
            }

        }


        public KeyValuePair<RecordKey, RecordData> FindNext(IComparable<RecordKey> keytest, bool equal_ok) {
            RecordKey found_key = new RecordKey();
            RecordData record = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());

            if (rangemapmgr.getNextRecord_LowLevel(keytest, true, ref found_key, ref record,equal_ok, false) == GetStatus.PRESENT) {
                return new KeyValuePair<RecordKey,RecordData>(found_key,record);
            }
            throw new KeyNotFoundException(String.Format("LayerManager.FindNext({0},{1}) found no key", keytest, equal_ok));

        }

        public KeyValuePair<RecordKey, RecordData> FindPrev(IComparable<RecordKey> keytest, bool equal_ok) {
            RecordKey found_key = new RecordKey();
            RecordData record = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());

            if (rangemapmgr.getNextRecord_LowLevel(keytest, false,ref found_key, ref record, equal_ok, false) == GetStatus.PRESENT) {
                return new KeyValuePair<RecordKey, RecordData>(found_key, record);
            }
            throw new KeyNotFoundException(String.Format("LayerManager.FindNext({0},{1}) found no key", keytest, equal_ok));

        }        

        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner) {
            return scan(scanner,true);
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner) {
            return scan(scanner, false);
        }


        private IEnumerable<KeyValuePair<RecordKey, RecordData>> scan(IScanner<RecordKey> scanner, bool direction_is_forward) {
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
            if (rangemapmgr.getNextRecord_LowLevel(cursor_key, direction_is_forward, ref found_key, ref found_record, true, false) == GetStatus.MISSING) {
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

                if (rangemapmgr.getNextRecord_LowLevel(cursor_key, direction_is_forward,ref found_key, ref found_record, false, false) == GetStatus.MISSING) {
                    yield break; // no keys
                }
            }

        }


        public void setValueParsed(String skey, String svalue)
        {
            WriteGroup implicit_txn = this.newWriteGroup();
            implicit_txn.setValueParsed(skey, svalue);
            implicit_txn.finish();            
        }

        public void setValue(RecordKey key, RecordUpdate value) {
            WriteGroup implicit_txn = this.newWriteGroup();
            implicit_txn.setValue(key, value);
            implicit_txn.finish();            
        }

        public void Dispose() {
            if (logwriter != null) {
                logwriter.Dispose(); logwriter = null;
            }

            foreach (ISortedSegment segment in segmentlayers) {
                segment.Dispose();
            }
        }
    }




}