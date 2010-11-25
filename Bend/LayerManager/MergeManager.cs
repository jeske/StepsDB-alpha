// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Text;

using System.IO;


namespace Bend {

    // TODO: make SegmentDescriptor IComparable

    public class MergeManager_Incremental {
        public BDSkipList<SegmentDescriptor, List<MergeCandidate>> segmentInfo;
        public BDSkipList<MergeCandidate,int> prioritizedMergeCandidates;
        public int MAX_MERGE_SIZE = 4;
        public int MAX_HISTO_MERGE_SIZE = 6;
        public RangemapManager rangemapmgr;
        
        public MergeManager_Incremental(RangemapManager rmm) {
            segmentInfo = new BDSkipList<SegmentDescriptor, List<MergeCandidate>>();
            prioritizedMergeCandidates = new BDSkipList<MergeCandidate,int>();
            this.rangemapmgr = rmm;
        }

        public  int getMaxGeneration() {            
            try {
                return (int)segmentInfo.FindPrev(new ScanRange<SegmentDescriptor>.maxKey(), true).Key.generation;
            } catch (KeyNotFoundException e) {
                return 0;
            }            
        }
        private void addMergeCandidate(List<SegmentDescriptor> source_segs, List<SegmentDescriptor> target_segs) {
            // create a merge candidate
            var mergeCandidate = new MergeCandidate(source_segs, target_segs);
            if (prioritizedMergeCandidates.ContainsKey(mergeCandidate)) {
                // we already know about this one...
                return;
            }
            
            // refernce the merge candidate from each of these segments
            foreach (var seg in source_segs) {
                segmentInfo[seg].Add(mergeCandidate);    
            }
            foreach (var seg in target_segs) {
                segmentInfo[seg].Add(mergeCandidate);
            }

            // add the merge candidate to our priority queue
            prioritizedMergeCandidates.Add(mergeCandidate, 1);
        }

        private void _generateMergeCandidatesUsingHistogram(SegmentDescriptor segdesc) {
            var foundTargets = new BDSkipList<SegmentDescriptor, int>();
            if (segdesc.generation < 1) {
                return; // nothing to do
            }

            // scan the keys of this segment, and build a merge histogram to see if
            // the histogram details provide a better merge ratio than the keyrange
            RecordKey found_key = new RecordKey();
            RecordData found_data = new RecordData(RecordDataState.NOT_PROVIDED, found_key);
            if (rangemapmgr.getNextRecord(segdesc.record_key, ref found_key, ref found_data, true) == GetStatus.PRESENT) {

                SegmentReader sr = rangemapmgr.segmentReaderFromRow(found_key, found_data);
                int key_count = 0;
                int max_target_generation = 0;
                foreach (var row in sr.sortedWalk()) {
                    key_count++;
                    // find the previous generation block this key must merge into                    
                    for (int target_generation = (int)segdesc.generation - 1; target_generation >= 0; target_generation--) {
                        SegmentDescriptor searchdesc = new SegmentDescriptor((uint)target_generation, row.Key, row.Key);

                        // check before 
                        try {
                            SegmentDescriptor foundseg = segmentInfo.FindPrev(searchdesc, true).Key;
                            if (foundseg.generation == target_generation) {
                                // check for overlap
                                if (foundseg.keyrangeOverlapsWith(segdesc)) {
                                    foundTargets[foundseg] = target_generation;
                                    max_target_generation = Math.Max(max_target_generation, target_generation);
                                    break;
                                }

                            }
                        } catch (KeyNotFoundException) { }

                        // then check after
                        try {
                            SegmentDescriptor foundseg = segmentInfo.FindNext(searchdesc, true).Key;
                            if (foundseg.generation == target_generation) {
                                // check for overlap
                                if (foundseg.keyrangeOverlapsWith(segdesc)) {
                                    foundTargets[foundseg] = target_generation;
                                    max_target_generation = Math.Max(max_target_generation, target_generation);
                                    break;
                                }
                                
                            }
                        } catch (KeyNotFoundException) { }                        
                    }

                    if (foundTargets.Count > MAX_HISTO_MERGE_SIZE) {
                        // the histogram blew up also
                        return;
                    }
                } // end foreach record in segment

                // TODO: we need to be careful if there are targets that span multiple generations, as 
                //     they might violate the merge rules. We need to either merge only to a single 
                //     generation, or we need to rescan based on the start/end of every block in the
                //     higher generation

                if (foundTargets.Count > 0) {

                    // assemble the merge target from the max_target_generation
                    var mergeTargetSegments = new List<SegmentDescriptor>();
                    foreach (var kvp in foundTargets) {
                        if (kvp.Value == max_target_generation) {
                            mergeTargetSegments.Add(kvp.Key);
                        }
                    }

                    System.Console.WriteLine("Histogram Merge Target ( key_count = " + key_count +
                             ", target_block_count = " + mergeTargetSegments.Count + ") " +
                             segdesc.ToString() + " -> " + String.Join(",", mergeTargetSegments));
                    var sourceSegments = new List<SegmentDescriptor>(); sourceSegments.Add(segdesc);
                    this.addMergeCandidate(sourceSegments, mergeTargetSegments);
                }
            }

        }

        private void _generateMergeCandidatesFor(SegmentDescriptor segdesc) {

            // find out which segments this could merge down into and if
            // the number is small enough, add it to the prioritized candidate list

            var sourceSegments = new List<SegmentDescriptor>(); sourceSegments.Add(segdesc);
            int subcount = 1;
            int merge_candidates = 0;
            RecordKey key_start = segdesc.start_key;
            RecordKey key_end = segdesc.end_key;

            for (int target_generation = ((int)segdesc.generation - 1); target_generation >= 0; target_generation--) {                
                var start = new SegmentDescriptor((uint)target_generation, key_start, new RecordKey());
                var end = new SegmentDescriptor((uint)target_generation, key_end, key_end);

                var targetSegments = new List<SegmentDescriptor>();

                // first get the record before the startkey, and see if we are inside it...
                try {
                    var kvp = segmentInfo.FindPrev(start, true);
                    
                    if (kvp.Key.generation == target_generation) {
                        // if overlap
                        if (kvp.Key.keyrangeOverlapsWith(key_start,key_end)) {
                            targetSegments.Add(kvp.Key);
                            subcount++;
                        }                        
                    }
                } catch (KeyNotFoundException) {
                }

                foreach (var kvp in segmentInfo.scanForward(new ScanRange<SegmentDescriptor>(start, end, null))) {
                    if (++subcount > MAX_MERGE_SIZE) {
                        // we don't want to scan forever, stop here
                        if (merge_candidates == 0) {
                            // but if we have not generated any merge candidates, double-check the histogram
                            _generateMergeCandidatesUsingHistogram(segdesc);
                        }
                        return; 
                    }
                    targetSegments.Add(kvp.Key);
                }
                
                if (targetSegments.Count > 0) {
                    // add the merge candidate
                    this.addMergeCandidate(sourceSegments, targetSegments);
                    merge_candidates++;

                    // add the target segments to the source so we can iterate again                    
                    sourceSegments.AddRange(targetSegments);

                    // expand the start/end range based on the targetSegments if necessary
                    foreach (var seg in targetSegments) {
                        if (seg.start_key.CompareTo(key_start) < 0) {                            
                            System.Console.WriteLine("*************************************************");
                            System.Console.WriteLine("extended keystart from: " + key_start + "  to: " + seg.start_key);
                            key_start = seg.start_key;
                        }
                        if (seg.end_key.CompareTo(key_end) > 0) {
                            System.Console.WriteLine("*************************************************");
                            System.Console.WriteLine("extended keyend from: " + key_end + "  to: " + seg.end_key);
                            key_end = seg.end_key;
                        }
                    }

                }
                targetSegments = null;
            }

        }

        public void notify_addSegment(SegmentDescriptor segdesc) {
            segmentInfo.Add(segdesc, new List<MergeCandidate>());

            // (1) generate downstream merge-candidates
            _generateMergeCandidatesFor(segdesc);                       

            // (2) trigger segments above this segment to recompute their candidates
            int max_generation = this.getMaxGeneration();
            for (int source_generation = (int)segdesc.generation + 1; source_generation <= max_generation; source_generation++) {
                var start = new SegmentDescriptor((uint)source_generation, segdesc.start_key, segdesc.start_key);
                var end = new SegmentDescriptor((uint)source_generation, segdesc.end_key, segdesc.end_key);

                foreach (var kvp in segmentInfo.scanForward(new ScanRange<SegmentDescriptor>(start, end, null))) {
                    // TODO: we should really only generate a new candidates IF it includes the new segment
                    _generateMergeCandidatesFor(kvp.Key);
                }
                
            }
        }

        public void notify_removeSegment(SegmentDescriptor segdesc) {
            if (! segmentInfo.ContainsKey(segdesc)) {
                throw new Exception("MergeManager notify_removeSegment() for unknown segment: " + segdesc);
            }

            // remove all merge candidates this segment is participating in
            foreach (var candidate in segmentInfo[segdesc]) {
                prioritizedMergeCandidates.Remove(candidate);
            }
            segmentInfo.Remove(segdesc);

        }


        public MergeCandidate getBestCandidate() {
            try {
                return prioritizedMergeCandidates.FindNext(new ScanRange<MergeCandidate>.minKey(), true).Key;
            } catch (KeyNotFoundException) {
                return null;
            }
        }

        public int getNumberOfCandidates() {
            return prioritizedMergeCandidates.Count;
        }
    }

    // ----------------

    public class MergeCandidate : IComparable<MergeCandidate> {
        public SegmentDescriptor[] source_segs;
        public SegmentDescriptor[] target_segs;
        float merge_ratio;

        public int CompareTo(MergeCandidate target) {
            switch (this.merge_ratio.CompareTo(target.merge_ratio)) {
                case -1:
                    return -1;
                case 1:
                    return 1;
            }
            switch (this.source_segs.Length.CompareTo(target.source_segs.Length)) {
                case -1:
                    return -1;
                case 1:
                    return 1;
            }
            switch (this.target_segs.Length.CompareTo(target.target_segs.Length)) {
                case -1:
                    return -1;
                case 1:
                    return 1;
            }
            for(int x=0;x<this.source_segs.Length;x++) {
                switch(this.source_segs[x].CompareTo(target.source_segs[x])) {
                    case -1:
                        return -1;
                    case 1:
                        return 1;                    
                }
            }
            for (int x = 0; x < this.target_segs.Length; x++) {
                switch (this.target_segs[x].CompareTo(target.target_segs[x])) {
                    case -1:
                        return -1;
                    case 1:
                        return 1;
                }
            }
            return 0;

        }

        public MergeCandidate(List<SegmentDescriptor> source_segs, List<SegmentDescriptor> target_segs) {
            this.source_segs = source_segs.ToArray();
            this.target_segs = target_segs.ToArray();

            this.merge_ratio = ((float)target_segs.Count / (float)source_segs.Count) / (float)(target_segs.Count + source_segs.Count);
        }

        public float score() {
            return this.merge_ratio;
        }

        public override string ToString() {
            return "MergeCandidate{ " + this.merge_ratio + " (" + String.Join(",", (IEnumerable<SegmentDescriptor>)source_segs) +
                ") -> (" + String.Join(",", (IEnumerable<SegmentDescriptor>)target_segs) + ") }";
        }
    }


}