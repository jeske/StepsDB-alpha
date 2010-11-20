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
        public int MAX_MERGE_SIZE = 10;

        public MergeManager_Incremental() {
            segmentInfo = new BDSkipList<SegmentDescriptor, List<MergeCandidate>>();
            prioritizedMergeCandidates = new BDSkipList<MergeCandidate,int>();
        }

        

        private void addMergeCandidate(List<SegmentDescriptor> source_segs, List<SegmentDescriptor> target_segs) {
            // create a merge candidate
            var mergeCandidate = new MergeCandidate(source_segs, target_segs);

            
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

        public void notify_addSegment(SegmentDescriptor segdesc) {
            segmentInfo.Add(segdesc, new List<MergeCandidate>());
            
            // (1) find out which segments this could merge down into and if
            // the number is small enough, add it to the prioritized candidate list           
            var sourceSegments = new List<SegmentDescriptor>(); sourceSegments.Add(segdesc);            
            int subcount = 0;                     
            for (int target_generation = ((int)segdesc.generation-1);target_generation>=0;target_generation--) {            
                var start = new SegmentDescriptor((uint)target_generation,segdesc.start_key,segdesc.start_key);              
                var end = new SegmentDescriptor((uint)target_generation,segdesc.end_key,segdesc.end_key);
                var targetSegments = new List<SegmentDescriptor>();

                foreach (var kvp in segmentInfo.scanForward(new ScanRange<SegmentDescriptor>(start, end, null))) {
                    if (++subcount > MAX_MERGE_SIZE) {
                        break; // this merge would be too big! 
                    }
                    targetSegments.Add(kvp.Key);
                }
                if (targetSegments.Count > 0) {
                    // add the merge candidate
                    this.addMergeCandidate(sourceSegments, targetSegments);

                    // add the target segments to the source so we can iterate again                    
                    sourceSegments.AddRange(targetSegments);                    
                    
                }
                targetSegments = null;
            }

            // (2) trigger segments above this segment to recompute their candidates

        }

        public void notify_removeSegment(SegmentDescriptor segdesc) {
            // remove all merge candidates this segment is participating in
            foreach (var candidate in segmentInfo[segdesc]) {
                prioritizedMergeCandidates.Remove(candidate);
            }
            segmentInfo.Remove(segdesc);

        }


        public MergeCandidate getBestCandidate() {           
            return prioritizedMergeCandidates.FindNext(null, true).Key;
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