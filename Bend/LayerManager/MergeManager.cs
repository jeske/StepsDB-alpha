// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Text;

using System.IO;


namespace Bend {

    // TODO: make SegmentDescriptor IComparable

    public class MergeManager_Incremental {
        BDSkipList<SegmentDescriptor, List<MergeCandidate>> segmentInfo;
        BDSkipList<float, MergeCandidate> prioritizedMergeCandidates;
        private int MAX_MERGE_SIZE = 10;

        public MergeManager_Incremental() {
            segmentInfo = new BDSkipList<SegmentDescriptor, List<MergeCandidate>>();
            prioritizedMergeCandidates = new BDSkipList<float, MergeCandidate>();
        }

        public class MergeCandidate {
            public SegmentDescriptor[] source_segs;
            public SegmentDescriptor[] target_segs;
            float merge_ratio;

            public MergeCandidate(List<SegmentDescriptor> source_segs, List<SegmentDescriptor> target_segs) {
                this.source_segs = source_segs.ToArray();
                this.target_segs = target_segs.ToArray();
                this.merge_ratio = (float)source_segs.Count / (float)target_segs.Count;
            }

            public float score() {
                return this.merge_ratio;
            }

            public override string ToString() {
                return "MergeCandidate{ (" + String.Join(",", (IEnumerable<SegmentDescriptor>)source_segs) +
                    ") -> (" + String.Join(",", (IEnumerable<SegmentDescriptor>)target_segs) + ") }";
            }
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
            prioritizedMergeCandidates.Add(mergeCandidate.score(), mergeCandidate);
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
                    sourceSegments = new List<SegmentDescriptor>();
                    sourceSegments.AddRange(sourceSegments);
                    sourceSegments.AddRange(targetSegments);                    
                    
                }
                targetSegments = null;
            }

            // (2) trigger segments above this segment to recompute their candidates

        }

        public void notify_removeSegment(SegmentDescriptor segdesc) {
            // remove all merge candidates this segment is participating in
            foreach (var candidate in segmentInfo[segdesc]) {
                prioritizedMergeCandidates.Remove(new KeyValuePair<float,MergeCandidate>(candidate.score(), candidate));
            }
            segmentInfo.Remove(segdesc);

        }


        public MergeCandidate getBestCandidate() {
            return prioritizedMergeCandidates.FindNext(0f, true).Value;
        }

        public int getNumberOfCandidates() {
            return prioritizedMergeCandidates.Count;
        }
    }

}