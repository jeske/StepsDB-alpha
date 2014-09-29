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
    partial class LayerManager
    {

        public void debugDump()
        {

            foreach (ISortedSegment layer in segmentlayers)
            {
                Console.WriteLine("--- Memory Layer : " + layer.GetHashCode());
                debugDump(layer, "  ", new HashSet<string>());
            }

            freespacemgr.debugDumbCurrentFreespace();
        }


        public void DEBUG_addNewWorkingSegmentWithoutFlush()
        {
            // (1) create a new working segment            
            SegmentMemoryBuilder newlayer = new SegmentMemoryBuilder();
            SegmentMemoryBuilder checkpointSegment;
            int checkpoint_segment_size;


            Console.WriteLine("************ WARNING ************ Using DEBUG_addNewWorkingSegmentWithoutFlush()");

            lock (flushLock)
            {
                lock (this.segmentlayers)
                {
                    checkpointSegment = workingSegment;
                    checkpoint_segment_size = checkpointSegment.RowCount;
                    workingSegment = newlayer;
                    segmentlayers.Insert(0, workingSegment);
                }
            }

            Console.WriteLine("*********** num memory layers = {0}", segmentlayers.Count);

            this.debugDump();

            // (2) wait a moment, then check that the old working segment was no longer written (or make it readonly)
        }


        private void debugDump(ISortedSegment seg, String indent, HashSet<string> seenGenerations)
        {
            HashSet<string> nextSeenGenerations = new HashSet<string>(seenGenerations);
            RecordKey genkey = new RecordKey().appendParsedKey(".ROOT/GEN");

            // first, print all our keys
            foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in seg.sortedWalk())
            {
                String value_str = kvp.Value.ToString();
                if (value_str.Length < 50)
                {
                    Console.WriteLine(indent + kvp.Key + " : " + value_str + "   ");
                }
                else
                {
                    Console.WriteLine(indent + kvp.Key + " : " + value_str.Substring(0, 10) + "..[" + (value_str.Length - 40) + " more bytes]");
                }

                if (kvp.Key.isSubkeyOf(genkey))
                {
                    nextSeenGenerations.Add(kvp.Key.ToString());
                }

            }

            // second, walk the rangemap
            foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in seg.sortedWalk())
            {
                // see if this is a range key (i.e.   .ROOT/GEN/###/</>   )
                // .. if so, recurse

                if (kvp.Key.isSubkeyOf(genkey) && kvp.Value.type == RecordUpdateTypes.FULL)
                {
                    if (seenGenerations.Contains(kvp.Key.ToString()))
                    {
                        Console.WriteLine("--- Skipping Tombstoned layer for Key " + kvp.Key.ToString());
                    }
                    else
                    {
                        Console.WriteLine("--- Layer for Keys: " + kvp.Key.ToString());
                        ISortedSegment newseg = rangemapmgr.getSegmentFromMetadata(kvp.Value);
                        debugDump(newseg, indent + " ", nextSeenGenerations);
                    }
                }
            }

        }

    }


}


