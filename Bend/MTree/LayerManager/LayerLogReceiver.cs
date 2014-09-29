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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;



namespace Bend {    
    public struct LayerLogReceiver : ILogReceiver {
        LayerManager mylayer;
        internal SegmentMemoryBuilder checkpointSegment;
        public LayerLogReceiver(LayerManager mylayer) {
            this.mylayer = mylayer;
            checkpointSegment = null;
        }
        public void requestLogExtension() {
            throw new NotImplementedException();
        }

        public void logStatusChange(long logUsedBytes, long logFreeBytes) {
            double logTotalBytesD = (double)(logUsedBytes + logFreeBytes);
            double logFreeBytesD = (double)logFreeBytes;

            double logPercentFreeD = logFreeBytesD / logTotalBytesD;
            if (logPercentFreeD < 0.30) {
                mylayer.needCheckpointNow = true;
            }
        }

        public void handleCommand(LogCommands cmd, byte[] cmddata) {
            if (cmd == LogCommands.UPDATE) {
                // decode basic block key/value writes
                BlockAccessor ba = new BlockAccessor(cmddata);
                ISegmentBlockDecoder decoder = new SegmentBlockBasicDecoder(ba);
                foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in decoder.sortedWalk()) {
                    // add the updates to our working segment...
                    lock (mylayer.segmentlayers) {
#if false
                            // some status debug code...
                            if (RangemapManager.RangeKey.isRangeKey(kvp.Key)) {
                                System.Console.WriteLine("LayerManager.handleCommand : setValue() {0} => {1}",
                                    kvp.Key.ToString(), kvp.Value.ToString());
                                if (kvp.Value.type != RecordUpdateTypes.DELETION_TOMBSTONE && kvp.Value.data.Length != 16) {
                                    throw new Exception("!!! corrupted rangekey appeared in handleCommand");
                                }
                            }
#endif

                        mylayer.workingSegment.setRecord(kvp.Key, kvp.Value);
                    }
                }
            } else if (cmd == LogCommands.CHECKPOINT_START) {
                // here we move aside the checkpoint segment... 
                //   - if this is during live operation, the checkpoint is running as soon as we return.
                //   - if this is during recovery, there is no checkpoint running and we'll need to 

                // TODO: we need some kind of key/checksum to be sure that we CHECKPOINT and DROP the right dataF
                checkpointSegment = mylayer.workingSegment;
                SegmentMemoryBuilder newsegment = new SegmentMemoryBuilder();
                lock (mylayer.segmentlayers) {
                    mylayer.workingSegment = newsegment;
                    mylayer.segmentlayers.Insert(0, mylayer.workingSegment);
                }
            } else if (cmd == LogCommands.CHECKPOINT_DROP) {
                // TODO: we need some kind of key/checksum to be sure that we CHECKPOINT and DROP the right data
                if (checkpointSegment != null) {
                    lock (mylayer.segmentlayers) {
                        mylayer.segmentlayers.Remove(checkpointSegment);
                        checkpointSegment = null;
                    }
                } else {
                    throw new Exception("can't drop, no segment to drop");
                }
            } else {
                throw new Exception("unimplemented command");
            }
        }
    }

}

