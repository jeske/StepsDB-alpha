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
        public void forceCheckpoint() {
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

        public void handleCommand(byte cmd, byte[] cmddata) {
            if (cmd == (byte)LogCommands.UPDATE) {
                // decode basic block key/value writes
                BlockAccessor ba = new BlockAccessor(cmddata);
                ISegmentBlockDecoder decoder = new SegmentBlockBasicDecoder(ba);
                foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in decoder.sortedWalk()) {
                    // populate our working segment
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
            } else if (cmd == (byte)LogCommands.CHECKPOINT_START) {
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
            } else if (cmd == (byte)LogCommands.CHECKPOINT_DROP) {
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

