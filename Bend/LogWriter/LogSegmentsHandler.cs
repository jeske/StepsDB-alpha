using System;
using System.IO;
using System.Collections.Generic;

using System.Threading;

// * About the Log
// 
// The log is organized as a set of separate log segments. Initially this is 5 segments, each 2MBs in length. 
// A log packet (which contains multiple log commands) is written contigiously to a single log segment. 
// If there is not enough room in the current log segment, the packet is written into the next available log segment. 
// 
// A log packet should be notably smaller than a single log segment, otherwise much space will be wasted at
// the end of each log-segment. If a log-packet is bigger than a log-segment, it can not be written.
// 
// The rootblock records a list of log segments, however, the list should not be assumed to be in proper order.
// Keeping the list in order would require re-writeing the root-block each time a log-segment was "freed"
// and rotated to the end during a checkpoint. Instead of performing this extra work frequently during writing,
// we offload this work to the "infrequent" activity of log recovery. Recovery must scan all log-segments, checking
// log-timestamps for the first record in the log, to determine the proper order of the log segments. Once
// the proper order is established, recovery can process them in the proper order. 
//
// **I
//
// * LogSegmentsHandler : encapsulates the details of handling multiple separate log segments, including:
//
//   - fitting log command packets into the next suitable log segment
//   - rotating and "clearing" log segments during log checkpoint
//   - discovering segment order during recovery


// TODO:
//   - check to see if a log entry fits into the current segment, if advance the active segment
//   - setup the current "log head" after recovery, to continue writing to the end
//   - add log-timestamps to each log-packet so we can properly order log segments during recovery
//   - add a log-checkpoint / rotation protocol 
//   - FIX: recovery if we crash suring a checkpoint attempt (currently this is busted)

namespace Bend {

    public class LogSegmentsHandler : IDisposable {
        IRegionManager regionmgr;
        LogWriter logwriter;

        BinaryWriter nextChunkBuffer;
        private long _logWaitSequenceNumber = 1; // this must be recovered during recovery...

        public long logWaitSequenceNumber {
            get { return _logWaitSequenceNumber; }
        }

        public delegate void handleLogFlushUpkeep();

        List<RootBlockLogSegment> empty_log_segments = new List<RootBlockLogSegment>();
        List<RootBlockLogSegment> active_log_segments = new List<RootBlockLogSegment>();

        List<RootBlockLogSegment> checkpoint_log_segments = null;
        bool checkpointReady = false;

        Stream currentLogHeadStream;
        RootBlockLogSegment currentLogSegmentInfo;

        public int activeLogSegments { get { return active_log_segments.Count; } }
        public int emptyLogSegments { get { return empty_log_segments.Count; } }

        // we always have to keep enough log reserve to handle a checkpoint so we can empty the log! 
        private static uint LOG_SEGMENT_RESERVE = LOG_END_MARKER_SIZE + LOG_CHECKPOINT_MARKER_SIZE;
        private static uint LOG_CHECKPOINT_MARKER_SIZE = 1024; // TODO: this should be configured by our client! 

        private bool debugLogSegments = false;

        public LogSegmentsHandler(LogWriter logwriter, IRegionManager regionmgr, RootBlockLogSegment[] segments) {
            nextChunkBuffer = new BinaryWriter(new MemoryStream());

            this.regionmgr = regionmgr;
            this.logwriter = logwriter;

            _organizeLogSegments(segments);
        }

        public void setDebugLogSegments() {
            debugLogSegments = true;
        }

        private void _processCommand(LogCommands cmdtype, byte[] cmdbytes) {
            if (cmdtype == LogCommands.CHECKPOINT_START) {
                // (1) make sure there is no pending checkpoint
                if (checkpoint_log_segments != null) {
                    throw new Exception("can't process two checkpoints at once!");
                }
                // setup for the checkpoint..
                checkpoint_log_segments = new List<RootBlockLogSegment>();
                checkpointReady = false;

                // (2) make a record of the "freeable" log segments
                lock (this) {
                    foreach (RootBlockLogSegment seg in active_log_segments) {
                        // skip the currently active segment because we can only drop whole log segments                        
                        if (!seg.Equals(currentLogSegmentInfo)) {
                            checkpoint_log_segments.Add(seg);
                        }
                    }
                }
            }
            // this happens in the lock to be sure the flag and the drop end up in the same packet
            if (cmdtype == LogCommands.CHECKPOINT_DROP) {
                // (1) match the checkpoint number against our current pending checkpoint info...
                // (2) "drop" the old log segments by moving them to the pending-free-list
                checkpointReady = true; // the actual flush will take care of the drop
            }   


        }

        internal void _addCommand(LogCommands cmdtype, byte[] cmdbytes, out long logWaitNumber) {


            lock (this) {
                this._processCommand(cmdtype, cmdbytes);
                nextChunkBuffer.Write((UInt32)cmdbytes.Length);
                nextChunkBuffer.Write((byte)cmdtype);
                nextChunkBuffer.Write(cmdbytes, 0, cmdbytes.Length);
                logWaitNumber = this._logWaitSequenceNumber;
            }
        }

        private void _organizeLogSegments(RootBlockLogSegment[] segments) {
            // (2) scan and organize the log-segments (so they are in proper order)            
            foreach (RootBlockLogSegment seg in segments) {
                Stream logsegment = regionmgr.readRegionAddr(seg.logsegment_start).getNewAccessStream();

                BinaryReader br = new BinaryReader(logsegment);
                if (br.ReadUInt32() != LogWriter.LOG_MAGIC) {
                    abortCorrupt("_organizeLogSegments() found corrupt log segment!");
                }
                UInt32 chunksize = br.ReadUInt32();
                UInt16 checksum = br.ReadUInt16();

                if (chunksize == 0 && checksum == 0) {                 
                    empty_log_segments.Add(seg);
                } else {
                    active_log_segments.Add(seg);
                }
                logsegment.Close();
            }            

        }


        private void abortCorrupt(String reason) {
            throw new Exception(String.Format("aborting from corrupt log reason: {0}", reason));
        }

        public void prepareLog() {            
            // make sure there is an active log segment
            if (active_log_segments.Count == 0) {
                // move an empty log segment to active...
                lock (this) {
                    active_log_segments.Add(empty_log_segments[0]);
                    empty_log_segments.RemoveAt(0);
                }
            }
            // open the current log stream...
            currentLogSegmentInfo = active_log_segments[active_log_segments.Count - 1]; // get last            
            currentLogHeadStream = regionmgr.writeExistingRegionAddr(currentLogSegmentInfo.logsegment_start).getNewAccessStream();


            // move to the "end" of this active segment
        }
        public void advanceActiveLogSegment() {
            Console.WriteLine("advance to next segment");

            // close the current active segment
            currentLogHeadStream.Close();

            // advance to the next empty log stream...
            if (empty_log_segments.Count == 0) {
                // no more empty log segments!                 
                this.logwriter.receiver.requestLogExtension();

                if (empty_log_segments.Count == 0) {
                    throw new Exception("forceCheckpoint() failed to free log segments");
                }
            }

            this.notifyLogStatus();

            lock (this) {
                currentLogSegmentInfo = empty_log_segments[0];
                empty_log_segments.RemoveAt(0);
                active_log_segments.Add(currentLogSegmentInfo);
            }

            // open the current log stream...
            currentLogHeadStream = regionmgr.writeExistingRegionAddr(currentLogSegmentInfo.logsegment_start).getNewAccessStream();
            if (currentLogHeadStream.Position != 0) {
                throw new Exception(String.Format("advanced to non-start of segment! pos = {0}", currentLogHeadStream.Position));
            }
        }   

        private void notifyLogStatus() {
            long logUsedBytes = 0;
            long logFreeBytes = 0;

            lock (this) {
                foreach (RootBlockLogSegment seg in active_log_segments) {
                    logUsedBytes += seg.logsegment_size;
                }
                foreach (RootBlockLogSegment seg in empty_log_segments) {
                    logFreeBytes += seg.logsegment_size;
                }
            }
            logwriter.receiver.logStatusChange(logUsedBytes, logFreeBytes);
        }
       
        public IEnumerable<LogCmd> recoverLogCmds() {

            // (0) are the log segments in the right order (assume they are)

            // (1) read commands from the log segments

            foreach (RootBlockLogSegment seg in active_log_segments) {
                // open the log segment
                Stream logstream = regionmgr.readRegionAddr(seg.logsegment_start).getNewAccessStream();

                BinaryReader br = new BinaryReader(logstream);

                while (true) {
                    UInt32 magic = br.ReadUInt32();
                    if (magic != LogWriter.LOG_MAGIC) {
                        abortCorrupt("invalid magic: " + magic);
                    }
                    UInt32 chunksize = br.ReadUInt32();
                    UInt16 checksum = br.ReadUInt16();

                    if (chunksize == 0 && checksum == 0) {
                        // we reached the end-of-chunks marker, seek back
                        br.BaseStream.Seek(-(4 + 4 + 2), SeekOrigin.Current);
                        break;
                    }

                    byte[] logchunk = new byte[chunksize];
                    if (br.Read(logchunk, 0, (int)chunksize) != chunksize) {
                        abortCorrupt("chunksize bytes not available");
                    }
                    // CRC the chunk and verify against checksum
                    UInt16 nchecksum = Util.Crc16.Instance.ComputeChecksum(logchunk);
                    if (nchecksum != checksum) {
                        abortCorrupt("computed checksum: " + nchecksum + " didn't match: " + checksum);
                    }
                    // decode and apply the records
                    BinaryReader mbr = new BinaryReader(new MemoryStream(logchunk));

                    while (mbr.BaseStream.Position != mbr.BaseStream.Length) {
                        UInt32 cmdsize = mbr.ReadUInt32();
                        byte cmdtype = mbr.ReadByte();
                        byte[] cmdbytes = new byte[cmdsize];
                        if (mbr.Read(cmdbytes, 0, (int)cmdsize) != cmdsize) {
                            abortCorrupt("error reading command bytes");
                        }

                        // construct the LogCmd yield result...
                        yield return ( new LogCmd((LogCommands)cmdtype, cmdbytes) );      
                  
                    }
                } // ... while there are still log records

                // close this segment to move onto the next one
                br = null;
                logstream.Close();
            } // .. foreach log segment
        }


        private static uint LOG_END_MARKER_SIZE = 16; // size of the _doLogEnd() marker

        // this writes a closing log packet and seeks back onto it
        private static void _doLogEnd(BinaryWriter logbr) {
            // write "end of log" marker  (magic, size=0, checksum=0);
            logbr.Write((UInt32)LogWriter.LOG_MAGIC);
            logbr.Write((UInt32)0); // size
            logbr.Write((UInt16)0); // checksum
            logbr.Flush();

            // ..then, seek back so it will be overwritten when the next log entry is written
            logbr.BaseStream.Seek(-(4 + 4 + 2), SeekOrigin.Current);
        }

        public static void InitLogSegmentStream(Stream log) {
            _doLogEnd(new BinaryWriter(log));
        }

        private void addLogPacket(byte[] logpacket) {
            if (currentLogHeadStream == null) {
                throw new Exception("log is not ready for writes, call prepareLog() ! ");
            }

            // (1) if there is not room on the current log segment, skip to the next segment
            {
                long pos = currentLogHeadStream.Position;
                if (debugLogSegments) {
                    // this helps us debug log segments by only allowing a single log packet per segment
                    // TODO: change the tests to actually "fill" log segments, and remove this.
                    if (pos != 0) {
                        this.advanceActiveLogSegment();
                    }
                } else {
                    
                    if ((pos + logpacket.Length + LOG_SEGMENT_RESERVE) > currentLogSegmentInfo.logsegment_size) {
                        // too big to fit!  Advance the curreng log segment;
                        this.advanceActiveLogSegment();
                    }
                }
            }

            // (2) check to make sure it will fit in this segment
            {
                long pos = currentLogHeadStream.Position;
             
                if ((pos + logpacket.Length + LOG_END_MARKER_SIZE) > currentLogSegmentInfo.logsegment_size) {
                    // too big to fit in an empty segment! 
                    throw new Exception("log packet too big to fit in log segment!");
                }
            }

            // (3) add the log packet

            BinaryWriter bw = new BinaryWriter(currentLogHeadStream);
            bw.Write(logpacket);

            _doLogEnd(bw);            
        }

        internal long flushPending(handleLogFlushUpkeep locked_cleanup) {
            byte[] cmds;
            long curLWSN;
            BinaryWriter newChunkBuffer = new BinaryWriter(new MemoryStream());
            bool shouldCleanupCheckpoint;
            
            lock (this) {
                shouldCleanupCheckpoint = checkpointReady;

                // grab the current chunkbuffer
                cmds = ((MemoryStream)(nextChunkBuffer.BaseStream)).ToArray();
                curLWSN = _logWaitSequenceNumber;

                // make a clean chunkbuffer
                this._logWaitSequenceNumber++; // increment the wait sequence number
                nextChunkBuffer = newChunkBuffer;

                locked_cleanup();
            }

            // construct the raw log packet
            if (cmds.Length != 0) {
                UInt16 checksum = Util.Crc16.Instance.ComputeChecksum(cmds);

                MemoryStream ms = new MemoryStream();
                BinaryWriter logbr = new BinaryWriter(ms);
                logbr.Write((UInt32)LogWriter.LOG_MAGIC);
                logbr.Write((UInt32)cmds.Length);
                logbr.Write((UInt16)checksum);
                logbr.Write(cmds);
                logbr.Flush();

                // hand the log packet to the log segment handler
                addLogPacket(ms.ToArray());
            }

            if (shouldCleanupCheckpoint) {
                lock (this) {
                    foreach (RootBlockLogSegment seg in checkpoint_log_segments) {
                        active_log_segments.Remove(seg);
                        empty_log_segments.Add(seg);
                    }
                    checkpointReady = false;
                    checkpoint_log_segments = null;
                }
            }

            return curLWSN;
        }

        public void Dispose() {
            // TODO: close all log segment streams
            if (this.currentLogHeadStream != null) { this.currentLogHeadStream.Close(); this.currentLogHeadStream = null; }
        }
    }


}
