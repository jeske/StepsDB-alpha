using System;
using System.IO;
using System.Collections.Generic;

using System.Threading;

using System.Runtime.InteropServices;

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
//   - setup the current "log head" after recovery, to continue writing to the end
//   - FIX: recovery if we crash suring a checkpoint attempt (currently this is busted)


// DONE ( ?? ):
//   - check to see if a log entry fits into the current segment, if advance the active segment
//   - add log-timestamps to each log-packet so we can properly order log segments during recovery
//   - add a log-checkpoint / rotation protocol 
//   - FIX: read/write log packet headers to use structs instead of manual read/write code


namespace Bend {

    [StructLayout(LayoutKind.Sequential,Pack=1)]
    public struct LogPacketHeader {
        public static UInt32 LOG_MAGIC = 0x44332211;

        public UInt32 magic;
        public Int64 curLWSN;
        public UInt32 cmddata_length;
        public UInt32 checksum;        
    }

    public class LogSegmentsHandler : IDisposable {

        

        IRegionManager regionmgr;
        LogWriter logwriter;

        BinaryWriter nextChunkBuffer;
        private long _logWaitSequenceNumber = 1; // this must be recovered during recovery...

        public long logWaitSequenceNumber {
            get { return _logWaitSequenceNumber; }
        }        

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
            BDSkipList<long, RootBlockLogSegment> order_segments = new BDSkipList<long, RootBlockLogSegment>();

            // (2) scan and organize the log-segments (so they are in proper order)            
            foreach (RootBlockLogSegment seg in segments) {
                Stream logsegment = regionmgr.readRegionAddr(seg.logsegment_start).getNewAccessStream();

                BinaryReader br = new BinaryReader(logsegment);
                LogPacketHeader hdr = Util.readStruct<LogPacketHeader>(br);                
                if (hdr.magic != LogPacketHeader.LOG_MAGIC) {
                    abortCorrupt(String.Format("_organizeLogSegments() found corrupt log segment! magic => {0}", hdr.magic));
                }
                

                if (hdr.cmddata_length == 0 && hdr.checksum == 0) {                 
                    empty_log_segments.Add(seg);
                } else {
                    order_segments.Add(hdr.curLWSN, seg);                    
                }
            
                logsegment.Close();
            }

            // now insert the active segments in proper order..
            foreach (var kvp in order_segments.scanForward(ScanRange<long>.All())) {
                Console.WriteLine("active segment: {0}", kvp.Key);
                active_log_segments.Add(kvp.Value);
            }

        }


        private void abortCorrupt(String reason) {
            throw new Exception(String.Format("aborting from corrupt log, reason: {0}", reason));
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
                    LogPacketHeader hdr = Util.readStruct<LogPacketHeader>(br);
                    
                    if (hdr.magic != LogPacketHeader.LOG_MAGIC) {
                        abortCorrupt("invalid magic: " + hdr.magic);
                    }

                    if (hdr.cmddata_length == 0 && hdr.checksum == 0) {
                        // we reached the end-of-chunks marker, seek back
                        // Console.WriteLine("Seek back... pos {0}, size {1}", br.BaseStream.Position,
                        //     Util.structSize<LogPacketHeader>(ref hdr));
                        br.BaseStream.Seek(-(Util.structSize<LogPacketHeader>(ref hdr)), SeekOrigin.Current);
                        goto next_log_segment;
                    }

                    byte[] logchunk = new byte[hdr.cmddata_length];
                    if (br.Read(logchunk, 0, (int)hdr.cmddata_length) != hdr.cmddata_length) {
                        abortCorrupt("chunksize bytes not available");
                    }
                    // CRC the chunk and verify against checksum
                    UInt16 nchecksum = Util.Crc16.Instance.ComputeChecksum(logchunk);
                    if (nchecksum != hdr.checksum) {
                        abortCorrupt("computed checksum: " + nchecksum + " didn't match: " + hdr.checksum);
                    }

                    // check the LWSN order
                    if (hdr.curLWSN < _logWaitSequenceNumber) {
                        abortCorrupt("out of order recovery, packet LWSN: " + hdr.curLWSN + " < " + _logWaitSequenceNumber);
                    }
                    _logWaitSequenceNumber = hdr.curLWSN;

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

            next_log_segment:
                // close this segment to move onto the next one
                br = null;
                logstream.Close();
            } // .. foreach log segment
        }


        private static uint LOG_END_MARKER_SIZE = 16; // size of the _doLogEnd() marker

        // this writes a closing log packet and seeks back onto it
        private static void _doLogEnd(BinaryWriter logbr) {
            // write "end of log" marker  (magic, size=0, checksum=0);
            LogPacketHeader hdr;
            hdr.magic = LogPacketHeader.LOG_MAGIC;
            hdr.checksum = 0;
            hdr.curLWSN = 0;
            hdr.cmddata_length = 0;
            Util.writeStruct<LogPacketHeader>(hdr, logbr);
            logbr.Flush();

            // ..then, seek back so it will be overwritten when the next log entry is written
            // Console.WriteLine("Seek back... pos {0}, size {1}", logbr.BaseStream.Position,
            //                 Util.structSize<LogPacketHeader>(ref hdr));

            logbr.BaseStream.Seek(-(Util.structSize<LogPacketHeader>(ref hdr)),SeekOrigin.Current);            
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

                    Console.WriteLine("*\n*\n*\n*\n*      LOG RAN OUT OF SPACE\n*\n*\n*\n");
                    Environment.Exit(1);
                    // too big to fit in an empty segment! 
                    throw new Exception("log packet too big to fit in log segment!");
                }
            }

            // (3) add the log packet

            BinaryWriter bw = new BinaryWriter(currentLogHeadStream);
            bw.Write(logpacket);

            _doLogEnd(bw);            
        }

        internal long flushPending() {
            byte[] cmds;
            long curLWSN;
            BinaryWriter newChunkBuffer = new BinaryWriter(new MemoryStream());
            bool shouldCleanupCheckpoint;
            
            lock (this) {
                shouldCleanupCheckpoint = checkpointReady;
                checkpointReady = false;
                // grab the current chunkbuffer
                cmds = ((MemoryStream)(nextChunkBuffer.BaseStream)).ToArray();
                curLWSN = _logWaitSequenceNumber;

                // make a clean chunkbuffer
                this._logWaitSequenceNumber++; // increment the wait sequence number
                nextChunkBuffer = newChunkBuffer;                
            }

            // construct the raw log packet
            if (cmds.Length != 0) {
                UInt16 checksum = Util.Crc16.Instance.ComputeChecksum(cmds);

                MemoryStream ms = new MemoryStream();
                BinaryWriter logbr = new BinaryWriter(ms);

                LogPacketHeader hdr;
                hdr.magic = LogPacketHeader.LOG_MAGIC;
                hdr.curLWSN = curLWSN;
                hdr.cmddata_length = (uint)cmds.Length;
                hdr.checksum = checksum;
                Util.writeStruct<LogPacketHeader>(hdr, logbr);
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
