using System;
using System.IO;
using System.Collections.Generic;

using System.Threading;

// * About the Log
// 
// The log is organized as a set of separate log segments. Initially this is 5 segments, each 2MBs in length. 
// A log packet (which contains multiple log commands) is written contigiously to a single log segment. 
// If there is no room in the current log segment, the packet is written into the next available log segment. 
// 
// A log packet should be notable smaller than a single log segment, otherwise much space will be wasted at
// the end of each log-segment. If a log-packet is bigger than a log-segment, it can not be written.
// 
// The rootblock records a list of log segments, however, the list should not be assumed to be in proper order.
// Keeping the list in order would require re-writeing the root-block each time a log-segment was "freed"
// and rotated to the end during a checkpoint. Instead of performing this extra work frequently during writing,
// we offload this work to the "infrequent" activity of log recovery. Recovery must scan all log-segments, checking
// log-timestamps for the first record in the log, to determine the proper order of the log segments. Once
// the proper order is established, recovery can process them in the proper order. 
//
// A note about FORCED CHECKPOINTS: if the log every actually runs out of space, it initiates a forcedCheckpoint
// prototocol through ILogReceiver. This is required because of a tricky ordering problem with log entries.
// At the time that the log runs out of space, there are pending entries trying to hit the log.
// It is hard to reason about the validity of operations other than a CHECKPOINT, because they might require
// log-entries to be committed which are only valid in the context of changes already pending flush to the log.
// The records which commit a checkpoint are guaranteed to be valid, since they should make permanent any
// pending changes stuck in the log (and thus pending changes can be thrown away once the forced checkpoint completes)
//
// Because of the cost of forced-checkpoints, it's really much better if they never happen, which is why the log 
// notify's ILogReceiver when the amount of logspace is getting low so it can do a non-forced checkpoint 
// before the log actually runs out of space. 

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

namespace Bend {

    public class LogSegmentsHandler : IDisposable {
        IRegionManager regionmgr;
        LogWriter logwriter;

        List<RootBlockLogSegment> empty_log_segments = new List<RootBlockLogSegment>();
        List<RootBlockLogSegment> active_log_segments = new List<RootBlockLogSegment>();

        Stream currentLogHeadStream;
        RootBlockLogSegment currentLogSegmentInfo;

        public int activeLogSegments { get { return active_log_segments.Count; } }
        public int emptyLogSegments { get { return empty_log_segments.Count; } }

        // we always have to keep enough log reserve to handle a checkpoint so we can empty the log! 
        private static uint LOG_SEGMENT_RESERVE = LOG_END_MARKER_SIZE + LOG_CHECKPOINT_MARKER_SIZE;
        private static uint LOG_CHECKPOINT_MARKER_SIZE = 1024; // TODO: this should be configured by our client! 

        private bool debugLogSegments = false;

        public LogSegmentsHandler(LogWriter logwriter, IRegionManager regionmgr, RootBlockLogSegment[] segments) {
            this.regionmgr = regionmgr;
            this.logwriter = logwriter;

            _organizeLogSegments(segments);
        }

        public void setDebugLogSegments() {
            debugLogSegments = true;
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
                this.logwriter.receiver.forceCheckpoint();

                if (empty_log_segments.Count == 0) {
                    throw new Exception("forceCheckpoint() failed to free log segments");
                }
            }

            lock (this) {
                currentLogSegmentInfo = empty_log_segments[0];
                empty_log_segments.RemoveAt(0);
                active_log_segments.Add(currentLogSegmentInfo);                
            }

            // open the current log stream...
            currentLogHeadStream = regionmgr.writeExistingRegionAddr(currentLogSegmentInfo.logsegment_start).getNewAccessStream();

            if (empty_log_segments.Count <= 1) {
                logwriter.receiver.recommendCheckpoint();
            }
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
                        yield return ( new LogCmd(cmdtype, cmdbytes) );      
                  
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

        public void addLogPacket(byte[] logpacket) {
            if (currentLogHeadStream == null) {
                throw new Exception("log is not ready for writes, call prepareLog() ! ");
            }

            // (1) if there is not room on the current log segment, skip to the next segment
            {
                long pos = currentLogHeadStream.Position;
                if ((pos + logpacket.Length + LOG_SEGMENT_RESERVE) > currentLogSegmentInfo.logsegment_size) {
                    // too big to fit!  Advance the curreng log segment;
                    this.advanceActiveLogSegment();
                }

                if (debugLogSegments) {
                    // this helps us debug log segments by only allowing a single log packet per segment
                    
                    // TODO: change the tests to actually "fill" log segments, and remove this.
                    
                    if (pos != 0) {
                        this.advanceActiveLogSegment();
                    }
                }
            }

            // (2) check to make sure it will fit in this segment
            {
                long pos = currentLogHeadStream.Position;
                if (pos != 0) {
                    throw new Exception(String.Format("advanced to non-start of segment! pos = {0}", pos));
                }
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

        public void Dispose() {
            // TODO: close all log segment streams
            if (this.currentLogHeadStream != null) { this.currentLogHeadStream.Close(); this.currentLogHeadStream = null; }
        }
    }


}
