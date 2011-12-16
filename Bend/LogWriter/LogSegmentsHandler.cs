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
        List<RootBlockLogSegment> log_segments = new List<RootBlockLogSegment>();

        Stream currentLogHeadStream;

        public LogSegmentsHandler(IRegionManager regionmgr, RootBlockLogSegment[] segments) {
            this.regionmgr = regionmgr;
            // (1) add the log segments 
            foreach (RootBlockLogSegment seg in segments) {
                log_segments.Add(seg);
            }

            // (2) scan and organize the log-segments (so they are in proper order)            
        }

        private void abortCorrupt(String reason) {
            throw new Exception(String.Format("aborting from corrupt log reason: {0}", reason));
        }

        public void prepareLog() {            
            // (3) open the current log stream
            currentLogHeadStream = regionmgr.writeExistingRegionAddr(log_segments[0].logsegment_start).getNewAccessStream();
        }


        public IEnumerable<LogCmd> recoverLogCmds() {

            // (0) are the log segments in the right order (assume they are)

            // (1) read commands from the log segments

            foreach (RootBlockLogSegment seg in log_segments) {
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

            // (2) add the log packet

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
