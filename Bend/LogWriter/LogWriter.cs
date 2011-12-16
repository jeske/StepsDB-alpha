// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;
using System.Collections.Generic;

using System.Threading;

// TODO: reserve enough space for a log truncation record, prevent us from "filling" the log without a
//       special flag saying we are allowed to take up the reserved space

namespace Bend
{
    interface ILogReceiver
    {
        void handleCommand(byte cmd, byte[] cmddata);
    }

    // ---------------------------------------------------------
    // The on-disk layout is initialized as:
    // 
    // 0 -> ROOTBLOCK_SIZE : root block
    // 5 x 2MB LogSegments  (10MB of logspace)
    // (remainder) : freespace
    //
    // Within the rootblock, there is a header, and then a list of RootBlockLogSegments
    //
    // 0->sizeof(RootBlockHeader) : RootBlockHeader
    // Array[RootBlockHeader.num_logsegments] of RootBlockLogSegment
    // 
    // Log segments are not kept in order of activity, so the recovery process checks the first
    // log entry in each logsegment to figure out the proper order to process them.


    // [StructLayout(LayoutKind.Sequential,Pack=1)]
    struct RootBlockHeader 
    {
        // static values don't consume space
        public static uint ROOTBLOCK_SIZE = 4096;
        public static uint MAGIC = 0xFE82a292;

        // data....

        public uint magic;
        public uint root_checksum;

        public uint num_logsegments; // the number of log segments following the header

        // utility...

        public byte[] constructRootBlock(RootBlockLogSegment[] segments) {
            this.num_logsegments = (uint)segments.Length;
            MemoryStream ms = new MemoryStream();
            Util.writeStruct(this, ms);
            foreach (RootBlockLogSegment seg in segments) {
                Util.writeStruct(seg, ms);
            }
            return ms.ToArray();
        }
        
        public bool IsValid() {
            if (magic != MAGIC) {
                return false;
            }
            // check size
            // check root_checksum
            return true;
        }
    }

    public struct RootBlockLogSegment 
    {
        public uint logsegment_start;   // absolute pointer to the start of the log on the region/volume
        public uint logsegment_size;    // size of the log segment in bytes        
    }

    
    public struct LogCmd {
        public byte cmd;
        public byte[] cmddata;
        public LogCmd(byte cmd, byte[] cmddata) {
            this.cmd = cmd;
            this.cmddata = cmddata;
        }
    };


    class LogWriter : IDisposable
    {
        bool USE_GROUP_COMMIT_THREAD = false;

        RootBlockHeader root;
        Stream rootblockstream;

        LogSegmentsHandler log_handler; 
        
        BinaryWriter nextChunkBuffer;
        ILogReceiver receiver;

        AutoResetEvent groupCommitWorkerHndl;
        ManualResetEvent groupCommitRequestorsHndl;
        Thread commitThread;
        bool commitThread_should_die = false;
        long logWaitSequenceNumber = 1; // this must never be zero
        long finishedLWSN = 0; 
        DateTime firstWaiter, lastWaiter;
        int numWaiters = 0;

        public static UInt32 LOG_MAGIC = 0x44332211;
        
        public static uint DEFAULT_LOG_SEGMENT_SIZE = 2 * 1024 * 1024; // 2MB
        public static uint DEFAULT_LOG_SEGMENTS = 5;  // 2MB * 5 => 10MB


        private LogWriter() {                   
            nextChunkBuffer = new BinaryWriter(new MemoryStream());

            groupCommitWorkerHndl = new AutoResetEvent(false);
            groupCommitRequestorsHndl = new ManualResetEvent(false);

            if (USE_GROUP_COMMIT_THREAD) {
                // setup the commitThread
                commitThread = new Thread(new ThreadStart(this._flushThread));
                commitThread.Start();
            }
        }

                // standard open/resume
        public LogWriter(InitMode mode, IRegionManager regionmgr, ILogReceiver receiver)
            : this() {
            this.receiver = receiver;

            switch (mode) {
                case InitMode.NEW_REGION:
                    LogWriter_NewRegion(regionmgr);
                    LogWriter_Resume(regionmgr);   // need to setup the logsegmenthandler
                    break;
                case InitMode.RESUME:
                    LogWriter_Resume(regionmgr);
                    break;
                default:
                    throw new Exception("unknown init mode: " + mode);                    
            }

            log_handler.prepareLog(); // prepare log for accepting writes
        }



        // special "init" of a region
        private void LogWriter_NewRegion(IRegionManager regionmgr) { 
            // test to see if there is already a root record there
            {
                try {
                    Stream testrootblockstream = regionmgr.readRegionAddr(0).getNewAccessStream();

                    RootBlockHeader nroot = Util.readStruct<RootBlockHeader>(testrootblockstream);
                    long rtblksz = testrootblockstream.Position;
                    if (nroot.IsValid()) {
                        // TODO: we should be careful not to overwrite an existing root record...
                        
                        // throw new Exception("existing root record present! Can't re-init.");
                    }
                    testrootblockstream.Close();
                } catch (RegionExposedFiles.RegionMissingException) {
                    
                }
            }

            // (1) initialize the log segments...
            RootBlockLogSegment[] log_segments = new RootBlockLogSegment[LogWriter.DEFAULT_LOG_SEGMENTS];

            uint alloc_address = RootBlockHeader.ROOTBLOCK_SIZE;
            for (int i=0;i<log_segments.Length;i++) {

                // calculate the segment start and length
                log_segments[i].logsegment_size = LogWriter.DEFAULT_LOG_SEGMENT_SIZE;
                log_segments[i].logsegment_start = alloc_address;

                alloc_address += LogWriter.DEFAULT_LOG_SEGMENT_SIZE;

                // open the segment 
                Stream logstream = regionmgr.writeFreshRegionAddr(log_segments[i].logsegment_start, log_segments[i].logsegment_size).getNewAccessStream();
               
                // write to the end of the log to be sure the file/stream has enough space
                logstream.Seek(log_segments[i].logsegment_size - 1, SeekOrigin.Begin);
                logstream.WriteByte(0x00);
                logstream.Flush();
                logstream.Seek(0, SeekOrigin.Begin);

                LogSegmentsHandler.InitLogSegmentStream(logstream);
                // write the initial "log-end" record at the beginning of the segment
                
                logstream.Close();
            }

            
            root.magic = RootBlockHeader.MAGIC;            
            root.root_checksum = 0;
            byte[] root_block_data = root.constructRootBlock(log_segments);

            // open the streams...
            Stream rootblockwritestream = regionmgr.writeFreshRegionAddr(0, (long)RootBlockHeader.ROOTBLOCK_SIZE).getNewAccessStream();           

            // this.logstream = logwritestream;
            this.rootblockstream = rootblockwritestream;

            // now write the root record
            rootblockstream.Seek(0, SeekOrigin.Begin);
            rootblockstream.Write(root_block_data,0,root_block_data.Length);            
            rootblockstream.Flush();
            rootblockstream.Close(); // must close the stream so resume can operate
        }

        private void LogWriter_Resume(IRegionManager regionmgr) {
            this.rootblockstream = regionmgr.readRegionAddr(0).getNewAccessStream();
            root = Util.readStruct<RootBlockHeader>(rootblockstream);
            if (!root.IsValid()) {
                throw new Exception("invalid root block");
            }
            RootBlockLogSegment[] log_segments = new RootBlockLogSegment[root.num_logsegments];
            for (int i=0;i<root.num_logsegments;i++) {
                log_segments[i] = Util.readStruct<RootBlockLogSegment>(rootblockstream);
            }

            // setup the log segment handler
            this.log_handler = new LogSegmentsHandler(regionmgr, log_segments);

            foreach (LogCmd cmd in log_handler.recoverLogCmds()) {
                 receiver.handleCommand(cmd.cmd, cmd.cmddata);     
            }
        }

        public void addCommand_NoLog(byte cmdtype, byte[] cmdbytes) {
            receiver.handleCommand(cmdtype, cmdbytes);
        }

        public void addCommand(byte cmdtype, byte[] cmdbytes, ref long logWaitNumber) {
            lock (this) {
                nextChunkBuffer.Write((UInt32)cmdbytes.Length);
                nextChunkBuffer.Write((byte)cmdtype);
                nextChunkBuffer.Write(cmdbytes, 0, cmdbytes.Length);
                logWaitNumber = this.logWaitSequenceNumber;
            }
            // we always expect the ILogReceiver to actuall apply the command
            receiver.handleCommand(cmdtype, cmdbytes);
        }

        public void addCommands(List<LogCmd> cmds, ref long logWaitNumber) {
            // TODO: make this atomically add ALL the log commands at once, not
            //  one by one! 
            lock (this) {
                foreach (var log_entry in cmds) {
                    this.addCommand(log_entry.cmd, log_entry.cmddata, ref logWaitNumber);
                }
            }
        }


        public void flushPendingCommands() {
            long waitForLWSN;
            lock (this) {
                waitForLWSN = this.logWaitSequenceNumber;
            }
            flushPendingCommandsThrough(waitForLWSN);
        }

        public void flushPendingCommandsThrough(long waitForLWSN) {
            if (USE_GROUP_COMMIT_THREAD) {
                DateTime started_waiting_at = DateTime.Now;
                if (finishedLWSN >= waitForLWSN) {
                    return;
                }
                lock (this) {
                    lastWaiter = started_waiting_at;
                    if (firstWaiter < started_waiting_at) {
                        firstWaiter = started_waiting_at;
                    }
                    numWaiters++;
                }
                do {
                    if ((DateTime.Now - started_waiting_at).TotalMilliseconds > 30000) {
                        throw new Exception("30s flush timeout exceeded");
                    }

                    // TODO: change this use a monitor instead! 

                    // groupCommitWorkerHndl.Set(); // wakeup the worker                
                    // groupCommitRequestorsHndl.WaitOne();

                    WaitHandle.SignalAndWait(groupCommitWorkerHndl, groupCommitRequestorsHndl);
                    //if (this.finishedLWSN < waitForLWSN) {
                    //    System.Console.WriteLine("still waiting... {0} < {1}",
                    //        this.finishedLWSN,waitForLWSN);
                    //}
                } while (this.finishedLWSN < waitForLWSN);
            } else {
                // not using the group commit thread
                _doWritePendingCmds();
            }
           
        }

        private void _flushThread() {

            while (true) {                
                Thread.Sleep(1);
                groupCommitWorkerHndl.WaitOne();
                if (commitThread_should_die) {
                    return;
                }
                _doWritePendingCmds();
            }
        }
        private void _doWritePendingCmds() {
            int groupSize;
            double groupDuration;
            byte[] cmds;
            long curLWSN;
            DateTime curFirstWaiter;
            BinaryWriter newChunkBuffer = new BinaryWriter(new MemoryStream());
            ManualResetEvent wakeUpThreads;

            lock (this) {                
                // grab the current chunkbuffer
                cmds = ((MemoryStream)(nextChunkBuffer.BaseStream)).ToArray();
                curLWSN = this.logWaitSequenceNumber;
                groupSize = numWaiters;
                curFirstWaiter = firstWaiter;

                // grab the monitor handle
                wakeUpThreads = this.groupCommitRequestorsHndl;
                this.groupCommitRequestorsHndl = new ManualResetEvent(false);

                // make a clean chunkbuffer
                this.logWaitSequenceNumber++; // increment the wait sequence number
                nextChunkBuffer = newChunkBuffer;
                numWaiters = 0;
                firstWaiter = DateTime.MinValue;
            }

            // construct the raw log packet
            if (cmds.Length != 0) {
                UInt16 checksum = Util.Crc16.Instance.ComputeChecksum(cmds);

                MemoryStream ms = new MemoryStream();
                BinaryWriter logbr = new BinaryWriter(ms);
                logbr.Write((UInt32)LOG_MAGIC);
                logbr.Write((UInt32)cmds.Length);
                logbr.Write((UInt16)checksum);
                logbr.Write(cmds);
                logbr.Flush();

                // hand the log packet to the log segment handler
                this.log_handler.addLogPacket(ms.ToArray());
            }
                
            // reset the group commit waiting machinery
            {                
                finishedLWSN = curLWSN;
                groupDuration = (DateTime.Now - curFirstWaiter).TotalMilliseconds;
                wakeUpThreads.Set();
                
            }
        
            //if (groupSize > 2) {
            //    System.Console.WriteLine("group commit {0}, longest wait {1} ms", 
            //        groupSize, groupDuration);
            //}

        
        }


        public void Dispose() {
            System.Console.WriteLine("LogWriter Dispose");

            if (this.commitThread != null) {
                commitThread_should_die = true;
                groupCommitWorkerHndl.Set();
                if (!this.commitThread.Join(2)) {
                    System.Console.WriteLine("commitThread join Timeout");
                    this.commitThread.Abort();
                } else {
                    System.Console.WriteLine("commitThread rejoined");
                }
            } else {
                System.Console.WriteLine("no committhread to dispose of");
            }
            if (this.log_handler != null) { this.log_handler.Dispose(); this.log_handler = null; }            
            if (this.rootblockstream != null) { this.rootblockstream.Close(); this.rootblockstream = null; }
            
        }
    }

}