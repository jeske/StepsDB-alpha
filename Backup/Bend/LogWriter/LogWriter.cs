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
        public static uint ROOTMAGIC = 0xFE82a292;

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
            if (magic != ROOTMAGIC) {
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
        public LogCommands cmd;
        public byte[] cmddata;
        public LogCmd(LogCommands cmd, byte[] cmddata) {
            this.cmd = cmd;
            this.cmddata = cmddata;
        }
    };


    // TODO: the log needs to understand CHECKPOINT_START and CHECKPOINT_DROP but it doesn't need to understand
    //        other command types. How do we better encapsulate this?  
    public enum LogCommands {
        UPDATE = 0,
        CHECKPOINT_START = 1,
        CHECKPOINT_DROP = 2   // says it's okay to drop everything up to the previous CHECKPOINT_START
    }

    public class LogWriter : IDisposable
    {
        bool USE_GROUP_COMMIT_THREAD = false;
        public static uint DEFAULT_LOG_SEGMENT_SIZE = 2 * 1024 * 1024; // 2MB
        public static uint DEFAULT_LOG_SEGMENTS = 5;  // 2MB * 5 => 10MB

        RootBlockHeader root;
        Stream rootblockstream;

        public LogSegmentsHandler log_handler; 
        
        public ILogReceiver receiver;
        IRegionManager regionmgr;

        AutoResetEvent groupCommitWorkerHndl;
        ManualResetEvent groupCommitRequestorsHndl;
        Thread commitThread;
        bool commitThread_should_die = false;        
        long finishedLWSN = 0; 
        DateTime firstWaiter, lastWaiter;
        int numWaiters = 0;

        public LogWriter(IRegionManager regionmgr) {
            this.regionmgr = regionmgr;     
            groupCommitWorkerHndl = new AutoResetEvent(false);
            groupCommitRequestorsHndl = new ManualResetEvent(false);

            if (USE_GROUP_COMMIT_THREAD) {
                // setup the commitThread
                commitThread = new Thread(new ThreadStart(this._flushThread));
                commitThread.Start();
            }
        }

        // standard open/resume
        public static LogWriter LogWriter_Resume(IRegionManager regionmgr, ILogReceiver receiver) {
            LogWriter lw = new LogWriter(regionmgr);
            lw.receiver = receiver;
            lw._InitResume(regionmgr);

            lw.log_handler.prepareLog();
            return lw;
        }

        public static LogWriter LogWriter_NewRegion(IRegionManager regionmgr, ILogReceiver receiver, out int system_reserved_space) {
            LogWriter lw = new LogWriter(regionmgr);
            lw.receiver = receiver;
            lw._InitNewRegion(regionmgr, out system_reserved_space);
            lw._InitResume(regionmgr);

            lw.log_handler.prepareLog();

            return lw;
        }


        // special "init" of a region
        private void _InitNewRegion(IRegionManager regionmgr, out int system_reserved_space) { 
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

            uint nextFreeAddress = RootBlockHeader.ROOTBLOCK_SIZE;
            for (int i=0;i<log_segments.Length;i++) {

                // calculate the segment start and length
                log_segments[i].logsegment_size = LogWriter.DEFAULT_LOG_SEGMENT_SIZE;
                log_segments[i].logsegment_start = nextFreeAddress;

                nextFreeAddress += LogWriter.DEFAULT_LOG_SEGMENT_SIZE;

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

            
            root.magic = RootBlockHeader.ROOTMAGIC;            
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

            system_reserved_space = (int)nextFreeAddress;
        }

        private void _InitResume(IRegionManager regionmgr) {
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
            this.log_handler = new LogSegmentsHandler(this,regionmgr, log_segments);

            foreach (LogCmd cmd in log_handler.recoverLogCmds()) {
                 receiver.handleCommand(cmd.cmd, cmd.cmddata);     
            }
        }

        // --- checkpoint protocol ---

        public int checkpointStart() {
            long logWaitNumber=0;
            byte[] emptydata = new byte[0];
            this.addCommand(LogCommands.CHECKPOINT_START, emptydata, out logWaitNumber);
            return 1; // checkpoint ID 
        }

        public void checkpointDrop(out long logWaitNumber) {            
            byte[] emptydata = new byte[0];
            this.addCommand(LogCommands.CHECKPOINT_DROP, emptydata, out logWaitNumber);
        }        


        // --- basic add commands ---

        public void addCommand_NoLog(LogCommands cmdtype, byte[] cmdbytes) {
            receiver.handleCommand(cmdtype, cmdbytes);
        }

        public void addCommand(LogCommands cmdtype, byte[] cmdbytes, out long logWaitNumber) {
            lock (log_handler) {
                log_handler._addCommand(cmdtype, cmdbytes, out logWaitNumber);                
            }            
            // we always expect the ILogReceiver to actually apply the command
            receiver.handleCommand(cmdtype, cmdbytes);
        }

        public void addCommands(List<LogCmd> cmds, ref long logWaitNumber) {
            // TODO: make this atomically add ALL the log commands at once, not
            //  one by one! 
            lock (log_handler) {
                foreach (var log_entry in cmds) {
                    this.addCommand(log_entry.cmd, log_entry.cmddata, out logWaitNumber);
                }
            }
        }

        public void flushPendingCommands() {
            long waitForLWSN = log_handler.logWaitSequenceNumber;
            
            flushPendingCommandsThrough(waitForLWSN);
        }

        public void flushPendingCommandsThrough(long waitForLWSN) {
            // if we're already done writing this LWSN, just return
            if (finishedLWSN >= waitForLWSN) {
                return;
            }

            // otherwise, we need to force flush...
            if (USE_GROUP_COMMIT_THREAD) {
                DateTime started_waiting_at = DateTime.Now;                
                lock (this) {
                    lastWaiter = started_waiting_at;
                    if (firstWaiter < started_waiting_at) {
                        firstWaiter = started_waiting_at;
                    }
                    numWaiters++;
                }
                do {
                    if ((DateTime.Now - started_waiting_at).TotalMilliseconds > 10000) {                        
                        throw new Exception("30s flush timeout exceeded");
                    }

                    // TODO: change this use a monitor instead! 

                    // groupCommitWorkerHndl.Set(); // wakeup the worker                
                    // groupCommitRequestorsHndl.WaitOne();

                    WaitHandle.SignalAndWait(groupCommitWorkerHndl, groupCommitRequestorsHndl,1000,true);
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
            try {
                while (true) {
                    Thread.Sleep(1);
                    groupCommitWorkerHndl.WaitOne();
                    if (commitThread_should_die) {
                        return;
                    }
                    _doWritePendingCmds();
                }
            } catch (Exception e) {
                Console.WriteLine("*\n*\n*\n*   UNCAUGHT EXCEPTION in LogWriter._flushThread() : {0}\n*\n*\n", e.ToString());
                Environment.Exit(1);
            }
        }

        private void _doWritePendingCmds() {
            int groupSize;
            double groupDuration;
            
            DateTime curFirstWaiter;            
            ManualResetEvent wakeUpThreads = null;

            groupSize = numWaiters;
            curFirstWaiter = firstWaiter;

            
            finishedLWSN = log_handler.flushPending();
            
            wakeUpThreads = this.groupCommitRequestorsHndl;
            this.groupCommitRequestorsHndl = new ManualResetEvent(false);

            numWaiters = 0;
            firstWaiter = DateTime.MinValue;
            
            // reset the group commit waiting machinery
            if (wakeUpThreads != null) {
                groupDuration = (DateTime.Now - curFirstWaiter).TotalMilliseconds;
                wakeUpThreads.Set();
            }            
              
#if false        
            // debug output.... 
            if (groupSize > 2) {
                System.Console.WriteLine("group commit {0}, longest wait {1} ms", 
                    groupSize, groupDuration);
            }
#endif
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