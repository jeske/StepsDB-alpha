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
using System.IO;
using System.Collections.Generic;

using NUnit.Framework;

using Bend;

namespace BendTests
{
    // ---------------------------------[ LogTests ]-----------------------------------------------------------------

    [TestFixture]
    public class A02_LogTests
    {
        [Test]
        public void T00_LogInit() {
            int system_reserved_space;

            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\BENDtst\\1");  // TODO, create random directory
            {
                
                LogWriter lr = LogWriter.LogWriter_NewRegion(rmgr, null, out system_reserved_space);                    
                lr.Dispose();
            }

            // open the rootblock stream...
            Stream rootblockstream = rmgr.readRegionAddr(0).getNewAccessStream();
                
            // read rootblock header
            rootblockstream.Seek(0, SeekOrigin.Begin);
            RootBlockHeader rootblockdata = Bend.Util.readStruct<RootBlockHeader>(rootblockstream);

            // check rootblock data
            {
                Assert.AreEqual(rootblockdata.magic, RootBlockHeader.ROOTMAGIC, "root block magic");
                Assert.AreEqual(rootblockdata.num_logsegments, LogWriter.DEFAULT_LOG_SEGMENTS);
                // TODO: check checksum
            }

            // read / check each log segment
            for (int i = 0; i < rootblockdata.num_logsegments; i++) {
                RootBlockLogSegment seg = Bend.Util.readStruct<RootBlockLogSegment>(rootblockstream);
                
                Stream logstream = rmgr.readRegionAddr(seg.logsegment_start).getNewAccessStream();
                logstream.Seek(0, SeekOrigin.Begin);

                // check that each log segment contains a valid closed log record
                logstream.Close();
            }

            rootblockstream.Close();
        }

        class TestForceCheckpointException : Exception { } 
        class TestReceiver : ILogReceiver
        {            
            public List<LogCmd> cmds;

            public TestReceiver() {
                cmds = new List<LogCmd>();
            }            
            
            public void handleCommand(LogCommands cmd, byte[] cmdbytes) {
                LogCmd newcmd = new LogCmd();
                newcmd.cmd = cmd;
                newcmd.cmddata = cmdbytes;
                
                // accumulate the command
                this.cmds.Add(newcmd);
            }

            public void requestLogExtension() {
                throw new TestForceCheckpointException();
            }

            public void logStatusChange(long logUsedBytes, long logFreeBytes) {
                Console.WriteLine("logStatusChange: {0} used, {1} free", logUsedBytes, logFreeBytes);
            }
        }

        [Test]
        public void T00_ResumeEmpty() {
            T00_LogInit();
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\BENDtst\\1");
            TestReceiver receiver = new TestReceiver();            
            LogWriter lr = LogWriter.LogWriter_Resume(rmgr, receiver);
                        
            Assert.AreEqual(receiver.cmds.Count, 0, "there should be no log records");
        }

        public class DummyLogReceiver : ILogReceiver {
            public void handleCommand(LogCommands cmd, byte[] cmddata) {
                // do nothing! 
            }
            public void requestLogExtension() {
                // do nothing! 
            }
            public void logStatusChange(long logUsedBytes, long logFreeBytes) {
                Console.WriteLine("logStatusChange: {0} used, {1} free", logUsedBytes, logFreeBytes);
            }
        }
        

        [Test]
        public void T00_ResumeWithRecords() {
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\BENDtst\\2");
            
            byte[] cmddata = { 0x81, 0x82, 0x83 };
            const int NUM_COMMANDS = 3;

            // make a new empty log
            {
                int system_reserved_space;
                LogWriter lr = LogWriter.LogWriter_NewRegion(rmgr, new DummyLogReceiver(), out system_reserved_space);
                lr.log_handler.setDebugLogSegments();

                Assert.AreEqual(1, lr.log_handler.activeLogSegments, "one active log segment");

                // add NUM_COMMANDS records to the log
                long logWaitNumber;
                for (int i=0;i<NUM_COMMANDS;i++) {
                    lr.addCommand(LogCommands.UPDATE, cmddata, out logWaitNumber);
                    lr.flushPendingCommands();

                    printLogStatus(lr, String.Format("addCommand iteration {0}", i));
                }                
                lr.Dispose();
            }
            // reinit and resume from the log
            {
                TestReceiver receiver = new TestReceiver();                
                LogWriter lr = LogWriter.LogWriter_Resume(rmgr, receiver);

                Assert.AreEqual(NUM_COMMANDS, lr.log_handler.activeLogSegments, "should be NUM_COMMANDS log segments");
                Assert.AreEqual(NUM_COMMANDS, receiver.cmds.Count, "number of log records incorrect");
                Assert.AreEqual(LogCommands.UPDATE, receiver.cmds[0].cmd, "cmdbyte should match");
                Assert.AreEqual(cmddata, receiver.cmds[0].cmddata, "cmddata should match");
                lr.Dispose();
            }
            // assert the log had the records
        }


        private void printLogStatus(LogWriter lw, string status) {
            LogSegmentsHandler lsh = lw.log_handler;
            Console.WriteLine("---- Log Segments {0} used, {1} free --- [{2}]", 
                lsh.activeLogSegments, lsh.emptyLogSegments, status);
        }

        [Test]
        public void T01_LogCheckpoint() {
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\BENDtst\\2");
            
            byte[] cmddata = { 0x81, 0x82, 0x83 };
            long logWaitNumber; 

            {
                // make a new empty log
                TestReceiver receiver = new TestReceiver();     
                int system_reserved_space;
                LogWriter lr = LogWriter.LogWriter_NewRegion(rmgr, receiver, out system_reserved_space);
                lr.log_handler.setDebugLogSegments(); // force one command per log segment

                // find out how many empty segments there are...
                int emptySegments = lr.log_handler.emptyLogSegments;

                // add a command to fill up the log segments..
                
                for (int i = 0; i < emptySegments; i++) {
                    lr.addCommand(LogCommands.UPDATE, cmddata, out logWaitNumber);
                    lr.flushPendingCommandsThrough(logWaitNumber);
                    printLogStatus(lr, String.Format("filling empty segments {0}",i));
                }
                Assert.AreEqual(1, lr.log_handler.emptyLogSegments, "should be no empty log segments");

                printLogStatus(lr, "log almost full");

                // now checkpoint the log
                lr.checkpointStart();
                                    
                lr.checkpointDrop(out logWaitNumber);
                lr.flushPendingCommandsThrough(logWaitNumber);
                                
                Assert.LessOrEqual(2, lr.log_handler.activeLogSegments, "should have only <= 2 active log segments");
                printLogStatus(lr, "log checkpoint complete");

                lr.Dispose();
            }
        }

        [Test]

        public void T02_LogCheckpointResumeOrder() {
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\BENDtst\\2");

            byte[] cmddata = { 0x00 };
            int timestamp = 5;
            long logWaitNumber;

            {
                // make a new empty log
                TestReceiver receiver = new TestReceiver();
                int system_reserved_space;
                LogWriter lr = LogWriter.LogWriter_NewRegion(rmgr, receiver, out system_reserved_space);
                lr.log_handler.setDebugLogSegments(); // force one command per log segment

                // find out how many empty segments there are...
                

                // (1) add commands to fill up the log segments..
                {
                    int emptySegments = lr.log_handler.emptyLogSegments;
                    for (int i = 0; i < emptySegments; i++) {
                        cmddata[0] = (byte)timestamp;
                        timestamp++;
                        lr.addCommand(LogCommands.UPDATE, cmddata, out logWaitNumber);
                        lr.flushPendingCommandsThrough(logWaitNumber);
                        printLogStatus(lr, String.Format("filling empty segments {0}", i));
                    }
                    Assert.AreEqual(1, lr.log_handler.emptyLogSegments, "should be no empty log segments 1");

                    printLogStatus(lr, "log almost full");
                }

                // (2) checkpoint the log
                {
                    lr.checkpointStart();

                    lr.checkpointDrop(out logWaitNumber);
                    lr.flushPendingCommandsThrough(logWaitNumber);

                    Assert.LessOrEqual(1, lr.log_handler.activeLogSegments, "should have only <= 2 active log segments");
                    printLogStatus(lr, "log checkpoint complete");
                }

                // (3) add commands to fill newly free log segments 
                {
                    int emptySegments = lr.log_handler.emptyLogSegments;
                    for (int i = 0; i < emptySegments; i++) {
                        cmddata[0] = (byte)timestamp;
                        timestamp++;
                        lr.addCommand(LogCommands.UPDATE, cmddata, out logWaitNumber);
                        lr.flushPendingCommandsThrough(logWaitNumber);
                        printLogStatus(lr, String.Format("filling empty segments {0}", i));
                    }
                    Assert.LessOrEqual(lr.log_handler.emptyLogSegments, 1, "should be no empty log segments 2");
                    printLogStatus(lr, "log almost full 2");
                }

                // (4) shutdown
                lr.Dispose();
            }


            // (5) now resume, make sure we resume in order
            {
                TestReceiver receiver = new TestReceiver();
                LogWriter lr = LogWriter.LogWriter_Resume(rmgr, receiver);

                int cur = 0;
                foreach (var cmd in receiver.cmds) {
                    if (cmd.cmd == LogCommands.UPDATE) {
                        Console.WriteLine("Resume Record : {0} {1}", cmd.cmd.ToString(), cmd.cmddata[0]);
                        Assert.Greater(cmd.cmddata[0], cur, "order should be increasing");

                        cur = cmd.cmddata[0];
                    } else {
                        Console.WriteLine("empty command : {0}", cmd.cmd.ToString());
                    }
                }
            }
        }


        [Test]
        public void T01_OverflowLog() {
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\BENDtst\\2");
            
            byte[] cmddata = { 0x00 };
                        
            {
                // make a new empty log
                TestReceiver receiver = new TestReceiver();
                int system_reserved_space;
                LogWriter lr = LogWriter.LogWriter_NewRegion(rmgr, receiver, out system_reserved_space);
                lr.log_handler.setDebugLogSegments(); // force one command per log segment

                // find out how many empty segments there are...
                int emptySegments = lr.log_handler.emptyLogSegments;

                // add a command to fill up the log segments..
                long logWaitNumber = 0;
                for (int i = 0; i <= emptySegments; i++) {
                    lr.addCommand(LogCommands.UPDATE, cmddata, out logWaitNumber);
                    lr.flushPendingCommands();
                }
                Assert.AreEqual(0, lr.log_handler.emptyLogSegments, "should be no empty log segments");

                // now add another command.. which should overflow the log segments and send us the log-extend request

                // make sure a write attempt blocks (or fails)

                // make sure a log-extend still succeeds.


                Assert.Fail("test not finished");

                lr.Dispose();
            }            
        }


        
        
        // TEST log hitting full-state (and erroring)
        // TEST log full does not obliterate the start of the log
        // TEST log truncate
        // TEST log re-circulation

        // TEST log data commit and recovery (randomly generated data)
        // TEST log corruption (write over valid log data and recover)
        // TEST log corruption error & "abort" setting 
        // TEST log corruption error & "perserve log and continue" setting 
    }

}