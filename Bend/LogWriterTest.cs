// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

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


            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\test\\1");  // TODO, create random directory
            {
                LogWriter lr = new LogWriter(InitMode.NEW_REGION, rmgr);
                lr.Dispose();
            }

            Stream rootblockstream = rmgr.readRegionAddr(0).getNewAccessStream();
            Stream logstream = rmgr.readRegionAddr(RootBlock.MAX_ROOTBLOCK_SIZE).getNewAccessStream();

            // check rootblock
            RootBlock root = new RootBlock();
            root.magic = RootBlock.MAGIC;
            root.logstart = RootBlock.MAX_ROOTBLOCK_SIZE;
            root.logsize = LogWriter.DEFAULT_LOG_SIZE;
            root.loghead = 0;
            rootblockstream.Seek(0, SeekOrigin.Begin);
            RootBlock root2 = Bend.Util.readStruct<RootBlock>(rootblockstream);
            Assert.AreEqual(root, root2, "root block written correctly");

            // check that log contains magic and final log record
            logstream.Seek(0, SeekOrigin.Begin);

            rootblockstream.Close();
            logstream.Close();
        }

        class TestReceiver : ILogReceiver
        {
            public struct cmdstruct
            {
                public byte cmd;
                public byte[] cmdbytes;
            }
            public List<cmdstruct> cmds;
            public TestReceiver() {
                cmds = new List<cmdstruct>();
            }
            public void handleCommand(byte cmd, byte[] cmdbytes) {
                cmdstruct newcmd = new cmdstruct();
                newcmd.cmd = cmd;
                newcmd.cmdbytes = cmdbytes;

                this.cmds.Add(newcmd);
            }
        }

        [Test]
        public void T00_ResumeEmpty() {
            T00_LogInit();
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\test\\1");
            TestReceiver receiver = new TestReceiver();
            LogWriter lr = new LogWriter(InitMode.RESUME, rmgr, receiver);
            // TODO: add a log handler that asserts there were no log events
            Assert.AreEqual(receiver.cmds.Count, 0, "there should be no log records");
        }

        [Test]
        public void T00_ResumeWithRecords() {
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\test\\2");

            byte cmd = 0x01;
            byte[] cmddata = { 0x81, 0x82, 0x83 };

            // make a new empty log
            {

                LogWriter lr = new LogWriter(InitMode.NEW_REGION, rmgr);

                // add ONE record to the log
                long logWaitNumber=0;
                lr.addCommand(cmd, cmddata, ref logWaitNumber);
                lr.flushPendingCommands();
                lr.Dispose();
            }
            // reinit and resume from the log
            {
                TestReceiver receiver = new TestReceiver();
                LogWriter lr = new LogWriter(InitMode.RESUME, rmgr, receiver);

                Assert.AreEqual(receiver.cmds.Count, 1, "there should be one record");
                Assert.AreEqual(receiver.cmds[0].cmd, cmd, "cmdbyte should match");
                Assert.AreEqual(receiver.cmds[0].cmdbytes, cmddata, "cmddata should match");
                lr.Dispose();
            }
            // assert the log had the records
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