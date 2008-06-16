// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;
using System.Collections.Generic;

using NUnit.Framework;

// TODO: handle circular log
// TODO: reserve enough space for a log truncation record, prevent us from "filling" the log without a
//       special flag saying we are allowed to take up the reserved space

namespace Bend
{
    interface ILogReceiver
    {
        void handleCommand(byte cmd, byte[] cmddata);
    }


    class LogWriter : IDisposable
    {
        RootBlock root;
        Stream rootblockstream;
        Stream logstream;
        BinaryWriter nextChunkBuffer;
        ILogReceiver receiver;

        static UInt32 LOG_MAGIC = 44332211;
        public static UInt32 DEFAULT_LOG_SIZE = 8 * 1024 * 1024;

        LogWriter() {
            nextChunkBuffer = new BinaryWriter(new MemoryStream());
        }
        // special "init" of a region
        public LogWriter(InitMode mode, Stream logstream, Stream rootblockstream, RootBlock root)
            : this() {
            if (mode != InitMode.NEW_REGION)  {
                throw new Exception("init method must be called with NEW_REGION init");
            }
            this.root = root;
            this.logstream = logstream;
            this.rootblockstream = rootblockstream;

            // fill the log empty 
            logstream.Seek(root.logsize-1, SeekOrigin.Begin);
            logstream.WriteByte(0x00);
            logstream.Flush();

            // write the initial "log-end" record
            logstream.Seek(0, SeekOrigin.Begin);
            flushPendingCommands();  // there should be no pending commands

            // now write the root record
            rootblockstream.Seek(0, SeekOrigin.Begin);
            Util.writeStruct(root, rootblockstream);
            rootblockstream.Flush();
        }

        // standard open/resume
        public LogWriter(InitMode mode, IRegionManager regionmgr, ILogReceiver receiver)
            : this() {
            this.receiver = receiver;

            if (mode != InitMode.RESUME)  {
                throw new Exception("init method must be called with RESUME init");
            }
            this.rootblockstream = regionmgr.readRegionAddr(0);
            root = Util.readStruct<RootBlock>(rootblockstream);
            if (!root.IsValid()) {
                throw new Exception("invalid root block");
            }
            this.logstream = regionmgr.readRegionAddr(root.logstart);
            recoverLog();
        }

        void recoverLog() {
            logstream.Seek(root.loghead, SeekOrigin.Begin);
            BinaryReader br = new BinaryReader(logstream);

            while (true) {
                UInt32 magic = br.ReadUInt32();
                if (magic != LOG_MAGIC) {
                    abortCorrupt("invalid magic: " + magic );
                }
                UInt32 chunksize = br.ReadUInt32();
                UInt16 checksum = br.ReadUInt16();

                if (chunksize == 0 && checksum == 0) {
                    // we reached the end-of-chunks marker, seek back
                    br.BaseStream.Seek(- (4 + 4 + 2), SeekOrigin.Current);
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
                    receiver.handleCommand(cmdtype, cmdbytes);
                }
                // log resume complete...
            }
        }
        public void addCommand(byte cmdtype, byte[] cmdbytes) {
            nextChunkBuffer.Write((UInt32)cmdbytes.Length);
            nextChunkBuffer.Write((byte)cmdtype);
            nextChunkBuffer.Write(cmdbytes, 0, cmdbytes.Length);
        }
        public void flushPendingCommands() {
            nextChunkBuffer.Seek(0, SeekOrigin.Begin);
            byte[] cmds = ((MemoryStream)(nextChunkBuffer.BaseStream)).ToArray();
            BinaryWriter logbr = new BinaryWriter(logstream);

            if (cmds.Length != 0) {
                UInt16 checksum = Util.Crc16.Instance.ComputeChecksum(cmds);

                logbr.Write((UInt32)LOG_MAGIC);
                logbr.Write((UInt32)cmds.Length);
                logbr.Write((UInt16)checksum);
                logbr.Write(cmds);
            }

            // TODO: write "end of log" marker  (magic, size=0, checksum=0);
            logbr.Write((UInt32)LOG_MAGIC);
            logbr.Write((UInt32)0); // size
            logbr.Write((UInt16)0); // checksum
            logbr.Flush();

            // ..then, seek back so it will be overwritten when the next log entry is written
            logbr.BaseStream.Seek(- (4+4+2), SeekOrigin.Current);
        
            nextChunkBuffer = new BinaryWriter(new MemoryStream());
        }

        void abortCorrupt(String reason) {
            throw new Exception(String.Format("aborting from corrupt log near {0}, reason: {1}",
                logstream.Position, reason));
        }

        public void Dispose() {
            if (this.logstream != null) { this.logstream.Close(); this.logstream = null; }
            if (this.rootblockstream != null) { this.rootblockstream.Close(); this.rootblockstream = null; }
        }
    }

    [TestFixture]
    public class LogTests
    {
        [Test]
        public void TestLogInit() {
            
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION,"c:\\test\\1");  // TODO, create random directory
            RootBlock root = new RootBlock();
            root.magic = RootBlock.MAGIC;
            root.logstart = RootBlock.MAX_ROOTBLOCK_SIZE;
            root.logsize = LogWriter.DEFAULT_LOG_SIZE;
            root.loghead = 0;
            Stream rootblockstream = rmgr.writeRegionAddr(0);
            Stream logstream = rmgr.writeRegionAddr(RootBlock.MAX_ROOTBLOCK_SIZE);

            LogWriter lr = new LogWriter(InitMode.NEW_REGION, logstream, rootblockstream, root);

            // check rootblock
            rootblockstream.Seek(0, SeekOrigin.Begin);
            RootBlock root2 = Util.readStruct<RootBlock>(rootblockstream);
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
        public void TestResumeEmpty() {
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\test\\1");
            TestReceiver receiver = new TestReceiver();
            LogWriter lr = new LogWriter(InitMode.RESUME, rmgr, receiver);
            // TODO: add a log handler that asserts there were no log events
            Assert.AreEqual(receiver.cmds.Count, 0, "there should be no log records");
        }

        [Test]
        public void TestResumeWithRecords() {
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\test\\2");
    
            byte cmd = 0x01;
            byte[] cmddata = { 0x81, 0x82, 0x83 };

            // make a new empty log
            {
                RootBlock root = new RootBlock();
                root.magic = RootBlock.MAGIC;
                root.logstart = RootBlock.MAX_ROOTBLOCK_SIZE;
                root.logsize = LogWriter.DEFAULT_LOG_SIZE;
                root.loghead = 0;
                Stream rootblockstream = rmgr.writeRegionAddr(0);
                Stream logstream = rmgr.writeRegionAddr(RootBlock.MAX_ROOTBLOCK_SIZE);

                LogWriter lr = new LogWriter(InitMode.NEW_REGION, logstream, rootblockstream, root);
                
                // add ONE record to the log
                lr.addCommand(cmd, cmddata);
                lr.flushPendingCommands();
                rootblockstream.Close();
                logstream.Close();
            }
            // reinit and resume from the log
            {
                TestReceiver receiver = new TestReceiver();
                LogWriter lr = new LogWriter(InitMode.RESUME, rmgr,receiver);
                
                Assert.AreEqual(receiver.cmds.Count, 1, "there should be one record" );
                Assert.AreEqual(receiver.cmds[0].cmd, cmd, "cmdbyte should match");
                Assert.AreEqual(receiver.cmds[0].cmdbytes, cmddata, "cmddata should match");

            }
            // assert the log had the records
        }

        // TEST log hitting full-state (and erroring)
        // TEST log full does not obliterate the start of the log
        // TEST log truncate
        // TEST log re-circulation

        // TEST log random data committ and recovery
        // TEST log corruption (write over valid log data and recover)
        // TEST log corruption error & "abort" setting 
        // TEST log corruption error & "perserve log and continue" setting 
    }
}