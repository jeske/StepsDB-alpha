// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;

using NUnit.Framework;

// TODO: handle circular log
// TODO: reserve enough space for a log truncation record, prevent us from "filling" the log without a
//       special flag saying we are allowed to take up the reserved space

namespace Bend
{

    class LogWriter
    {
        RootBlock root;
        Stream rootblockstream;
        Stream logstream;
        BinaryWriter nextChunkBuffer;

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
        public LogWriter(InitMode mode, IRegionManager regionmgr)
            : this() {
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
            bool LogEndReached = false;
            BinaryReader br = new BinaryReader(logstream);

            while (!LogEndReached) {
                UInt32 magic = br.ReadUInt32();
                if (magic != LOG_MAGIC) {
                    abortCorrupt("invalid magic: " + magic );
                }
                UInt32 chunksize = br.ReadUInt32();
                UInt16 checksum = br.ReadUInt16();
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
                    recoverCommand(cmdtype, cmdbytes);
                }
                // log resume complete...
            }
        }
        void recoverCommand(byte cmdtype, byte[] cmdbytes) {

        }
        void addCommand(byte cmdtype, byte[] cmdbytes) {
            nextChunkBuffer.Write((UInt32)cmdbytes.Length);
            nextChunkBuffer.Write((byte)cmdtype);
            nextChunkBuffer.Write(cmdbytes, 0, cmdbytes.Length);
        }
        public void flushPendingCommands() {
            nextChunkBuffer.Seek(0, SeekOrigin.Begin);
            byte[] cmds = ((MemoryStream)(nextChunkBuffer.BaseStream)).ToArray();

            UInt16 checksum = Util.Crc16.Instance.ComputeChecksum(cmds);

            BinaryWriter logbr = new BinaryWriter(logstream);
            logbr.Write((UInt32)LOG_MAGIC);
            logbr.Write((UInt32)cmds.Length);
            logbr.Write((UInt16)checksum);
            logbr.Write(cmds);

            long savePosition = logbr.BaseStream.Position;
            // TODO: write "end of log" marker  (magic, size=0, checksum=0);
            logbr.Write((UInt32)LOG_MAGIC);
            logbr.Write((UInt32)0); // size
            logbr.Write((UInt16)0); // checksum
            
            // ..then, seek back so it will be overwritten when the next log entry is written
            logbr.BaseStream.Seek(savePosition, SeekOrigin.Begin);
            logbr.Flush();
            nextChunkBuffer = new BinaryWriter(new MemoryStream());
        }

        void abortCorrupt(String reason) {
            throw new Exception(String.Format("aborting from corrupt log near {0}, reason: {1}",
                logstream.Position, reason));
        }
    }

    [TestFixture]
    public class LogTests
    {
        [Test]
        public void TestLogInit() {
            
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION,"c:\\test");  // TODO, create random directory
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

        [Test]
        public void TestResumeEmpty() {
            IRegionManager rmgr = new RegionExposedFiles(InitMode.NEW_REGION, "c:\\test");
            LogWriter lr = new LogWriter(InitMode.RESUME, rmgr);
        }

        // TEST log resume with records
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