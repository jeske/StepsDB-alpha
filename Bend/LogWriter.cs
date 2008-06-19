// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;
using System.Collections.Generic;

// TODO: handle circular log
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
    // 0 -> MAX_ROOTBLOCK_SIZE : root block
    // MAX_ROOTBLOCK_SIZE -> (MAX_ROOTBLOCK_SIZE+logsize) : log
    // (remainder) : freespace


    // [StructLayout(LayoutKind.Sequential,Pack=1)]
    struct RootBlock
    {
        public uint magic;
        // public uint mysize;
        public uint logstart;   // absolute pointer to the start of the log on the region/volume
        public uint logsize;    // size of the current log segment in bytes
        public uint loghead;    // relative pointer to the head of the log
        public uint root_checksum;

        // static values don't consume space
        public static uint MAGIC = 0xFE82a292;
        public static uint MAX_ROOTBLOCK_SIZE = 4096;

        public bool IsValid() {
            if (magic != MAGIC) {
                return false;
            }
            // check size
            // check root_checksum
            return true;
        }
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
        public LogWriter(InitMode mode, IRegionManager regionmgr)
            : this() {
            if (mode != InitMode.NEW_REGION) {
                throw new Exception("init method must be called with NEW_REGION init");
            }

            // TODO: establish the size of the region
            int regionsize = 20 * 1024 * 1024;

            // test to see if there is already a root record there
            {
                Stream testrootblockstream = regionmgr.readRegionAddr(0);
                if (testrootblockstream != null) {
                    RootBlock nroot = Util.readStruct<RootBlock>(testrootblockstream);
                    long rtblksz = testrootblockstream.Position;
                    if (nroot.IsValid()) {
                        // we should be careful not to override this...
                    }
                    testrootblockstream.Close();
                } 

            }

            // create the log and root record
            root.magic = RootBlock.MAGIC;
            root.logstart = RootBlock.MAX_ROOTBLOCK_SIZE;
            root.logsize = LogWriter.DEFAULT_LOG_SIZE;
            root.loghead = 0;
            root.root_checksum = 0;
            Stream rootblockwritestream = regionmgr.writeRegionAddr(0);
            Stream logwritestream = regionmgr.writeRegionAddr(RootBlock.MAX_ROOTBLOCK_SIZE);


            this.root = root;
            this.logstream = logwritestream;
            this.rootblockstream = rootblockwritestream;

            // fill the log empty 
            logstream.Seek(root.logsize - 1, SeekOrigin.Begin);
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

}