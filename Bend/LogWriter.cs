using System;
using System.IO;

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
            logstream.Seek(root.logsize, SeekOrigin.Begin);

            // write the initial "log-end" record
            logstream.Seek(0, SeekOrigin.Begin);
            // TODO: write the record
            logstream.Flush();

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

            this.logstream = regionmgr.readRegionAddr(root.logstart);
            recoverLog();
        }

        static UInt32 LOG_MAGIC = 0x11223344;

        void recoverLog() {
            logstream.Seek(root.loghead, SeekOrigin.Begin);
            bool LogEndReached = false;
            BinaryReader br = new BinaryReader(logstream);

            while (!LogEndReached) {
                UInt32 magic = br.ReadUInt32();
                if (magic != LOG_MAGIC) {
                    abortCorrupt("invalid magic");
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

    class LogTests
    {
        public void TestLogRead() {
            // we need to provide a sample log to read
        }
    }
}