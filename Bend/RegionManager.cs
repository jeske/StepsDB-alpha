using System;
using System.IO;

using System.Threading;
using System.Collections;
using System.Collections.Generic;

// TODO: raw file, raw partition region managers, tests

namespace Bend
{

    // -----------------[ IRegionManager ]---------------------------------------------------
    //
    // This exposes a linear address space called a Region. The LayerManager will divide this
    // Region into segments to hold the root block, log, and segments. We further maintain
    // the invariant that Segments are written in one sweep beginning at their start address. 
    // Once they are closed, they may be read any number of times, but they are disposed of 
    // before being written again. The log does not follow this pattern. 

    public interface IRegionManager
    {
        IRegion readRegionAddr(long region_addr);
        IRegion readRegionAddrNonExcl(long region_addr);
        IRegion writeFreshRegionAddr(long region_addr,long length);
        IRegion writeExistingRegionAddr(long region_addr);
        void disposeRegionAddr(long region_addr);
    }

    public interface IRegion : IDisposable
    {
        Stream getNewAccessStream();
        BlockAccessor getNewBlockAccessor(int rel_block_start, int block_len);
        long getStartAddress();
        long getSize();   // TODO: do something better with this, so we can't break
    }

    // read only block access abstraction
    public class BlockAccessor
    {
        byte[] data;
        int seek_pointer;
        
        public BlockAccessor(byte[] data) {            
            this.data = data;
            seek_pointer = 0;
        }

        public int Length {
            get { return data.Length; }
        }
        public int Position {
            get { return seek_pointer; }
        }

        public void Seek(int pos, SeekOrigin origin) {
            if (origin == SeekOrigin.Begin) {
                seek_pointer = pos;
            } else {
                throw new Exception("not implemented");
            }
        }

        public byte ReadByte() {
            return data[seek_pointer++];
        }

        public int Read(byte[] buf, int buf_offset, int count) {
            int num_to_copy = Math.Min(this.data.Length - buf_offset, count);
            Buffer.BlockCopy(this.data, this.seek_pointer, buf, buf_offset, num_to_copy);
            return num_to_copy;
        }
        

    }


    public interface IRegionWriter : IRegion
    {
        long getMaxSize();
    }
   
    // -----------------[ RegionExposedFiles ]-----------------------------------------------


    // manage a region of exposed files. each 'block start' makes a new filename
    // TODO: we should really treat blocks as fixed size even in the region manager

    class RegionExposedFiles : IRegionManager
    {
        String dir_path;

        Dictionary<long, EFRegion> region_cache;

        enum EFRegionMode {
            READ_ONLY_EXCL,
            READ_ONLY_SHARED,
            WRITE_NEW,
            READ_WRITE
        }
        
        // ------------ IRegion -------------
        class EFRegion : IRegion
        {
            
            string filepath;
            EFRegionMode mode;            
            long address;
            long length;

            Dictionary<int, Stream> my_streams;
            LRUCache<int, byte[]> block_cache;


            // -------------

            internal EFRegion(long address, long length, string filepath, EFRegionMode mode) {
                this.address = address;
                this.length = length;
                this.mode = mode;
                this.filepath = filepath;
                my_streams = new Dictionary<int, Stream>();
                block_cache = new LRUCache<int, byte[]>(5000);                    
                
            }

            public Stream getNewAccessStream() {
                if (this.mode == EFRegionMode.READ_ONLY_EXCL) {
                    FileStream reader = File.Open(filepath, FileMode.Open,FileAccess.Read, FileShare.None);
                    return reader;
                } else if (this.mode == EFRegionMode.READ_ONLY_SHARED) {
                    FileStream reader = File.Open(filepath, FileMode.Open, FileAccess.Read, FileShare.Read);
                    return reader;
                } else if (this.mode == EFRegionMode.WRITE_NEW) {
                    FileStream writer = File.Open(filepath, FileMode.CreateNew,FileAccess.Write, FileShare.None);
                    writer.SetLength(length);
                    return writer;
                } else if (this.mode == EFRegionMode.READ_WRITE) {
                    FileStream writer = File.Open(filepath, FileMode.Open,FileAccess.ReadWrite, FileShare.None);
                    writer.SetLength(length);
                    return writer;
                } else {
                    throw new Exception("unknown EFRegionMode: " + this.mode.ToString());
                }
            }

            private Stream getThreadStream() {
                int thread_id = Thread.CurrentThread.ManagedThreadId;
                lock (my_streams) {
                    if (my_streams.ContainsKey(thread_id)) {
                        return my_streams[thread_id];
                    } 
                }

                Stream new_stream = this.getNewAccessStream();
                lock (my_streams) {                 
                    my_streams[thread_id] = new_stream;
                }
                return new_stream;                                
            }

            public Stream getBlockAccessStream(int rel_block_start, int block_len) {
                // OLD:
                return new OffsetStream(this.getNewAccessStream(), rel_block_start, block_len);
            }

            public BlockAccessor getNewBlockAccessor(int rel_block_start, int block_len) {
                // return it from the block cache if it's there
                lock (block_cache) {
                    try {
                        byte[] datablock = this.block_cache.Get(rel_block_start);
                        if (datablock.Length == block_len) {
                            return new BlockAccessor(datablock);
                        }
                    } catch (KeyNotFoundException) {
                        // fall through below...
                    }
                }

                // System.Console.WriteLine(altdebug_pad + "zz uncached block");
                Stream mystream = this.getThreadStream();

                byte[] block = new byte[block_len];
                mystream.Seek(rel_block_start, SeekOrigin.Begin);
                DateTime before_read = DateTime.Now;
                if (mystream.Read(block, 0, block_len) != block_len) {
                    throw new Exception("couldn't read entire block: " + this.ToString());
                }
                double duration_ms = (DateTime.Now - before_read).TotalMilliseconds;

                lock (block_cache) {
                    block_cache.Add(rel_block_start, block);
                }

                if (duration_ms > 6.0) {
                    // TODO: check for reasons it might not have been a disk seek:
                    //  - garbage collector occured
                    //  - system load is high
                    System.Console.WriteLine("getNewBlockAccessor({0},{1}) may have caused disk seek (ms={2},totalmem={3})", 
                        rel_block_start,block_len, duration_ms, System.GC.GetTotalMemory(false));
                }

                return new BlockAccessor(block);                
            }

            public long getStartAddress() {
                return address;
            }
            public long getSize() {
                return this.length;
            }
            public void Dispose() {
                // no streams to dispose
            }
        } // ------------- IRegion END ----------------------------
       
        public class RegionMissingException : Exception { 
            public RegionMissingException(String msg) : base(msg) { }
        }
        public RegionExposedFiles(String location) {
            this.dir_path = location;
            region_cache = new Dictionary<long, EFRegion>();
        }

        // first time init        
        public RegionExposedFiles(InitMode mode, String location) : this(location) {
            if (mode != InitMode.NEW_REGION) {
                throw new Exception("first time init needs NEW_REGION paramater");
            }
            if (!Directory.Exists(dir_path)) {
                Console.WriteLine("LayerManager, creating directory: " + dir_path);
                Directory.CreateDirectory(dir_path);
            }
        }

        private String makeFilepath(long region_addr) {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            String addr = enc.GetString(Lsd.numberToLsd((int)region_addr,13));
            String filepath = dir_path + String.Format("\\addr{0}.rgm", addr);
            return filepath;
        }
        // impl ...

        public IRegion readRegionAddr(long region_addr) {
            String filepath = makeFilepath(region_addr);
            if (File.Exists(filepath)) {                
                FileStream reader = File.Open(filepath, FileMode.Open);
                long length = reader.Length;                
                reader.Dispose();

                return new EFRegion(region_addr, length, filepath, EFRegionMode.READ_ONLY_EXCL);

            } else {
                throw new RegionMissingException("no such region address: " + region_addr);
                
            }
        }

        public IRegion readRegionAddrNonExcl(long region_addr) {
            lock (region_cache) {
                if (region_cache.ContainsKey(region_addr)) {
                    return region_cache[region_addr];
                }
            }

            System.Console.WriteLine(RangemapManager.altdebug_pad + "zz uncached region");
            String filepath = makeFilepath(region_addr);
            if (File.Exists(filepath)) {
                // open non-exclusive
                
                FileStream reader = File.Open(filepath, FileMode.Open, FileAccess.Read, FileShare.Read);
                long length = reader.Length;
                reader.Dispose();
                
                EFRegion newregion = new EFRegion(region_addr, length, filepath, EFRegionMode.READ_ONLY_SHARED);
                lock (region_cache) {
                    region_cache[region_addr] = newregion;
                }
                return newregion;
            } else {
                throw new RegionMissingException("no such region address: " + region_addr);
            }
        }


        public IRegion writeExistingRegionAddr(long region_addr) {
            String filepath = makeFilepath(region_addr);
            FileStream reader = File.Open(filepath, FileMode.Open, FileAccess.Read, FileShare.Read);
            long length = reader.Length;
            reader.Dispose();

            return new EFRegion(region_addr, length, filepath, EFRegionMode.READ_WRITE);
        }

        public IRegion writeFreshRegionAddr(long region_addr, long length) {
            String filepath = makeFilepath(region_addr);
            if (File.Exists(filepath)) {
                System.Console.WriteLine("Exposed Region Manager, deleting: {0}", filepath);
                this.disposeRegionAddr(region_addr);
            }
            return new EFRegion(region_addr,length,filepath,EFRegionMode.WRITE_NEW);
        }
        public void disposeRegionAddr(long region_addr) {
            String filepath = this.makeFilepath(region_addr);
            String del_filename = String.Format("\\del{0}addr{1}.region", DateTime.Now.ToBinary(), region_addr);
            File.Move(filepath, dir_path + del_filename);
        }
    }
}


namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public class A01_RegionExposedFiles
    {
        // TODO: make a basic region test
        long BLOCK_SIZE = 4 * 1024 * 1024;

        [Test]
        public void T05_Region_Concurrency() {
            RegionExposedFiles rm = new RegionExposedFiles(InitMode.NEW_REGION,
                    "C:\\BENDtst\\T05_Region_Concurrency");
            byte[] data = { 1 , 3, 4, 5, 6, 7, 8, 9, 10 };

            {
                // put some data in the region
                IRegion region1 = rm.writeFreshRegionAddr(0, BLOCK_SIZE);
                {
                    Stream output = region1.getNewAccessStream();
                    output.Write(data, 0, data.Length);
                    output.Dispose();
                }
            }
            
            {
                IRegion region1 = rm.readRegionAddrNonExcl(0);
                Stream rd1 = region1.getNewAccessStream();
               

                Stream rd2 = region1.getNewAccessStream();

                // Assert.AreNotEqual(rd1, rd2, "streams should be separate");

                for (int i = 0; i < data.Length; i++) {
                    Assert.AreEqual(0, rd2.Position, "stream rd2 position should be indpendent");

                    Assert.AreEqual(i, rd1.Position, "stream rd1 position");
                    Assert.AreEqual(data[i], rd1.ReadByte(), "stream rd1 data correcness");
                }

            }


        }
    }

}