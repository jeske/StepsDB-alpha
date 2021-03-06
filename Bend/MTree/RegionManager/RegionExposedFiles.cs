﻿// Copyright (C) 2008-2014 David W. Jeske
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

using System.Threading;
using System.Collections;
using System.Collections.Generic;

// TODO: raw file, raw partition region managers, tests

namespace Bend {


    // -----------------[ RegionExposedFiles ]-----------------------------------------------


    // manage a region of exposed files. each 'block start' makes a new filename
    // TODO: we should really treat blocks as fixed size even in the region manager

    class RegionExposedFiles : IRegionManager {
        String dir_path;

        LRUCache<long, WeakReference<EFRegion>> region_cache;

        public static int REGION_CACHE_SIZE = 20;

        public class RegionMissingException : Exception {
            public RegionMissingException(String msg) : base(msg) { }
        }
        public RegionExposedFiles(String location) {
            this.dir_path = location;
            region_cache = new LRUCache<long, WeakReference<EFRegion>>(REGION_CACHE_SIZE);
        }

        // first time init        
        public RegionExposedFiles(InitMode mode, String location)
            : this(location) {
            if (mode != InitMode.NEW_REGION) {
                throw new Exception("first time init needs NEW_REGION paramater");
            }
            if (!Directory.Exists(dir_path)) {
                Console.WriteLine("RegionExposedFiles, creating directory: " + dir_path);
                Directory.CreateDirectory(dir_path);
            }
        }


        // --------------------------


        internal class RegionFileStream : FileStream {
            EFRegion region;
            internal RegionFileStream(EFRegion region, string filepath, FileMode mode, 
                FileAccess access, FileShare share) : 
                base(filepath,mode,access,share) {
                this.region = region;
            }
        }

        internal enum EFRegionMode {
            READ_ONLY_EXCL,
            READ_ONLY_SHARED,
            WRITE_NEW,
            READ_WRITE
        }

        // ------------ IRegion -------------
        internal class EFRegion : IRegion, IDisposable {

            string filepath;
            EFRegionMode mode;
            long address;
            long length;

            handleRegionSafeToFreeDelegate del = null;

            Dictionary<int, WeakReference<Stream>> my_streams;
            LRUCache<int, byte[]> subblock_cache;

            public static int EFREGION_SUBBLOCK_CACHE_SIZE = 10; // sub-blocks **per region** to cache, TODO: should this even exist?


            // -------------

            internal EFRegion(long address, long length, string filepath, EFRegionMode mode) {
                this.address = address;
                this.length = length;
                this.mode = mode;
                this.filepath = filepath;
                my_streams = new Dictionary<int, WeakReference<Stream>>();
                subblock_cache = new LRUCache<int, byte[]>(EFREGION_SUBBLOCK_CACHE_SIZE);

            }

            ~EFRegion () {
                this.Dispose();
            }

            public void Dispose() {
                // (1) be sure all the filestreams are closed.. this happens through RegionFileStream
                //     holding a reference to us...
                System.Console.WriteLine("EFRegion({0}): Dispose", this.address);
                if (this.del != null) {
                    this.del(this.address);
                    this.del = null;
                }
            }

            public override string ToString() {
                return String.Format("addr:{0}  len:{1}", this.address, this.length);
            }

            internal void addDisposeDelegate(handleRegionSafeToFreeDelegate delfn) {
                this.del = delfn;                
            }

            public Stream getNewAccessStream() {
                if (this.mode == EFRegionMode.READ_ONLY_EXCL) {                      
                    FileStream reader = new RegionFileStream(this, filepath, FileMode.Open, FileAccess.Read, FileShare.None);
                    return reader;
                } else if (this.mode == EFRegionMode.READ_ONLY_SHARED) {
                    FileStream reader = new RegionFileStream(this, filepath, FileMode.Open, FileAccess.Read, FileShare.Read);
                    return reader;
                } else if (this.mode == EFRegionMode.WRITE_NEW) {
                    FileStream writer = new RegionFileStream(this, filepath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                    writer.SetLength(length);
                    return writer;
                } else if (this.mode == EFRegionMode.READ_WRITE) {
                    FileStream writer = new RegionFileStream(this, filepath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                    writer.SetLength(length);
                    return writer;
                } else {
                    throw new Exception("unknown EFRegionMode: " + this.mode.ToString());
                }
            }

            

#if false
            // TODO: Is this really safe? 
            [Obsolete]
            private Stream getThreadStream() {
                int thread_id = Thread.CurrentThread.ManagedThreadId;
                lock (my_streams) {
                    if (my_streams.ContainsKey(thread_id)) {
                        Stream a_stream = my_streams[thread_id].Target;
                        if (a_stream != null) {
                            return a_stream;
                        }
                    }
                }

                Stream new_stream = this.getNewAccessStream();
                lock (my_streams) {
                    my_streams[thread_id] = new WeakReference<Stream>(new_stream);
                }
                return new_stream;
            }
#endif

            [Obsolete]
            public Stream getBlockAccessStream(int rel_block_start, int block_len) {                
                return new OffsetStream(this.getNewAccessStream(), rel_block_start, block_len);
            }

            public BlockAccessor getNewBlockAccessor(int rel_block_start, int block_len) {
                // return it from the block cache if it's there
                lock (subblock_cache) {
                    try {
                        byte[] datablock = this.subblock_cache.Get(rel_block_start);
                        if (datablock.Length == block_len) {
                            return new BlockAccessor(datablock);
                        }
                    } catch (KeyNotFoundException) {
                        // fall through below...
                    }
                }

                // System.Console.WriteLine("**** zz uncached sub-block");
                // Stream mystream = this.getThreadStream();
                Stream mystream = this.getNewAccessStream();

                byte[] block = new byte[block_len];
                mystream.Seek(rel_block_start, SeekOrigin.Begin);
                DateTime before_read = DateTime.Now;
                if (mystream.Read(block, 0, block_len) != block_len) {
                    throw new Exception("couldn't read entire block: " + this.ToString());
                }
                double duration_ms = (DateTime.Now - before_read).TotalMilliseconds;

                lock (subblock_cache) {
                    subblock_cache.Add(rel_block_start, block);
                }

                if (duration_ms > 6.0) {
                    // TODO: check for reasons it might not have been a disk seek:
                    //  - garbage collector occured
                    //  - system load is high
                    System.Console.WriteLine("getNewBlockAccessor({0},{1}) may have caused disk seek (ms={2},totalmem={3})",
                        rel_block_start, block_len, duration_ms, System.GC.GetTotalMemory(false));
                }

                return new BlockAccessor(block);
            }

            public long getStartAddress() {
                return address;
            }
            public long getSize() {
                return this.length;
            }
        } // ------------- IRegion END ----------------------------


        public String makeFilepath(long region_addr) {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            String addr = enc.GetString(Lsd.numberToLsd(region_addr, 13));
            String filepath = dir_path + String.Format("\\addr{0}.rgm", addr);

            Console.WriteLine("makeFilepath({0}) -> {1}", region_addr, filepath);
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
            return INTERNAL_readRegionAddrNonExcl(region_addr);
        }

        EFRegion INTERNAL_readRegionAddrNonExcl(long region_addr) {
            lock (region_cache) {
                if (region_cache.ContainsKey(region_addr)) {
                    EFRegion region = region_cache[region_addr].Target;
                    if (region != null) {
                        return region;
                    }
                    
                }

                System.Console.WriteLine(RangemapManager.altdebug_pad + "zz uncached region");
                String filepath = makeFilepath(region_addr);
                if (File.Exists(filepath)) {
                    // open non-exclusive

                    // TODO: FIX THIS HACK, where we figure out the length by opening the file.. it should be passed
                    // in when the region is opened...
                    FileStream reader = File.Open(filepath, FileMode.Open, FileAccess.Read, FileShare.Read);
                    long length = reader.Length;
                    reader.Close();
                    reader.Dispose();

                    EFRegion newregion = new EFRegion(region_addr, length, filepath, EFRegionMode.READ_ONLY_SHARED);
                    region_cache[region_addr] = new WeakReference<EFRegion>(newregion);
             
                    return newregion;
                } else {
                    throw new RegionMissingException("no such region address: " + region_addr);
                }
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
            return new EFRegion(region_addr, length, filepath, EFRegionMode.WRITE_NEW);
        }
        public void disposeRegionAddr(long region_addr) {
            String filepath = this.makeFilepath(region_addr);
            String del_filename = String.Format("\\del{0}addr{1}.region", DateTime.Now.ToBinary(), region_addr);
            File.Move(filepath, dir_path + del_filename);
        }

        public void notifyRegionSafeToFree(long region_addr, handleRegionSafeToFreeDelegate del) {
            EFRegion region_handler = INTERNAL_readRegionAddrNonExcl(region_addr);
            region_handler.addDisposeDelegate(del);            
        }
    }

}