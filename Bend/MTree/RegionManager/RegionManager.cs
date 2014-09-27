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

    public delegate void handleRegionSafeToFreeDelegate(long start_addr);

    public interface IRegionManager
    {
        IRegion readRegionAddr(long region_addr);
        IRegion readRegionAddrNonExcl(long region_addr);
        IRegion writeFreshRegionAddr(long region_addr,long length);
        IRegion writeExistingRegionAddr(long region_addr);

        void notifyRegionSafeToFree(long region_addr, handleRegionSafeToFreeDelegate del);
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
    public class BlockAccessor : MemoryStream
    {        
        public BlockAccessor(byte[] data) : base(data) {                        
        }
    }


    public interface IRegionWriter : IRegion
    {
        long getMaxSize();
    }
   
}

