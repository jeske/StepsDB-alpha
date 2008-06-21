// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.



using System;
using System.IO;

// .ROOT metadata


// Number representation - we will start with a simple positive integer zero-padded representation    
namespace Bend
{
    // .ROOT/FREELIST/(address start:10)/(address end:10) -> ()
    // .ROOT/FREELIST/0000004000/0000008000 -> ()
    // .ROOT/FREELIST/0000008000/* -> ()                    * is a special dynamic growth end marker


    // TODO: this should be talking to our high-level indexing manager, so the
    // indexing manager can keep an index on the blocks_by_size automatically, 
    // and so the table format can be read by the higher level table manager
    public class FreespaceManager
    {
        int next_allocation = (int)(RootBlock.MAX_ROOTBLOCK_SIZE + LogWriter.DEFAULT_LOG_SIZE);
        static int DEFAULT_SEGMENT_SIZE = 8 * 1024 * 1024;
        LayerManager store;
        public FreespaceManager(LayerManager store) {
            this.store = store;
            // read the freelist and "index" into memory for now (TODO: fix this hack)
        }

        // right now we're going to use a "top of heap" allocation strategy with no reclamation
        // .ROOT/FREELIST/HEAD -> "top of heap"
        public IRegion allocateNewSegment(LayerManager.Txn tx, int remaining_bytes_to_write) {
            int new_addr;
            // grab a chunk

            new_addr = next_allocation;
            next_allocation = next_allocation + DEFAULT_SEGMENT_SIZE;

            // write our new top of heap pointer
            {
                RecordKey key = new RecordKey();
                key.appendKeyParts(".ROOT", "FREELIST", "HEAD");
                tx.setValue(key, new RecordUpdate(RecordUpdateTypes.FULL, Lsd.numberToLsd(next_allocation, 10)));
            }

            return store.regionmgr.writeRegionAddr((uint)new_addr);
        }
    }
}