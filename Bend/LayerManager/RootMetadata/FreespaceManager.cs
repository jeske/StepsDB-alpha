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
        long next_allocation;
        
        LayerManager store;
        public FreespaceManager(LayerManager store) {
            this.store = store;
            // read the freelist and "index" into memory for now (TODO: use a real freelist)
            RecordData data;
            if (store.getRecord(new RecordKey().appendParsedKey(".ROOT/FREELIST/HEAD"), out data) == GetStatus.MISSING) {
                // TODO: fix this init hack
                next_allocation = (int)(RootBlock.MAX_ROOTBLOCK_SIZE + LogWriter.DEFAULT_LOG_SIZE);

                // this was a test to be able to start with big address numbers..
                // next_allocation = 4294971392;
                // next_allocation = 4294971392 - 8*1024*1024;
            } else {
                next_allocation = Lsd.lsdToNumber(data.data);
            }
        }

        // right now we're going to use a "top of heap" allocation strategy with no reclamation
        // .ROOT/FREELIST/HEAD -> "top of heap"
        public IRegion allocateNewSegment(LayerManager.WriteGroup tx, int length) {
            long new_addr;
            // grab a chunk

            new_addr = next_allocation;
            next_allocation = next_allocation + (long)length;

            Console.WriteLine("allocateNewSegment - next address: " + new_addr);
            // write our new top of heap pointer
            {
                RecordKey key = new RecordKey().appendParsedKey(".ROOT/FREELIST/HEAD");
                tx.setValue(key, RecordUpdate.WithPayload(Lsd.numberToLsd(next_allocation, 13)));
            }

            if (new_addr <= 0) {
                throw new Exception("invalid address in allocateNewSegment: " + new_addr);
            }

            return store.regionmgr.writeFreshRegionAddr(new_addr, length);
        }
    }
}