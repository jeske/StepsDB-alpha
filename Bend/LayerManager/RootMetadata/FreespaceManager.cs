// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.



using System;
using System.IO;
using System.Runtime.InteropServices;


// .ROOT metadata

// Number representation - we will start with a simple positive integer zero-padded representation    

namespace Bend
{
    // .ROOT/FREELIST/HEAD -> (address for top of heap)
    // .ROOT/FREELIST/PENDING/(address start) -> (start, end)
    // .ROOT/FREELIST/EXTENTS/(address end:10) -> (start,end)
    // .ROOT/FREELIST/EXTENTS/0000004000 -> (start,end)
    
    // We store extents by the "end address" so we can carve blocks off the start of the extent
    // by writing the data-payload, without changing the row key.

    // TODO: maintain this data through a "table manager" so table 
    //       data can be indexed/exposed via normal mechanisms.     

    [StructLayout(LayoutKind.Sequential)]
    public struct FreespaceExtent { 
        public long start_addr;
        public long end_addr;

        public byte[] pack() {
            byte[] data; Util.writeStruct(this, out data); return data;
        }
        public static FreespaceExtent unpack(byte[] buf) {
            return Util.readStruct<FreespaceExtent>(new MemoryStream(buf));
        }
    }

    public class FreespaceManager
    {
        long next_allocation;
        BDSkipList<long, FreespaceExtent> freespace = new BDSkipList<long, FreespaceExtent>();

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

        public IRegion allocateNewSegment(LayerManager.WriteGroup tx, int length) {

            lock (this) {   // use one big nasty lock to prevent race conditions

                // try to find an extent with enough space to carve off a chunk

                // if we can't find a free segment, grow the heap
                return growHeap(tx, length);

                // then carve a segment out of the new grown heap

            }
        }

        // grow the top "top of heap" 
        // .ROOT/FREELIST/HEAD -> "top of heap"
        private IRegion growHeap(LayerManager.WriteGroup tx, int length) {            
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

        public void freeSegment(LayerManager.WriteGroup tx, FreespaceExtent segment_extent) {
            
            if (tx.type != LayerManager.WriteGroup.WriteGroupType.DISK_ATOMIC_FLUSH) {
                throw new Exception("freeSegment() requires DISK_ATOMIC write group");
            }            

            RecordKey key = new RecordKey().appendParsedKey(".ROOT/FREELIST/EXTENTS")
                .appendKeyPart(new RecordKeyType_Long(segment_extent.end_addr));

            RecordUpdate payload = RecordUpdate.WithPayload("");
            tx.setValue(key, payload);
            
            // NOTE: DISK_ATOMIC writes are not seen in the memory segment until the atomic write group applies
            //       so these changes will not be seen until then
            
        }
    }
}