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
    // .ROOT/FREELIST/PENDING/(address start) -> FreelistExtent(start, end)
    // .ROOT/FREELIST/EXTENTS/(address end:10) -> FreelistExtent(start, end)    
    
    // We store extents by the "end address" so we can carve blocks off the start of the extent
    // by writing the data-payload, without changing the row key.

    // TODO: maintain this data through a "table manager" so table 
    //       data can be indexed/exposed via normal mechanisms.     


    // ---- helper classes ----------------

    [StructLayout(LayoutKind.Sequential)]
    public struct FreespaceExtent { 
        public long start_addr;
        public long end_addr;

        public long length() {
            return (end_addr - start_addr);
        }
        public byte[] pack() {
            byte[] data; Util.writeStruct(this, out data); return data;
        }
        public static FreespaceExtent unpack(byte[] buf) {
            return Util.readStruct<FreespaceExtent>(new MemoryStream(buf));
        }
        public override string ToString() {
            return String.Format("{0}-{1} ({2})", start_addr, end_addr, end_addr - start_addr);
        }
    }

    public class NewUnusedSegment {
        FreespaceExtent location;
        LayerManager store; 
        public NewUnusedSegment(LayerManager store, FreespaceExtent extent) {
            this.location = extent;
            this.store = store;
        }
        public IRegion getWritableRegion() {
            return store.regionmgr.writeFreshRegionAddr(location.start_addr, location.length());
        }

        public void mapSegment(LayerManager.WriteGroup tx, int use_gen, 
            RecordKey start_key, RecordKey end_key, IRegion reader) {

            if (! (tx.type == LayerManager.WriteGroup.WriteGroupType.DISK_ATOMIC_NOFLUSH ||
                   tx.type == LayerManager.WriteGroup.WriteGroupType.DISK_ATOMIC_FLUSH)) {
                       throw new Exception("NewUnusedSegment.mapSegment() must be provided an ATOMIC write group");
            } 

            // remove the pending entry
            RecordKey key = new RecordKey().appendParsedKey(".ROOT/FREELIST/PENDING");
            key.appendKeyPart(new RecordKeyType_Long(reader.getStartAddress()));
            tx.setValue(key, RecordUpdate.DeletionTombstone());
            
            // add the new map
            tx.mylayer.rangemapmgr.mapGenerationToRegion(tx, use_gen, start_key, end_key, reader);
        }

    }


    // ---------------------------------------------------------------------------
    //                     FreespaceManager
    // ---------------------------------------------------------------------------

    public class FreespaceManager
    {
        long next_allocation;
        BDSkipList<long, FreespaceExtent> freespace = new BDSkipList<long, FreespaceExtent>();

        LayerManager store;

        RecordKey pending_prefix = new RecordKey().appendParsedKey(".ROOT/FREELIST/PENDING");
        RecordKey freelist_prefix = new RecordKey().appendParsedKey(".ROOT/FREELIST/EXTENTS");

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

        public NewUnusedSegment allocateNewSegment(LayerManager.WriteGroup tx, int length) {

            // use one big nasty lock to prevent race conditions
            lock (this) {   

                // try to find an extent with enough space to carve off a chunk
                foreach (var rec in store.scanForward(new ScanRange<RecordKey>(freelist_prefix,
                                RecordKey.AfterPrefix(freelist_prefix), null))) {
                    FreespaceExtent extent = FreespaceExtent.unpack(rec.Value.data);

                    if (extent.length() == length) {
                        // the extent is exactly the right size... make it pending
                        LayerManager.WriteGroup makepending_wg =
                            tx.mylayer.newWriteGroup(type: LayerManager.WriteGroup.WriteGroupType.DISK_ATOMIC_NOFLUSH);

                        // add a pending entry for this block
                        {
                            RecordKey key = new RecordKey().appendParsedKey(".ROOT/FREELIST/PENDING");
                            key.appendKeyPart(new RecordKeyType_Long(extent.start_addr));
                            makepending_wg.setValue(key, RecordUpdate.WithPayload(extent.pack()));
                        }

                        // remove the freelist entry
                        {
                            RecordKey key = new RecordKey().appendParsedKey(".ROOT/FREELIST/EXTENTS");
                            key.appendKeyPart(new RecordKeyType_Long(extent.end_addr));
                            makepending_wg.setValue(key, RecordUpdate.DeletionTombstone());
                        }

                        makepending_wg.finish();
                        return new NewUnusedSegment(store, extent);
                    } else if (extent.length() > length) {
                        
                        
                        // TODO: carve a piece off the extent and return the pending piece

                    }
                }

                // if we can't find a free segment, grow the heap
                return growHeap(tx, length);

                // TODO: then carve a segment out of the new grown heap

            }
        }

        private RecordKey pendingKeyForAddr(long start_addr) {
            RecordKey key = new RecordKey().appendParsedKey(".ROOT/FREELIST/PENDING");
            key.appendKeyPart(new RecordKeyType_Long(start_addr));
            return key;
        }

        // grow the top "top of heap" 
        // .ROOT/FREELIST/HEAD -> "top of heap"
        private NewUnusedSegment growHeap(LayerManager.WriteGroup tx, int length) {
            long new_addr;
            FreespaceExtent newblock_info;

            // make an atomic write-group to "carve off" a pending chunk
            LayerManager.WriteGroup carveoff_wg =
                tx.mylayer.newWriteGroup(type: LayerManager.WriteGroup.WriteGroupType.DISK_ATOMIC_NOFLUSH);

            // lock to make sure two threads don't carve off at the same time...
            lock (this) {
                // HACK: currently we just grab off the top of heap
                new_addr = next_allocation;
                next_allocation = next_allocation + (long)length;

                if (new_addr <= 0) {
                    throw new Exception("invalid address in allocateNewSegment: " + new_addr);
                }

                newblock_info = new FreespaceExtent();
                newblock_info.start_addr = new_addr;
                newblock_info.end_addr = new_addr + length;

                // add the pending chunk
                carveoff_wg.setValue(this.pendingKeyForAddr(new_addr),
                                     RecordUpdate.WithPayload(newblock_info.pack()));

                Console.WriteLine("allocateNewSegment - next address: " + new_addr);
                // add our new top of heap pointer
                {
                    RecordKey key = new RecordKey().appendParsedKey(".ROOT/FREELIST/HEAD");
                    carveoff_wg.setValue(key, RecordUpdate.WithPayload(Lsd.numberToLsd(next_allocation, 13)));
                }

                // commit the metadata tx
                carveoff_wg.finish();
            }

            return new NewUnusedSegment(store,newblock_info);            
        }

        public void freeSegment(LayerManager.WriteGroup tx, FreespaceExtent segment_extent) {
            
            // (1) add the segment to the pending list (pending free)

            if (tx.type != LayerManager.WriteGroup.WriteGroupType.DISK_ATOMIC_FLUSH) {
                throw new Exception("freeSegment() requires DISK_ATOMIC write group");
            }

            // NOTE: DISK_ATOMIC writes are not seen in the memory segment until the atomic write group applies
            //       so these changes will not be seen until then

            RecordKey key = new RecordKey().appendParsedKey(".ROOT/FREELIST/PENDING")
                .appendKeyPart(new RecordKeyType_Long(segment_extent.end_addr));

            RecordUpdate payload = RecordUpdate.WithPayload(segment_extent.pack());
            tx.setValue(key, payload);
            
            // (2) add a handler to get notified when the block is no longer referenced, so it can
            //     be moved from pending to actually free.

            LayerManager.WriteGroup fwg = this.store.newWriteGroup(LayerManager.WriteGroup.WriteGroupType.DISK_ATOMIC_NOFLUSH);

            tx.mylayer.regionmgr.notifyRegionSafeToFree(segment_extent.start_addr,
                delegate(long addr) { this.handleRegionSafeToFree(addr,segment_extent, fwg); });
                    
        }

        // move the pending address into the freelist
        private void handleRegionSafeToFree(long start_addr, FreespaceExtent extent, LayerManager.WriteGroup wg) {
            System.Console.WriteLine("*\n*\n*\n* handleRegionSafeToFree {0} \n*\n*\n*", start_addr);
            // (1) remove pending entry
            wg.setValue(pendingKeyForAddr(start_addr), RecordUpdate.DeletionTombstone());

            // (2) write real freelist entry (TODO: merge with neighboring entries)
            {
                RecordKey key = new RecordKey().appendParsedKey(".ROOT/FREELIST/EXTENTS");
                key.appendKeyPart(new RecordKeyType_Long(extent.end_addr));
                wg.setValue(key, RecordUpdate.WithPayload(extent.pack()));
            }
            wg.finish();
        }

        public void debugDumbCurrentFreespace() {
            long total_freespace = 0;
            long total_pendingspace = 0;

            System.Console.WriteLine("------------------- Freelist Extents (Pending) ---------------");
            foreach (var rec in store.scanForward(new ScanRange<RecordKey>(pending_prefix,
                                            RecordKey.AfterPrefix(pending_prefix), null))) {
                FreespaceExtent extent = FreespaceExtent.unpack(rec.Value.data);
                System.Console.WriteLine("{0} -> {1}",
                    rec.Key.ToString(),
                    extent.ToString());
                total_pendingspace += extent.length();
            }

            System.Console.WriteLine("------------------- Freelist Extents (FREE)-------------------");
            foreach (var rec in store.scanForward(new ScanRange<RecordKey>(freelist_prefix,
                                            RecordKey.AfterPrefix(freelist_prefix), null))) {
                FreespaceExtent extent = FreespaceExtent.unpack(rec.Value.data);
                System.Console.WriteLine("{0} -> {1}",
                    rec.Key.ToString(),
                    extent.ToString());
                total_freespace += extent.length();
            }
            System.Console.WriteLine("------------------- Freelist Extents END (pending: {0}, free {1}) ---------------",
                total_pendingspace,total_freespace);
                    
        }

    } // class FreespaceManager
} // namespace Bend