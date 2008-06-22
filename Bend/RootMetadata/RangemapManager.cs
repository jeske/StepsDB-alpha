// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;


namespace Bend
{

    // RANGEs are represented with an implicit prefix '='. This allows special endpoint markers:
    // "<" - the key before all keys
    // "=KEYDATA" - keys after and including "KEYDATA"
    // ">" - the key after all keys


    // .ROOT/VARS/NUMGENERATIONS -> 1
    // .ROOT/GEN/(gen #:3)/(start key)/(end key) -> (segment metadata)
    // .ROOT/GEN/000/</> -> addr:length

    public class RangemapManager
    {
        LayerManager store;
        int num_generations;
        private static int GEN_LSD_PAD = 3;

        public RangemapManager(LayerManager store) {
            this.store = store;
            // get the current number of generations
            
            RecordUpdate update;
            if (store.workingSegment.getRecordUpdate(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                out update) == GetStatus.MISSING) {
                throw new Exception("RangemapManager can't init without NUMGENERATIONS");
            }
            num_generations = (int)Lsd.lsdToNumber(update.data);
        }
        public static void Init(LayerManager store) {
            // setup "zero" initial generations
            store.setValue(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                new RecordUpdate(RecordUpdateTypes.FULL,0.ToString()));
        }
        public void newGeneration(LayerManager.Txn tx, IRegion region) {
            // allocate a new generation number
            int newgen = num_generations;
            num_generations++;
    
            // TODO: write the new generation count, and the rangemap entry for the generation

            tx.setValue(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                new RecordUpdate(RecordUpdateTypes.FULL,num_generations.ToString()));

            RecordKey key = new RecordKey();
            key.appendParsedKey(".ROOT/GEN");
            key.appendKeyPart(Lsd.numberToLsd(newgen, GEN_LSD_PAD));
            key.appendParsedKey("</>");
            
            // TODO: pack the metdata record <addr>:<size>
            // String segmetadata = String.Format("{0}:{1}", region.getStartAddress(), region.getSize());            
            String segmetadata = "" + region.getStartAddress();
            tx.setValue(key, new RecordUpdate(RecordUpdateTypes.FULL, segmetadata));

        }

        public ISortedSegment getSegmentFromMetadata(RecordUpdate update) {
            // TODO:unpack the update data when we change it to "<addr>:<length>"
            byte[] segmetadata_addr = update.data;

            // we now have a pointer to a segment addres for GEN<max>
            uint region_addr = (uint)Lsd.lsdToNumber(segmetadata_addr);


            IRegion region = store.regionmgr.readRegionAddrNonExcl(region_addr);
            SegmentReader sr = new SegmentReader(region.getStream());
            return sr;
        }

        public int genCount() {
            return num_generations;
        }

        // ------------[ public segmentWalkForKey ] --------------

        public RecordUpdateResult segmentWalkForKey(
            RecordKey key,
            ISortedSegment curseg,
            ref RecordData record) {

            HashSet<int> handledGenerations = new HashSet<int>();
            return this.segmentWalkForKey(
                key, curseg, handledGenerations, num_generations, ref record);
        }

        // ------------[ ** INTERNAL ** segmentWalkForKey ]-------
        //
        // This is the meaty internal function that does the "magic"
        // of the segment walk.
        
        private RecordUpdateResult segmentWalkForKey(
            RecordKey key,
            ISortedSegment curseg,
            HashSet<int> handledGenerations,
            int maxgen,
            ref RecordData record) {

            // first look in this segment for the key
            {
                RecordUpdate update;
                if (curseg.getRecordUpdate(key, out update) == GetStatus.PRESENT) {
                    if (record.applyUpdate(update) == RecordUpdateResult.FINAL) {
                        return RecordUpdateResult.FINAL;
                    }
                }
            }

            // make a note of which generation range references have precedence in this segment
            HashSet<int> nextHandledGenerations = new HashSet<int>(handledGenerations);
            for (int i = maxgen; i >= 0; i--) {
                RecordKey rangekey = new RecordKey().appendParsedKey(".ROOT/GEN")
                    .appendKeyPart(Lsd.numberToLsd(i,GEN_LSD_PAD)).appendParsedKey("</>");
                RecordUpdate update;  
                // TODO: make a "getRecordExists()" call in ISortedSegment to make this more efficient
                //  .. then make sure we use that optimization to avoid calling getRecordUpdate on those
                //  .. entries in the next loop below.
                if (curseg.getRecordUpdate(rangekey,out update) == GetStatus.PRESENT) {
                    nextHandledGenerations.Add(i);  // mark the generation handled at this level
                }
            }

            // now repeat the walk of range references in this segment, this time actually descending
            for (int i = maxgen; i >= 0; i--) {
                RecordKey rangekey = new RecordKey().appendParsedKey(".ROOT/GEN")
                    .appendKeyPart(Lsd.numberToLsd(i,3)).appendParsedKey("</>");
                RecordUpdate update;  
                if (curseg.getRecordUpdate(rangekey,out update) == GetStatus.PRESENT) {
                    // TODO:unpack the update data when we change it to "<addr>:<length>"
                    byte[] segmetadata_addr = update.data;
                    
                    // we now have a pointer to a segment addres for the gen pointer
                    uint region_addr = (uint)Lsd.lsdToNumber(segmetadata_addr);
                                        
                    IRegion region = store.regionmgr.readRegionAddrNonExcl(region_addr);
                    SegmentReader sr = new SegmentReader(region.getStream());

                    // RECURSE
                    if (segmentWalkForKey(key,sr,nextHandledGenerations,maxgen-1,ref record) == RecordUpdateResult.FINAL) {
                        return RecordUpdateResult.FINAL;
                    }
                }
            }

            return RecordUpdateResult.SUCCESS;
        }

    }

}