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
                RecordUpdate.WithPayload(0.ToString())); // TODO: this should be a var-enc number
        }
        public void mapGenerationToRegion(LayerManager.Txn tx, int gen_number, IRegion region) {
            RecordKey key = new RecordKey();
            key.appendParsedKey(".ROOT/GEN");
            key.appendKeyPart(Lsd.numberToLsd(gen_number, GEN_LSD_PAD));
            key.appendParsedKey("</>");

            // TODO: pack the metdata record <addr>:<size>
            // String segmetadata = String.Format("{0}:{1}", region.getStartAddress(), region.getSize());            
            String seg_metadata = "" + region.getStartAddress();
            tx.setValue(key, RecordUpdate.WithPayload(seg_metadata));

        }
        private RecordKey makeGenerationKey(int gen_number) {
            RecordKey genkey = new RecordKey()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(Lsd.numberToLsd(gen_number, GEN_LSD_PAD))
                .appendParsedKey("</>");

            return genkey;

        }


        public void unmapGeneration(LayerManager.Txn tx, int gen_number) {
            // TODO: somehow verify this is a valid thing to do!!
            RecordKey key = new RecordKey();
            key.appendParsedKey(".ROOT/GEN");
            key.appendKeyPart(Lsd.numberToLsd(gen_number, GEN_LSD_PAD));
            key.appendParsedKey("</>");
            tx.setValue(key, RecordUpdate.DeletionTombstone());
        }

        public void shrinkGenerationCount() {
            // see if we can shrink the number of generations

            int highest_valid_gen = num_generations-1;
            RecordData record;

            while (highest_valid_gen >= 0 &&
                store.getRecord(makeGenerationKey(highest_valid_gen), out record) == GetStatus.MISSING) {
                highest_valid_gen--;
            }

            if (highest_valid_gen + 1 < num_generations) {
                num_generations = highest_valid_gen + 1;
                store.setValue(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                    RecordUpdate.WithPayload(num_generations.ToString()));
            }
            

        }
        public void newGeneration(LayerManager.Txn tx, IRegion region) {
            // allocate a new generation number
            int newgen = num_generations;
            num_generations++;
    
            // TODO: write the new generation count, and the rangemap entry for the generation

            tx.setValue(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                RecordUpdate.WithPayload(num_generations.ToString()));

            mapGenerationToRegion(tx, newgen, region);
        }

        private ISortedSegment getSegmentFromMetadataBytes(byte[] data) {
            // TODO:unpack the update data when we change it to "<addr>:<length>"
            byte[] segmetadata_addr = data;

            // we now have a pointer to a segment addres for GEN<max>
            uint region_addr = (uint)Lsd.lsdToNumber(segmetadata_addr);


            IRegion region = store.regionmgr.readRegionAddrNonExcl(region_addr);
            SegmentReader sr = new SegmentReader(region.getStream());
            return sr;

        }

        public ISortedSegment getSegmentFromMetadata(RecordData data) {
            return getSegmentFromMetadataBytes(data.data);
        }

        public ISortedSegment getSegmentFromMetadata(RecordUpdate update) {
            return getSegmentFromMetadataBytes(update.data);
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
            return this.INTERNAL_segmentWalkForKey(
                key, curseg, handledGenerations, num_generations, ref record);


        }

        // ------------[ ** INTERNAL ** segmentWalkForKey ]-------
        //
        // This is the meaty internal function that does the "magic"
        // of the segment walk.
        
        private RecordUpdateResult INTERNAL_segmentWalkForKey(
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
                    if (update.type == RecordUpdateTypes.FULL) {
                        // TODO:unpack the update data when we change it to "<addr>:<length>"
                        byte[] segmetadata_addr = update.data;

                        // we now have a pointer to a segment addres for the gen pointer
                        uint region_addr = (uint)Lsd.lsdToNumber(segmetadata_addr);

                        IRegion region = store.regionmgr.readRegionAddrNonExcl(region_addr);
                        SegmentReader sr = new SegmentReader(region.getStream());

                        // RECURSE
                        if (INTERNAL_segmentWalkForKey(key, sr, nextHandledGenerations, maxgen - 1, ref record) == RecordUpdateResult.FINAL) {
                            return RecordUpdateResult.FINAL;
                        }
                    } else if (update.type == RecordUpdateTypes.DELETION_TOMBSTONE) {
                        // TODO: handle tombstones for ranges!
                        throw new Exception("TBD: implement handling for rangemap tombstones");
                    } else {
                        throw new Exception("Invalid rangerecord updatetype in walk: " + update.type.ToString());
                    }

                }
            }

            return RecordUpdateResult.SUCCESS;
        }

    }

}