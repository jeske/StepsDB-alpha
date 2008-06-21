// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;


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
        int num_generations = 0;

        public RangemapManager(LayerManager store) {
            this.store = store;
        }

        public void newGeneration(LayerManager.Txn tx, IRegion region) {
            // TODO: get the current number of generations

            // allocate a new generation number
            int newgen = num_generations;
            num_generations++;
    
            // TODO: write the new generation count, and the rangemap entry for the generation

            tx.setValue(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"),
                new RecordUpdate(RecordUpdateTypes.FULL,num_generations.ToString()));

            RecordKey key = new RecordKey();
            key.appendParsedKey(".ROOT/GEN");
            key.appendKeyPart(Lsd.numberToLsd(newgen, 3));
            key.appendParsedKey("</>");
            tx.setValue(key, new RecordUpdate(RecordUpdateTypes.FULL,
                    String.Format("{0}:{1}", region.getStartAddress(), region.getSize())));            
        }

    }

}