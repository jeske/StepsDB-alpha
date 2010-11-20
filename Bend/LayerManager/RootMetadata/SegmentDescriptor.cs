// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;

using System.Diagnostics;


namespace Bend {

    // currently...

    // .ROOT/GEN/000/</> -> addr:length

    public class SegmentDescriptor : IComparable<SegmentDescriptor> {
        public RecordKey record_key;
        public uint generation;
        public RecordKey start_key;
        public RecordKey end_key;

        public SegmentDescriptor(RecordKey key) {
            RecordKey expected_prefix = new RecordKey().appendParsedKey(".ROOT/GEN");
            if (!key.isSubkeyOf(expected_prefix)) {
                throw new Exception("can't decode key as segment descriptor: " + key.ToString());
            }
            record_key = key;

            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

            generation = (uint)Lsd.lsdToNumber(enc.GetBytes(key.key_parts[2]));
            start_key = new RecordKey().appendParsedKey(key.key_parts[3]);
            end_key = new RecordKey().appendParsedKey(key.key_parts[4]);

            {
                var testkey = new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS");
                if (start_key.Equals(testkey)) {
                    System.Console.WriteLine("%%%%%% GEN started with numgenerations ..." + this);
                    System.Console.WriteLine(WhoCalls.StackTrace());
                }
            }

        }

        public int CompareTo(SegmentDescriptor target) {
            
            switch(this.generation.CompareTo(target.generation)) {
                case -1: 
                    return -1;
                case 1:
                    return 1;
                case 0:
                    switch(this.start_key.CompareTo(target.start_key)) {
                        case -1:
                            return -1;
                        case 1:
                            return 1;
                        case 0:
                            return this.end_key.CompareTo(target.end_key);
                    
                    }
                    break;
            }
            throw new Exception("invalid values in SegmentDescriptor.CompareTo()");
        }

        public SegmentDescriptor(uint generation, RecordKey start_key, RecordKey end_key) {

            RecordKey genkey = new RecordKey()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(Lsd.numberToLsd((int)generation, RangemapManager.GEN_LSD_PAD))
                .appendKeyPart(start_key.encode())
                .appendKeyPart(end_key.encode());

            this.record_key = genkey;
            this.generation = generation;
            this.start_key = start_key;
            this.end_key = end_key;

        }

        public override string ToString() {
            return "SegmentDescriptor{" + generation + ":" + start_key + ":" +
                end_key + "}";
        }
        public ISortedSegment getSegment(RangemapManager rmm) {
            RecordKey found_key = new RecordKey();
            RecordData found_record = new RecordData(RecordDataState.NOT_PROVIDED, found_key);
            if (rmm.getNextRecord(this.record_key, ref found_key, ref found_record, true) == GetStatus.PRESENT) {
                if (this.record_key.Equals(found_key) && found_record.State == RecordDataState.FULL) {
                    return rmm.getSegmentFromMetadata(found_record);
                }
                if (found_record.State == RecordDataState.DELETED) {
                    throw new Exception("Segment{" + this.ToString() + "}.getSegment() returned tombstone");
                }
            }
            throw new Exception("Could not load Segment from SegmentDescriptor: " + this);
        }

    }

}