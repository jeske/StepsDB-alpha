// Copyright (C) 2008-2011 by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Threading;

using NUnit.Framework;

using Bend;


namespace Bend {

    // 
    // TODO: change subset not to use a NESTED key, it should just use a prefix addition

    class SubsetStage : IStepsKVDB {
        readonly IStepsKVDB next_stage;
        readonly RecordKeyType_String subset_name;

        public SubsetStage(RecordKeyType_String subset_name, IStepsKVDB next_stage) {
            this.next_stage = next_stage;
            this.subset_name = subset_name;
        }


        public void setValue(RecordKey key, RecordUpdate update) {
            // RecordKey key = key.clone();

            // add our partition subset key to the begning
            RecordKey newkey = new RecordKey().appendKeyPart(this.subset_name).appendKeyPart(key);

            next_stage.setValue(newkey, update);
        }
        public KeyValuePair<RecordKey, RecordData> FindNext(IComparable<RecordKey> keytest, bool equal_ok) {
            var nested_keytest = new RecordKeyComparator().appendKeyPart(this.subset_name).appendKeyPart(keytest);
            var rec = next_stage.FindNext(nested_keytest, equal_ok);
            if (this.subset_name.CompareTo(rec.Key.key_parts[0]) != 0) {
                throw new KeyNotFoundException("SubsetStage.FindNext: no more records");
            }
            RecordKeyType_RecordKey orig_key = (RecordKeyType_RecordKey)rec.Key.key_parts[1];

            // strip off the prefix

            return new KeyValuePair<RecordKey, RecordData>(orig_key.GetRecordKey(), rec.Value);

        }

        public KeyValuePair<RecordKey, RecordData> FindPrev(IComparable<RecordKey> keytest, bool equal_ok) {
            var nested_keytest = new RecordKeyComparator().appendKeyPart(this.subset_name).appendKeyPart(keytest);
            var rec = next_stage.FindPrev(nested_keytest, equal_ok);

            if (this.subset_name.CompareTo(rec.Key.key_parts[0]) != 0) {
                throw new KeyNotFoundException("SubsetStage.FindPrev: no more records");
            }
            RecordKeyType_RecordKey orig_key = (RecordKeyType_RecordKey)rec.Key.key_parts[1];
           
            // strip off the prefix

            return new KeyValuePair<RecordKey, RecordData>(orig_key.GetRecordKey(), rec.Value);

        }


        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner) {
            var new_scanner = new ScanRange<RecordKey>(
                new RecordKeyComparator().appendKeyPart(this.subset_name).appendKeyPart(scanner.genLowestKeyTest()),
                new RecordKeyComparator().appendKeyPart(this.subset_name).appendKeyPart(scanner.genHighestKeyTest()), null);

            foreach (var rec in next_stage.scanForward(new_scanner)) {
            
                if (this.subset_name.CompareTo(rec.Key.key_parts[0]) != 0) {
                    throw new KeyNotFoundException("SubsetStage.scanForward: no more records");
                }                
                RecordKeyType_RecordKey orig_key = (RecordKeyType_RecordKey)rec.Key.key_parts[1];

                yield return new KeyValuePair<RecordKey, RecordData>(orig_key.GetRecordKey(), rec.Value);
            }
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner) {
            throw new Exception("NOT IMPLEMENTED");
        }
    }

}