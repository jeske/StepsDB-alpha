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

    public interface IStepsKVDB {
         void setValue(RecordKey key, RecordUpdate update);
         // public GetStatus getRecord(RecordKey key, out RecordData record);
         IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner);

         IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner);
    }

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
            
            next_stage.setValue(newkey,update);
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner) {
            var new_scanner = new ScanRange<RecordKey>(
                new RecordKeyComparator().appendKeyPart(this.subset_name).appendKeyPart(scanner.genLowestKeyTest()),
                new RecordKeyComparator().appendKeyPart(this.subset_name).appendKeyPart(scanner.genHighestKeyTest()), null);

            foreach (var rec in next_stage.scanForward(new_scanner)) {
                RecordKeyType_RecordKey orig_key = (RecordKeyType_RecordKey) rec.Key.key_parts[1];

                yield return new KeyValuePair<RecordKey, RecordData>(orig_key.GetRecordKey(), rec.Value);
            }
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner) {
            throw new Exception("NOT IMPLEMENTED");
        }
    }

    public class TimestampSnapshotStage : IStepsKVDB {
        IStepsKVDB next_stage;
        long current_timestamp;
        bool is_frozen;
        long frozen_at_timestamp = 0;

        private long _nowAsTime_T() {
            DateTime start_time = new DateTime(1970, 1, 1);
            return (long)(DateTime.Now - start_time).TotalSeconds;
        }

        public TimestampSnapshotStage(IStepsKVDB next_stage) {
            this.is_frozen = false;
            this.next_stage = next_stage;
            this.current_timestamp = (_nowAsTime_T() << 16);
        }

        private TimestampSnapshotStage(IStepsKVDB next_stage, long frozen_at_timestamp) : this(next_stage) {
            this.is_frozen = true;
            this.frozen_at_timestamp = frozen_at_timestamp;
        }

        private long _nextTimestamp() {
            return Interlocked.Increment(ref this.current_timestamp);
        }
        
        public void setValue(RecordKey key, RecordUpdate update) {
            // RecordKey key = key.clone();
            if (this.is_frozen) {
                throw new Exception("snapshots not writable!");
            }

            // (1) get our timestamp
            long timestamp = this._nextTimestamp();
            // (2) add our timestamp attribute to the end of the keyspace
            key.appendKeyPart(new RecordKeyType_AttributeTimestamp(timestamp));
            next_stage.setValue(key,update);
        }

        public TimestampSnapshotStage getSnapshot() {
            return new TimestampSnapshotStage(this.next_stage,this._nextTimestamp());            
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner) {
            long max_valid_timestamp = 0;
            var max_valid_record = new KeyValuePair<RecordKey, RecordData>(null,null);

            RecordKey last_key = null;

            foreach (KeyValuePair<RecordKey,RecordData> row in next_stage.scanForward(scanner)) {
                last_key = row.Key;

                if (this.is_frozen) {
                    Console.WriteLine("Frozen Snapshot({0}) stage saw: {1}", 
                        this.frozen_at_timestamp,row);
                } else {
                    Console.WriteLine("Timestamp Snapshot stage saw: {0}", row);
                }
                RecordKeyType_AttributeTimestamp our_attr = 
                    (RecordKeyType_AttributeTimestamp)row.Key.key_parts[row.Key.key_parts.Count - 1];
                long cur_timestamp = our_attr.GetLong();

                // remove our timestamp keypart
                // TODO: THIS IS A SUPER HACK AND STOP DOING IT!!! 
                row.Key.key_parts.RemoveAt(row.Key.key_parts.Count - 1);
                RecordKey clean_key = row.Key;

                if (last_key != null && clean_key.CompareTo(last_key) != 0) {
                    if (max_valid_record.Key != null) {
                        yield return max_valid_record;
                        max_valid_record = new KeyValuePair<RecordKey,RecordData>(null,null);
                        max_valid_timestamp = 0;
                    } 
                }
                
                // record the current record
                
                if (cur_timestamp > max_valid_timestamp) {
                    if (this.is_frozen && (cur_timestamp > this.frozen_at_timestamp)) {
                        continue;
                    }
                    
                   
                    max_valid_timestamp = cur_timestamp;
                    max_valid_record = new KeyValuePair<RecordKey, RecordData>(clean_key, row.Value);
                }                                                
              }

              
              if (max_valid_record.Key != null) {
                  yield return max_valid_record;
                  max_valid_record = new KeyValuePair<RecordKey,RecordData>(null,null);
                  max_valid_timestamp = 0;
              }
        }


        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner) {
            throw new Exception("NOT IMPLEMENTED");
        }

    }


    public class StepsDatabase {
        private readonly LayerManager db;
        public StepsDatabase(LayerManager db) {
            this.db = db;
        }
        public IStepsKVDB getDatabase() {
            return new TimestampSnapshotStage(new SubsetStage(new RecordKeyType_String("STUFF"), this.db));
        }
    }

}


#if false

namespace BendTests {
   
    abstract class RecAttr {
        
    }

    public class RecAttrTimestamp : RecAttr {
        int value; 
        public RecAttrTimestamp(int timestamp) {
            this.value = timestamp;
        }


    }


    [TestFixture]
    public class A00_AttributeTests {
        [Test]
        public void T00_AddAttribute() {
            var key = new RecordKey()
                .appendParsedKey("test/1")
                .appendKeyPart(new RecordKeyType_Attribute(1));

                

        }
    }

#endif