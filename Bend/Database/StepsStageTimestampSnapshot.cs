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


    public class TimestampSnapshotStage : IStepsSnapshotKVDB {

        #region Instance Data and Constructors

        IStepsKVDB next_stage;

        bool is_frozen;
        long frozen_at_timestamp = 0;
        private static FastUniqueIds id_gen = new FastUniqueIds();

        public TimestampSnapshotStage(IStepsKVDB next_stage) {
            this.is_frozen = false;
            this.next_stage = next_stage;
        }

        private TimestampSnapshotStage(IStepsKVDB next_stage, long frozen_at_timestamp)
            : this(next_stage) {
            this.is_frozen = true;
            this.frozen_at_timestamp = frozen_at_timestamp;
        }

        #endregion

        #region Public IStepsKVDB interface

        public void setValue(RecordKey key, RecordUpdate update) {
            // RecordKey key = key.clone();
            if (this.is_frozen) {
                throw new Exception("snapshot not writable! " + this.frozen_at_timestamp);
            }

            // (1) get our timestamp
            long timestamp = id_gen.nextTimestamp();
            // (2) add our timestamp attribute to the end of the keyspace
            key.appendKeyPart(new RecordKeyType_AttributeTimestamp(timestamp));
            next_stage.setValue(key, update);
        }

        public IStepsKVDB getSnapshot() {
            return new TimestampSnapshotStage(this.next_stage, id_gen.nextTimestamp());
        }

        public KeyValuePair<RecordKey, RecordData> FindNext(IComparable<RecordKey> keytest, bool equal_ok) {
            var rangekey = new ScanRange<RecordKey>(keytest, new ScanRange<RecordKey>.maxKey(), null);
            foreach (var rec in this.scanForward(rangekey)) {
                if (!equal_ok && keytest.CompareTo(rec.Key) == 0) {
                    continue;
                }
                return rec;
            }
            throw new KeyNotFoundException("SubSetStage.FindNext: no record found after: " + keytest + " equal_ok:" + equal_ok);
        }
        public KeyValuePair<RecordKey, RecordData> FindPrev(IComparable<RecordKey> keytest, bool equal_ok) {
            var rangekey = new ScanRange<RecordKey>(new ScanRange<RecordKey>.minKey(), keytest, null);
            foreach (var rec in this.scanBackward(rangekey)) {
                if (!equal_ok && keytest.CompareTo(rec.Key) == 0) {
                    continue;
                }
                return rec;
            }
            throw new KeyNotFoundException("SubSetStage.FindPrev: no record found before: " + keytest + " equal_ok:" + equal_ok);
        }


        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner) {
            return this._scan(scanner, direction_is_forward: true);
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner) {
            return this._scan(scanner, direction_is_forward: false);
        }

        #endregion

        #region Private Members

        private IEnumerable<KeyValuePair<RecordKey, RecordData>> _scan(IScanner<RecordKey> scanner, bool direction_is_forward) {
            long max_valid_timestamp = 0;
            var max_valid_record = new KeyValuePair<RecordKey, RecordData>(null, null);

            RecordKey last_key = null;

            IEnumerable<KeyValuePair<RecordKey, RecordData>> scan_enumerable;

            if (direction_is_forward) {
                scan_enumerable = next_stage.scanForward(scanner);
            } else {
                scan_enumerable = next_stage.scanBackward(scanner);
            }

            foreach (KeyValuePair<RecordKey, RecordData> row in scan_enumerable) {
                last_key = row.Key;

                if (this.is_frozen) {
                    Console.WriteLine("Frozen Snapshot({0}) stage saw: {1}",
                        this.frozen_at_timestamp, row);
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
                        max_valid_record = new KeyValuePair<RecordKey, RecordData>(null, null);
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
                max_valid_record = new KeyValuePair<RecordKey, RecordData>(null, null);
                max_valid_timestamp = 0;
            }
        }

        #endregion


    }

}