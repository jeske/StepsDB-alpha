// Copyright (C) 2008-2011 by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;
using System.Collections.Generic;
using System.Text;

using System.Threading;

using NUnit.Framework;

using Bend;

using MongoDB.Bson;
using MongoDB.Bson.IO;

namespace Bend {

    public interface IStepsDocumentDB {
        void ensureIndex(string[] keys_to_index);        
        void Insert(BsonDocument doc);
        IEnumerable<BsonDocument> Find(BsonDocument query_doc);
    }



    public class DocumentDatabaseStage : IStepsDocumentDB {
        IStepsKVDB next_stage;

        List<string> pk_spec = null;
        BDSkipList<long,List<string>> indicies = new BDSkipList<long,List<string>>();

        public DocumentDatabaseStage(IStepsKVDB next_stage) {
            this.next_stage = next_stage;

            this.pk_spec = new List<string>{ "_id" };

        }

        public void ensureIndex(string[] keys_to_index) {
            List<string> index_spec = new List<string>();
            foreach (var key in keys_to_index) {
                index_spec.Add(key);
            }

            long index_id = DateTime.Now.Ticks;

            this.indicies.Add(index_id, index_spec);
        }

        private RecordKeyType _bsonLookupToRecordKeyType(BsonDocument doc, string field_name) {
            var element = doc.GetElement(field_name);
            switch (element.Value.BsonType) {
                case BsonType.String:
                    return new RecordKeyType_String(element.Value.AsString);
                    break;
                case BsonType.Int64:
                    return new  RecordKeyType_Long(element.Value.AsInt64);
                    break;
                case BsonType.Int32:
                    return new RecordKeyType_Long(element.Value.AsInt32);
                    break;               
            }
            throw new Exception("unsupported index type");
        }

        private RecordKey _appendKeypartsForIndex(BsonDocument doc, List<string> index_spec, RecordKey index_key) {            
            foreach (var index_field_name in index_spec) {
                index_key.appendKeyPart(
                    _bsonLookupToRecordKeyType(doc, index_field_name));
            }
            return index_key;
        }

        public void Insert(BsonDocument doc) {

            // serialize the BsonDocument
            var ms = new MemoryStream();
            doc.WriteTo(ms);  

            // write the primary key
            RecordKey primary_key = new RecordKey()
                .appendKeyPart(new RecordKeyType_Long(0));
            _appendKeypartsForIndex(doc, this.pk_spec, primary_key);
            this.next_stage.setValue(primary_key, RecordUpdate.WithPayload(ms.ToArray()));

            byte[] encoded_primary_key = primary_key.encode();

            // write any other keys
            foreach (var index_spec in indicies) {
                Assert.AreNotEqual(0, index_spec.Key, "index id can't be zero!");

                // assemble index
                RecordKey index_key = new RecordKey()
                    .appendKeyPart(new RecordKeyType_Long(index_spec.Key));
                _appendKeypartsForIndex(doc, index_spec.Value, index_key);


                // append primary keyparts
                _appendKeypartsForIndex(doc, this.pk_spec, index_key);

                this.next_stage.setValue(index_key, RecordUpdate.WithPayload(new byte[0]));
            }
        }

        public struct ValuePair<A,B> : IComparable<ValuePair<A,B>> 
            where A : IComparable<A> 
            where B : IComparable<B> {
            public readonly A value1;
            public readonly B value2;
            public int CompareTo(ValuePair<A, B> target) {
                int cmpval = value1.CompareTo(target.value1);
                if (cmpval != 0) { return cmpval; }
                return value2.CompareTo(target.value2);
            }
            public ValuePair(A a,B b) {
                value1 = a;
                value2 = b;
            }
        }

        private float _scoreIndex(BsonDocument query_doc, List<string> index_spec) {
            float score = 0.0f;

            // (1) We walk the prefix of each index against the query and count the 
            // number of prefix-terms in the index match against specified parts 
            // of the query. The longest match becomes the best index to use,
            // and the query is executed against it.

            foreach (var key_part in index_spec) {                
                if (query_doc.Contains(key_part)) {
                    score += 1.0f;
                } else {
                    break;
                }
            }
            return score;
        }

        private ScanRange<RecordKey> _scanRangeForQueryAndIndex(BsonDocument query_doc, long index_id) {
            var key_prefix = new RecordKey().appendKeyPart(new RecordKeyType_Long(index_id));            

            List<string> index_spec;
            if (index_id == 0) { 
                index_spec = pk_spec;
            } else {
                index_spec = indicies[index_id];
            }

            foreach (var index_part in index_spec) {
                key_prefix.appendKeyPart(_bsonLookupToRecordKeyType(query_doc, index_part));                
            }
            
            return new ScanRange<RecordKey>(key_prefix, RecordKey.AfterPrefix(key_prefix), null);
        }

        private KeyValuePair<RecordKey,RecordData> _getValueHack(RecordKey key) {
            foreach (var rec in next_stage.scanForward(
                new ScanRange<RecordKey>(key, new ScanRange<RecordKey>.maxKey(), null))) {
                    if (rec.Key.CompareTo(key) == 0) {
                        return rec;
                    }
            }
            throw new KeyNotFoundException(String.Format("_getValueHack lookup failed {0}",key));
        }

        private BsonDocument _unpackDoc(RecordData data) {
            var ms = new MemoryStream(data.data);
            var doc = BsonDocument.ReadFrom(ms);
            return doc;
        }

        public IEnumerable<BsonDocument> Find(BsonDocument query_doc) {
            // (1) index selection
            var scored_indicies = new BDSkipList<ValuePair<float, long>, long>();
            float pk_score = _scoreIndex(query_doc, this.pk_spec);

            scored_indicies.Add(new ValuePair<float, long>(pk_score, 0), 0);
             
            foreach (var index in this.indicies) {
                float idx_score = _scoreIndex(query_doc, index.Value);
                scored_indicies.Add(new ValuePair<float, long>(idx_score, index.Key), index.Key);
            }

            foreach (var scored_index_entry in scored_indicies) {
                var scored_index = scored_index_entry.Key;
                Console.WriteLine(" score:{1} idx:{0} spec:{2}",
                    scored_index.value2, scored_index.value1,
                    String.Join(",", scored_index.value2 == 0 ? pk_spec : indicies[scored_index.value2]));
            }

            // (2) create a query-plan to execute
            long use_index_id = 0;

            if (scored_indicies.Count != 0) {
                var best_index_rec = scored_indicies.FindPrev(null, true);
                use_index_id = best_index_rec.Key.value2;
            }

            if (use_index_id == 0) {
                // PK scan
            } else {
                List<string> index_spec;
                if (use_index_id == 0) {
                    index_spec = pk_spec;
                } else {
                    index_spec = indicies[use_index_id];
                }
                // secondary index scan
                var scanrange = _scanRangeForQueryAndIndex(query_doc, use_index_id);
                foreach (var idx_rec in next_stage.scanForward(scanrange)) {
                    // unpack the secondary index rec into a primary key
                    var pk_lookup_key = new RecordKey().appendKeyPart(new RecordKeyType_Long(0));
                    for (int x = index_spec.Count+1; x < idx_rec.Key.key_parts.Count; x++) {
                        pk_lookup_key.appendKeyPart(idx_rec.Key.key_parts[x]);
                    }
                    Console.WriteLine("found rec {0}, lookup data {1}", idx_rec.Key, pk_lookup_key);
                    KeyValuePair<RecordKey, RecordData> data_rec = new KeyValuePair<RecordKey, RecordData>(null, null);
                    try {
                        data_rec = _getValueHack(pk_lookup_key);                        
                    } catch (KeyNotFoundException) {
                        // the index didn't point to a valid record, CRAP!
                        Console.WriteLine("dangling index record");
                    }
                    if (data_rec.Key != null) {
                        yield return _unpackDoc(data_rec.Value);
                    }
                }
            }


            // (3) execute
            yield break;
        }

    }
}







#if false
        var buffer = new BsonBuffer();
        buffer.LoadFrom(tcpClient.GetStream());
        var reply = new MongoReplyMessage<TDocument>();
        reply.ReadFrom(buffer);
        return reply;
  

#endif
