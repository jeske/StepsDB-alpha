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

        private RecordKey _appendKeypartsForIndex(BsonDocument doc, List<string> index_spec, RecordKey index_key) {            
            foreach (var index_field_name in index_spec) {
                var element = doc.GetElement(index_field_name);
                switch (element.Value.BsonType) {
                    case BsonType.String:
                        index_key.appendKeyPart(new RecordKeyType_String(element.Value.AsString));
                        break;
                    case BsonType.Int64:
                        index_key.appendKeyPart(new RecordKeyType_Long(element.Value.AsInt64));
                        break;
                    case BsonType.Int32:
                        index_key.appendKeyPart(new RecordKeyType_Long(element.Value.AsInt32));
                        break;
                    default:
                        throw new Exception("unsupported index type");
                        break;
                }
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
                Console.WriteLine(" idx:{0} score:{1} spec:{2}",
                    scored_index.value2, scored_index.value1,
                    String.Join(",", scored_index.value2 == 0 ? pk_spec : indicies[scored_index.value2]));
            }

            // (2) create a query-plan to execute


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
