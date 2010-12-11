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


    public class IndexPartSpec {
        string field_name;
        public IndexPartSpec(string field_name) {
            this.field_name = field_name;
        }
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
                index_key.appendKeyPart(new RecordKeyType_String(element.Value.AsString));
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


#if false
        var buffer = new BsonBuffer();
        buffer.LoadFrom(tcpClient.GetStream());
        var reply = new MongoReplyMessage<TDocument>();
        reply.ReadFrom(buffer);
        return reply;
  

#endif

        public IEnumerable<BsonDocument> Find(BsonDocument query_doc) {
            yield break;
        }

    }
}