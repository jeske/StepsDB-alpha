using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MongoDB.Bson;

namespace StepsDBLib {

    public class StepsNode {

        private StepsNode() {

        }

        public static StepsNode Connect(string url) {
            return new StepsNode();    
        }

        public StepsDatabase GetDatabase(string dbname) {
            return new StepsDatabase(this, dbname);
        }

        public StepsDatabase CreateDatabase(string dbname) {
            throw new Exception("unimplemented");
        }        

    }

    public class StepsDatabase {
        StepsNode mynode;
        public StepsDatabase(StepsNode node, string dbname) {
            mynode = node;
        }

        public StepsCollection<T> GetCollection<T>(string collection_name) where T : BsonDocument  {
            return new StepsCollection<T>(this);
        }

    }

    public class StepsCollection<T> where T:BsonDocument {
        StepsDatabase db;
        public StepsCollection(StepsDatabase db) {
            this.db = db;            
        }

        public void Insert(T doc) {
            throw new Exception("unimplemented");
        }

        // perhaps make compatibility method for mongo, with a document batch?
        public void InsertBatch(List<T> batch) {
            throw new Exception("unimplemented");
        }

        public StepsCursor<T> Find(BsonDocument query) {
            throw new Exception("unimplemented");

        }

        public StepsCursor<T_OTHER> FindAs<T_OTHER>(BsonDocument query) where T_OTHER : BsonDocument {
            throw new Exception("unimplemented");
        }
        
        public void Save(T doc) {
            throw new Exception("unimplemented");
        }

        public void Update(T doc) {
            throw new Exception("unimplemented");
        }


    }

    public class StepsCursor<T> where T : BsonDocument {

        public IEnumerator<T> GetEnumerator() {
            throw new Exception("unimplemented");
        }
    }
}
