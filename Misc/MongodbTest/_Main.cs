using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MongoDB.Bson;
using MongoDB.Driver;


using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.DefaultSerializer;
using MongoDB.Driver.Builders;

namespace MongoDBTest {
    class MongoDBTest {
        static void Main(string[] args) {

            var server = MongoServer.Create("mongodb://localhost");

            MongoDatabase test = server.GetDatabase("test");
            MongoCredentials credentials = new MongoCredentials("username", "password");
            MongoDatabase salaries = server.GetDatabase("salaries", credentials);

            MongoCollection docs = test.GetCollection("docs");
            docs.EnsureIndex("title");

            Console.WriteLine("adding some test documents");
            
            for (int x = 0; x < 1000; x++) {

                BsonDocument doc = new BsonDocument();
                doc.Add("title", "title: " + x);
                doc.Add("body", "body: " + x);
                docs.Insert(doc);
            }


            Console.WriteLine("querying");

            foreach (var doc in docs.FindAllAs<BsonDocument>()) {
                Console.WriteLine("doc: {0}", doc["body"]);
            }
            



        }
    }
}
