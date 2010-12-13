
// BsonHelperTest - Tests for BsonHelper, an evaluation engine for MongoDB style update commands
//
// authored by David W. Jeske (2008-2010)
//
// This code is provided without warranty to the public domain. You may use it for any
// purpose without restriction.

using System;
using System.Text;
using System.Threading;

using System.Collections.Generic;

using MongoDB.Bson;

namespace BendTests {
    using Bend;
    using NUnit.Framework;


    [TestFixture]
    public class A00_BsonHelperTests {
        [SetUp]
        public void TestSetup() {
            System.GC.Collect(); // cause full collection to try and dispose/free filehandles
        }

        [Test]  // $inc
        public void T00_TestIncrement() {

            // basic $inc
            {
                var doc1 = new BsonDocument { { "count", 4 } };
                var update_cmds = new BsonDocument { { "$inc", new BsonDocument { { "count", 5 } } } };
                var doc2 = new BsonDocument { { "count", 9 } };

                Assert.True(!doc2.Equals(doc1), "(basic inc) shouldn't be equal before apply");
                BsonHelper.applyUpdateCommands(doc1, update_cmds);
                Assert.True(doc2.Equals(doc1), "(basic inc) inc didn't apply");

            }

            // $inc a.b.c + 2
            {

                var doc1 = new BsonDocument { 
                 { "a", new BsonDocument {
                     { "count" , 4 }
                   } 
                 }}; 


                var update_cmds = new BsonDocument { { "$inc", new BsonDocument { { "a.count", 5 } } } };
                var doc2 = new BsonDocument { 
                 { "a", new BsonDocument {
                     { "count" , 9 }
                   } 
                 }}; 

                Assert.True(!doc2.Equals(doc1), "(nested key inc) shouldn't be equal before apply");
                BsonHelper.applyUpdateCommands(doc1, update_cmds);
                Assert.True(doc2.Equals(doc1), "(nested key inc) inc didn't apply");
            }

        }
    }


}