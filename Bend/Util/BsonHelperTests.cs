
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
            var doc1 = new BsonDocument{ { "count", 4 } };

            var update_cmds = new BsonDocument{ { "$inc", new BsonDocument{ { "count" , 5 } } } };

            var doc2 = new BsonDocument{ { "count", 9 } };

            Assert.True(!doc2.Equals(doc1), "shouldn't be equal before apply");
            
            BsonHelper.applyUpdateCommands(doc1, update_cmds);

            Assert.True(doc2.Equals(doc1), "inc didn't apply");

        }
    }


}