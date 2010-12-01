// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using NUnit.Framework;

using Bend;
using System.Threading;

namespace BendTests {

    [TestFixture]
    public class A02_RangemapManagerTests {
        [SetUp]
        public void TestSetup() {
            System.GC.Collect(); // cause full collection to try and dispose/free filehandles
        }

        [Test]
        public void T000_RangeKey() {
            RecordKey target = new RecordKey().appendParsedKey("D");

            RangemapManager.RangeKey segptr = RangemapManager.RangeKey.newSegmentRangeKey(
                new RecordKey().appendParsedKey("A"),
                new RecordKey().appendParsedKey("Z"), 0);

            Assert.AreEqual(true, segptr.eventuallyContainsKey(target), "should be in segptr");

            RangemapManager.RangeKey metasegptr = RangemapManager.RangeKey.newSegmentRangeKey(
                segptr.toRecordKey(),
                new RecordKey().appendParsedKey("A"), 0);

            Assert.AreEqual(true,metasegptr.eventuallyContainsKey(target), "should be in metasegptr");


            RangemapManager.RangeKey segptr2 = RangemapManager.RangeKey.newSegmentRangeKey(
                new RecordKey().appendParsedKey("E"),
                new RecordKey().appendParsedKey("Z"), 0);

            Assert.AreEqual(false,segptr2.eventuallyContainsKey(target), "should not be in segptr2");

            RangemapManager.RangeKey metasegptr2 = RangemapManager.RangeKey.newSegmentRangeKey(
                segptr2.toRecordKey(),
                new RecordKey().appendParsedKey("A"), 0);

            Assert.AreEqual(false,metasegptr2.eventuallyContainsKey(target), "should not be in metasegptr2");
        }
    }
}