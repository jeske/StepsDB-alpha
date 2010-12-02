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
        public void T000_RangeKey_EncodeDecode() {
            RecordKey a_key = new RecordKey().appendParsedKey("AAAAAAAAAA/ZZZZZZZZZZZZZZ");
            RecordKey b_key = new RecordKey().appendParsedKey("B/ZZ");

            var rangekey_preencode = RangemapManager.RangeKey.newSegmentRangeKey(a_key, b_key, 0);
            RecordKey a_range = rangekey_preencode.toRecordKey();

            var rangekey = RangemapManager.RangeKey.decodeFromRecordKey(a_range);

            Assert.AreEqual(rangekey.lowkey, a_key, "firstkey mismatch");
            Assert.AreEqual(rangekey.highkey, b_key, "lastkey mismatch");            
        }

        [Test]
        public void T000_RangeKey_EncodedSort() {
            RecordKey a_key = new RecordKey().appendParsedKey("AAAAAAA/ZZZZZZZZZ/s/s/s/s/s");
            RecordKey b_key = new RecordKey().appendParsedKey("B/ZZ");

            Assert.True(a_key.CompareTo(b_key) < 0, "a key should be less than b key");

            RecordKey a_range = RangemapManager.RangeKey.newSegmentRangeKey(a_key, a_key, 0).toRecordKey();
            RecordKey b_range = RangemapManager.RangeKey.newSegmentRangeKey(b_key, b_key, 0).toRecordKey();

            Console.WriteLine(Lsd.ToHexString(a_range.encode()));
            Console.WriteLine(Lsd.ToHexString(b_range.encode()));

            Assert.True(a_range.CompareTo(b_range) < 0, "a range should also be less than b range!!");
            Assert.True(b_range.CompareTo(a_range) > 0, "b range should be greater than a range!!");
        }

        [Test]
        public void T001_RangeKey_ContainmentTesting() {
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

            // .zdata.index.jeske not in .ROOT.FREELIST.HEAD -> .zdata.index.</tr>.c:\EmailTest\Data\trakken-stats:6919.143
           
        }

        [Test]
        public void T001_RangeKey_Bug() {
            // .zdata.index.jeske not in .ROOT.FREELIST.HEAD -> .zdata.index.</tr>.c:\EmailTest\Data\trakken-stats:6919.143

            RecordKey target = new RecordKey().appendParsedKey(".zdata/index/jeske");
            RecordKey lowkey = new RecordKey().appendParsedKey(".ROOT/FREELIST/HEAD");
            RecordKey highkey = new RecordKey().appendParsedKey(".zdata/index/<tr>");

            RangemapManager.RangeKey segptr = RangemapManager.RangeKey.newSegmentRangeKey(
                lowkey, highkey, 0);
            Assert.AreEqual(true, segptr.eventuallyContainsKey(target), "bug failed");
        }
    }
}