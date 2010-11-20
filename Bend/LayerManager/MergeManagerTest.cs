// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using NUnit.Framework;

using Bend;
using System.Threading;

namespace BendTests {

    [TestFixture]
    public class A02_MergeManagerTests {
        [SetUp]
        public void TestSetup() {
            System.GC.Collect(); // cause full collection to try and dispose/free filehandles
        }

        [Test]
        public void T000_BasicTests() {

            var mm = new MergeManager_Incremental();
            var gen0seg = new SegmentDescriptor(0, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("Z"));
            var gen1seg = new SegmentDescriptor(1, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("Z"));
            var gen2seg = new SegmentDescriptor(2, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("Z"));

            // add two segments on top of eachother
            mm.notify_addSegment(gen0seg);
            mm.notify_addSegment(gen1seg);

            var mc = mm.getBestCandidate();
            System.Console.WriteLine(mc.ToString());
            Assert.AreEqual(2, mm.getNumberOfCandidates());
            Assert.AreEqual(new SegmentDescriptor[1] { gen1seg }, mc.source_segs);
            Assert.AreEqual(new SegmentDescriptor[1] { gen0seg }, mc.target_segs);
            

            // add a third segment
            mm.notify_addSegment(gen2seg);

            mc = mm.getBestCandidate();
            System.Console.WriteLine(mc.ToString());
            Assert.AreEqual(2, mm.getNumberOfCandidates());
            Assert.AreEqual(new SegmentDescriptor[2] { gen2seg, gen1seg }, mc.source_segs);
            Assert.AreEqual(new SegmentDescriptor[1] { gen0seg }, mc.target_segs);
        }

    }
}