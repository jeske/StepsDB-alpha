﻿// Copyright (C) 2008, by David W. Jeske
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

        public void dumpPrioritizedMergeList(MergeManager_Incremental mm) {
            System.Console.WriteLine("-- dumpPrioritizedMergeList start");
            foreach (var mc in mm.prioritizedMergeCandidates.Keys) {
                System.Console.WriteLine(mc.ToString());
            }
            System.Console.WriteLine("-- dumpPrioritizedMergeList end");
        }

        [Test]
        public void T000_BasicTests() {
            MergeCandidate mc;

            var mm = new MergeManager_Incremental();
            var gen0seg = new SegmentDescriptor(0, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("Z"));
            var gen1seg = new SegmentDescriptor(1, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("Z"));
            var gen2seg = new SegmentDescriptor(2, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("Z"));

            // add two segments on top of eachother
            mm.notify_addSegment(gen0seg);
            mm.notify_addSegment(gen1seg);

            Assert.AreEqual(1, mm.getNumberOfCandidates());
            mc = mm.getBestCandidate();            
            Assert.AreEqual(new SegmentDescriptor[1] { gen1seg }, mc.source_segs);
            Assert.AreEqual(new SegmentDescriptor[1] { gen0seg }, mc.target_segs);
            

            // add a third segment
            mm.notify_addSegment(gen2seg);

            mc = mm.getBestCandidate();
            System.Console.WriteLine(mc.ToString());
            Assert.AreEqual(3, mm.getNumberOfCandidates());
            Assert.AreEqual(new SegmentDescriptor[2] { gen2seg, gen1seg }, mc.source_segs);
            Assert.AreEqual(new SegmentDescriptor[1] { gen0seg }, mc.target_segs);


            // test removal
            mm.notify_removeSegment(gen2seg);
            Assert.AreEqual(1, mm.getNumberOfCandidates());
            mc = mm.getBestCandidate();
            Assert.AreEqual(new SegmentDescriptor[1] { gen1seg }, mc.source_segs);
            Assert.AreEqual(new SegmentDescriptor[1] { gen0seg }, mc.target_segs);

            mm.notify_removeSegment(gen1seg);
            Assert.AreEqual(0, mm.getNumberOfCandidates());

            mm.notify_addSegment(gen1seg);
            Assert.AreEqual(1, mm.getNumberOfCandidates());

            mm.notify_removeSegment(gen0seg);
            Assert.AreEqual(0, mm.getNumberOfCandidates());
        }

        [Test]
        public void T000_TestRangeBoundaries() {
            MergeCandidate mc;

            var mm = new MergeManager_Incremental();
            var gen0abseg = new SegmentDescriptor(0, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("B"));
            var gen0dfseg = new SegmentDescriptor(0, new RecordKey().appendParsedKey("D"), new RecordKey().appendParsedKey("F"));
            var gen1acseg = new SegmentDescriptor(1, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("C"));
            var gen2azseg = new SegmentDescriptor(2, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("Z"));

            // add two gen0 segs
            mm.notify_addSegment(gen0abseg);
            mm.notify_addSegment(gen0dfseg);

            Assert.AreEqual(0, mm.getNumberOfCandidates());

            // add gen1 seg
            mm.notify_addSegment(gen1acseg);
            Assert.AreEqual(1, mm.getNumberOfCandidates());
            mc = mm.getBestCandidate();
            Assert.AreEqual(new SegmentDescriptor[1] { gen1acseg }, mc.source_segs);
            Assert.AreEqual(new SegmentDescriptor[1] { gen0abseg }, mc.target_segs);


            // add gen2 seg
            mm.notify_addSegment(gen2azseg);

            dumpPrioritizedMergeList(mm);
            Assert.AreEqual(3, mm.getNumberOfCandidates());
            mc = mm.getBestCandidate();
            Assert.AreEqual(new SegmentDescriptor[2] { gen2azseg, gen1acseg }, mc.source_segs);
            Assert.AreEqual(new SegmentDescriptor[2] { gen0abseg, gen0dfseg }, mc.target_segs);


            // simulate "merge down" of gen1ac -> gen0abseg
            var gen0acseg = new SegmentDescriptor(0, new RecordKey().appendParsedKey("A"), new RecordKey().appendParsedKey("C"));            
            mm.notify_removeSegment(gen1acseg);
            mm.notify_removeSegment(gen0abseg);
            Assert.AreEqual(0, mm.getNumberOfCandidates());
            mm.notify_addSegment(gen0acseg);

            dumpPrioritizedMergeList(mm);
            Assert.AreEqual(1, mm.getNumberOfCandidates());
            mc = mm.getBestCandidate();
            Assert.AreEqual(new SegmentDescriptor[1] { gen2azseg}, mc.source_segs);
            Assert.AreEqual(new SegmentDescriptor[2] { gen0acseg, gen0dfseg }, mc.target_segs);


        }
    }
}