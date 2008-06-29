// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;

using Bend;

namespace BendTests
{

    [TestFixture]
    public class A02_SortedSegmentTests
    {

        [Test]
        public void T02_BuilderReader() {
            SegmentMemoryBuilder builder = new SegmentMemoryBuilder();
            builder.setRecord(new RecordKey().appendParsedKey("test/1"),
                RecordUpdate.WithPayload("3"));

            MemoryStream ms = new MemoryStream();

            SegmentWriter segmentWriter = new SegmentWriter(builder.sortedWalk());
            segmentWriter.writeToStream(ms);

            // rewind
            ms.Seek(0, SeekOrigin.Begin);
            SegmentReader reader = new SegmentReader(ms);
            RecordUpdate update;
            GetStatus status = reader.getRecordUpdate(new RecordKey().appendParsedKey("test/1"), out update);
            Assert.AreEqual(GetStatus.PRESENT, status);
            Assert.AreEqual("3", update.ToString());
        }

    }

    [TestFixture]
    public class ZZ_TODO_SortedSegment
    {

        [Test]
        public void T00_SortedFindNext_TypeRegistry() {
            // TODO: use a type registry to ask for the preferred implementation of 
            //   IScannableDictionary
            Assert.Fail("need to findnext");
        }

    }
}