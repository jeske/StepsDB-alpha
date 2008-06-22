// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;
using NUnit.Framework;

namespace Bend
{

    [TestFixture]
    public class SortedSegmentTests
    {
        [Test]
        public void TestBuilderReader() {
            SegmentBuilder builder = new SegmentBuilder();
            builder.setRecord(new RecordKey().appendParsedKey("test/1"),
                RecordUpdate.WithPayload("3"));

            MemoryStream ms = new MemoryStream();

            builder.writeToStream(ms);

            // rewind
            ms.Seek(0, SeekOrigin.Begin);
            SegmentReader reader = new SegmentReader(ms);
            RecordUpdate update;
            GetStatus status = reader.getRecordUpdate(new RecordKey().appendParsedKey("test/1"), out update);
            Assert.AreEqual(GetStatus.PRESENT, status);
            Assert.AreEqual("3", update.ToString());
        }
    }
}