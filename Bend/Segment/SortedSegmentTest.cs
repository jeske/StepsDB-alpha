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


        // -------------------------------- RangeScan -----------------------------------
        // TODO: change this to happen without PipeQualifier, and pull those tests up to
        //   a LayerManager Integration level
        [Test]
        public void T03_RangeScan() {
            SegmentMemoryBuilder builder = new SegmentMemoryBuilder();

            // generate a Pipe
            PipeStagePartition p =
                new PipeStagePartition("tablename",
                    new PipeStagePartition("id",
                        new PipeStageEnd()
                    )
                );

            // generate a set of data into a segment
            {
                // setup the HDF context
                PipeHdfContext ctx = new PipeHdfContext();
                ctx.setQualifier("tablename", "datatable");


                for (int i = 0; i < 1000; i += 30) {
                    ctx.setQualifier("id", i.ToString());
                    // generate a PipeRowQualifier
                    PipeRowQualifier row = p.generateRowFromContext(ctx);

                    // produce a RecordKey
                    RecordKey key = new RecordKey();
                    foreach (QualifierBase qualpart in row) {
                        if (qualpart.GetType() == typeof(QualifierExact)) {
                            key.appendKeyPart(qualpart.ToString());
                        } else {
                            throw new Exception("only exactly qualifiers are allowed in row updates");
                        }
                    }
                    // put it in the memory segment
                    builder.setRecord(key, RecordUpdate.WithPayload(i.ToString()));
                }
            }

            // scan for a set of matching records (a subset of all records)
            {
                // .. build a context qualifier for the pipe
                PipeHdfContext ctx = new PipeHdfContext();
                ctx.setQualifier("tablename", "datatable");
                ctx.setQualifier("id", new QualifierAny());

                // foreach(KeyValuePair<RecordKey,RecordUpdate> row in builder.walkQualifier(ctx,
                
            }


            Assert.Fail("not implemented");
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