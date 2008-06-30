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
    // this class adapts our pipe-qualifier to be able to test against record key
    public class QualAdaptor : IScanner<RecordKey>
    {
        PipeRowQualifier qual;

        

        public QualAdaptor(PipeRowQualifier qual) {
            this.qual = qual;
        }
        public bool MatchTo(RecordKey row_key) {
            IEnumerator<QualifierBase> qual_enum = qual.GetEnumerator();
            IEnumerator<string> key_enum = row_key.GetEnumeratorForKeyparts();

            bool qual_hasmore = qual_enum.MoveNext();
            bool key_hasmore = key_enum.MoveNext();

            bool last_result = true;
            while (qual_hasmore && key_hasmore && (last_result == true)) {
                QualifierBase q_part = qual_enum.Current;
                string key_part = key_enum.Current;

                last_result = q_part.MatchTo(key_part);

                qual_hasmore = qual_enum.MoveNext();
                key_hasmore = key_enum.MoveNext();
            }

            return last_result;
        }
        public IComparable<RecordKey> genLowestKeyTest() {
            RecordKey key = new RecordKey();
            foreach (QualifierBase part in this.qual) {
                key.appendKeyPart(part.genLowestKeyTest().ToString());
            }
            return key;
        }
        public IComparable<RecordKey> genHighestKeyTest() {
            RecordKey key = new RecordKey();
            foreach (QualifierBase part in this.qual) {
                key.appendKeyPart(part.genHighestKeyTest().ToString());
            }
            return key;

        }
    }

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
            // TODO: remove this hacky converting from byte[] to string
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();            

            SegmentMemoryBuilder builder = new SegmentMemoryBuilder();
            int records_written = 0;
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
                    ctx.setQualifier("id", enc.GetString(Lsd.numberToLsd(i,10)));
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
                    builder.setRecord(key, RecordUpdate.WithPayload(Lsd.numberToLsd(i,10)));
                    records_written++;
                }
            }

            int max_i = 0;
            // scan FORWARD for a set of matching records (a subset of all records)
            {
                // .. build a context qualifier for the pipe
                PipeHdfContext ctx = new PipeHdfContext();
                ctx.setQualifier("tablename", "datatable");
                ctx.setQualifier("id", new QualifierAny());
                int records_scanned = 0;

                PipeRowQualifier qualifier = p.generateRowFromContext(ctx);
                int i = 0;
                foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in builder.scanForward(new QualAdaptor(qualifier))) {
                    // check that the recordupdate equals our iterator
                    Assert.AreEqual(enc.GetString(Lsd.numberToLsd(i,10)), enc.GetString(kvp.Value.data), "check recordupdate payload");
                    max_i = Math.Max(i, max_i);
                    i = i + 30;
                    records_scanned++;
                }
                Assert.AreEqual(records_written, records_scanned, "scanForward: expect to read back the same number of records we wrote");
                
            }

            // scan BACKWARD for a set of matching records (a subset of all records)
            {
                // .. build a context qualifier for the pipe
                PipeHdfContext ctx = new PipeHdfContext();
                ctx.setQualifier("tablename", "datatable");
                ctx.setQualifier("id", new QualifierAny());
                int records_scanned = 0;

                PipeRowQualifier qualifier = p.generateRowFromContext(ctx);
                int i = max_i;                
                foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in builder.scanBackward(new QualAdaptor(qualifier))) {
                    // check that the recordupdate equals our iterator
                    Assert.AreEqual(enc.GetString(Lsd.numberToLsd(i, 10)), enc.GetString(kvp.Value.data), "check recordupdate payload");
                    i = i - 30;
                    records_scanned++;
                }
                Assert.AreEqual(records_written, records_scanned, "scanBackward: expect to read back the same number of records we wrote");

            }
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