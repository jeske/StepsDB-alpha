// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;
using System.Collections.Generic;

using NUnit.Framework;
using Bend;

namespace BendTests
{
    // TEST: basic block encoder/decoder

    [TestFixture]
    public class A02_SegmentBasicBlock
    {
        // eventually we make a test to do data-format verification of the encoder.
        // however, this will be more useful after the format is constant

        [Test]
        public void T01_BasicBlock_Find() {
            string[] testvalues = { "test/1", "test/2", "test/3" };
            byte[] databuffer;

            // encode a buffer
            {
                MemoryStream ms = new MemoryStream();
                // add some values to the block encoder
                SegmentBlockBasicEncoder enc = new SegmentBlockBasicEncoder();
                enc.setStream(ms);
                for (int i = 0; i < testvalues.Length; i++) {
                    RecordKey tkey = new RecordKey().appendParsedKey(testvalues[i]);
                    RecordUpdate tupdate = RecordUpdate.WithPayload("data: " + testvalues[i]);

                    enc.add(tkey, tupdate);
                }
                enc.flush();

                databuffer = ms.ToArray();
            }

            // test FindNext(key,equal_ok=true)
            {
                BlockAccessor rs = new BlockAccessor(databuffer);
                SegmentBlockBasicDecoder decoder = new SegmentBlockBasicDecoder(rs);
                for (int i = testvalues.Length -1; i >= 0; i--) {
                    RecordKey tkey = new RecordKey().appendParsedKey(testvalues[i]); 
                    RecordUpdate tupdate = RecordUpdate.WithPayload("data: " + testvalues[i]);

                    KeyValuePair<RecordKey,RecordUpdate> row = decoder.FindNext(tkey, true);
                    Assert.AreEqual(tkey, row.Key,  "record keys should match");
                    Assert.AreEqual(tupdate, row.Value, "record values should match:");
                }
            }

            // test FindNext(key,equal_ok=false)
            {
                BlockAccessor rs = new BlockAccessor(databuffer);
                SegmentBlockBasicDecoder decoder = new SegmentBlockBasicDecoder(rs);
                for (int i = testvalues.Length - 2; i >= 0; i--) {
                    RecordKey tkey = new RecordKey().appendParsedKey(testvalues[i]);
                    RecordUpdate tupdate = RecordUpdate.WithPayload("data: " + testvalues[i]);

                    RecordKey fkey = new RecordKey().appendParsedKey(testvalues[i + 1]);
                    RecordUpdate fupdate = RecordUpdate.WithPayload("data: " + testvalues[i+1]);

                    KeyValuePair<RecordKey, RecordUpdate> row = decoder.FindNext(tkey, false);
                    Assert.AreEqual(fkey, row.Key, "findnext(,false) should find next key");
                    Assert.AreEqual(fupdate, row.Value, "findnext(,false) should finx next key (value was botched)");
                }


                // test for "next" after end of buffer, and we should get an exception
                // test for "next" at beginning and we should get first
                // test some random values in the middle



            }
                       
        } // testend


       

        [Test]
        public void T02_BasicBlock_Enumerators() {
            // exercize the iscannable interface
            string[] testvalues = { "test/1", "test/2", "test/3" };
            byte[] databuffer;

            // encode a buffer
            {
                MemoryStream ms = new MemoryStream();
                // add some values to the block encoder
                SegmentBlockBasicEncoder enc = new SegmentBlockBasicEncoder();
                enc.setStream(ms);
                for (int i = 0; i < testvalues.Length; i++) {
                    RecordKey tkey = new RecordKey().appendParsedKey(testvalues[i]);
                    RecordUpdate tupdate = RecordUpdate.WithPayload("data: " + testvalues[i]);

                    enc.add(tkey, tupdate);
                }
                enc.flush();

                databuffer = ms.ToArray();
            }

            // decode and test the buffer, scan enumerators
            {
                BlockAccessor rs = new BlockAccessor(databuffer);
                SegmentBlockBasicDecoder decoder = new SegmentBlockBasicDecoder(rs);
                int pos = 0;
                IScanner<RecordKey> scanner = ScanRange<RecordKey>.All();
                foreach (KeyValuePair<RecordKey, RecordUpdate> row in decoder.scanForward(scanner)) {
                    RecordKey tkey = new RecordKey().appendParsedKey(testvalues[pos]);
                    RecordUpdate tupdate = RecordUpdate.WithPayload("data: " + testvalues[pos]);

                    Assert.AreEqual(tkey, row.Key, "forward, record keys should match");
                    Assert.AreEqual(tupdate, row.Value, "forward, record values should match:");
                    pos++;
                }
                Assert.AreEqual(testvalues.Length, pos, "forward, should return all values we put in");


                pos = testvalues.Length - 1;
                foreach (KeyValuePair<RecordKey, RecordUpdate> row in decoder.scanBackward(scanner)) {
                    RecordKey tkey = new RecordKey().appendParsedKey(testvalues[pos]);
                    RecordUpdate tupdate = RecordUpdate.WithPayload("data: " + testvalues[pos]);

                    Assert.AreEqual(tkey, row.Key, "backward, record keys should match, pos=" + pos);
                    Assert.AreEqual(tupdate, row.Value, "backward, record values should match:");
                    pos--;
                }
                Assert.AreEqual(-1, pos, "backward, should return all values we put in");

            }


            
        }  // testend
    }



    [TestFixture]
    public class ZZ_TODO_SegmentBlockBasic
    {
 
        [Test]
        public void T01_TestRecordDecodeChecksum() {
            // add a checksum to the record so we can catch decode offset mistakes.
            Assert.Fail("not implemented");
        }
    }
}


namespace BendPerfTest
{
    using System.Threading;
    using BendTests;

    
    public partial class A01_Block_Perf {

        class BasicBlockTestFactory : IBlockTestFactory
        {

            public ISegmentBlockEncoder makeEncoder() {
                return new SegmentBlockBasicEncoder();
            }

            public ISegmentBlockDecoder makeDecoder(BlockAccessor block) {
                return new SegmentBlockBasicDecoder(block);
            }
        }

        [Test]
        public void T10_BlockBasic_Perftest() {
            SegmentBlock_Tests.Block_Perftest(new BasicBlockTestFactory());
        } // testend
    }
}
