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

    [TestFixture]
    public class A01_BasicBlock_Perf {

        [Test]
        public void T10_BlockBasic_Perftest() {
            // iterate through blocksizes, randomly generating input data, and then doing some
            // random key queries to see how fast retrieval is

            int[] block_sizes = { 2 * 1024, 40 * 1024, 100 * 1024, 2 * 1024 * 1024 };
            int[] value_sizes = { 4, 10, 30, 100, 1000, 10000 };
            int[,] perf_results = new int[block_sizes.Length,value_sizes.Length];
            int READ_COUNT = 2000;

            Random rnd = new Random((int)DateTime.Now.ToBinary());

            foreach (int block_size in block_sizes) {
                foreach (int value_size in value_sizes) {
                    SegmentBlockBasicEncoder enc = new SegmentBlockBasicEncoder();
                    MemoryStream ms = new MemoryStream();
                    enc.setStream(ms);
                    
                    // encode the block
                    int curblock_size = 0;
                    while (curblock_size < block_size) {
                        // generate a random key
                        RecordKey key = new RecordKey().appendParsedKey("" + rnd.Next(0xFFFFFF));

                        // generate a random value
                        byte[] data = new byte[value_size];
                        for (int i=0;i<value_size;i++) {
                            data[i] = (byte)rnd.Next(40,50);
                        }
                        RecordUpdate upd = RecordUpdate.WithPayload(data);
                        curblock_size += value_size;

                        enc.add(key,upd);
                    }
                    enc.flush();

                    // init the decoder
                    SegmentBlockBasicDecoder dec = new SegmentBlockBasicDecoder(new BlockAccessor(ms.ToArray()));
                    
                    System.GC.Collect();  // force GC so it may not happen during the test                    
                    // perform random access test
                    DateTime start = DateTime.Now;
                    for (int i=0;i<READ_COUNT;i++) {
                        RecordKey key = new RecordKey().appendParsedKey("" + rnd.Next(0xFFFFFF));

                        try {
                            dec.FindNext(key, true);
                        }
                        catch (KeyNotFoundException) {
                            // no problem
                        }
                    }
                    double duration_ms = (DateTime.Now - start).TotalMilliseconds;
                    double reads_per_second = (READ_COUNT * 1000.0) / (duration_ms) ;
                    System.Console.WriteLine("BlockSize {0,10}, ValueSize {1,10}, {2,10} reads in {3,10}ms,  {4,10} read/sec",
                        block_size, value_size, READ_COUNT, duration_ms, reads_per_second);


                }
            }

        } // testend
    }
}
