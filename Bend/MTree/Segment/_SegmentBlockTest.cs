// Copyright (C) 2008-2014 David W. Jeske
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

using System;
using System.IO;
using System.Collections.Generic;

using NUnit.Framework;
using Bend;

namespace BendTests
{
    [TestFixture]
    public partial class A00_Block {
        [Test]
        public static void T01_BlockEncodeDecodeTest() {
            MemoryStream ms = new MemoryStream();
            byte[] testdata = { 0x00, 0x10, 0x78, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x80, 0x01, 0x00, 0x00, 0x00, 0x00 };

            // init an encoder and add one key which requires escaping
            {
                ISegmentBlockEncoder enc = new SegmentBlockBasicEncoder();
                enc.setStream(ms);

                RecordKey key = new RecordKey().appendParsedKey("TESTSETEST");                
                RecordUpdate update = RecordUpdate.WithPayload(testdata);

                enc.add(key, update);
                enc.flush();
                ms.Flush();

                System.Console.WriteLine("Test Update: " + update.ToString());
            }

            byte[] block_contents = ms.ToArray();
            System.Console.WriteLine("Block Output: " + BitConverter.ToString(block_contents));


            // init the decoder
            {
                ISegmentBlockDecoder dec = new SegmentBlockBasicDecoder(new BlockAccessor(ms.ToArray()));

                foreach (var kvp in dec.sortedWalk()) {
                    System.Console.WriteLine("Payload Update: " + kvp.Value.ToString());

                    byte[] payload = kvp.Value.data;                    
                    Assert.AreEqual(testdata, payload, "payload data mismatch!");
                }
            }

        }
    
    }

}

namespace BendPerfTest
{
    public interface IBlockTestFactory {
        ISegmentBlockEncoder makeEncoder();
        ISegmentBlockDecoder makeDecoder(BlockAccessor block);
    }

    public class SegmentBlockPerf_Tests
    {
        public static void Block_Perftest(IBlockTestFactory factory) {
            // iterate through blocksizes, randomly generating input data, and then doing some
            // random key queries to see how fast retrieval is

            int[] block_sizes = { 2 * 1024, 40 * 1024, 100 * 1024, 512 * 1025, 2 * 1024 * 1024 };
            int[] value_sizes = { 10, 30, 100, 1000, 10000 };
            int[] num_levels = { 2, 3, 4 };
            int[,] perf_results = new int[block_sizes.Length, value_sizes.Length];
            int READ_COUNT = 3000;

            
            Random rnd = new Random((int)DateTime.Now.ToBinary());

            foreach (int key_part_count in num_levels) {
                System.Console.WriteLine("--");
                foreach (int block_size in block_sizes) {
                    foreach (int value_size in value_sizes) {
                        if (value_size > (block_size / 8)) {
                            // we want at least 8 values
                            continue;
                        }

                        System.GC.Collect();
                        // setup the block for encoding
                        ISegmentBlockEncoder enc = factory.makeEncoder();
                        MemoryStream ms = new MemoryStream();
                        enc.setStream(ms);
                        int curblock_size = 0;

                        // do the sorted block create.. we nest it so we can dispose the SkipList
                        {
                            var sorted_input = new BDSkipList<RecordKey, RecordUpdate>();
                            // first create the sorted input                        
                            
                            while (curblock_size < block_size) {
                                // generate a random key
                                RecordKey key = new RecordKey();
                                for (int i = 0; i < key_part_count; i++) {
                                    key.appendParsedKey("" + rnd.Next(0xFFFFFF) + rnd.Next(0xFFFFFF) + rnd.Next(0xFFFFFF));
                                }

                                // generate a random value
                                byte[] data = new byte[value_size];
                                for (int i = 0; i < value_size; i++) {
                                    data[i] = (byte)rnd.Next(40, 50);
                                }
                                RecordUpdate upd = RecordUpdate.WithPayload(data);
                                curblock_size += key.encode().Length;
                                curblock_size += value_size;

                                sorted_input.Add(key, upd);
                            }

                            

                            // encode the block
                            foreach (var kvp in sorted_input) {
                                enc.add(kvp.Key, kvp.Value);
                            }
                            enc.flush();
                            sorted_input = null;  // free the skiplist
                        }

                        // init the decoder
                        ISegmentBlockDecoder dec = factory.makeDecoder(new BlockAccessor(ms.ToArray()));
                        int num_misses = 0;
                        System.GC.Collect();  // force GC so it may not happen during the test                    
                        // perform random access test
                        DateTime start = DateTime.Now;
                        for (int i = 0; i < READ_COUNT; i++) {
                            RecordKey key = new RecordKey();
                            for (int ki = 0; ki < key_part_count; ki++) {
                                key.appendParsedKey("" + rnd.Next(8) + rnd.Next(0xFFFFFF) + rnd.Next(0xFFFFFF));
                            }

                            try {
                                dec.FindNext(key, true);
                            } catch (KeyNotFoundException) {
                                num_misses++;
                                // System.Console.WriteLine("misfetch: {0}", key);
                                // no problem, but this shouuld be small
                            }
                        }
                        double duration_ms = (DateTime.Now - start).TotalMilliseconds;
                        double reads_per_second = (READ_COUNT * 1000.0) / (duration_ms);
                        System.Console.WriteLine("BlockSize src{0,10}  final{6,10}  ratio ({7:0.000}), ValueSize {1,6}, Keyparts {5,3}, {2,6} reads in {3,10:0.0}ms,  {8,6} misses, {4,9:0.00} read/sec",
                            curblock_size, value_size, READ_COUNT, duration_ms, reads_per_second,
                            key_part_count, ms.Length, ((double)ms.Length / (double)curblock_size) * (double)100.0, num_misses);
                    }
                }
            }

        } // testend


    }

}