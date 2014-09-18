// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;
using System.Collections.Generic;

using NUnit.Framework;
using Bend;

namespace BendTests {
    // TEST: RecordOffsetList encoder/decoder

    [TestFixture]
    public class A02_SegmentBlockRecordOffsetListTests {
        [Test]
        public void T01_RecordOffsetList_sortedWalk() {
            string[] testvalues = { "test/1", "test/2", "test/3" };
            byte[] databuffer;

            // encode a buffer
            {
                MemoryStream ms = new MemoryStream();
                // add some values to the block encoder
                SegmentBlockEncoderRecordOffsetList enc = new SegmentBlockEncoderRecordOffsetList();
                enc.setStream(ms);
                for (int i = 0; i < testvalues.Length; i++) {
                    RecordKey tkey = new RecordKey().appendParsedKey(testvalues[i]);
                    RecordUpdate tupdate = RecordUpdate.WithPayload("data: " + testvalues[i]);

                    enc.add(tkey, tupdate);
                }
                enc.flush();

                databuffer = ms.ToArray();
            }

            Console.WriteLine("databuffer len : " + databuffer.Length);
            Console.WriteLine("Hex: " + Lsd.ToHexString(databuffer));

            // test sortedWalk
            {
                BlockAccessor rs = new BlockAccessor(databuffer);
                var decoder = new SegmentBlockDecoderRecordOffsetList(rs);
                int count = 0;
                foreach (var row in decoder.sortedWalk()) {
                    Console.WriteLine(row);
                    count++;
                }

                Assert.AreEqual(testvalues.Length, count, "wrong number of elements in sorted walk");
            }
        }
    }

}

