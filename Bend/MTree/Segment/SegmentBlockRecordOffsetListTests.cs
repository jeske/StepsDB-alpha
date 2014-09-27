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

