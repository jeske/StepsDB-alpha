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
        [Test]
        public void T00_BasicBlockEncoder() {
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

            // decode and test the buffer, testing edgecases
            {
                MemoryStream rs = new MemoryStream(databuffer);
                SegmentBlockBasicDecoder dec = new SegmentBlockBasicDecoder(rs);
                for (int i = testvalues.Length -1; i >= 0; i--) {
                    RecordKey tkey = new RecordKey().appendParsedKey(testvalues[i]); 
                    RecordUpdate tupdate = RecordUpdate.WithPayload("data: " + testvalues[i]);

                    KeyValuePair<RecordKey,RecordUpdate> row = 
                        dec.FindNext(tkey);
                    Assert.AreEqual(tkey, row.Key,  "record keys should match");
                    Assert.AreEqual(tupdate, row.Value, "record values should match:");
                }

                // test for "next" after end of buffer, and we should get an exception
                // test for "next" at beginning and we should get first
                // test some random values in the middle
            }

            
        }

       

        [Test]
        public void T02_BasicBlock_IScannable() {
            // exercize the iscannable interface

            Assert.Fail("not implemented");
        }


    }
}
