// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NUnit.Framework;

using Bend;

namespace BendTests
{

    [TestFixture]
    public class A01_RecordTests
    {
        [Test]
        public void T00_RecordKeyEquality() {
            RecordKey key1 = new RecordKey().appendParsedKey("test/1");
            RecordKey key2 = new RecordKey().appendParsedKey("test/1");

            
            Assert.AreEqual(key1, key2, "object equal");
            Assert.AreEqual(true, key1.Equals(key2), "key1.Equals(key2)");

            Assert.AreEqual(key1.GetHashCode(), key2.GetHashCode(), 
                "equals keys need equal HashCodes");

            RecordKey key3 = new RecordKey().appendKeyPart(new RecordKeyType_RawBytes(new byte[0]));
            RecordKey key4 = new RecordKey().appendKeyPart(new byte[0]);
            Assert.AreEqual(true, key3.Equals(key4), "null keys should match");
        }

        [Test]
        public void T12_RecordKeyStringCompBugTest() {
            var key5 = new RecordKey().appendParsedKey(@".zdata/index/>you/c:\EmailTest\Data\saved_mail_2002:1407/182");
            var key6 = new RecordKey().appendParsedKey(@".zdata/index/?/c:\EmailTest\Data\saved_mail_2002:908/184");

            Assert.True('>'.CompareTo('?') < 0, "check char comparison");
            Assert.True(String.Compare(">", "?", System.Threading.Thread.CurrentThread.CurrentCulture,
                System.Globalization.CompareOptions.Ordinal) < 0, "ordinal string comparison");
            // Assert.True(">".CompareTo("?") < 0, "check string comparison");
            Assert.True(key5.CompareTo(key6) < 0, " '>' should be < '?' ");

        }

        [Test]
        public void T00_RecordUpdateEquality() {
            RecordUpdate upd1 = RecordUpdate.WithPayload("test/1");
            RecordUpdate upd2 = RecordUpdate.WithPayload("test/1");
            Assert.AreEqual(upd1, upd2, "object.Equal(object)");
            Assert.AreEqual(true, upd1.Equals(upd2), "upd1.Equals(key2)");
        }

        [Test]
        public void T01_RecordKey() {
            String[] parts1 = { "test", "test2", "blah" };

            {
                RecordKey key = new RecordKey();
                key.appendKeyParts(parts1);
                byte[] data = key.encode();

                // decode
                RecordKey key2 = new RecordKey(data);

                // verify tostring matches
                Assert.AreEqual(key.ToString(), key2.ToString());

                // verify comparison
                Assert.AreEqual(0, key.CompareTo(key2));

                // verify individual parts

            }

            // verify hierarchial sorting
            {
                RecordKey key1 = new RecordKey().appendParsedKey(".ROOT/GEN/000");
                RecordKey key2 = new RecordKey().appendParsedKey(".ROOT/GEN/000/</aaaa");   // ord('<') -> 60
                RecordKey key3 = new RecordKey().appendParsedKey(".ROOT/GEN/000/>");     // ord('>') -> 62
                RecordKey key4 = new RecordKey().appendParsedKey(".ROOT/GEN");

                Assert.AreEqual(true, ">".CompareTo("<") > 0, "expect '>' > '<'");

                Assert.AreEqual(true, key1.CompareTo(key2) < 0, "prefix key should be considered earlier");
                Assert.AreEqual(true, key3.CompareTo(key2) > 0, "but parts are considered individually before using length ord('_') > ord('<')");

                IComparable<RecordKey> prefixend = RecordKey.AfterPrefix(key1);
                Assert.AreEqual(true, prefixend.CompareTo(key1) > 0, "prefix1");
                Assert.AreEqual(true, prefixend.CompareTo(key2) > 0, "prefix2");
                Assert.AreEqual(true, prefixend.CompareTo(key3) > 0, "prefix3");
                Assert.AreEqual(true, prefixend.CompareTo(key4) > 0, "prefix4");

            }


        }
        [Test]
        public void T02a_RecordKeySort() {
            String[] parts1 = { "test", "test2", "blah" };
            String[] parts2 = { "test", "test3", "blah" }; // > parts 1
            String[] parts3 = { "test", "test2a", "blah" }; // > parts 1 (testing per-segment sorting order!)

            RecordKey key1 = new RecordKey();
            key1.appendKeyParts(parts1);

            RecordKey key2 = new RecordKey();
            key2.appendKeyParts(parts2);

            RecordKey key3 = new RecordKey();
            key3.appendKeyParts(parts3);

            // key2 > key1
            Assert.AreEqual(1, key2.CompareTo(key1));
            Assert.AreEqual(-1, key1.CompareTo(key2));

            // key3 > key1
            Assert.AreEqual(1, key3.CompareTo(key1));
            Assert.AreEqual(-1, key1.CompareTo(key3));

        }

        
        [Test]
        public void T02b_RecordKeyNesting() {
            String[] parts1 = { "aaaa", "bbbb" };
            String[] parts2 = { "xxxx", "yyyy", "zzzz" };

            RecordKey nestedkey = new RecordKey();
            nestedkey.appendKeyParts(parts2);

            RecordKey parentkey = new RecordKey();
            parentkey.appendKeyPart(parts1[0]); // "aaaa"
            parentkey.appendKeyPart(nestedkey.encode());  // (xxxx,yyyy,zzzz)
            parentkey.appendKeyPart(parts1[1]); // "bbbb"

            RecordKey decodedkey = new RecordKey(parentkey.encode());

            Assert.AreEqual(decodedkey.ToString(), parentkey.ToString(), "tostring versions of keys don't match");
            Assert.AreEqual(decodedkey.numParts(), parentkey.numParts(), "nested delimiters are changing number of keyparts");
            
            Assert.AreEqual(decodedkey, parentkey, "nested key encode/decode error");                       

        }

        [Test]
        public void T03_RecordDataAssembly() {
            RecordData data = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());
            Assert.AreEqual("[NOT_PROVIDED] ", data.ToString(), "empty rec 1");
            RecordDataUpdateResult result;


            result = data.applyUpdate(RecordUpdate.NoUpdate());
            Assert.AreEqual("[NOT_PROVIDED] ", data.ToString(), "empty rec 2");
            Assert.AreEqual(result, RecordDataUpdateResult.SUCCESS, "apply result 2");
            Assert.AreEqual(RecordDataState.NOT_PROVIDED, data.State, "apply state 2");

            result = data.applyUpdate(RecordUpdate.WithPayload("1"));
            Assert.AreEqual("1", data.ToString(), "apply 3");
            Assert.AreEqual(result, RecordDataUpdateResult.FINAL, "apply result 3");
            Assert.AreEqual(RecordDataState.FULL, data.State, "apply state 3");

            // if we already have a full update, it should be an error
            /* NOT ANYMORE
            {
                bool err = false;
                try { data.applyUpdate(RecordUpdate.WithPayload("2")); } catch { err = true; }
                Assert.AreEqual(true, err);
            }
            {
                bool err = false;
                try { data.applyUpdate(RecordUpdate.DeletionTombstone()); } catch { err = true; }
                Assert.AreEqual(true, err);
            }
            Assert.AreEqual("1", data.ToString());
            */
            // if we already have a full update, our update should not change it
            {
                data.applyUpdate(RecordUpdate.WithPayload("2"));
                Assert.AreEqual("1", data.ToString());

                data.applyUpdate(RecordUpdate.DeletionTombstone());
                Assert.AreEqual("1", data.ToString());
            }
            

        }


        [Test]
        public void T04_RecordTombstones() {
            RecordData data = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());
            Assert.AreEqual("[NOT_PROVIDED] ", data.ToString(), "empty rec 1");

            RecordDataUpdateResult result = data.applyUpdate(RecordUpdate.DeletionTombstone());
            Assert.AreEqual("[DELETED] ", data.ToString(), "tomb update 1"); 
            Assert.AreEqual(RecordDataUpdateResult.FINAL,result, "tomb result 1");
            Assert.AreEqual(RecordDataState.DELETED, data.State, "tomb state 1");

            /* NOT ANYMORE
            bool err = false;
            try {
                data.applyUpdate(RecordUpdate.WithPayload("2"));
                data.applyUpdate(RecordUpdate.DeletionTombstone());
            } catch {
                err = true;
            }
             * Assert.AreEqual(err, true);
             */
            // data after a tombstone should be ignored
            Assert.AreEqual(RecordDataUpdateResult.FINAL, data.applyUpdate(RecordUpdate.WithPayload("1"))); 
            Assert.AreEqual("[DELETED] ", data.ToString(), "tomb after update 2"); // still empty...
            Assert.AreEqual(RecordDataState.DELETED, data.State);

        }

        [Test]
        public void T07_RecordKeyParsedEndInDelimiter() {
            // ending a parsed key with the delimiter should be an error, it's just too 
            // easily a source of bugs..

            bool err = false;
            try {
                RecordKey key = new RecordKey().appendParsedKey("TEST/1" + new String(RecordKey.PRINTED_DELIMITER,1));
            } catch {
                err = true;
            }
            Assert.AreEqual(true, err, "ending a parsed key with the delimiter should throw an error");
        }

        [Test]
        public void T08_TombstoneEncodeDecode() {
            RecordUpdate update = RecordUpdate.DeletionTombstone();
            RecordUpdate decoded_update = RecordUpdate.FromEncodedData(update.encode());

            Assert.AreEqual(RecordUpdateTypes.DELETION_TOMBSTONE,decoded_update.type);
        }

        [Test]
        public void T09_RecordKeyIsSubkeyOf() {
            RecordKey a = new RecordKey().appendParsedKey(".zdata/index/you");
            RecordKey b = new RecordKey().appendParsedKey(".zdata/index/you").appendKeyPart(new byte[0]);

            RecordKey g = new RecordKey().appendParsedKey(".zdata/index/you/doc1/word1");
            RecordKey f = new RecordKey().appendParsedKey(".zdata/index/your/doc1/word1");

            Assert.AreEqual(true, g.isSubkeyOf(a));
            // Assert.AreEqual(true, g.isSubkeyOf(b));

            Assert.AreEqual(false, f.isSubkeyOf(a));
            // Assert.AreEqual(false, f.isSubkeyOf(b));
            
        }

        [Test]
        public void T10_RecordKeyEncodeDecodeBugTest() {

            // test encode/decode with byte[] parts  

            // 92 43 0
            byte[] chars = { 92, 43, 0 };
            {

                System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
                String keystring = enc.GetString(chars);
                enc.GetBytes(keystring);

                Assert.AreEqual(chars, enc.GetBytes(keystring), "string encoding not reversible");

                RecordKey key = new RecordKey();
                key.appendKeyPart(chars);
                byte[] data = key.encode();
                Assert.AreEqual(key, new RecordKey(data), "check encode/decode with binary data");

                // check nested key

                var wrap_key = new RecordKey().appendKeyPart(key.encode());
                byte[] wrap_encoded = wrap_key.encode();

                RecordKey wrap_decoded = new RecordKey(wrap_encoded);
                
            }
        }

        [Test]
        public void T11_RecordKey_ComposableIComparableTest() {

            IComparable<RecordKey> start_keytest = new RecordKeyComparator()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(new RecordKeyType_Long(1))
                .appendKeyPart(RecordKey.AfterPrefix(new RecordKey().appendParsedKey("B")));

            var range_a = new RecordKey()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(new RecordKeyType_Long(1))
                .appendKeyPart(new RecordKey().appendKeyPart("A"));

            var range_c = new RecordKey()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(new RecordKeyType_Long(1))
                .appendKeyPart(new RecordKey().appendKeyPart("C"));

            Assert.True(start_keytest.CompareTo(range_a) > 0, "after range a");
            Assert.True(start_keytest.CompareTo(range_c) < 0, "before range c");

            var random_key = new RecordKey()
                .appendParsedKey(".ROOT/GEN")
                .appendKeyPart(new RecordKeyType_Long(1))
                .appendParsedKey("a");                            

            Assert.True(start_keytest.CompareTo(random_key) > 0, "should be after this random_key");
        }

        [Test]
        public void T12_RecordKey_Bug() {
            var testkey = new RecordKey().appendParsedKey(".zdata/index/fax");
            var jeskekey = new RecordKey().appendParsedKey(".zdata/index/jeske");
            var jeske2key = new RecordKey().appendParsedKey(".zdata/index/jeske").appendKeyPart("");
            var jeske2keyAP = RecordKey.AfterPrefix(jeske2key);

            Assert.True(jeskekey.CompareTo(testkey) > 0, "1");
            Assert.True(jeske2key.CompareTo(testkey) > 0, "2");
            Assert.True(jeske2keyAP.CompareTo(testkey) > 0, "3");
            
            RecordKeyComparator startrk = new RecordKeyComparator()
                            .appendKeyPart(jeske2key);


            Assert.True(startrk.CompareTo(testkey) > 0, "4");
        }
    }



    [TestFixture]
    public class ZZ_Todo_RecordTypesTest {

        [Test]
        public void T05_RecordPartialUpdate() {
            RecordData data = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());
            Assert.AreEqual("", data.ToString());

            // TODO: what the heck is a partial update?!?!?
            // result = data.applyUpdate(new RecordUpdate(RecordUpdateTypes.PARTIAL, "1"));
            Assert.Fail("partial update not implemented yet");
        }

        [Test]
        public void T06_RecordKeyDelimiterEscape() {
            string DELIM = new String(RecordKey.PRINTED_DELIMITER, 1);


            RecordKey key1 = new RecordKey();
            key1.appendKeyParts("1", "2", "3");
            Assert.AreEqual(3, key1.numParts());
            RecordKey dkey1 = new RecordKey(key1.encode());
            Assert.AreEqual(3, dkey1.numParts(), "dkey1 delimiter decode");

            RecordKey key2 = new RecordKey();
            key2.appendKeyPart("1" + DELIM + "2" + DELIM + "3");
            Assert.AreEqual(1, key2.numParts());
            RecordKey dkey2 = new RecordKey(key2.encode());
            Assert.AreEqual(1, dkey2.numParts(), "dkey2 delimiter decode");

            // key2 > key1
            Assert.AreEqual(1, key2.CompareTo(key1));
            Assert.AreEqual(-1, key1.CompareTo(key2));

        }

        [Test]
        public void T09_ImplementTombstonesWithAttributes() {
            // TODO: we really need to move tombstones into the keyspace and implement them as
            //       key-transaction-attributes, so they properly participate in
            //       transaction-attribute merge.
            Assert.Fail("implement tombstones with attributes");
        }
    }

}


namespace BendPerfTest {

    [TestFixture]
    public class A01_RecordTypesTest {
        [Test]
        public void T12_RecordKeyDecodePerfTest() {
            RecordKey rk = new RecordKey().appendParsedKey(".data/test/unpack/with/lots/of/parts");
            int NUM_ITERATIONS = 100000;

            // encode test
            {
                GC.Collect();
                DateTime start = DateTime.Now;
                for (int x = 0; x < NUM_ITERATIONS; x++) {
                    byte[] data = rk.encode();
                }
                DateTime end = DateTime.Now;
                double elapsed_s = (end - start).TotalMilliseconds / 1000.0;
                double rec_per_s = (double)NUM_ITERATIONS / elapsed_s;

                Console.WriteLine("packed {0} record keys in {1} seconds, {2} keys/second",
                    NUM_ITERATIONS, elapsed_s, rec_per_s);

                Assert.Less(500000, rec_per_s, "minimum records per second of pack");

            }


            // decode test
            {
                byte[] data = rk.encode();
                GC.Collect();
                DateTime start = DateTime.Now;
                for (int x = 0; x < NUM_ITERATIONS; x++) {
                    RecordKey unpacked = new RecordKey(data);
                }
                DateTime end = DateTime.Now;
                double elapsed_s = (end - start).TotalMilliseconds / 1000.0;
                double rec_per_s = (double)NUM_ITERATIONS / elapsed_s;

                Console.WriteLine("unpacked {0} record keys in {1} seconds, {2} keys/second",
                    NUM_ITERATIONS, elapsed_s, rec_per_s);

                Assert.Less(500000, rec_per_s, "minimum records per second of unpack");

            }



        }

    }
}