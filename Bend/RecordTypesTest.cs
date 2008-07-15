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
    public class A01_RecordTypesTest
    {
        [Test]
        public void T00_RecordKeyEquality() {
            RecordKey key1 = new RecordKey().appendParsedKey("test/1");
            RecordKey key2 = new RecordKey().appendParsedKey("test/1");

            
            Assert.AreEqual(key1, key2, "object equal");
            Assert.AreEqual(true, key1.Equals(key2), "key1.Equals(key2)");

            Assert.AreEqual(key1.GetHashCode(), key2.GetHashCode(), 
                "equals keys need equal HashCodes");

            RecordKey key3 = new RecordKey().appendParsedKey("");
            RecordKey key4 = new RecordKey().appendKeyPart(new byte[0]);
            Assert.AreEqual(true, key3.Equals(key4), "null keys should match");


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
        public void T02_RecordSort() {
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
        public void T03_RecordDataAssembly() {
            RecordData data = new RecordData(RecordDataState.NOT_PROVIDED, new RecordKey());
            Assert.AreEqual("", data.ToString());
            RecordUpdateResult result;


            result = data.applyUpdate(RecordUpdate.NoUpdate());
            Assert.AreEqual("", data.ToString());
            Assert.AreEqual(result, RecordUpdateResult.SUCCESS);
            Assert.AreEqual(RecordDataState.NOT_PROVIDED, data.State);

            result = data.applyUpdate(RecordUpdate.WithPayload("1"));
            Assert.AreEqual("1", data.ToString());
            Assert.AreEqual(result, RecordUpdateResult.FINAL);
            Assert.AreEqual(RecordDataState.FULL, data.State);

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
            Assert.AreEqual("", data.ToString());

            RecordUpdateResult result = data.applyUpdate(RecordUpdate.DeletionTombstone());
            Assert.AreEqual("", data.ToString()); // empty.. (TODO: consider making tombstones ToString to '-|')
            Assert.AreEqual(RecordUpdateResult.FINAL,result);
            Assert.AreEqual(RecordDataState.DELETED, data.State);

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
            Assert.AreEqual(RecordUpdateResult.FINAL, data.applyUpdate(RecordUpdate.WithPayload("1"))); 
            Assert.AreEqual("", data.ToString()); // still empty...
            Assert.AreEqual(RecordDataState.DELETED, data.State);

        }

        [Test]
        public void T07_RecordKeyParsedEndInDelimiter() {
            // ending a parsed key with the delimiter should be an error, it's just too 
            // easily a source of bugs..

            bool err = false;
            try {
                RecordKey key = new RecordKey().appendParsedKey("TEST/1" + new String(RecordKey.DELIMITER,1));
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

    }


    [TestFixture]
    public class ZZ_Todo_RecordTypesTest
    {

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
            string DELIM = new String(RecordKey.DELIMITER, 1);


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