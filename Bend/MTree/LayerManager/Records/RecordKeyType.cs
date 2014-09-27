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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using System.Diagnostics;

namespace Bend
{

    abstract public class RecordKeyType : IComparable<RecordKeyType> {
        public enum RecordKeySubtype {
            LONG = 1,
            STRING = 2,
            RECORD_KEY = 10,
            RAW_BYTES = 11,
            TS_ATTRIBUTE = 40
            // only a byte!! 
        }
        public RecordKeySubtype subtype;
       
        // (1) first, compare the target type in the type registry
        // (2) then, defer to the subtype to do comparisons if they are the same
        public int CompareTo(RecordKeyType target) {
            int subtyperesult = this.subtype.CompareTo(target.subtype);

            if (subtyperesult == 0) {
                // double check that they are the same classtype
                if (this.GetType() != target.GetType()) {
                    throw new Exception("RecordKey subtypes match but classes don't");
                }
                return this.CompareToPeer(target);
            } else {                
                return subtyperesult;
            }
        }
        public override bool Equals(object target) {
            if (target.GetType() != this.GetType()) {
                return false;
            } else {
                return (this.CompareTo((RecordKeyType)target) == 0);
            }
        }

        public override int GetHashCode() {
            throw new Exception(String.Format("RecordKeyType subtype({0}) must implement GetHashCode", this.GetType()));
        }

        public abstract int CompareToPeer(RecordKeyType peer_target);  
        internal abstract void encodeSubtypeTo(BinaryWriter w);        

        public static RecordKeyType decodeFrom(BinaryReader r) {
            byte subtype_byte = r.ReadByte();
            RecordKeySubtype subtype_enum = (RecordKeySubtype)subtype_byte;
            switch (subtype_enum) {
                case RecordKeySubtype.STRING:
                    return RecordKeyType_String.decodeSubtypeFrom(r);                    
                case RecordKeySubtype.LONG:
                    return RecordKeyType_Long.decodeSubtypeFrom(r);                    
                case RecordKeySubtype.RECORD_KEY:
                    return RecordKeyType_RecordKey.decodeSubtypeFrom(r);                   
                case RecordKeySubtype.RAW_BYTES:
                    return RecordKeyType_RawBytes.decodeSubtypeFrom(r);                    
                case RecordKeySubtype.TS_ATTRIBUTE:
                    return RecordKeyType_AttributeTimestamp.decodeSubtypeFrom(r);                   
            }
            throw new Exception("RecordKeyType.decodeFrom: no decoder for subtype: " + subtype_enum.ToString());
        }

        public void encodeTo(BinaryWriter w) {
            if ((int)this.subtype > 0xFF) { throw new Exception("RecordKeyTYpe.encodeTo(): subtype out of range"); }
            w.Write((byte)this.subtype); 
            this.encodeSubtypeTo(w);
        }

        
        
    }



    public class RecordKeyType_String : RecordKeyType {
        internal string value;
        
        // TODO support other encodings and collations

        public RecordKeyType_String(string value) {
            this.subtype = RecordKeySubtype.STRING;
            this.value = value;
        }

        public override int CompareToPeer(RecordKeyType peer_target) {
            RecordKeyType_String converted_peer_target = (RecordKeyType_String)peer_target;
            
            return this.value.CompareTo(converted_peer_target.value);

            // switch to forcing ORDINAL comparisons so we don't end up with
            // sort orders changing based on locale.... eventually we will
            // record the locale sort order of this keytype. 
            
            // return String.Compare(this.value, converted_peer_target.value,
            //     System.Threading.Thread.CurrentThread.CurrentCulture,
            //     System.Globalization.CompareOptions.Ordinal);
            
        }

        public static RecordKeyType decodeSubtypeFrom(BinaryReader r) {
            int str_len = r.ReadInt32();
            byte[] str_bytes = r.ReadBytes(str_len);    
                        
            String keystring = System.Text.Encoding.UTF8.GetString(str_bytes);
            return new RecordKeyType_String(keystring);
        }
        // TODO: how to we guarantee this encoding/decoding is symmetric?
        internal override void encodeSubtypeTo(BinaryWriter w) {            
            byte[] str_bytes = System.Text.Encoding.UTF8.GetBytes(this.value);            

            w.Write( (int) str_bytes.Length);
            w.Write(str_bytes);            
        }
        public string GetString() {
            return value;
        }
        public override int GetHashCode() {
            return value.GetHashCode();
        }
        public override string ToString() {
            return String.Format("\"{0}\"", this.value);
        }
    }

    public class RecordKeyType_Long : RecordKeyType {
        internal long value;
        
        public RecordKeyType_Long(long value) {
            this.subtype = RecordKeySubtype.LONG;
            this.value = value;
        }
        public override int CompareToPeer(RecordKeyType peer_target) {
            RecordKeyType_Long converted_peer_target = (RecordKeyType_Long)peer_target;
            return this.value.CompareTo(converted_peer_target.value);
        }
        internal override void  encodeSubtypeTo(BinaryWriter w) {
            w.Write( (long) value);
        }
        public static RecordKeyType decodeSubtypeFrom(BinaryReader r) {
            return new RecordKeyType_Long(r.ReadInt64());
        }
        public long GetLong() {
            return value;
        }
        public override int GetHashCode() {
            return value.GetHashCode();
        }
        public override string ToString() {
            return String.Format("{0}L", this.value);
        }
    }

    public class RecordKeyType_AttributeTimestamp : RecordKeyType {
        internal long value;

        public RecordKeyType_AttributeTimestamp(long value) {
            this.subtype = RecordKeySubtype.TS_ATTRIBUTE;
            this.value = value;
        }
        public override int CompareToPeer(RecordKeyType peer_target) {
            RecordKeyType_AttributeTimestamp converted_peer_target = (RecordKeyType_AttributeTimestamp)peer_target;
            return this.value.CompareTo(converted_peer_target.value);
        }
        internal override void encodeSubtypeTo(BinaryWriter w) {
            w.Write((long)value);
        }
        public static RecordKeyType decodeSubtypeFrom(BinaryReader r) {
            return new RecordKeyType_AttributeTimestamp(r.ReadInt64());
        }
        public long GetLong() {
            return value;
        }
        public override int GetHashCode() {
            return value.GetHashCode();
        }
        public override string ToString() {
            return String.Format("TS:0x{0:X}", this.value);
        }
    }
    

    public class RecordKeyType_RecordKey : RecordKeyType {
        internal RecordKey value;
        public RecordKeyType_RecordKey(RecordKey value) {
            subtype = RecordKeySubtype.RECORD_KEY;
            this.value = value;
        }
        public override int CompareToPeer(RecordKeyType peer_target) {
            RecordKeyType_RecordKey converted_peer_target = (RecordKeyType_RecordKey)peer_target;
            return this.value.CompareTo(converted_peer_target.value);
        }
        internal override void encodeSubtypeTo(BinaryWriter w) {
            byte[] encoded_bytes = value.encode();
            w.Write((int)encoded_bytes.Length);
            w.Write(encoded_bytes);
        }
        public static RecordKeyType decodeSubtypeFrom(BinaryReader r) {
            int num_bytes = r.ReadInt32();
            byte[] encoded_bytes = r.ReadBytes(num_bytes);
            RecordKey rk = new RecordKey(encoded_bytes);
            return new RecordKeyType_RecordKey(rk);
        }
        public RecordKey GetRecordKey() {
            return value;
        }
        public override int GetHashCode() {
            return value.GetHashCode();
        }
        public override string ToString() {
            return String.Format("({0})", this.value);
        }
    }


    public class RecordKeyType_RawBytes : RecordKeyType {
        internal byte[] value;
        public RecordKeyType_RawBytes(byte[] value) {
            subtype = RecordKeySubtype.RAW_BYTES;
            this.value = value;
        }
        public override int CompareToPeer(RecordKeyType peer_target) {
            RecordKeyType_RawBytes conv_target = (RecordKeyType_RawBytes)peer_target;

            int compare_result = 0;
            int pos = 0;
            while (compare_result == 0) {
                if ((pos == this.value.Length) && (pos == conv_target.value.Length)) {
                    // equal length and equal
                    return 0;
                }
                if (pos == conv_target.value.Length) {
                    // equal and conv_target shorter, we're greater
                    return 1;
                }
                if (pos == this.value.Length) {
                    // equal and we are shorter, we're less
                    return -1;
                }
                compare_result = this.value[pos].CompareTo(conv_target.value[pos]);
                pos++;
            }
            return compare_result;            
        }
        internal override void encodeSubtypeTo(BinaryWriter w) {
            
            w.Write((int)value.Length);
            w.Write(value);
        }
        public static RecordKeyType decodeSubtypeFrom(BinaryReader r) {
            int num_bytes = r.ReadInt32();
            byte[] raw_bytes = r.ReadBytes(num_bytes);           
            return new RecordKeyType_RawBytes(raw_bytes);
        }
        public byte[] GetBytes() {
            return value;
        }
        public override int GetHashCode() {
            return value.GetHashCode();
        }
        public override string ToString() {
            return String.Format("RawBytes:", Lsd.ToHexString(this.value.Take(10).ToArray()));
        }
    }
}


namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public class A01_RecordKeyType {
        [Test]
        public void T00_RecordKeyType_Sort() {
            var a_value = new RecordKeyType_String("A_test");
            var c_value = new RecordKeyType_String("C_test");

            Assert.True(a_value.CompareTo(c_value) < 0, "string comparison");

            RecordKeyType b_value = new RecordKeyType_String("B_test");
            Assert.True(a_value.CompareTo(b_value) < 0, "string to unknown type comparison");
            Assert.True(b_value.CompareTo(a_value) > 0, "unknown type to string");
            Assert.True(b_value.CompareTo(c_value) < 0, "unknown type to string");


            var num_value = new RecordKeyType_Long(12131);
            var str_value = new RecordKeyType_String("12131");

            Assert.True(num_value.CompareTo(str_value) < 0, "long value should always be less");



            var rk_1 = new RecordKeyType_RecordKey(new RecordKey().appendParsedKey("1/2/3"));
            var rk_2 = new RecordKeyType_RecordKey(new RecordKey().appendParsedKey("1/2/3"));

            Assert.True(rk_1.Equals(rk_2), "record key equals");

            
        }

        [Test]
        public void T01_RecordKeyType_EncodeDecode() {
            var val_str = new RecordKeyType_String("A_test");
            var val_long = new RecordKeyType_Long(123);

            List<RecordKeyType> fields = new List<RecordKeyType>();

            byte[] encoded_bytes;
            {
                var ms = new MemoryStream();
                var w = new BinaryWriter(ms);
                val_str.encodeTo(w);
                val_long.encodeTo(w);
                w.Flush();
                encoded_bytes = ms.ToArray();
            }

            {
                var ms = new MemoryStream(encoded_bytes);
                var r = new BinaryReader(ms);
                var decoded_val_str = RecordKeyType.decodeFrom(r);
                var decoded_val_long = RecordKeyType.decodeFrom(r);                

                Assert.True(decoded_val_str.CompareTo(val_str) == 0, "string encode decode failed");
                Assert.True(decoded_val_long.CompareTo(val_long) == 0, "number encode decode failed");
            }


        }

        [Test]
        public void T02_RecordKeyType_GetHashCode() {
            Assert.AreEqual(new RecordKeyType_String("Foo").GetHashCode(), new RecordKeyType_String("Foo").GetHashCode(), "string equal hash codes");
            Assert.AreEqual(new RecordKeyType_Long(1).GetHashCode(), new RecordKeyType_Long(1).GetHashCode(), "long equal hash codes");
        }
    }
}


