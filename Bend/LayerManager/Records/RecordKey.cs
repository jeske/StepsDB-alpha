// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using System.Diagnostics;

namespace Bend {
    //------------------------------------------------------------------------------------

    //-----------------------------------[ RecordKey ]------------------------------------

    //------------------------------------------------------------------------------------


    public class RecordKey : IComparable<RecordKey> {
        // TODO: key_parts really shouldn't be public, people should be using pipes to read keys
        // TODO: maybe key parts shouldn't be limited to strings
        public List<RecordKeyType> key_parts;
        public static char PRINTED_DELIMITER = '/';
        public static byte DELIMITER_B = 47;


        public RecordKey() {
            key_parts = new List<RecordKeyType>();
        }
        public RecordKey(byte[] data)
            : this() {
            RecordKey.encoder.decode(data, this);
        }

        public RecordKey appendParsedKey(String keyToParse) {
            char[] delimiters = { PRINTED_DELIMITER };

            if (keyToParse == null) {
                throw new Exception("appendParsedKey() handed a null pointer");
            }

            if (keyToParse.Length > 0) {
                if (keyToParse[keyToParse.Length - 1] == PRINTED_DELIMITER) {
                    throw new Exception(
                        String.Format("appendParsedKey({0}) may not end in DELIMTER({1})",
                        keyToParse, PRINTED_DELIMITER));
                }
            }

            String[] keystring_parts = keyToParse.Split(delimiters);
            foreach (String keypart in keystring_parts) {
                this.appendKeyPart(new RecordKeyType_String(keypart));
            }
            return this;
        }

        public RecordKey appendKeyParts(params object[] args) {
            foreach (object arg in args) {
                if (arg == null) {
                    throw new Exception("appendKeyParts(params object[] args) handed null pointer");
                }
                if (arg.GetType() == typeof(String)) {
                    this.appendKeyPart(new RecordKeyType_String((string)arg));
                } else if (arg.GetType() == typeof(byte[])) {
                    this.appendKeyPart(new RecordKeyType_RawBytes((byte[])arg));
                } else {
                    throw new Exception("unknown argument type");
                }
            }
            return this;
        }


        public RecordKey appendKeyPart(String part) {
            if (part == null) {
                throw new Exception("appendKeyPart(string part) handed null pointer");
            }
            key_parts.Add(new RecordKeyType_String(part));
            return this;
        }

        public RecordKey appendKeyPart(byte[] data) {
            if (data == null) {
                throw new Exception("appendKeyPart(byte[] data) handed null pointer");
            }            
            key_parts.Add(new RecordKeyType_RawBytes(data));
            return this;
        }

        public RecordKey appendKeyPart(RecordKey keydata) {            
            key_parts.Add(new RecordKeyType_RecordKey(keydata));
            return this;
        }
        public RecordKey appendKeyPart(RecordKeyType keypart) {
            key_parts.Add(keypart);
            return this;
        }

        public int numParts() {
            return key_parts.Count;
        }

        // ---------------------------------------------
        // RecordKey.CompareTo()
        //
        // It is important that this does HIERARCHIAL sorting, not typical byte[] sorting.
        // The delimiter is currently "/", but it plays NO part in the sorting calculation. 
        // For example, (/) == ascii(47), wheras (!) == ascii(33), but because the
        // delimiters are not involved in the calculation. For example:
        //
        //           .ROOT/GEN/1/a    is less than
        //           .ROOT/GEN/1!/a         
        // 
        // Which is evaluated as:
        //   .ROOT == .ROOT
        //     GEN == GEN
        //       1  < 1!
        // 
        // This is different than a bytewise comparison of the encoded form:
        //
        //  .ROOT/GEN/1/
        //  ===========>     <- because '/' > '!'
        //  .ROOT/GEN/1!/
        //        
        // As a result, our entire database is dependent on the packing and unpacking 
        // into RecordKeys in order for the data to be properly sorted and re-sorted!
        // This is worth it, because hierarchial sorting allows us to do much more
        // predictable things for the higher layers.
        //
        // TODO: deal with field types within a part (i.e. numbers, dates, nested keys)

        public int CompareTo(RecordKey target) {
            int pos = 0;
            int cur_result = 0;

            int thislen = this.key_parts.Count;
            int objlen = target.key_parts.Count;

            while (cur_result == 0)  // while equal
            {

                // if the objects were equal, and have no more parts
                if ((pos == thislen) &&
                     (pos == objlen)) {
                    // equal and at the end
                    return 0; // equal
                }
                    // if the objects are equal so far, but one is done
                else {
                    if ((pos == thislen) &&
                        (pos < objlen)) {
                        // equal and 'target' longer
                        return -1; // target longer, so we're less than target
                    }
                    if ((pos == objlen) &&
                        (pos < thislen)) {
                        // equal and 'this' longer
                        return 1; // this longer, we're greater than target
                    }
                }

                // CompareTo()  this keypart with target keypart
                // TODO: consider making singleton keypart objects so we can do
                //    MUCH faster equality testing for each part (a common case)
                cur_result = this.key_parts[pos].CompareTo(target.key_parts[pos]);

                pos++; // move pointer to next
            }


            return cur_result;
        }

        private class RecordKeyAfterPrefixComparable : IComparable<RecordKey> {
            RecordKey prefixkey;
            internal RecordKeyAfterPrefixComparable(RecordKey prefixkey) {
                this.prefixkey = prefixkey;
            }

            public override String ToString() {
                return "AfterPrefix(" + prefixkey.ToString() + ")";
            }
            // < 0 this instance is less than target
            // > 0 this instance is greater than target
            // 0   this instance equals target
            public int CompareTo(RecordKey target) {
                int pos = 0;
                int cur_result = 0;

                int thislen = prefixkey.key_parts.Count;
                int objlen = target.key_parts.Count;

                while (cur_result == 0)  // while equal
                {

                    // if the objects were equal, and have no more parts
                    if ((pos == thislen) &&
                         (pos == objlen)) {
                        // equal and at the end, WE ARE GREATER
                        return 1; // equal
                    }
                        // if the objects are equal so far, but one is done
                    else {
                        if ((pos == thislen) &&
                            (pos < objlen)) {
                            // equal and 'target' longer
                            return 1; // target longer, WE ARE GREATER
                        }
                        if ((pos == objlen) &&
                            (pos < thislen)) {
                            // equal and 'this' longer
                            return 1; // this longer, WE ARE LESS
                        }
                    }

                    // consider this keypart
                    cur_result = prefixkey.key_parts[pos].CompareTo(target.key_parts[pos]);
                    pos++; // move pointer to next
                }


                return cur_result;
            }
        }

        // AfterPrefix() -> make a comparable that appears after all keys with
        //  matching prefixkey in the sort order
        public static IComparable<RecordKey> AfterPrefix(RecordKey prefixkey) {
            return new RecordKeyAfterPrefixComparable(prefixkey);
        }

        /// <summary>
        ///  determine if we are a subkey of the supplied potential prefix key.
        /// </summary>
        /// <param name="potential_parent_key"></param>
        /// <returns>true if the supplied potential_parent_key is shorter and matches our prefix, false otherwise</returns>
        public bool isSubkeyOf(RecordKey potential_parent_key) {
            // we are a subkey of the other key we are longer AND 
            // and all their parts match our parts

            if (potential_parent_key.key_parts.Count > this.key_parts.Count) {
                return false; // they are longer, so we can't be a subkey
            }

            for (int pos = 0; pos < potential_parent_key.key_parts.Count; pos++) {
                if (this.key_parts[pos].CompareTo(potential_parent_key.key_parts[pos]) != 0) {
                    return false; // one of their parts didn't match
                }
            }
            return true; // all their parts matched, and they were shorter or equal, so we are a subkey
        }

        public override bool Equals(object obj) {
            try {
                return this.CompareTo((RecordKey)obj) == 0;
            } catch (InvalidCastException) {
                return false;
            }
        }
        public override int GetHashCode() {
            int hash_code = 0;
            foreach (RecordKeyType part in key_parts) {
                hash_code += part.GetHashCode();
            }
            return hash_code;
        }

        public string DebugToString() {
            return "K(" + this.ToString() + ")";
        }

        public override string ToString() {
            // return String.Join(new String(DELIMITER, 1), key_parts.ToArray());
            return String.Format("RecordKey({0})", String.Join<RecordKeyType>(".", key_parts.ToArray()));
        }

        // TODO: this is hacky, we should find a better abstraction to let people see our keyparts
        public IEnumerator<RecordKeyType> GetEnumeratorForKeyparts() {
            return this.key_parts.GetEnumerator();
        }

        // -----------------------------------------------------------
        // encoding/decoding of keyparts
        //
        // switching between these is BINARY INCOMPATIBLE in the datafiles, so don't be doing it
        // unless you know what you are doing!!! - jeske

        public byte[] encode() {
            return RecordKey.encoder.encode(this);
        }
        private static RecordKeyEncoder encoder = new RecordKeyEncoderKeyTypes();
        abstract class RecordKeyEncoder {
            public abstract byte[] encode(RecordKey key);
            public abstract void decode(byte[] data, RecordKey key);
        }

        // ------
        // actual encoders

        class RecordKeyEncoderKeyTypes : RecordKeyEncoder {
            public override void decode(byte[] data, RecordKey key) {
                key.key_parts.Clear();  // empty our keyparts                
                MemoryStream ms = new MemoryStream(data);
                BinaryReader b = new BinaryReader(ms);
                ushort num_fields = b.ReadUInt16();
                for (ushort x = 0; x < num_fields; x++) {
                    key.key_parts.Add(RecordKeyType.decodeFrom(b));                    
                }

            }
            public override byte[] encode(RecordKey key) {
                MemoryStream ms = new MemoryStream();
                BinaryWriter b = new BinaryWriter(ms);
                b.Write((ushort)key.key_parts.Count);  // number of fields
                foreach (var part in key.key_parts) {
                    part.encodeTo(b);
                }
                b.Flush();
                return ms.ToArray();
            }

        }

#if OLD_ENCODERS

        class RecordKeyEncoderBetter : RecordKeyEncoder {
            // [int16] number of fields
            //   [int16 field length]
            //   [field-length bytes]

            public override void decode(byte[] data, RecordKey key) {
                key.key_parts.Clear();  // empty our keyparts
                System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
                MemoryStream ms = new MemoryStream(data);
                BinaryReader b = new BinaryReader(ms);
                ushort num_fields = b.ReadUInt16();
                for (ushort x = 0; x < num_fields; x++) {
                    ushort field_len = b.ReadUInt16();
                    byte[] field_data = b.ReadBytes(field_len);
                    key.key_parts.Add(enc.GetString(field_data));
                }

            }
            public override byte[] encode(RecordKey key) {
                // approximate size
                System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

                MemoryStream ms = new MemoryStream();
                BinaryWriter b = new BinaryWriter(ms);
                b.Write((ushort)key.key_parts.Count);  // number of fields
                foreach (var part in key.key_parts) {
                    part.encodeTo(b);
                }
                b.Flush();
                return ms.ToArray();
            }
        }

        class RecordKeyEncoderSimple : RecordKeyEncoder {
            static byte[] escape_list = { DELIMITER_B, 43 };
            static byte escape_char = 43;
            SimpleEncoder kpenc = new SimpleEncoder(escape_list, escape_char);

            public override void decode(byte[] data, RecordKey key) {
                key.key_parts.Clear();  // empty our keyparts
                System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

                int chpos = 0;
                MemoryStream ms = new MemoryStream();
                while (chpos < data.Length) {
                    byte ch = data[chpos];
                    if (ch != DELIMITER_B) {
                        ms.WriteByte(ch);
                    }
                    if (ch == DELIMITER_B || (chpos + 1 == data.Length)) {
                        // handle the part
                        byte[] decoded = kpenc.decode(ms.ToArray());
                        String keystring = enc.GetString(decoded);
                        key.key_parts.Add(keystring);
                        ms = new MemoryStream();
                    }
                    chpos++;
                }
            }

            // encode
            public override byte[] encode(RecordKey key) {
                MemoryStream ms = new MemoryStream();
                String[] keypart_strings = key.key_parts.ToArray();
                System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

                for (int x = 0; x < keypart_strings.Length; x++) {
                    if (x != 0) { ms.WriteByte(DELIMITER_B); }
                    String kp = keypart_strings[x];
                    byte[] encodedform = kpenc.encode(enc.GetBytes(kp));
                    ms.Write(encodedform, 0, encodedform.Length);
                }
                return ms.ToArray();
            }
        }

#endif


    }
}