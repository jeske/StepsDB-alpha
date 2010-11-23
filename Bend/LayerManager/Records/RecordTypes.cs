// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using System.Diagnostics;

namespace Bend
{

    // ---------------[ Record* ]---------------------------------------------------------

    public enum RecordDataState
    {
        NOT_PROVIDED,
        FULL,
        INCOMPLETE,
        DELETED
    }
    public enum RecordUpdateResult
    {
        SUCCESS,
        FINAL
    }

    //-----------------------------------[ RecordData ]------------------------------------
    public class RecordData
    {
        RecordKey key;
        RecordDataState state;
        public RecordDataState State {
            get { return state; }
        }
    
        public byte[] data;
        public RecordData(RecordDataState initialState, RecordKey key)
        {
            this.key = key;
            this.state = initialState;
            this.data = new byte[0];
        }

        public RecordUpdateResult applyUpdate(RecordUpdate update)
        {
            if ((state == RecordDataState.FULL) || (state == RecordDataState.DELETED)) {
                // throw new Exception("applyUpdate() called on fully populated record!");
                Debug.WriteLine("warn: applyUpdate() called on fully populated record. ignoring.");
                return RecordUpdateResult.FINAL;
            }
            switch (update.type)
            {
                case RecordUpdateTypes.DELETION_TOMBSTONE:
                    this.state = RecordDataState.DELETED;
                    return RecordUpdateResult.FINAL;

                case RecordUpdateTypes.FULL:
                    this.state = RecordDataState.FULL;
                    this.data = update.data;
                    return RecordUpdateResult.FINAL;

                case RecordUpdateTypes.NONE:
                    return RecordUpdateResult.SUCCESS;

                case RecordUpdateTypes.PARTIAL:
                    throw new Exception("partial update not implemented");

                default:
                    throw new Exception("unknown update type");

            }
        }

        public override String ToString() {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

            if (state == RecordDataState.FULL) {
                return enc.GetString(data);
            } else {
                return String.Format("[{0}] {1}", state.ToString(), enc.GetString(data));
            }
        }

        public String DebugToString()
        {
            return "RD(" + this.ToString() + ")";
        }
    }

    public enum RecordUpdateTypes
    {
        DELETION_TOMBSTONE,
        PARTIAL,
        FULL,
        NONE
    }



    //------------------------------------------------------------------------------------

    //-----------------------------------[ RecordUpdate ]---------------------------------

    //------------------------------------------------------------------------------------



    public class RecordUpdate : IEquatable<RecordUpdate>
    {
        public RecordUpdateTypes type;
        public byte[] data;
        private RecordUpdate(RecordUpdateTypes type, String sdata)
        {
            this.type = type;
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            this.data = enc.GetBytes(sdata);
           
        }
        private RecordUpdate(RecordUpdateTypes type, byte[] data) {
            this.type = type;
            this.data = data;
        }

        private RecordUpdate() {
        }

        public bool Equals(RecordUpdate target) {
            // we need to scan the bytestream ourselves, but keep in mind this could be expensive...
            if (this.data.Length != target.data.Length) {
                return false;
            }
            for (int i = 0; i < this.data.Length; i++) {
                if (this.data[i] != target.data[i]) {
                    return false;
                }
            }

            return true;
        }
        public override bool Equals(object target) {
            if (target.GetType() != this.GetType()) {
                return false;
            }
            return this.Equals((RecordUpdate)target);
        }
        public override int GetHashCode() {
            return data.GetHashCode();
        }

        public static RecordUpdate WithPayload(byte[] payload) {
            return new RecordUpdate(RecordUpdateTypes.FULL, payload);
        }

        public static RecordUpdate WithPayload(String payload) {
            return new RecordUpdate(RecordUpdateTypes.FULL, payload);
        }
        public static RecordUpdate NoUpdate() {
            RecordUpdate update = new RecordUpdate();
            update.type = RecordUpdateTypes.NONE;
            update.data = new byte[0];
            return update;
        }
        public static RecordUpdate FromEncodedData(byte[] encoded_data) {
            RecordUpdate update = new RecordUpdate();
            update.type = (RecordUpdateTypes)encoded_data[0];

            update.data = encoded_data.Skip(1).ToArray();
            return update;
        }

        [Obsolete]
        public static RecordUpdate FromEncodedData(String encoded_data) {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            byte[] data = enc.GetBytes(encoded_data);
            return RecordUpdate.FromEncodedData(data);
        }
        public static RecordUpdate DeletionTombstone() {
            // TODO: should make this object a singleton
            RecordUpdate update = new RecordUpdate();
            update.type = RecordUpdateTypes.DELETION_TOMBSTONE;
            update.data = new byte[0];
            return update;
        }

        public override String ToString()
        {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            String keystring = enc.GetString(data);
            if (type != RecordUpdateTypes.FULL) {
                keystring = keystring + " [" + type.ToString() + "]";
            }
            return keystring;
        }


        public String DebugToString() {
            return "RU(" + this.ToString() + ")";
        }

        public byte[] encode() {
            byte[] typeprefix = { (byte)this.type };
            return typeprefix.Concat(this.data).ToArray();
        }
    }


    //------------------------------------------------------------------------------------

    //-----------------------------------[ RecordKey ]------------------------------------

    //------------------------------------------------------------------------------------


    public class RecordKey : IComparable<RecordKey>
    {
        // TODO: key_parts really shouldn't be public, people should be using pipes to read keys
        // TODO: maybe key parts shouldn't be limited to strings
        public List<String> key_parts;  
        public static char DELIMITER = '/';
        public static byte DELIMITER_B = 47;
        
        public RecordKey()
        {
            key_parts = new List<String>();
        }
        public RecordKey(byte[] data)
            : this() {
            decode(data);
        }

        public RecordKey appendParsedKey(String keyToParse) {
            char[] delimiters = { DELIMITER };

            if (keyToParse.Length > 0) {
                if (keyToParse[keyToParse.Length - 1] == DELIMITER) {
                    throw new Exception(
                        String.Format("appendParsedKey({0}) may not end in DELIMTER({1})",
                        keyToParse, DELIMITER));
                }
            }

            String[] keystring_parts = keyToParse.Split(delimiters);
            foreach (String keypart in keystring_parts) {
                this.appendKeyPart(keypart);
            }
            return this;
        }

        public RecordKey appendKeyParts(params object[] args) {
            foreach (object arg in args) {
                if (arg.GetType() == typeof(String)) {
                    this.appendKeyPart((String)arg);
                } else if (arg.GetType() == typeof(byte[])) {
                    this.appendKeyPart((byte[])arg);
                } else {
                    throw new Exception("unknown argument type");
                }
            }
            return this;
        }

        public RecordKey appendKeyPart(String part) {
            key_parts.Add(part);
            return this;
        }
        public RecordKey appendKeyPart(byte[] data) {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            String keystring = enc.GetString(data);
            key_parts.Add(keystring);
            return this;
        }

        public RecordKey appendKeyPart(RecordKey keydata) {
            key_parts.Add(keydata.ToString());
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
        
        public int CompareTo(RecordKey target)
        {
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

        private class RecordKeyAfterPrefixComparable : IComparable<RecordKey>
        {
            RecordKey prefixkey;
            internal RecordKeyAfterPrefixComparable(RecordKey prefixkey) {
                this.prefixkey = prefixkey;
            }

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

        public bool isSubkeyOf(RecordKey potential_parent_key) {
            // we are a subkey of the other key we are longer AND 
            // and all their parts match our parts

            if (potential_parent_key.key_parts.Count > this.key_parts.Count) {
                return false; // they are longer, so we can't be a subkey
            }

            for (int pos = 0; pos < potential_parent_key.key_parts.Count; pos++) {
                if (potential_parent_key.key_parts[pos] != this.key_parts[pos]) {
                    return false; // one of their parts didn't match
                }
            }
            return true; // all their parts matched, and they were shorter or equal, so we are a subkey
        }

        public override bool Equals(object obj) {
            try {
                return this.CompareTo((RecordKey)obj) == 0;
            }
            catch (InvalidCastException) {
                return false;
            }
        }
        public override int GetHashCode() {
            int hash_code = 0;
            foreach (string part in key_parts) {
                hash_code += part.GetHashCode();
            }
            return hash_code;
        }

        public string DebugToString()
        {
            return "K(" + this.ToString() + ")";
        }

        public override string ToString() {
            return String.Join(new String(DELIMITER, 1), key_parts.ToArray());
        }

        // TODO: this is hacky, we should find a better abstraction to let people see our keyparts
        public IEnumerator<string> GetEnumeratorForKeyparts() {
            return this.key_parts.GetEnumerator();
        }

        // -----------------------------------------------------------
        // encoding/decoding of keyparts

        // TODO: handle delimiters in the key


        static byte[] escape_list = {DELIMITER_B, 43};
        static byte escape_char = 43;
        SimpleEncoder kpenc = new SimpleEncoder(escape_list, escape_char);

        void decode(byte[] data) {
            key_parts.Clear();  // empty our keyparts
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

            int chpos = 0;
            MemoryStream ms = new MemoryStream();
            while (chpos < data.Length) {                
                byte ch = data[chpos];
                if (ch != DELIMITER_B) {
                    ms.WriteByte(ch);
                }
                if (ch == DELIMITER_B || (chpos+1 == data.Length)) {               
                    // handle the part
                    byte[] decoded = kpenc.decode(ms.ToArray());
                    String keystring = enc.GetString(decoded);
                    key_parts.Add(keystring);
                    ms = new MemoryStream();
                } 
                chpos++;
            }
        }

        // decode
        void decode2(byte[] data) {
            char[] delimiters = { DELIMITER };
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            String keystring = enc.GetString(data);
            String[] keystring_parts = keystring.Split(delimiters);
            
            key_parts.AddRange(keystring_parts);
        }

        // encode
        public byte[] encode() {
            MemoryStream ms = new MemoryStream();
            String[] keypart_strings = key_parts.ToArray();
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

            for(int x=0;x<keypart_strings.Length;x++) {
                if (x != 0) { ms.WriteByte(DELIMITER_B); }
                String kp = keypart_strings[x];
                byte[] encodedform = kpenc.encode(enc.GetBytes(kp));
                ms.Write(encodedform,0,encodedform.Length);                
            }
            return ms.ToArray();
        }
    }


}