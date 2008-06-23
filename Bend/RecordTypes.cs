// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
            if (state == RecordDataState.FULL) {
                throw new Exception("applyUpdate() called on fully populated record!");
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
            return enc.GetString(data);
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
    //-----------------------------------[ RecordUpdate ]------------------------------------

    public class RecordUpdate
    {
        public RecordUpdateTypes type;
        public byte[] data;
        private RecordUpdate(RecordUpdateTypes type, String sdata)
        {
            this.type = type;
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            this.data = enc.GetBytes(sdata);
           
        }
        private  RecordUpdate(RecordUpdateTypes type, byte[] data) {
            this.type = type;
            this.data = data;
        }

        private RecordUpdate() {
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

    //-----------------------------------[ RecordKey ]------------------------------------

    public class RecordKey : IComparable<RecordKey>
    {
        List<String> key_parts;
        public static char DELIMITER = '/';
        
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

            if (keyToParse[keyToParse.Length - 1] == DELIMITER) {
                throw new Exception(
                    String.Format("appendParsedKey({0}) may not end in DELIMTER({1})",
                    keyToParse, DELIMITER));
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


        public int numParts() {
            return key_parts.Count;
        }

        // TODO: deal with field types (i.e. number sorting)
        public int CompareTo(RecordKey obj)
        {
            int pos = 0;
            int cur_result = 0;

            int thislen = key_parts.Count;
            int objlen = key_parts.Count;

            while (cur_result == 0)  // while equal
            {
                if (((thislen - pos) == 0) &&
                     ((objlen - pos) == 0))
                {
                    // equal and at the end
                    return 0; // equal
                }
                if (((thislen - pos) == 0) &&
                     ((objlen - pos) > 0))
                {
                    // equal and obj longer
                    return -1; // obj longer, so obj is greater
                }
                if (((thislen - pos) > 0) &&
                     ((objlen - pos) == 0))
                {
                    // equal and this longer
                    return 1; // this longer, so this greater
                }
                cur_result = this.key_parts[pos].CompareTo(obj.key_parts[pos]);
                pos++; // consider the next keypart
            }


            return cur_result;
        }

        public bool isSubkeyOf(RecordKey potential_parent_key) {
            // we are a subkey of the other key if they are shorter or equal length,
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
            if (obj.GetType() != this.GetType()) {
                return false;
            }
            RecordKey okey = (RecordKey)obj;
            return this.CompareTo(okey) == 0;
        }

        public string DebugToString()
        {
            return "K(" + this.ToString() + ")";
        }

        public override string ToString() {
            return String.Join(new String(DELIMITER, 1), key_parts.ToArray());
        }


        // -----------------------------------------------------------
        // encoding/decoding of keyparts


        // decode
        void decode(byte[] data) {
            char[] delimiters = { DELIMITER };
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            String keystring = enc.GetString(data);
            String[] keystring_parts = keystring.Split(delimiters);
            
            key_parts.AddRange(keystring_parts);
        }

        // encode
        public byte[] encode() {
            String srep = String.Join(new String(DELIMITER,1), key_parts.ToArray());

            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            byte[] data = enc.GetBytes(srep);
            return data;
        }
    }


}