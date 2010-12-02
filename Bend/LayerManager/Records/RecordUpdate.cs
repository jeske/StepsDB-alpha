// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using System.Diagnostics;

namespace Bend {

    public enum RecordUpdateTypes {
        DELETION_TOMBSTONE,
        PARTIAL,
        FULL,
        NONE
    }



    //------------------------------------------------------------------------------------

    //-----------------------------------[ RecordUpdate ]---------------------------------

    //------------------------------------------------------------------------------------



    public class RecordUpdate : IEquatable<RecordUpdate> {
        public RecordUpdateTypes type;
        public byte[] data;
        private RecordUpdate(RecordUpdateTypes type, String sdata) {
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

        public override String ToString() {
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

}