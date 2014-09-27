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
      
        public static RecordUpdate DeletionTombstone() {
            // TODO: should make this object a singleton
            RecordUpdate update = new RecordUpdate();
            update.type = RecordUpdateTypes.DELETION_TOMBSTONE;
            update.data = new byte[0];
            return update;
        }

        public override String ToString() {
            // see if it looks like ASCII
            bool is_ascii = true;
            foreach (byte x in this.data) {
                if (x > 0x80 || x < 12) {
                    is_ascii = false;
                    break;
                }
            }
            if (is_ascii) {
                System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
                String keystring = "RU: " + type.ToString() + ":" + enc.GetString(this.data);
                return keystring;
            } else {
                String keystring = "RU: " + type.ToString() + ":" + BitConverter.ToString(this.data);
                return keystring;
            }

        }

        public String DebugToString() {
            return this.ToString();
        }

        public byte[] encode() {
            byte[] typeprefix = { (byte)this.type };
            return typeprefix.Concat(this.data).ToArray();
        }
    }

}