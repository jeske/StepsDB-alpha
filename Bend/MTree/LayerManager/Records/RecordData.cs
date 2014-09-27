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

    // ---------------[ Record* ]---------------------------------------------------------

    public enum RecordDataState {
        NOT_PROVIDED,
        FULL,
        INCOMPLETE,
        DELETED
    }

    public enum RecordDataUpdateResult {
        SUCCESS,
        FINAL
    }

    //-----------------------------------[ RecordData ]------------------------------------
    public class RecordData {
        RecordKey key;
        RecordDataState state;
        public RecordDataState State {
            get { return state; }
        }

        public byte[] data;
        public RecordData(RecordDataState initialState, RecordKey key) {
            this.key = key;
            this.state = initialState;
            this.data = new byte[0];
        }

        public RecordDataUpdateResult applyUpdate(RecordUpdate update) {
            if ((state == RecordDataState.FULL) || (state == RecordDataState.DELETED)) {
                // throw new Exception("applyUpdate() called on fully populated record!");
                Debug.WriteLine("warn: applyUpdate() called on fully populated record. ignoring.");
                return RecordDataUpdateResult.FINAL;
            }
            switch (update.type) {
                case RecordUpdateTypes.DELETION_TOMBSTONE:
                    this.state = RecordDataState.DELETED;
                    return RecordDataUpdateResult.FINAL;

                case RecordUpdateTypes.FULL:
                    this.state = RecordDataState.FULL;
                    this.data = update.data;
                    return RecordDataUpdateResult.FINAL;

                case RecordUpdateTypes.NONE:
                    return RecordDataUpdateResult.SUCCESS;

                case RecordUpdateTypes.PARTIAL:
                    throw new Exception("partial update not implemented");

                default:
                    throw new Exception("unknown update type");

            }
        }

        public String ReadDataAsString() {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            return enc.GetString(this.data);
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
                String keystring = "RD: " + state.ToString() + ":" + enc.GetString(this.data);
                return keystring;
            } else {
                String keystring = "RD: " + state.ToString() + ":" + BitConverter.ToString(this.data);
                return keystring;
            }
        }

        public String DebugToString() {
            return "RD(" + this.ToString() + ")";
        }
    }

}