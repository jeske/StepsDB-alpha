// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

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

        public override String ToString() {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

            if (state == RecordDataState.FULL) {
                return enc.GetString(data);
            } else {
                return String.Format("[{0}] {1}", state.ToString(), enc.GetString(data));
            }
        }

        public String DebugToString() {
            return "RD(" + this.ToString() + ")";
        }
    }

}