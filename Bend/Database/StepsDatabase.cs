﻿// Copyright (C) 2008-2011 by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Threading;

using NUnit.Framework;

using Bend;


namespace Bend {

    public interface IStepsKVDB {
         void setValue(RecordKey key, RecordUpdate update);         
         // public GetStatus getRecord(RecordKey key, out RecordData record);
         IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner);
         IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner);
         KeyValuePair<RecordKey, RecordData> FindNext(IComparable<RecordKey> keytest, bool equal_ok);
         KeyValuePair<RecordKey, RecordData> FindPrev(IComparable<RecordKey> keytest, bool equal_ok);
    }

   


    public class StepsDatabase {
        private readonly LayerManager db;
        public StepsDatabase(LayerManager db) {
            this.db = db;
        }


        public IStepsDocumentDB getDocumentDatabase() {
            return new DocumentDatabaseStage(new SubsetStage(new RecordKeyType_String("DOCDB"), this.db));
        }

        public IStepsKVDB getSnapshotDatabase() {
            return new TimestampSnapshotStage(new SubsetStage(new RecordKeyType_String("SNAPDB"), this.db));
        }
    }

}

