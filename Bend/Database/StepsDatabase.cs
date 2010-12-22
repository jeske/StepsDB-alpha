// Copyright (C) 2008-2011 by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Threading;

using NUnit.Framework;

using Bend;
using Bend.Repl;


namespace Bend {

    public interface IStepsKVDB {
         void setValue(RecordKey key, RecordUpdate update);         
         // public GetStatus getRecord(RecordKey key, out RecordData record);
         IEnumerable<KeyValuePair<RecordKey, RecordData>> scanForward(IScanner<RecordKey> scanner);
         IEnumerable<KeyValuePair<RecordKey, RecordData>> scanBackward(IScanner<RecordKey> scanner);
         KeyValuePair<RecordKey, RecordData> FindNext(IComparable<RecordKey> keytest, bool equal_ok);
         KeyValuePair<RecordKey, RecordData> FindPrev(IComparable<RecordKey> keytest, bool equal_ok);
    }

    // -----------------------------------

    public class StepsDatabase {
        private readonly LayerManager db;        

        ServerConnector connector = new ServerConnector();

        public StepsDatabase(LayerManager db) {
            this.db = db;
        }

        public IStepsDocumentDB getDocumentDatabase() {
            return new DocumentDatabaseStage(new StepsStageSubset(new RecordKeyType_String("DOCDB"), this.db));
        }

        public IStepsKVDB getSnapshotDatabase() {
            return new StepsStageSnapshot(new StepsStageSubset(new RecordKeyType_String("SNAPDB"), this.db));
        }

        public ReplHandler getReplicatedDatabase_Fresh(string server_guid) {
            ServerContext ctx = new ServerContext();
            ctx.server_guid = server_guid;
            ctx.connector = connector;

            return ReplHandler.InitFresh(new StepsStageSnapshot(new StepsStageSubset(new RecordKeyType_String(ctx.server_guid), this.db)), ctx);
        }

        public ReplHandler getReplicatedDatabase_Join(string new_server_guid, string join_server_guid) {
            ServerContext ctx = new ServerContext();
            ctx.server_guid = new_server_guid;
            ctx.connector = connector;



            return ReplHandler.InitJoin(
                new StepsStageSnapshot(new StepsStageSubset(new RecordKeyType_String(ctx.server_guid), this.db)),
                ctx,
                join_server_guid);
        }

        public ReplHandler getReplicatedDatabase_Resume(string server_guid) {
            ServerContext ctx = new ServerContext();
            ctx.server_guid = server_guid;
            ctx.connector = connector;

            return ReplHandler.InitResume(
                new StepsStageSnapshot(new StepsStageSubset(new RecordKeyType_String(ctx.server_guid), this.db)),
                ctx);
        }

    }

}

