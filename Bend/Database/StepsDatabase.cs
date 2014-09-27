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

using System.Threading;

using NUnit.Framework;

using Bend;
using Bend.Repl;



namespace Bend {

   

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

