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
using System.IO;
using System.Linq;

using System.Threading;

using Bend;

namespace Bend.Repl {


    public class ServerConnector {
        Dictionary<string, IReplConnection> server_list = new Dictionary<string, IReplConnection>();
        public IReplConnection getServerHandle(string server_guid) {

            try {
                return server_list[server_guid];
            } catch (KeyNotFoundException) {
                throw new KeyNotFoundException("could not connect to server: " + server_guid);
            }
        }

        public void registerServer(string name, IReplConnection instance) {
            server_list[name] = instance;
        }
        public void unregisterServer(string server_guid) {
            try {
                server_list.Remove(server_guid);
            } catch (KeyNotFoundException) {
                // nothing to remove
            }
        }
    }

    public class ServerContext {
        public string server_guid;
        public ServerConnector connector;
    }


    // ---------------------------------------------------------


    public interface IReplConnection {
        string getDataInstanceId();
        string getServerGuid();
        IStepsKVDB getSnapshot();
        IEnumerable<LogStatus> getStatusForLogs();
        JoinInfo requestToJoin(string server_guid);
        ReplState getState();
        int getEstimatedRemainingLogData(string server_guid, RecordKeyType log_start_key);
        IEnumerable<KeyValuePair<RecordKey, RecordData>> fetchLogEntries(
                        string log_server_guid,
                        RecordKeyType log_start_key,bool block = false);

    }
    public class JoinInfo {
        public string data_instance_id;
        public List<string> seed_servers;
    }
    public class LogStatus {
        public string server_guid;
        public RecordKeyType_Long log_commit_head;
        public RecordKeyType_Long oldest_entry_pointer;
        // public string newest_pending_entry_pointer;
        public override string ToString() {
            return String.Format("LogStatus( {0} oldest:{1} head:{2} )", server_guid, oldest_entry_pointer, log_commit_head);
        }
    }
    public class LogEntry {
        public long logstamp;
        public string server_guid;
        public byte[] data;
    }

}

