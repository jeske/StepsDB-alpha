
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Bend;



// our keysapce schema

// _my/config/ID = <server guid>
// _my/config/quorum_requirement = <number of servers before we advance the repl tail>

// _server/<SERVER GUID>/location = host:port

// _logs/<SERVER GUID>/<logid> -> [update info]

// _log_status/<SERVER_GUID>/repl_tail -> the oldest <logid> that may not be replicated for this server-guid

namespace Bend.ReplTest1 {


    public class ReplHandler {
        LayerManager db;
        public ReplHandler(LayerManager db) {
            this.db = db;
        }
    }

}