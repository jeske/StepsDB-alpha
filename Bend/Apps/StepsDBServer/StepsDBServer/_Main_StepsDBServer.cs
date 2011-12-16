using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.IO;
using System.Threading;

using Bend;


namespace StepsDBServer {


    class _Main_StepsDBServer {
        static void Main(string[] args) {
            bool isInitialStartup = false;

            // (1) read the config file

            string DBPATH = @"c:\BENDtst\main";


            // ... if we are doing an initial startup...

            if (isInitialStartup) {
                // DO initial database setup and then end...
                LayerManager new_db = new LayerManager(InitMode.NEW_REGION, DBPATH);                
                
                // ...
                
                return;
            }

            // (2) startup a snapshot/replica/document database

            LayerManager raw_db = new LayerManager(InitMode.RESUME, DBPATH);
            StepsDatabase db_broker = new StepsDatabase(raw_db);

            // how do we address subsetting / databases / collections??? 
            IStepsDocumentDB doc_db = db_broker.getDocumentDatabase();

            // (3) startup the web-status interface

            StepsStatusServer myStatusServer = new StepsStatusServer(81, raw_db);
            Thread thread = new Thread(new ThreadStart(myStatusServer.listen));
            thread.Start();

            // (4) start the REST api handler (listening for client connections)

            StepsRestAPIServer myServer = new StepsRestAPIServer(5985, doc_db);
            myServer.listen(); // (main runloop)

            // !! SHUTDOWN !!

            thread.Abort();
        }
    }
}
