using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.IO;
using System.Threading;

using Bend;

// couch's restful api
// 
// http://nefariousdesigns.co.uk/archive/2009/08/couchdbs-restful-api/

namespace StepsDBServer {

    //  URL definition
    //  
    //  GET /_all_dbs  -> list array of database names   
    //  GET /<dbname>  -> list info about the database
    //  GET /<dbname>/_all_docs -> get metadata for all docs in the database
    //  GET /<dbname>/<doc id>  -> get specific doc-id
    
    //  POST /<dbname> (document) -> add the document
    //  PUT /<dbname>/<doc id> (document) -> add the document with specified ID
    //  DELETE /<dbname>/<doc id > -> delete the document ID from the db
    
    
    public class StepsRestAPIServer : HttpServer {
        IStepsDocumentDB db;

        public StepsRestAPIServer(int port, IStepsDocumentDB db)
            : base(port) {
            this.db = db;
        }
        
        public override void handleGETRequest(HttpProcessor p) {
            // 

            throw new NotImplementedException();
        }

        public override void handlePOSTRequest(HttpProcessor p, System.IO.StreamReader inputData) {
            // if we put an empty message to a url, it's a request to create that database


            throw new NotImplementedException();
        }
    }
}