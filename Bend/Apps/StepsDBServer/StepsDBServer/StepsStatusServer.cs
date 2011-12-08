
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


using System.IO;
using System.Threading;

using Bend;

namespace StepsDBServer {
    public class StepsStatusServer : HttpServer {
        LayerManager raw_db;
        public StepsStatusServer(int port, LayerManager raw_db)
            : base(port) {
            this.raw_db = raw_db;
        }

        public override void handleGETRequest(HttpProcessor p) {
            // right now we just render status onto any URL
            p.writeSuccess();
            StreamWriter s = p.outputStream;

            s.WriteLine("<html><body> <h1> StepsDB Server Status <h1> \n");
            s.WriteLine("<p>");
            s.WriteLine("<table border=1>\n");
            s.WriteLine(String.Format("<tr><td>Working Segment Size</td><td>{0}</td></tr>\n", raw_db.workingSegmentSize()));
            s.WriteLine("<tr><td>Segments</td><td>\n");

            // write out list of all segments
            {
                s.WriteLine("<table border=1>\n");
                foreach (var segment in raw_db.listAllSegments()) {
                    s.WriteLine(String.Format("<tr><td>{0}</td><Td>{1}</td></tr>\n", segment.start_key, segment.end_key));
                }
                s.WriteLine("</table>\n");
            }

            s.WriteLine("</table>\n");

        }

        public override void handlePOSTRequest(HttpProcessor p, System.IO.StreamReader inputData) {
            throw new NotImplementedException();
        }
    }

}