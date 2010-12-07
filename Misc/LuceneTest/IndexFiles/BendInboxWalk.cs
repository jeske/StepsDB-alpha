using System;
using System.IO;

using System.Collections.Generic;
using System.Linq;
using System.Text;

using anmar.SharpMimeTools;
using Lucene.Net.Documents;

namespace IndexFiles {

    public class BendInboxWalk {
        internal static readonly System.IO.FileInfo INDEX_DIR = new System.IO.FileInfo(@"c:\EmailTest\LuceneDB");

        [STAThread]
        public static void Main(System.String[] args) {
            System.DateTime start = System.DateTime.Now;
            try
            {
                var writer = new Lucene.Net.Index.IndexWriter(Lucene.Net.Store.FSDirectory.Open(INDEX_DIR), 
                    new Lucene.Net.Analysis.Standard.StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_CURRENT), 
                    true, 
                    Lucene.Net.Index.IndexWriter.MaxFieldLength.LIMITED);
                System.Console.Out.WriteLine("Indexing to directory '" + INDEX_DIR + "'...");

                // want to clean up if indexing gets "cancelled"
                try {
                    parse_email_messages(writer);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(" caught a " + e.GetType() + "\n with message: " + e.Message);
                }


                System.Console.Out.WriteLine(System.DateTime.Now.Millisecond - start.Millisecond + "ms inserting docs");
                System.Console.Out.WriteLine("Optimizing...");

                writer.Optimize();
                writer.Close();
                
                System.Console.Out.WriteLine(System.DateTime.Now.Millisecond - start.Millisecond + "ms total");
            }
            catch (System.IO.IOException e)
            {
                System.Console.Out.WriteLine(" caught a " + e.GetType() + "\n with message: " + e.Message);
            }

            
        }

        public static string UnixReadLine(Stream stream) {
            string line = "";
            while (stream.Position < stream.Length - 1) {

                int ch = stream.ReadByte();
                if (ch != 10) {
                    line = line + Char.ConvertFromUtf32(ch);
                } else {
                    return line;
                }
            }
            return "";
        }


        public static void parse_msg(Lucene.Net.Index.IndexWriter writer, string docid, string msgtxt) {
            if (msgtxt.Length > 4 * 1024) {
                msgtxt = msgtxt.Substring(0, 4 * 1024 - 1);
            }
            // db.setValueParsed(".zdata/doc/" + docid, msgtxt);
            // gui.debugDump(db);


            if (true) {
                // sharptools                
                SharpMessage msg = new anmar.SharpMimeTools.SharpMessage(msgtxt);
                System.Console.WriteLine("Subject: " + msg.Subject);
                var doc = new Lucene.Net.Documents.Document();
                doc.Add(new Field("body", msg.Body, Field.Store.YES, Field.Index.ANALYZED));
                doc.Add(new Field("docid", docid, Field.Store.YES, Field.Index.NOT_ANALYZED));
                writer.AddDocument(doc);
                // indexer.index_document(docid, msg.Body);
            } 

            //foreach (SharpAttachment msgpart in msg.Attachments) {
            //    if (msgpart.MimeTopLevelMediaType == MimeTopLevelMediaType.text &&
            //        msgpart.MimeMediaSubType == "plain") {
            //        System.Console.WriteLine("Attachment: " + msgpart.Size);
            //   }
            //}                    
        }

        public static void parse_email_messages(Lucene.Net.Index.IndexWriter writer) {
            string basepath = @"c:\EmailTest\Data";

            // http://www.csharp-examples.net/get-files-from-directory/
            string[] filePaths = Directory.GetFiles(basepath);

            int count = 1;

            foreach (var fn in filePaths) {
                String fullpath = Path.Combine(basepath, fn);
                System.Console.WriteLine(fullpath);
                FileStream r = File.Open(fullpath, FileMode.Open, FileAccess.Read, FileShare.Read);
                BufferedStream reader = new BufferedStream(r);
                // http://msdn.microsoft.com/en-us/library/system.io.streamreader.readline.aspx

                List<string> lines = new List<string>();
                // LayerManager.WriteGroup txwg = new LayerManager.WriteGroup(db);
                // txwg.add_to_log = false;

                while (reader.Position < reader.Length - 1) {
                    string line = UnixReadLine(reader);
                    if (line.Length > 6 && line.Substring(0, 5) == "From ") {
                        if (lines.Count > 0) {
                            string msg = String.Join("\n", lines);
                            count++;

                            string docid = fullpath + ":" + count;
                            parse_msg(writer, docid, msg);
                        }
                            
      
                        lines = new List<string>();
                    } else {
                        lines.Add(line);
                    }    
                    
                }
                
            }                      


        }        

    }
}
