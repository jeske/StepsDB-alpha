
using System;
using System.IO;
using System.Collections.Generic;
using System.Windows.Forms;

using System.Threading;

using LumiSoft.Net.Mime;
using anmar.SharpMimeTools;

using Bend.Indexer;

// TODO: put this in a different namespace from Bend, in a separate build target

// http://stackoverflow.com/questions/903711/reading-an-mbox-file-in-c
// http://www.codeproject.com/KB/cs/mime_project.aspx?print=true

namespace Bend.EmailIndexerTest {
     
   
    public class EmailInjector {

        LayerManager db;
        DbgGUI gui;
        TextIndexer indexer;


        [STAThread]
        static void Main(string[] args) {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            var window = new DbgGUI();
            window.SetDesktopLocation(700, 200);

            Thread newThread = new Thread(delegate() {
                EmailInjector.do_test(window);
            });
            newThread.Start();
            Application.Run(window);
        }

        public static void do_test(DbgGUI window) {
            LayerManager db = new LayerManager(InitMode.NEW_REGION, @"c:\EmailTest\DB");
            EmailInjector injector = new EmailInjector(db, window);
            injector.DoEmailTest();
            Console.WriteLine("done...press any key.");
            Console.ReadLine();
        }

        public EmailInjector(LayerManager db, DbgGUI gui) {
            this.db = db;
            this.gui = gui;
            this.indexer = new TextIndexer(db);
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

        public void parse_msg(LayerManager.WriteGroup txwg, string docid, string msgtxt) {
            if (msgtxt.Length > 4 * 1024) {
                msgtxt = msgtxt.Substring(0, 4 * 1024 - 1);
            }
            // db.setValueParsed(".zdata/doc/" + docid, msgtxt);
            // gui.debugDump(db);


            if (true) {
                // sharptools                
                SharpMessage msg = new anmar.SharpMimeTools.SharpMessage(msgtxt);
                System.Console.WriteLine("Subject: " + msg.Subject);

                indexer.index_document(txwg, docid, msg.Body);
            } else {
                // LumiSoft                
                Mime msg = LumiSoft.Net.Mime.Mime.Parse(System.Text.Encoding.Default.GetBytes(msgtxt));
                System.Console.WriteLine("Subject: " + msg.MainEntity.Subject);
                indexer.index_document(txwg, docid, msg.MainEntity.DataText);

            }

            //foreach (SharpAttachment msgpart in msg.Attachments) {
            //    if (msgpart.MimeTopLevelMediaType == MimeTopLevelMediaType.text &&
            //        msgpart.MimeMediaSubType == "plain") {
            //        System.Console.WriteLine("Attachment: " + msgpart.Size);
            //   }
            //}                    
        }

        public void parse_email_messages() {
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
                LayerManager.WriteGroup txwg = new LayerManager.WriteGroup(db);
                txwg.add_to_log = false;


                while (reader.Position < reader.Length - 1) {
                    string line = UnixReadLine(reader);
                    if (line.Length > 6 && line.Substring(0, 5) == "From ") {
                        if (lines.Count > 0) {
                            string msg = String.Join("\n", lines);
                            count++;

                            System.Console.WriteLine("count: " + count);
                            if (count % 400 == 0) {
                                gui.debugDump(db);
                                db.flushWorkingSegment();
                                gui.debugDump(db);
                                for (int x = 0; x < 5; x++) {
                                    var mc = db.rangemapmgr.mergeManager.getBestCandidate();
                                    gui.debugDump(db, mc);
                                    if (mc == null) { break; }
                                    db.performMerge(mc);
                                    gui.debugDump(db);
                                }                                
                                // return;
                            }

                            string docid = fullpath + ":" + count;
                            parse_msg(txwg, docid,msg);

                            // if (count > 40) { return; }

                            if (count > 1000) {
                                db.flushWorkingSegment();
                                gui.debugDump(db);
                                for (int x = 0; x < 5; x++) {
                                    var mc = db.rangemapmgr.mergeManager.getBestCandidate();
                                    gui.debugDump(db, mc);
                                    if (mc == null) { break; }
                                    db.performMerge(mc);
                                    gui.debugDump(db);
                                }                                
                                // db.debugDump();
                                return;
                           }
                        }
                        lines = new List<string>();
                    } else {
                        lines.Add(line);
                    }
                    
                    
                }
                
            }
        }

        public void DoEmailTest() {
            parse_email_messages();
            indexer.find_email_test();
        }

    }
}

