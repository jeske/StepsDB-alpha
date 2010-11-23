
using System;
using System.IO;
using System.Collections.Generic;

using anmar.SharpMimeTools;

// TODO: put this in a different namespace from Bend, in a separate build target

// http://stackoverflow.com/questions/903711/reading-an-mbox-file-in-c
// http://www.codeproject.com/KB/cs/mime_project.aspx?print=true

namespace Bend {

    public class EmailInjector {
        LayerManager db;
        DbgGUI gui;

        public EmailInjector(LayerManager db, DbgGUI gui) {
            this.db = db;
            this.gui = gui;
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

        public void parse_msg(string docid, string msgtxt) {
            if (msgtxt.Length > 4 * 1024) {
                msgtxt = msgtxt.Substring(0, 4 * 1024 - 1);
            }
            db.setValueParsed(".zdata/doc/" + docid, msgtxt);
            gui.debugDump(db);

            SharpMessage msg = new anmar.SharpMimeTools.SharpMessage(msgtxt);
            System.Console.WriteLine("Subject: " + msg.Subject);

            // System.Console.WriteLine(msg.Body);
            foreach (SharpAttachment msgpart in msg.Attachments) {
                if (msgpart.MimeTopLevelMediaType == MimeTopLevelMediaType.text &&
                    msgpart.MimeMediaSubType == "plain") {
                    System.Console.WriteLine("Attachment: " + msgpart.Size);
                }
            }                    
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

                while (reader.Position < reader.Length - 1) {
                    string line = UnixReadLine(reader);
                    if (line.Length > 6 && line.Substring(0, 5) == "From ") {
                        if (lines.Count > 0) {
                            string msg = String.Join("\n", lines);
                            count++;

                            System.Console.WriteLine("count: " + count);
                            if (count % 1000 == 0) {                                
                                db.flushWorkingSegment();
                                gui.debugDump(db);
                                var mc = db.rangemapmgr.mergeManager.getBestCandidate();
                                db.performMerge(mc);
                                gui.debugDump(db);
                                // return;
                            }

                            string docid = fullpath + ":" + count;
                            parse_msg(docid,msg);
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

