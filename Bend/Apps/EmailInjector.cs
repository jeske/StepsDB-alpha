
using System;
using System.IO;
using System.Collections.Generic;

using anmar.SharpMimeTools;

using System.Text.RegularExpressions; // used to split body msg into words

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
            // db.setValueParsed(".zdata/doc/" + docid, msgtxt);
            gui.debugDump(db);

            SharpMessage msg = new anmar.SharpMimeTools.SharpMessage(msgtxt);
            System.Console.WriteLine("Subject: " + msg.Subject);

            index_document(docid, msg.Body);

            //foreach (SharpAttachment msgpart in msg.Attachments) {
            //    if (msgpart.MimeTopLevelMediaType == MimeTopLevelMediaType.text &&
            //        msgpart.MimeMediaSubType == "plain") {
            //        System.Console.WriteLine("Attachment: " + msgpart.Size);
            //   }
            //}                    
        }


        public void index_document(string docid, string txtbody) {
            //System.Console.WriteLine(msg.Body);
            int count = 0;
           
            foreach (var srcword in Regex.Split(txtbody, @"\W+")) {
                // System.Console.Write(word + "/");
                if (srcword.Length == 0) { continue; }

                // clean up word.
                var word = srcword.ToLower();
                // remove 's , do stimming, ignore non-words.

                // create a key and insert into the db
                // TODO: docid may have / on UNIX .
                var key = new RecordKey().appendParsedKey(".zdata/index/" + word + "/" + docid + "/" + count); 
                
                // System.Console.WriteLine(key);
                this.db.setValue(key, RecordUpdate.WithPayload(""));
                count++;
            }

        }

        public class EndPrefixMatch : IComparable<RecordKey> {
            RecordKey key;
            public EndPrefixMatch(RecordKey k) {
                this.key = k;
            }
            public int CompareTo(RecordKey target) {
                if (target.isSubkeyOf(key)) {
                    return -1;
                } else {
                    return 1;
                }
            }
            public override string ToString() {
                return "EndPrefixMatch{" + key.ToString() + "}";
            }
        }

        public void find_email_test() {
            string[] words_to_find = { "you", "about"};

            System.Console.WriteLine("### In email test");
            foreach (var word in words_to_find) {
                var start = new RecordKey().appendParsedKey(".zdata/index/" + word);
                var end = new EndPrefixMatch(new RecordKey().appendParsedKey(".zdata/index/" + word));

                foreach (var hit in db.scanForward(new ScanRange<RecordKey> (start, end, null))) {
                    System.Console.WriteLine(hit);
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
                            if (count > 10) {
                                db.debugDump();
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
            find_email_test();
        }

    }
}

