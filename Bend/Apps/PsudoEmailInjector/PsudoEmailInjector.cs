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
using System.Text;

using System.IO;
using System.Windows.Forms;
using System.Threading;


using Bend.Indexer;

namespace Bend.PsudoEmailInjector {
    class PsudoEmailInjector {
        LayerManager db;
        DbgGUI gui;
        TextIndexer indexer;
        Random id_gen = new Random(100);


        static string DB_PATH = @"c:\EmailTest\DBrnd";

        [STAThread]
        static void Main(string[] args) {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            var window = new DbgGUI();
            window.SetDesktopLocation(700, 200);

            Thread newThread = new Thread(delegate() {
                PsudoEmailInjector.do_test(window, args);
            });
            newThread.Start();
            Application.Run(window);
        }

        public static void do_test(DbgGUI window, string[] args) {
            if (args.Length < 1) {
                Console.WriteLine("Usage:\n  index - clear the db and index email\n   search - perform search tests");
                Environment.Exit(1);
            }
            if (args[0].CompareTo("index") == 0) {
                LayerManager db = new LayerManager(InitMode.NEW_REGION, DB_PATH );
                db.startMaintThread();
                PsudoEmailInjector injector = new PsudoEmailInjector(db, window);
                injector.parse_email_messages();
                injector.indexer.find_email_test();
            } else if (args[0].CompareTo("search") == 0) {
                LayerManager db = new LayerManager(InitMode.RESUME, DB_PATH);
                PsudoEmailInjector injector = new PsudoEmailInjector(db, window);
                window.debugDump(db);
                injector.indexer.find_email_test();
            } else if (args[0].CompareTo("merge") == 0) {
                LayerManager db = new LayerManager(InitMode.RESUME, DB_PATH);
                window.debugDump(db);
                // merge...                
                for (int x = 0; x < 30; x++) {
                    var mc = db.rangemapmgr.mergeManager.getBestCandidate();
                    window.debugDump(db, mc);
                    if (mc == null) {
                        Console.WriteLine("no more merge candidates.");
                        break;
                    }
                    db.performMerge(mc);
                    window.debugDump(db);
                }
            } else if (args[0].CompareTo("test") == 0) {
                LayerManager db = new LayerManager(InitMode.RESUME, DB_PATH);
                window.debugDump(db);
                var key1 = new RecordKey()
                    .appendParsedKey(@".zdata/index/which/c:\EmailTest\Data\Sent:5441/10");
                var key2 = new RecordKey()
                    .appendParsedKey(@".zdata/index/zzn/c:\EmailTest\Data\saved_mail_2003:4962/385");

                var segkey = new RecordKey()
                    .appendParsedKey(".ROOT/GEN")
                    .appendKeyPart(new RecordKeyType_Long(1))
                    .appendKeyPart(key1)
                    .appendKeyPart(key2);

                var nextrow = db.FindNext(segkey, false);

                Console.WriteLine("next: {0}", nextrow);

                var exactRow = db.FindNext(nextrow.Key, true);

                Console.WriteLine("refind: {0}", exactRow);

            }
            Console.WriteLine("done....");

            Environment.Exit(0);
        }


        public PsudoEmailInjector(LayerManager db, DbgGUI gui) {
            this.db = db;
            this.gui = gui;
            this.indexer = new TextIndexer(db);
        }


        // generate a bunch of data and insert it
        public void parse_email_messages() {

            LayerWriteGroup txwg = new LayerWriteGroup(db, type: LayerWriteGroup.WriteGroupType.MEMORY_ONLY);                

            List<string> word_list = new List<String>();
            
            for (int docnum = 0; docnum < 100; docnum++) {
                for (int wordnum = 0; wordnum < 10000; wordnum++) {
                    int word = id_gen.Next(5000);
                    string word_s = "" + word;
                    word_list.Add(word_s);
                }

                // for each msg, do this
                indexer.index_document(txwg, "" + docnum, word_list);
                // indexer.index_document(txwg, docid, msg.MainEntity.DataText);
                if (docnum % 10 == 0) {
                    System.Console.WriteLine("doc {0}", docnum);
                    gui.debugDump(db); 
                }

            }

        }
    }
}
