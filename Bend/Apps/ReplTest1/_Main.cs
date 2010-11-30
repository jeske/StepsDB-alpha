using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Windows.Forms;
using System.Threading;

using Bend;

namespace Bend.ReplTest1 {
    class _Main {

        [STAThread]
        static void Main(string[] args) {
            if (false) {
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                var window = new DbgGUI();
                window.SetDesktopLocation(700, 200);
                Thread newThread = new Thread(_Main.do_test);
                newThread.Start();
                Application.Run(window);
            } else {
                _Main.do_test();
            }

            
        }


        static void do_test() {
            Console.WriteLine("ReplTest1 startup...");
            LayerManager db = new LayerManager(InitMode.NEW_REGION, @"C:\BENDtest\repl");
            Random rnd = new Random();
            ServerConnector connector = new ServerConnector();
            
            ServerContext ctx_1 = new ServerContext();         
            ctx_1.server_guid = "guid" + rnd.Next();
            ctx_1.prefix_hack = ctx_1.server_guid + "/repl";
            ctx_1.connector = connector;
            ReplHandler repl_1 = ReplHandler.InitFresh(db, ctx_1);            


            ServerContext ctx_2 = new ServerContext();
            ctx_2.server_guid = "guid" + rnd.Next();
            ctx_2.prefix_hack = ctx_2.server_guid + "/repl";
            ctx_2.connector = connector;
            ReplHandler repl_2 = ReplHandler.InitJoin(db, ctx_2, ctx_1.server_guid);

            repl_1.setValueParsed("/a/1", "1");
            repl_1.setValueParsed("/b/2", "2");
            repl_1.setValueParsed("/a/1", "3");

            Console.WriteLine("debug dump DB");
            db.debugDump();

            repl_2.setValueParsed("/a/2", "5");

            Console.WriteLine("debug dump DB");
            db.debugDump();



            ServerContext ctx_3 = new ServerContext();
            ctx_3.server_guid = "guid" + rnd.Next();
            ctx_3.prefix_hack = ctx_2.server_guid + "/repl";
            ctx_3.connector = connector;
            ReplHandler repl_3 = ReplHandler.InitJoin(db, ctx_3, ctx_2.server_guid);            

            db.debugDump();

            repl_3.setValueParsed("/qq", "10");
            db.debugDump();

            Console.WriteLine("waiting for key");
            Console.ReadKey();
            Console.WriteLine("quitting..");
            Environment.Exit(0);
        }

    }



}
