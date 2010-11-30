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
                try {
                    _Main.do_test();
                } catch (Exception e) {
                    Console.WriteLine(e.ToString());
                }
            }

            
        }

        public static void waitUntilActive(LayerManager db, ReplHandler srvr) {

            for (int x = 0; x < 20; x++) {
                if (srvr.State == ReplHandler.ReplState.active) {
                    break;
                }
                if (srvr.State == ReplHandler.ReplState.shutdown) {
                    Console.WriteLine("waiting for ({0}) to become active.. and he shutdown!!! quitting");
                    Environment.Exit(1);
                }
                Console.WriteLine("waiting for ({0}) to become active.. (currently: {1})", 
                    srvr.ToString(),srvr.State);
                db.debugDump();
                Thread.Sleep(1000);
            }
            if (srvr.State != ReplHandler.ReplState.active) {
                Console.WriteLine("server({0}) failed to become active, aborting test", srvr.ToString());
                Environment.Exit(1);
            }

            Console.WriteLine("Server ({0}) is now active!", srvr);

        }

        static void do_test() {
            Console.WriteLine("ReplTest1 startup...");
            LayerManager db = new LayerManager(InitMode.NEW_REGION, @"C:\BENDtest\repl");
            Random rnd = new Random();
            ServerConnector connector = new ServerConnector();

            Console.WriteLine("----------------[ init two servers together, write some records ]-----------------");

            ServerContext ctx_1 = new ServerContext();         
            ctx_1.server_guid = ".guid1-" + rnd.Next();
            ctx_1.prefix_hack = ctx_1.server_guid + "/repl";
            ctx_1.connector = connector;
            ReplHandler repl_1 = null;
            try {
                 repl_1 = ReplHandler.InitFresh(db, ctx_1);
            } catch (Exception e) {
                db.debugDump();
                Console.WriteLine(e);
                Environment.Exit(1);
            }

            waitUntilActive(db, repl_1);

            ServerContext ctx_2 = new ServerContext();
            ctx_2.server_guid = ".guid2-" + rnd.Next();
            ctx_2.prefix_hack = ctx_2.server_guid + "/repl";
            ctx_2.connector = connector;
            ReplHandler repl_2 = ReplHandler.InitJoin(db, ctx_2, ctx_1.server_guid);
            
            waitUntilActive(db, repl_2);

            repl_1.setValueParsed("/a/1", "1");
            repl_2.setValueParsed("/a/2", "5");

            Console.WriteLine("-----------------");
            db.debugDump();


            Thread.Sleep(10000);

            db.debugDump();

            Environment.Exit(1);

            Console.WriteLine("-----------------[ remove one server, write some records ]----------------");

            repl_2.Shutdown();

            repl_1.setValueParsed("/c/1", "10");

            db.debugDump();


            Console.WriteLine("----------------[ reinit server 2 ]-----------------------------");

            repl_2 = ReplHandler.InitResume(db, ctx_2);

            
            // wait until it comes online

            Console.WriteLine("debug dump DB");
            db.debugDump();




            return; // not ready for this yet

            ServerContext ctx_3 = new ServerContext();
            ctx_3.server_guid = ".guid3-" + rnd.Next();
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
