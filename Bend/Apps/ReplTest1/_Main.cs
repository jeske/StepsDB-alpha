using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

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
            LayerManager raw_db = new LayerManager(InitMode.NEW_REGION, @"C:\BENDtst\repl");
            StepsDatabase db_factory = new StepsDatabase(raw_db);

            Console.WriteLine("----------------[ init two servers together, write some records ]-----------------");

            ReplHandler repl_1 = db_factory.getReplicatedDatabase_Fresh("guid1");

            waitUntilActive(raw_db, repl_1);
            repl_1.setValueParsed("a/1", "1");

            ReplHandler repl_2 = db_factory.getReplicatedDatabase_Join("guid2", repl_1.getServerGuid());
                
            waitUntilActive(raw_db, repl_2);
            repl_2.setValueParsed("a/2", "5");

            Console.WriteLine("-----------------");
            raw_db.debugDump();

            Thread.Sleep(7000);

            raw_db.debugDump();

            
            Console.WriteLine("-----------------[ remove one server, write some records ]----------------");

            
            repl_2.Shutdown();

            // wait until repl2 is really shutdown


            // make sure our log does not continue from repl_2 logs
            repl_1.setValueParsed("c/1", "10");            
            repl_1.truncateLogs_Hack();
            
            raw_db.debugDump();

            Console.WriteLine("----------------[ reinit server 2 ]-----------------------------");

            repl_2 = db_factory.getReplicatedDatabase_Resume("guid2");            
            
            // wait until it comes online

            Thread.Sleep(7000);
            
            raw_db.debugDump();

            Thread.Sleep(7000);

            raw_db.debugDump();

            Environment.Exit(1);  // exit



            Console.WriteLine("----------------[ create server 3 ]-----------------------------");
           
            ReplHandler repl_3 = db_factory.getReplicatedDatabase_Join("guid3", "guid2");
                
            raw_db.debugDump();

            repl_3.setValueParsed("/qq", "10");
            raw_db.debugDump();

            Console.WriteLine("waiting for key");
            Console.ReadKey();
            Console.WriteLine("quitting..");
            Environment.Exit(0);
        }

    }



}
