using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using System.Windows.Forms;
using System.Threading;

using Bend;
using Bend.Repl;

namespace Bend.ReplTest1 {
    class _Main {

        [STAThread]
        static void Main(string[] args) {

#if false            
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                var window = new DbgGUI();
                window.SetDesktopLocation(700, 200);
                Thread newThread = new Thread(_Main.do_test);
                newThread.Start();
                Application.Run(window);
#else            
                try {
                    _Main.do_test();
                } catch (Exception e) {
                    Console.WriteLine(e.ToString());
                }
#endif            
        }

        public static void waitUntilActive(LayerManager db, ReplHandler srvr) {
            waitUntilState(db, srvr, ReplState.active);
        }

        public static void waitUntilState(LayerManager db, ReplHandler srvr, ReplState state) {

            for (int x = 0; x < 20; x++) {
                if (srvr.State == state) {
                    break;
                }
                Console.WriteLine("waiting for ({0}) to become {1}.. (currently: {2})", 
                    srvr.ToString(),state, srvr.State);
                
                Thread.Sleep(1000);
            }
            if (srvr.State != state) {
                db.debugDump();
                Console.WriteLine("server({0}) failed to become {1}, aborting test", 
                    srvr.ToString(), state);
                
                Environment.Exit(1);
            }

            Console.WriteLine("Server ({0}) is now {1}!", srvr, state);

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

            repl_2.setValueParsed("a/2", "6");
            Thread.Sleep(7000);

            
            raw_db.debugDump();

            
            Console.WriteLine("-----------------[ remove one server, write some records ]----------------");

            
            repl_2.Shutdown();

            // wait until repl2 is really shutdown
            waitUntilState(raw_db, repl_2, ReplState.shutdown);


            // make sure our log does not continue from repl_2 logs
            repl_1.setValueParsed("c/1", "10");            
            repl_1.truncateLogs_Hack(); 

            
            raw_db.debugDump();

            Console.WriteLine("----------------[ reinit server 2 ]-----------------------------");

            repl_2 = db_factory.getReplicatedDatabase_Resume("guid2");

            waitUntilActive(raw_db, repl_2);

            Thread.Sleep(7000);
            
            raw_db.debugDump();

            Thread.Sleep(7000);            

            // Environment.Exit(1);  // exit
            repl_2.setValueParsed("d/1", "20");
            repl_1.setValueParsed("c/1", "10");
            Thread.Sleep(1000);

            repl_2.truncateLogs_Hack();

            Thread.Sleep(1000);

            repl_1.truncateLogs_Hack();

            Console.WriteLine("----------------[ both logs should be truncated ]-----------------------------");

            raw_db.debugDump();

            Console.WriteLine("----------------[ create server 3 ]-----------------------------");
           
            ReplHandler repl_3 = db_factory.getReplicatedDatabase_Join("guid3", "guid2");

            Thread.Sleep(7000);
            raw_db.debugDump();

            repl_3.setValueParsed("q/1", "10");
            Thread.Sleep(7000);
            raw_db.debugDump();


            Console.WriteLine("quitting..");
            Environment.Exit(0);
        }

    }



}
