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
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            var window = new DbgGUI();
            window.SetDesktopLocation(700, 200);

            Thread newThread = new Thread(_Main.do_test);            
            newThread.Start();
            Application.Run(window);
        }


        static void do_test() {
            Console.WriteLine("ReplTest1 startup...");
            LayerManager db = new LayerManager(InitMode.NEW_REGION,@"C:\BENDtest\repl");
            Random rnd = new Random();

            ReplHandler repl_1 = new ReplHandler(db, "server_1", "guid"+rnd.Next());
            ReplHandler repl_2 = new ReplHandler(db, "server_2", "guid" + rnd.Next());

            repl_1.addServer(repl_2);
            repl_2.addServer(repl_1);

            repl_1.setValueParsed("/a/1", "1");
            repl_1.setValueParsed("/b/2", "2");
            repl_1.setValueParsed("/a/1", "3");

            Console.WriteLine("debug dump DB");
            db.debugDump();

            repl_2.setValueParsed("/a/2", "5");

            Console.WriteLine("debug dump DB");
            db.debugDump();


            ReplHandler repl_3 = new ReplHandler(db, "server_3", "guid"+rnd.Next());
            
            repl_3.addServer(repl_1);
            repl_3.addServer(repl_2);
            repl_1.addServer(repl_3);
            repl_2.addServer(repl_3);

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
