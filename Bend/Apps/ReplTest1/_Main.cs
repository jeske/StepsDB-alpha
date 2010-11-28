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
            ReplHandler repl = new ReplHandler(db);


            repl.setValueParsed("/a", "1");
            repl.setValueParsed("/b", "2");
            repl.setValueParsed("/a", "3");
            Console.WriteLine("debug dump DB");
            db.debugDump();



            Console.WriteLine("waiting for key");
            Console.ReadKey();
            Console.WriteLine("quitting..");
            Environment.Exit(0);
        }

    }



}
