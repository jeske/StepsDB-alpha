using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Windows.Forms;
using System.Threading;

namespace Bend {

    public partial class DbgGUI : Form {
    
        public DbgGUI() {
            InitializeComponent();

            this.Size = new System.Drawing.Size(600, 600);

            this.Location = new Point(700, 700);
            this.Show();

            Thread newThread = new Thread(this.RunStuff);
            newThread.Start();
        }

        public void RunStuff() {
                        
            try {
                MainBend.do_bringup_test(this);
            } catch (Exception exc) {
                System.Console.WriteLine("died to exception: " + exc.ToString());
                Console.WriteLine("press any key...");

            }
            //            Console.ReadKey();


        }

        public void debugDump(LayerManager db) {
            var segments = db.listAllSegments();

            // compute the data I need first...

            var segments_by_generation = new Dictionary<uint,List<SegmentDescriptor>>();

            foreach (SegmentDescriptor segdesc in segments) {
                try {
                    segments_by_generation[segdesc.generation].Add(segdesc);
                } catch (KeyNotFoundException) {
                    var listofsegs = new List<SegmentDescriptor>();
                    listofsegs.Add(segdesc);
                    segments_by_generation[segdesc.generation] = listofsegs;
                }
            }


            // now draw stuff! 
            Graphics dc = this.CreateGraphics();
            Pen BluePen = new Pen(Color.Blue, 1);            
            Size regionsize = this.ClientSize;

            dc.Clear(Color.White);

            int cur_x = 10, cur_y = 0;
            foreach (uint generation in segments_by_generation.Keys) {

                // draw the generation rect
                int box_height = regionsize.Height - 20;
                dc.DrawRectangle(BluePen, cur_x, 10, 50, box_height);

                var listofsegs = segments_by_generation[generation];
                int count = listofsegs.Count;

                // draw the segment boundaries
                int seg_height = box_height / count;
                for (int x = 0; x < count; x++) {
                    dc.DrawLine(BluePen, cur_x, 10 + seg_height * x, cur_x + 50, 10 + seg_height * x);
                }

                // reset for next time through the loop
                cur_x = cur_x + 60;
            }
                        
            

        }

        private void InitializeComponent() {
            this.SuspendLayout();
            // 
            // DbgGUI
            // 
            this.ClientSize = new System.Drawing.Size(284, 262);
            this.Name = "DbgGUI";
            this.ResumeLayout(false);

        }

    } // end DbgGUI Form
}