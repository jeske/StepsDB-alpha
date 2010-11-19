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
            Size regionsize = this.ClientSize;

            // compute the data I need first...

            var segments_by_generation = new Dictionary<uint,List<SegmentDescriptor>>();
            var unique_keys = new Dictionary<RecordKey, int>();

            foreach (SegmentDescriptor segdesc in segments) {
                unique_keys[segdesc.start_key] = 1;
                unique_keys[segdesc.end_key] = 1;
                try {
                    segments_by_generation[segdesc.generation].Add(segdesc);
                } catch (KeyNotFoundException) {
                    var listofsegs = new List<SegmentDescriptor>();
                    listofsegs.Add(segdesc);
                    segments_by_generation[segdesc.generation] = listofsegs;
                }
            }

            // assign y-locations to keys
            int y_loc = 0;
            int segment_height = regionsize.Height / unique_keys.Count;

            var key_to_position_map = new Dictionary<RecordKey, int>();
            foreach (var key in unique_keys.Keys) {
                key_to_position_map[key] = y_loc;
                y_loc += segment_height;
            }

            // now draw stuff! 
            Graphics dc = this.CreateGraphics();
            Pen BluePen = new Pen(Color.Blue, 1);                        

            dc.Clear(Color.White);

            int cur_x = 10, cur_y = 0;
            foreach (uint generation in segments_by_generation.Keys) {
                foreach (var seg in segments_by_generation[generation]) {
                    int y_top = key_to_position_map[seg.start_key];
                    int y_bottom = key_to_position_map[seg.end_key];

                    dc.DrawRectangle(BluePen, cur_x, y_top, 50, y_bottom - y_top);
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