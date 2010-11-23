
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Windows.Forms;
using System.Threading;

namespace Bend {

    // http://social.msdn.microsoft.com/Forums/en-US/csharpide/thread/64c77755-b0c1-4447-8ac9-b5a63a681b78

    [System.ComponentModel.DesignerCategory("code")]
    public class LayerVisualization : UserControl {        
        private List<SegmentDescriptor> segments = null;

        public void refreshFromDb(LayerManager db) {            
            segments = new List<SegmentDescriptor>();
            // this is much faster than using listAllSegments
            foreach(var kvp in db.rangemapmgr.mergeManager.segmentInfo) {
                segments.Add(kvp.Key);
            }
            
            // we should be doing this, but .Keys is not implemented in BDSkipList
            // segments.AddRange(db.rangemapmgr.mergeManager.segmentInfo.Keys);
            // segments.AddRange(db.listAllSegments());
            this.Invoke((MethodInvoker) delegate() {
                this.Refresh(); 
                });
        }


        protected override void OnResize(EventArgs ev) {
            this.Refresh();
        }
        protected override void OnPaint(PaintEventArgs e) {
            Graphics dc = e.Graphics;


            // how to tell we are in design mode
            // http://msdn.microsoft.com/en-us/magazine/cc164048.aspx
            if (this.Site != null && this.Site.DesignMode) {
                dc.Clear(Color.Beige);
                return;
            }

            if (segments == null) {                
                return;
            }            
            Size regionsize = this.ClientSize;

            // compute the data I need first...

            var segments_by_generation = new Dictionary<uint, List<SegmentDescriptor>>();
            var unique_keys = new BDSkipList<RecordKey, int>();

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

            // now draw stuff!             
            Pen BluePen = new Pen(Color.Blue, 1);
            Pen GrayPen = new Pen(Color.Gray, 1);

            dc.Clear(Color.White);


            // assign y-locations to keys
            if (unique_keys.Count == 0) {
                return;
            }

            int y_loc = 0;
            int segment_height = regionsize.Height / unique_keys.Count;

            var key_to_position_map = new Dictionary<RecordKey, int>();
            foreach (var key in unique_keys.Keys) {
                key_to_position_map[key] = y_loc;
                y_loc += segment_height;
            }




            int cur_x = 10, cur_y = 0;
            foreach (uint generation in segments_by_generation.Keys) {
                foreach (var seg in segments_by_generation[generation]) {
                    int y_top = key_to_position_map[seg.start_key];
                    int y_bottom = key_to_position_map[seg.end_key];
                    int mid_y = (y_top + y_bottom) / 2;
                    int y_mid_top = mid_y - segment_height / 2;

                    dc.DrawRectangle(BluePen, cur_x, y_mid_top, 50, segment_height);
                   // dc.DrawRectangle(BluePen, cur_x, y_top, 50, segment_height);
                    if (generation != 0 && (y_bottom != y_top+segment_height)) {
                        dc.DrawLine(GrayPen, cur_x, y_mid_top, cur_x - 10, y_top);
                        dc.DrawLine(GrayPen, cur_x, y_mid_top + segment_height, cur_x - 10, y_bottom);

                     //   dc.DrawLine(BluePen, cur_x, y_top + segment_height, cur_x - 10, y_bottom);
                     //   dc.DrawLine(BluePen, cur_x, y_top, cur_x - 10, y_top);
                    }
                }

                // reset for next time through the loop
                cur_x = cur_x + 60;
            }
                      
            

        }

        private void InitializeComponent() {
            this.SuspendLayout();
            // 
            // LayerVisualization
            // 
            this.Name = "LayerVisualization";
            this.Size = new System.Drawing.Size(492, 463);
            this.ResumeLayout(false);

        }


        public LayerVisualization() {
        }
    }

}