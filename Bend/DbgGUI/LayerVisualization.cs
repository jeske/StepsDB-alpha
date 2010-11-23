
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
            var seg = new List<SegmentDescriptor>();
            // this is much faster than using listAllSegments
            foreach(var kvp in db.rangemapmgr.mergeManager.segmentInfo) {
                seg.Add(kvp.Key);
            }

            segments = seg;


            // we should be doing this, but .Keys is not implemented in BDSkipList
            // segments.AddRange(db.rangemapmgr.mergeManager.segmentInfo.Keys);
            // segments.AddRange(db.listAllSegments());
            this.Invoke((MethodInvoker) delegate() {
                try {
                    this.Refresh();
                } catch (Exception e) {
                    System.Console.WriteLine("######" + e.ToString());
                    throw e;
                }
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
            uint max_gen = 0;

            foreach (SegmentDescriptor segdesc in segments) {
                unique_keys[segdesc.start_key] = 1;
                unique_keys[segdesc.end_key] = 1;
                try {
                    segments_by_generation[segdesc.generation].Add(segdesc);
                } catch (KeyNotFoundException) {
                    var listofsegs = new List<SegmentDescriptor>();
                    listofsegs.Add(segdesc);
                    segments_by_generation[segdesc.generation] = listofsegs;
                    max_gen = segdesc.generation > max_gen ? segdesc.generation : max_gen;
                }
            }

            // now draw stuff!             
            Pen BluePen = new Pen(Color.Blue, 1);
            Pen GrayPen = new Pen(Color.LightGray, 1);
            Pen GenPen = new Pen(Color.LightGreen, 1);


            Color[] ColorArray = {
                    Color.LightCoral, Color.LightCyan, Color.LightGoldenrodYellow, Color.LightGray, Color.LightGreen, 
                    Color.LightPink, Color.LightSalmon, Color.LightSeaGreen, Color.LightSkyBlue, Color.LightSlateGray, 
                    Color.LightSteelBlue, Color.LightYellow};
            
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


            int cur_x = 10;
            for (uint generation = 0; generation < max_gen; generation++ ) {
                bool emptygen = !segments_by_generation.ContainsKey(generation);
                // generation vertical lines
                
                dc.DrawLine(GenPen, cur_x-10, 0, cur_x-10, regionsize.Height);                

                if (emptygen) {
                    cur_x += 10;
                    continue;
                }

                foreach (var seg in segments_by_generation[generation]) {
                    int y_top = key_to_position_map[seg.start_key];
                    int y_bottom = key_to_position_map[seg.end_key];
                    int mid_y = (y_top + y_bottom) / 2;
                    int y_mid_top = mid_y - segment_height / 2;

                    
                    // color the inside of the box.
                    var h1 = seg.start_key.ToString().GetHashCode();
                    var h2 = seg.end_key.ToString().GetHashCode();
                    var offset = Math.Abs(h1 + h2) % (ColorArray.Length);
                    var fill = new SolidBrush(ColorArray[offset]);
                    dc.FillRectangle(fill, cur_x, y_mid_top, 50, segment_height);
                    dc.DrawRectangle(BluePen, cur_x, y_mid_top, 50, segment_height);

                    // dc.DrawRectangle(BluePen, cur_x, y_top, 50, segment_height);
                    if (generation != 0 && (y_bottom != y_top + segment_height)) {
                        dc.DrawLine(GrayPen, cur_x, y_mid_top, cur_x - 10, y_top);
                        dc.DrawLine(GrayPen, cur_x, y_mid_top + segment_height, cur_x - 10, y_bottom);

                        //   dc.DrawLine(BluePen, cur_x, y_top + segment_height, cur_x - 10, y_bottom);
                        //   dc.DrawLine(BluePen, cur_x, y_top, cur_x - 10, y_top);
                    }
                }

                // reset for next time through the loop
                cur_x += 70;
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