﻿
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
        private MergeCandidate lastmerge = null;

        static int BLOCK_WIDTH = 50;

        public void refreshFromDb(LayerManager db, MergeCandidate mc = null) {            
            var seg = new List<SegmentDescriptor>();
            // this is much faster than using listAllSegments
            foreach(var kvp in db.rangemapmgr.mergeManager.segmentInfo) {
                seg.Add(kvp.Key);
            }

            segments = seg;
            this.lastmerge = mc;

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


            // figure out what the "maximum number of segments in a column" is, so we can compute segment height            
            float segment_height = 50;
            if (unique_keys.Count > 0) {
                segment_height = Math.Max(1, (float)regionsize.Height / (float)unique_keys.Count); // make sure segment heights are at least 1
            }


            // now draw stuff!             
            Pen BluePen = new Pen(Color.Blue, 1);
            Pen RangeLinePen = new Pen(Color.LightGray, 1);
            Pen GenPen = new Pen(Color.LightGreen, 1);


            dc.Clear(Color.White);


            // assign y-locations to keys
            if (unique_keys.Count == 0) {
                return;
            }

            float y_loc = 0;


            var key_to_position_map = new Dictionary<RecordKey, float>();
            foreach (var key in unique_keys.Keys) {
                key_to_position_map[key] = y_loc;
                y_loc += segment_height;
            }

            var segs_in_merge = new HashSet<string>();
            if (lastmerge != null) {
                foreach (var seg in lastmerge.source_segs) {
                    segs_in_merge.Add(seg.ToString());
                }
                foreach (var seg in lastmerge.target_segs) {
                    segs_in_merge.Add(seg.ToString());
                }
            }

            int cur_x = 20;
            for (uint generation = 0; generation <= max_gen; generation++) {

                // generation vertical lines                
                dc.DrawLine(GenPen, cur_x - 10, 0, cur_x - 10, regionsize.Height);

                if (!segments_by_generation.ContainsKey(generation)) {
                    // generation is empty!
                    cur_x += 10;
                    continue;
                }

                foreach (var seg in segments_by_generation[generation]) {
                    int y_top = (int)key_to_position_map[seg.start_key];
                    int y_bottom =(int)key_to_position_map[seg.end_key];
                    int mid_y = (y_top + y_bottom) / 2;
                    int y_mid_top = (int)(mid_y - (float)segment_height / 2);

                    


                    // color the inside of the box. 
                    if (lastmerge != null && segs_in_merge.Contains(seg.ToString())) {
                        // if it's currently merging, color it blue
                        var fill = new SolidBrush(Color.Red);
                        dc.FillRectangle(fill, cur_x, y_mid_top, BLOCK_WIDTH, segment_height);
                    } else {
                        // pseudorandom color based on a hash of the start/end keys of a block
                        var h1 = Math.Abs(seg.start_key.ToString().GetHashCode());
                        var h2 = Math.Abs(seg.end_key.ToString().GetHashCode());

                        // vary the alpha and colors based on key hashes %N+K makes sure alpha is in a good range
                        var fill = new SolidBrush(Color.FromArgb((h1 % 60) + 80, (h1 + h2) & 0xff, (h2 >> 8) & 0xff, h2 & 0xff));
                        dc.FillRectangle(fill, cur_x, y_mid_top, BLOCK_WIDTH, segment_height);
                    }

                    if (seg.keyrangeContainsSegmentPointers()) {                        
                        dc.FillRectangle(new SolidBrush(Color.Black), cur_x+BLOCK_WIDTH-5, y_mid_top, 5, segment_height);
                    }


                    // when we get a lot of blocks, the outline covers 100% of the inside color
                    //dc.DrawRectangle(BluePen, cur_x, y_mid_top, 50, segment_height);

                    if (generation != 0 && (y_bottom != y_top + segment_height)) {
                        dc.DrawLine(RangeLinePen, cur_x, y_mid_top, cur_x - 10, y_top);
                        dc.DrawLine(RangeLinePen, cur_x, y_mid_top + segment_height, cur_x - 10, y_bottom);
                    }
                }

                // reset for next time through the loop
                cur_x += 20 + BLOCK_WIDTH;
            }



            // paint some other stuff
            {
                int y_pos = 0 + 20;
                long memsize = GC.GetTotalMemory(false);
                
                Brush bb = new SolidBrush(Color.Black);
                String msg = String.Format("Memsize: {0:0.00} MB", ((double)memsize / 1024 / 1024));
                SizeF msgsz = dc.MeasureString(msg, this.Font);
                dc.DrawString(msg, this.Font, bb, 
                    new Point(regionsize.Width - (int)msgsz.Width, y_pos + (int)msgsz.Height));
                y_pos += (int)msgsz.Height + 10;
                int max_gc_generation = GC.MaxGeneration;
                msg = "GC generations: " + (max_gc_generation + 1);
                msgsz = dc.MeasureString(msg, this.Font);
                dc.DrawString(msg, this.Font, bb, new Point(regionsize.Width - (int)msgsz.Width, y_pos + (int)msgsz.Height));
                y_pos += (int)msgsz.Height + 10;
                for (int x = 0; x <= max_gc_generation; x++) {
                    int collection_count = GC.CollectionCount(x);
                    msg = String.Format("[{0}] = [{1}]",x,collection_count);
                    msgsz = dc.MeasureString(msg, this.Font);
                    dc.DrawString(msg, this.Font, bb, new Point(regionsize.Width - (int)msgsz.Width, y_pos + (int)msgsz.Height));
                    y_pos += (int)msgsz.Height + 10;

                }

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