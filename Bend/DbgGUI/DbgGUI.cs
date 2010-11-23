using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Windows.Forms;
using System.Threading;

namespace Bend {

    public partial class DbgGUI : Form {
        
        private LayerVisualization layerVisualization1;

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

        public void debugDump(LayerManager db, MergeCandidate mc = null) {
            this.layerVisualization1.refreshFromDb(db, mc);
        }
    
       private void InitializeComponent() {
            this.layerVisualization1 = new Bend.LayerVisualization();
            this.SuspendLayout();
            // 
            // layerVisualization1
            // 
            this.layerVisualization1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
                        | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.layerVisualization1.AutoSizeMode = System.Windows.Forms.AutoSizeMode.GrowAndShrink;
            this.layerVisualization1.Location = new System.Drawing.Point(12, 12);
            this.layerVisualization1.Name = "layerVisualization1";
            this.layerVisualization1.Size = new System.Drawing.Size(579, 505);
            this.layerVisualization1.TabIndex = 0;
            // 
            // DbgGUI
            // 
            this.ClientSize = new System.Drawing.Size(603, 529);
            this.Controls.Add(this.layerVisualization1);
            this.Name = "DbgGUI";
            this.ResumeLayout(false);

        }

    } // end DbgGUI Form
}