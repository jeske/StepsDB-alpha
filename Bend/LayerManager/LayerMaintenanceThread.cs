using System;
using System.Collections.Generic;
using System.Text;

using System.Threading;

namespace Bend {
    public class LayerMaintenanceThread {
        WeakReference<LayerManager> db_wr;    // we use a weakref so the db can still be collected while the thread is active 
        bool keepRunning = true;
        public static LayerMaintenanceThread startMaintThread(LayerManager db) {
            LayerMaintenanceThread worker = new LayerMaintenanceThread(db);
            Thread workerThread = new Thread(worker.doWork);
            workerThread.Name = "LayerMaintenanceThread";
            workerThread.Start();
            return worker;
        }
        private LayerMaintenanceThread(LayerManager db) {
            this.db_wr = new WeakReference<LayerManager>(db);
        }

        private void doWork() {
            bool didMerge;
            bool needFlush;
            LayerManager db;
            while (keepRunning) {
                didMerge = false;
                needFlush = false;
                // deref the weak reference
                try {
                    db = this.db_wr.Target;
                } catch (InvalidOperationException) {
                    keepRunning = false;
                    return;
                }
                try {

                    // (1) check if the log is running out of space...
                    if (db.needCheckpointNow) {
                        needFlush = true;
                        db.needCheckpointNow = false; // reset the trigger flag
                    } else {
                        // (2) check the working segment size vs threshold, flush if necessary             
                        lock (db.segmentlayers) {
                            // TODO: this currently does not account for compression, which could make it off by orders of magnitude
                            if (db.workingSegmentSize() > db.SEGMENT_BLOCKSIZE * 15) {
                                needFlush = true;
                            }
                        }
                    }
                    if (needFlush) {
                        System.Console.WriteLine("************************ LayerMaintenanceThread did FLUSH");
                        db.flushWorkingSegment();
                        db.needCheckpointNow = false; // reset the trigger flag
                    }
                    // (3) check for merge
                    didMerge = db.mergeIfNeeded();
                    if (didMerge) {
                        System.Console.WriteLine("************************ LayerMaintenanceThread did MERGE");
                    }
                } catch (Exception e) {
                    System.Console.WriteLine("LayerMaintenanceThread Exception\n" + e.ToString());
                    db = null;
                    Thread.Sleep(100);
                }
                db = null;

                Thread.Sleep(didMerge ? 10 : 2000);
            }
        }
        public void end() {
            keepRunning = false;
            Thread.Sleep(5);
        }
    }
}
