// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;
using System.Reflection;

// TODO: eliminate the dual paths for "manual apply" and "log apply"... make it always just do log apply

namespace Bend
{

    public enum InitMode
    {
        NEW_REGION,    // initialize the region fresh, error if it looks like there is a region still there
        RESUME         // standard resume/recover from an existing region
    }

    // ---------------[ LayerManager ]---------------------------------------------------------

    public class LayerManager : IDisposable
    {
        internal List<SegmentBuilder> segmentlayers;  // newest to oldest list of the in-memory segments
        internal SegmentBuilder workingSegment;
        internal String dir_path;   // should change this to not assume directories/files
        public  IRegionManager regionmgr;
        internal List<WeakReference<Txn>> pending_txns;
        RangemapManager rangemapmgr;
        FreespaceManager freespacemgr;

        LogWriter logwriter;
        Receiver receiver;

        // constructors ....

        public LayerManager() {
            pending_txns = new List<WeakReference<Txn>>();

            segmentlayers = new List<SegmentBuilder>();   // a list of segment layers, newest to oldest
            workingSegment = new SegmentBuilder();
            segmentlayers.Add(workingSegment);
            
        }

        public LayerManager(InitMode mode, String dir_path)
            : this() {
            this.dir_path = dir_path;


            if (mode == InitMode.NEW_REGION) {
                // right now we only have one region type
                regionmgr = new RegionExposedFiles(InitMode.NEW_REGION, dir_path);

                // get our log online...
                logwriter = new LogWriter(InitMode.NEW_REGION, regionmgr);

                // setup the initial numgenerations record
                RangemapManager.Init(this);
                // TODO: do something sane with initial freespace setup
            } else if (mode == InitMode.RESUME) {
                regionmgr = new RegionExposedFiles(dir_path);
                receiver = new Receiver(this);
                logwriter = new LogWriter(InitMode.RESUME, regionmgr, receiver);
            } else {
                throw new Exception("unknown init mode");
            }

            // once the rangemap is initialized we can actually READ data!!
            rangemapmgr = new RangemapManager(this);   
            freespacemgr = new FreespaceManager(this);
        }

        // inner classes ...
        public struct Receiver : ILogReceiver
        {
            LayerManager mylayer;
            SegmentBuilder checkpointSegment;
            public Receiver(LayerManager mylayer) {
                this.mylayer = mylayer;
                checkpointSegment = null;
            }
            public void handleCommand(byte cmd, byte[] cmddata) {
                if (cmd == (byte)LogCommands.UPDATE) {
                    // decode basic block key/value writes
                    MemoryStream ms = new MemoryStream(cmddata);
                    ISegmentBlockDecoder decoder = new SegmentBlockBasicDecoder(ms, 0, cmddata.Length);
                    foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in decoder.sortedWalk()) {
                        // populate our working segment
                        mylayer.workingSegment.setRecord(kvp.Key, kvp.Value);
                    }
                } else if (cmd == (byte)LogCommands.CHECKPOINT) {
                    // TODO: we need some kind of key/checksum to be sure that we CHECKPOINT and DROP the right data
                    checkpointSegment = mylayer.workingSegment;
                    mylayer.workingSegment = new SegmentBuilder();
                    mylayer.segmentlayers.Insert(0, mylayer.workingSegment);
                } else if (cmd == (byte)LogCommands.CHECKPOINT_DROP) {
                    // TODO: we need some kind of key/checksum to be sure that we CHECKPOINT and DROP the right data
                    if (checkpointSegment != null) {
                        mylayer.segmentlayers.Remove(checkpointSegment);
                    } else {
                        throw new Exception("can't drop, no segment to drop");
                    }
                } else {
                    throw new Exception("unimplemented command");
                }
            }
        }

        enum LogCommands
        {
            UPDATE = 0,
            CHECKPOINT = 1,
            CHECKPOINT_DROP = 2
        }

        public class Txn : IDisposable
        {
            LayerManager mylayer;
            long tsn; // transaction sequence number
            enum TxnState
            {
                PENDING,
                PREPARED,
                COMMITTED,
                ABORTED
            }
            TxnState state = TxnState.PENDING;
            internal Txn(LayerManager _layer) {
                this.mylayer = _layer;
                this.tsn = System.DateTime.Now.ToBinary();
                // TODO: store the stack backtrace of who created this if we're in debug mode
            }

            public void setValue(RecordKey key, RecordUpdate update) {
                // build a byte[] for the updates using the basic block encoder
                {
                    MemoryStream writer = new MemoryStream();
                    ISegmentBlockEncoder encoder = new SegmentBlockBasicEncoder();
                    encoder.setStream(writer);
                    encoder.add(key, update);
                    encoder.flush();
                    mylayer.logwriter.addCommand((byte)LogCommands.UPDATE, writer.ToArray());

                }
                mylayer.workingSegment.setRecord(key, update); // add to working set
            }

            public void setValueParsed(String skey, String svalue) {
                RecordKey key = new RecordKey();
                key.appendParsedKey(skey);
                RecordUpdate update = new RecordUpdate(RecordUpdateTypes.FULL, svalue);

                this.setValue(key, update);
            }
            public void addCommand(byte cmd, byte[] cmddata) {
                mylayer.logwriter.addCommand(cmd, cmddata);
            }

            public void commit() {
                // TODO: commit should be what finalizes pending writes into final writes
                //     and cleans up locks and state

                // TODO: this flush concept is slighly broken. It assumes we are the only
                //   thread adding anything to the logwriter. We should probably hand
                //   logwriter this entire object and ask it to flush, so it could be holding
                //   multiple valid TXs. Currently if someone else is in the middle of
                //   adding elements to the log, some of their stuff will be flushed with this
                //   flush, while others will go in the next flush -- BAD.
                mylayer.logwriter.flushPendingCommands();

                state = TxnState.COMMITTED;
            }
            public void abort() {
                state = TxnState.ABORTED;
            }
            public void Dispose() {
                if (state == TxnState.PENDING) {
                    throw new Exception("disposed Txn still pending " + this.tsn);
                }
            }
        }

        // impl ....

        public Txn newTxn() {
            Txn newtx = new Txn(this);
            pending_txns.Add(new WeakReference<Txn>(newtx));  // make sure we don't prevent collection
            return newtx;
        }

        public void flushWorkingSegment() {
            // create a new working segment
            SegmentBuilder checkpointSegment = workingSegment;
            workingSegment = new SegmentBuilder();
            segmentlayers.Insert(0, workingSegment);
            {
                byte[] emptydata = new byte[0];
                this.logwriter.addCommand((byte)LogCommands.CHECKPOINT, emptydata);
            }

            IRegion reader;
            {
                Txn tx = new Txn(this);
                // allocate new segment address from freespace
                IRegion writer = freespacemgr.allocateNewSegment(tx, -1);

                // write the checkpoint segment and flush
                checkpointSegment.writeToStream(writer.getStream());
                writer.getStream().Close();

                // reopen that segment for reading
                // TODO: figure out how much data we wrote exactly, adjust the freespace, and 
                //       'truncate' the segment, then pass that length to the reader instantiation
                reader = regionmgr.readRegionAddr((uint)writer.getStartAddress());


                rangemapmgr.newGeneration(tx, reader);   // add the checkpoint segment to the rangemap
                {
                    byte[] emptydata = new byte[0];
                    tx.addCommand((byte)LogCommands.CHECKPOINT_DROP, emptydata);
                }
                tx.commit();                             // commit the freespace and rangemap transaction

            }

            
            // TODO: make this atomic            
            
            // FIXME: don't do this anymore, because we are now rangemap walking!!
              // re-read the segment and use it to replace the checkpoint segment            
              // SegmentReader sr = new SegmentReader(reader.getStream());
              // segmentlayers.Insert(1, sr); // working segment is zero, so insert this after it
            reader.getStream().Close(); // force close the reader

            segmentlayers.Remove(checkpointSegment);

        }

        public GetStatus getRecord(RecordKey key, out RecordData record) {
            record = new RecordData(RecordDataState.NOT_PROVIDED, key);

            if (rangemapmgr.segmentWalkForKey(key, workingSegment, ref record) == RecordUpdateResult.FINAL) {
                return GetStatus.PRESENT;
            } else {
                return GetStatus.MISSING;
            }
        }

        public GetStatus getRecordOLD(RecordKey key, out RecordData record)
        {
            RecordUpdate update;
            // we need to go through layers from newest to oldest. If we find a full record
            // or a deletion tombstone we can stop, otherwise we need to merge the
            // partial recordupdates we find.

            record = new RecordData(RecordDataState.NOT_PROVIDED, key);
            GetStatus cur_status = GetStatus.MISSING;

            // start with a quick check of working segment for the key
            // TODO: check all in-memory segments
            if (workingSegment.getRecordUpdate(key, out update) == GetStatus.PRESENT) {
                cur_status = GetStatus.PRESENT;
                if (record.applyUpdate(update) == RecordUpdateResult.FINAL) {
                    return cur_status;
                }
            }
            // if we're still here, we need to do a generation scan, start with the rangemap lookups
            
            if (workingSegment.getRecordUpdate(new RecordKey().appendParsedKey(".ROOT/VARS/NUMGENERATIONS"), 
                out update) == GetStatus.MISSING) {
                throw new Exception("missing NUMGENERATIONS record");
            }
            int numgen = (int)Lsd.lsdToNumber(update.data);

            // find all (relevant) occurances of our record in decreasing generation order
            //   - in order to do this, we first need to find the specific segments which
            //     contain our records. This means finding the following records. 
            //          .ROOT/GEN/<maxgen>/<key> -> metadata
            //          .ROOT/GEN/001/<key>
            //          .ROOT/GEN/000/<key>
            //     These records form a "tree" of pointers from newest to oldest segments.
            //     ( see FindSegments() ) 


            while (numgen-- > 0) {

                ISortedSegment layer = this.rangemapmgr.getSegmentForKey(key, numgen);
                if (layer != null && layer.getRecordUpdate(key, out update) == GetStatus.PRESENT)
                {
                    cur_status = GetStatus.PRESENT;
                    if (record.applyUpdate(update) == RecordUpdateResult.FINAL)
                    {
                        return cur_status; // we received a final update
                    }
                }
                layer.Dispose();
            }
            return cur_status;
        }

        public void debugDump()
        {
            foreach (ISortedSegment layer in segmentlayers)
            {
                Console.WriteLine("--- Layer");
                foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in layer.sortedWalk())
                {
                    Console.WriteLine("  " + kvp.Key + " : " + kvp.Value);
                }
            }

        }


        public void setValueParsed(String skey, String svalue)
        {
            Txn implicit_txn = this.newTxn();
            implicit_txn.setValueParsed(skey, svalue);
            implicit_txn.commit();            
        }

        public void setValue(RecordKey key, RecordUpdate value) {
            Txn implicit_txn = this.newTxn();
            implicit_txn.setValue(key, value);
            implicit_txn.commit();            
        }

        public void Dispose() {
            if (logwriter != null) {
                logwriter.Dispose(); logwriter = null;
            }

            foreach (ISortedSegment segment in segmentlayers) {
                segment.Dispose();
            }
        }
    }




}