// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;
using System.Reflection;

namespace Bend
{

   

    public enum InitMode
    {
        NEW_REGION,    // initialize the region fresh, error if it looks like there is a region still there
        RESUME         // standard resume/recover from an existing region
    }

    // .ROOT metadata
    // TODO: make a class to encapsulate this

    // RANGEs are represented with an implicit prefix '='. This allows use special endpoint markers:
    // "<" - the key before all keys
    // "=KEYDATA" - keys after and including "KEYDATA"
    // ">" - the key after all keys

    // Number representation - we will start with a simple positive integer zero-padded representation

    // .ROOT/VARS/NUMGENERATIONS -> 1
    // .ROOT/FREELIST/(address start:10)/(address end:10) -> ()
    // .ROOT/FREELIST/0000004000/0000008000 -> ()
    // .ROOT/GEN/(gen #:3)/(start key)/(end key) -> (segment metadata)
    // .ROOT/GEN/000/</> -> addr
    


    // ---------------[ LayerManager ]---------------------------------------------------------

    public class LayerManager : IDisposable
    {
        internal List<ISortedSegment> segmentlayers;
        internal SegmentBuilder workingSegment;
        internal String dir_path;   // should change this to not assume directories/files
        internal IRegionManager regionmgr;
        internal List<WeakReference<Txn>> pending_txns;

        LogWriter logwriter;
        Receiver receiver;

        public struct Receiver : ILogReceiver
        {
            LayerManager mylayer;
            public Receiver(LayerManager mylayer) {
                this.mylayer = mylayer;
            }
            public void handleCommand(byte cmd, byte[] cmddata) {
                if (cmd == 0x00) {
                    // decode basic block key/value writes
                    MemoryStream ms = new MemoryStream(cmddata);
                    ISegmentBlockDecoder decoder = new SegmentBlockBasicDecoder(ms, 0, cmddata.Length);
                    foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in decoder.sortedWalk()) {
                        // populate our working segment
                        mylayer.workingSegment.setRecord(kvp.Key, kvp.Value);
                    }
                } else {
                    throw new Exception("unimplemented command");
                }
            }
        }


        public class Txn
        {
            LayerManager mylayer;
            internal Txn(LayerManager _layer) {
                this.mylayer = _layer;
            }
            public void setValue(String skey, String svalue) {
                RecordKey key = new RecordKey();
                key.appendKeyPart(skey);

                RecordUpdate update = new RecordUpdate(RecordUpdateTypes.FULL, svalue);
                mylayer.workingSegment.setRecord(key, update);
                

                // build a byte[] for the updates using the basic block encoder
                {
                    MemoryStream writer = new MemoryStream();
                    ISegmentBlockEncoder encoder = new SegmentBlockBasicEncoder();
                    encoder.setStream(writer);
                    encoder.add(key,update);
                    encoder.flush();
                    mylayer.logwriter.addCommand(0, writer.ToArray());
                    mylayer.logwriter.flushPendingCommands();
                }
            }
            public void commit() {
                // TODO: commit should be what finalizes pending writes into final writes
                //     and cleans up locks and state
            }
        }

        public LayerManager() {
            pending_txns = new List<WeakReference<Txn>>();

            segmentlayers = new List<ISortedSegment>();   // a list of segment layers, newest to oldest
            workingSegment = new SegmentBuilder();
            segmentlayers.Add(workingSegment);

        }

        public LayerManager(InitMode mode, String dir_path) : this()
        {
            this.dir_path = dir_path;
            

            if (mode == InitMode.NEW_REGION) {
                // right now we only have one region type
                regionmgr = new RegionExposedFiles(InitMode.NEW_REGION, dir_path);

                // first write a log init record into the log
                logwriter = new LogWriter(InitMode.NEW_REGION, regionmgr);

            } else if (mode == InitMode.RESUME) {
                regionmgr = new RegionExposedFiles(InitMode.RESUME, dir_path);
                receiver = new Receiver(this);
                logwriter = new LogWriter(InitMode.RESUME, regionmgr, receiver);
            } else {
                throw new Exception("unknown init mode");
            }

            
        }

        public Txn newTxn() {
            Txn newtx = new Txn(this);
            pending_txns.Add(new WeakReference<Txn>(newtx));  // make sure we don't prevent collection
            return newtx;
        }

        public void flushWorkingSegment()
        {
            // create a new working segment
            SegmentBuilder checkpointSegment = workingSegment;
            workingSegment = new SegmentBuilder();
            segmentlayers.Add(workingSegment);

            // TODO: allocate new segment address from freespace
            // write the current segment and flush
            FileStream writer = File.Create(dir_path + "\\curseg.sg");
            checkpointSegment.writeToStream(writer);
            writer.Close();
            
            // reopen that segment, and use it to replace the checkpoint segment
            // TODO: make this atomic
            FileStream reader = File.Open(dir_path + "\\curseg.sg", FileMode.Open);
            SegmentReader sr = new SegmentReader(reader);
            segmentlayers.Add(sr);
            segmentlayers.Remove(checkpointSegment);
        }

        public GetStatus getRecord(RecordKey key)
        {
            // we need to go through layers from newest to oldest. If we find a full record
            // or a deletion tombstone we can stop, otherwise we need to merge the
            // partial recordupdates we find.

            RecordData record = new RecordData(RecordDataState.NOT_PROVIDED, key);
            GetStatus cur_status = GetStatus.MISSING;

            foreach (ISortedSegment layer in segmentlayers)
            {
                RecordUpdate update;
                if (layer.getRecordUpdate(key, out update) == GetStatus.PRESENT)
                {
                    cur_status = GetStatus.PRESENT;
                    if (record.applyUpdate(update) == RecordUpdateResult.FINAL)
                    {
                        break; // we received a final update
                    }
                }
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


        public void setValue(String skey, String svalue)
        {
            Txn implicit_txn = this.newTxn();
            implicit_txn.setValue(skey, svalue);
            implicit_txn.commit();
        }

        public void Dispose() {
            if (logwriter != null) {
                logwriter.Dispose(); logwriter = null;
            }
        }
    }




}