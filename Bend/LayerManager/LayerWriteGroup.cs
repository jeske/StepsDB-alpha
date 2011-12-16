using System;
using System.Collections.Generic;

using System.Text;
using System.IO;

using System.ComponentModel;

namespace Bend {

    public class LayerWriteGroup : IDisposable {
        public LayerManager mylayer;
        public readonly long tsn; // transaction sequence number
        long last_logwaitnumber = 0; // log-sequence-number from our most recent addCommand

        public delegate void handleWriteGroupCompletion();

        List<LogCmd> pending_cmds = new List<LogCmd>();
        List<handleWriteGroupCompletion> pending_completions = new List<handleWriteGroupCompletion>();

        public const WriteGroupType DEFAULT_WG_TYPE = WriteGroupType.DISK_INCREMENTAL;

        public enum WriteGroupType {
            [Description("Changes are immediately added to the pending-log-queue and working-segment. They will opportunistically reach the log")]
            DISK_INCREMENTAL,

            [Description("Changes are immediately added only to the working-segment. They will only survive if a checkpoint occurs before shutdown.")]
            MEMORY_ONLY,

            [Description("Changes accumulate in a pending atomic-log-packet. They will appear in the working segment and log only after a .finish(). In addition, .finish() will only return once they are flushed to the log")]
            DISK_ATOMIC_FLUSH,

            [Description("Changes accumulate in a pending atomic-log-packet. They will appear in the working segment and log only after a .finish().")]
            DISK_ATOMIC_NOFLUSH,
        };

        // DISK_INCREMENTAL : changes are immediately added to the pending-log-queue and the working segment.
        //                    The log is written to disk optimistically, so individual changes may be written to the 
        //                    log separately. In the case of a crash, some may survive while others are not logged. 
        // MEMORY_ONLY : changes are only added to the working segment. They will disappear if the system 
        //               crashes before a checkpoint. However, this avoids double-writing them to the log.
        // DISK_ATOMIC_FLUSH : Changes are buffered and not applied to the working segment until the writegroup is 
        //               flushed. The changes will be written as a single atomic log packet which will either apply
        //               or not. Likewise they will all either appear in the working segment or not, though
        //               they do not appear when originally issued.


        public readonly WriteGroupType type;

        enum WriteGroupState {
            PENDING,
            PREPARED,
            CLOSED,
        }
        WriteGroupState state = WriteGroupState.PENDING;



        public LayerWriteGroup(LayerManager _layer, WriteGroupType type = DEFAULT_WG_TYPE) {
            this.mylayer = _layer;
            this.tsn = _layer.tsnidgen.nextTimestamp();
            this.type = type;

            mylayer.pending_txns.Add(tsn, new WeakReference<LayerWriteGroup>(this));  // track pending transactions without preventing collection

            // TODO: store the stack backtrace of who created this if we're in debug mode
        }

        ~LayerWriteGroup() {
            this.Dispose();
        }

        public void addCompletion(handleWriteGroupCompletion fn) {
            pending_completions.Add(fn);
        }

        public void setValue(RecordKey key, RecordUpdate update) {
            // build a byte[] for the updates using the basic block encoder
            MemoryStream writer = new MemoryStream();
            // TODO: this seems like a really inefficient way to write out a key
            ISegmentBlockEncoder encoder = new SegmentBlockBasicEncoder();
            encoder.setStream(writer);
            encoder.add(key, update);
            encoder.flush();
            writer.Flush();
            this.addCommand((byte)LogCommands.UPDATE, writer.ToArray());

            // Writes are actually applied to the workingSegment when the LgoWriter pushes them to the ILogReceiver.
            // This assures, for example, that DISK_ATOMIC writes to not apply to the segments until the writegroup is flushed.
        }

        public void setValueParsed(String skey, String svalue) {
            RecordKey key = new RecordKey();
            key.appendParsedKey(skey);
            RecordUpdate update = RecordUpdate.WithPayload(svalue);

            this.setValue(key, update);
        }

        internal SegmentMemoryBuilder checkpointStart() {
            // atomically add the checkpoint start marker
            byte[] emptydata = new byte[0];
            int checkpointID = mylayer.logwriter.checkpointStart();
            return this.mylayer.receiver.checkpointSegment;
        }

        internal void checkpointDrop() {
            mylayer.logwriter.checkpointDrop(0);
        }

        private void addCommand(byte cmd, byte[] cmddata) {
            if (this.type == WriteGroupType.DISK_INCREMENTAL) {
                // if we are allowed to add each write to the disk log, then do it now
                mylayer.logwriter.addCommand(cmd, cmddata, out this.last_logwaitnumber);
            } else if (this.type == WriteGroupType.DISK_ATOMIC_FLUSH) {
                // add it to our pending list of commands to flush at the end...
                pending_cmds.Add(new LogCmd(cmd, cmddata));
            } else if (this.type == WriteGroupType.MEMORY_ONLY) {
                // we don't need to add to the log at all, but it still needs to be pushed
                // through the LogWriter so it can be applied to the working segment
                mylayer.logwriter.addCommand_NoLog(cmd, cmddata);
            } else if (this.type == WriteGroupType.DISK_ATOMIC_NOFLUSH) {
                // add it to our pending list of commands to flush at the end...
                pending_cmds.Add(new LogCmd(cmd, cmddata));
            } else {
                throw new Exception("unknown write group type in addCommand() " + type.ToString());
            }
        }

        public void cancel() {
            this.state = WriteGroupState.CLOSED;
        }

        public void finish() {
            // TODO: make a higher level TX commit that finalizes pending writes into final writes
            //     and cleans up locks and state

            // TODO: this is a flush not a commmit. When other
            // writers are concurrent, some of their stuff is also written to the
            // log when we flush. Therefore, as soon as you write, your write is
            // "likely to occur" whether you commit or not. We need to layer 
            // an MVCC on top of this

            if (this.state == WriteGroupState.CLOSED) {
                System.Console.WriteLine("finish() called on closed WriteGroup"); // TODO: add LSN/info
                return;
            }
            if (mylayer == null || mylayer.logwriter == null) {
                System.Console.WriteLine("finish() called on torn-down LayerManager"); // TODO: add LSN/info
                return;
            }
            switch (type) {
                case WriteGroupType.DISK_INCREMENTAL:
                    // we've been incrementally writing commands, so ask them to flush
                    if (this.last_logwaitnumber != 0) {
                        mylayer.logwriter.flushPendingCommandsThrough(last_logwaitnumber);
                    }
                    break;
                case WriteGroupType.DISK_ATOMIC_FLUSH:
                    // send the group of commands to the log and clear the pending list
                    mylayer.logwriter.addCommands(this.pending_cmds, ref this.last_logwaitnumber);
                    this.pending_cmds.Clear();

                    // wait until the atomic log packet is flushed
                    mylayer.logwriter.flushPendingCommandsThrough(last_logwaitnumber);
                    break;
                case WriteGroupType.DISK_ATOMIC_NOFLUSH:
                    // send the group of commands to the log and clear the pending list
                    mylayer.logwriter.addCommands(this.pending_cmds, ref this.last_logwaitnumber);
                    this.pending_cmds.Clear();
                    break;
                case WriteGroupType.MEMORY_ONLY:
                    // we turned off logging, so the only way to commit is to checkpoint!
                    // TODO: force checkpoint ?? 
                    break;
                default:
                    throw new Exception("unknown write group type in .finish(): " + type.ToString());
            }

            if (this.pending_cmds.Count != 0) {
                throw new Exception("pending commands left after finish!!");
            }

            state = WriteGroupState.CLOSED;
            mylayer.pending_txns.Remove(this.tsn);

            // call each of the pending completions.
            foreach (handleWriteGroupCompletion completion_fn in pending_completions) {
                completion_fn();
            }
            pending_completions.Clear();
        }

        public void Dispose() {
            if (state == WriteGroupState.PENDING) {
                System.Console.WriteLine("disposed transaction still pending " + this.tsn);
                // throw new Exception("disposed Txn still pending " + this.tsn);

            }
            mylayer.pending_txns.Remove(this.tsn);
        }
    }

}
