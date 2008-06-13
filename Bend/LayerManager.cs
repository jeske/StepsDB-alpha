using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Bend
{

    // ---------------[ LayerManager ]---------------------------------------------------------

    class LayerManager
    {
        List<ISortedSegment> segmentlayers;
        SegmentBuilder workingSegment;
        String dir_path;

        public enum InitMode
        {
            NEW_SEGMENT
        }

        public LayerManager(InitMode mode, String dir_path)
        {
            this.dir_path = dir_path;

            if (!Directory.Exists(dir_path))
            {
                Console.WriteLine("LayerManager, creating directory: " + dir_path);
                Directory.CreateDirectory(dir_path);
            }

            segmentlayers = new List<ISortedSegment>();   // a list of segment layers, newest to oldest
            workingSegment = new SegmentBuilder();

            segmentlayers.Add(workingSegment);
        }

        public void flushWorkingSegment()
        {
            // write the current segment and flush
            FileStream writer = File.Create(dir_path + "\\curseg.sg");
            workingSegment.writeToStream(writer);
            writer.Close();
            
            
            // reopen that segment
            FileStream reader = File.Open(dir_path + "\\curseg.sg", FileMode.Open);
            SegmentReader sr = new SegmentReader(reader);
            segmentlayers.Add(sr);

            // replace the old working segment with the new one (TODO: make this atomic?)
            segmentlayers.Remove(workingSegment);
            workingSegment = new SegmentBuilder();
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
            RecordKey key = new RecordKey();
            key.appendKeyPart(skey);

            RecordUpdate update = new RecordUpdate(RecordUpdateTypes.FULL, svalue);
            workingSegment.setRecord(key, update);
        }
    }
}