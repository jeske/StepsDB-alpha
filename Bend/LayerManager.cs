using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;
using System.Reflection;

namespace Bend
{

    // IRegionManager
    //
    // This exposes a linear address space called a Region. The LayerManager will divide this
    // Region into segments to hold the root block, log, and segments. We further maintain
    // the invariant that Segments are written in one sweep beginning at their start address. 
    // Once they are closed, they may be read any number of times, but they are disposed of 
    // before being written again. The log does not follow this pattern. 

    interface IRegionManager
    {
        Stream readRegionAddr(uint region_addr);
        Stream writeRegionAddr(uint region_addr);
        void disposeRegionAddr(uint region_addr);
    }

    // manage a region of exposed files
    class RegionExposedFiles : IRegionManager
    {
        String dir_path;
        public RegionExposedFiles(InitMode mode, String location) {
            this.dir_path = location;

            if (!Directory.Exists(dir_path)) {
                Console.WriteLine("LayerManager, creating directory: " + dir_path);
                Directory.CreateDirectory(dir_path);
            }
        }

        public Stream readRegionAddr(uint region_addr) {
            String filename = String.Format("addr{0}.region", region_addr);
            FileStream reader = File.Open(dir_path + filename, FileMode.Open);
            return reader;
        }
        public Stream writeRegionAddr(uint region_addr) {
            String filename = String.Format("addr{0}.region", region_addr);
            FileStream reader = File.Open(dir_path + filename, FileMode.CreateNew);
            return reader;
        }
        public void disposeRegionAddr(uint region_addr) {
            String filename = String.Format("addr{0}.region", DateTime.Now, region_addr);
            String del_filename = String.Format("del{0}addr{1}.region",  region_addr);
            File.Move(dir_path + filename, dir_path + del_filename);
        }
    }


    // ---------------------------------------------------------
    // The on-disk layout is initialized as:
    // 
    // 0 -> MAX_ROOTBLOCK_SIZE : root block
    // MAX_ROOTBLOCK_SIZE -> (MAX_ROOTBLOCK_SIZE+logsize) : log
    // (remainder) : freespace


    // [StructLayout(LayoutKind.Sequential,Pack=1)]
    struct RootBlock
    {
        public uint magic;
        public uint mysize;
        public uint logstart;   // absolute pointer to the start of the log on the region/volume
        public uint logsize;    // size of the current log segment in bytes
        public uint loghead;    // relative pointer to the head of the log
        public uint root_checksum;

        // static values don't consume space
        public static uint MAGIC = 0xFE82a292;
        public static uint MAX_ROOTBLOCK_SIZE = 4096;

        public bool IsValid() {
            if (magic != MAGIC) {
                return false;
            }
            // check size
            // check root_checksum
            return true;
        }
    }

    public enum InitMode
    {
        NEW_REGION,    // initialize the region fresh, error if it looks like there is a region still there
        RESUME         // standard resume/recover from an existing region
    }

    

    // ---------------[ LayerManager ]---------------------------------------------------------

    class LayerManager
    {

        List<ISortedSegment> segmentlayers;
        SegmentBuilder workingSegment;
        String dir_path;   // should change this to not assume directories/files
        IRegionManager regionmgr;




        public LayerManager(InitMode mode, String dir_path)
        {
            this.dir_path = dir_path;
            LogWriter logwriter;

            if (mode == InitMode.NEW_REGION) {
                // right now we only have one region type
                regionmgr = new RegionExposedFiles(InitMode.NEW_REGION, dir_path);

                // TODO: establish the size of the region
                int regionsize = 20 * 1024 * 1024;

                // test to see if there is already a root record there

                Stream rootblockstream = regionmgr.readRegionAddr(0);
                RootBlock root = Util.readStruct<RootBlock>(rootblockstream);
                long rtblksz = rootblockstream.Position;
                if (root.IsValid()) {
                    // we should be careful not to override this...
                }

                // create the log and root record
                root.magic = RootBlock.MAGIC;
                root.logstart = RootBlock.MAX_ROOTBLOCK_SIZE;
                root.logsize = 8 * 1024 * 1024;
                root.loghead = 0;
                root.root_checksum = 0;
                Stream rootblockwritestream = regionmgr.writeRegionAddr(0);

                Stream logwritestream = regionmgr.writeRegionAddr(RootBlock.MAX_ROOTBLOCK_SIZE);

                // first write a log init record into the log
                logwriter = new LogWriter(InitMode.NEW_REGION,
                    logwritestream, rootblockwritestream, root);

            } else {
                regionmgr = new RegionExposedFiles(InitMode.RESUME, dir_path);
                logwriter = new LogWriter(InitMode.RESUME, regionmgr);

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