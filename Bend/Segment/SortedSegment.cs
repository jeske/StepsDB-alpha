// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

namespace Bend
{


    // ---------------[ SortedSegment interface]------------------------------------------------

    public enum GetStatus
    {
        PRESENT,
        MISSING
    };

    public interface ISortedSegment : IDisposable
    {
        GetStatus getRecordUpdate(RecordKey key, out RecordUpdate update);
        IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk();
    }

    public interface ISegmentBlockEncoder
    {
        void setStream(Stream output);
        void add(RecordKey key, RecordUpdate data);
        void flush();
    }

    public interface ISegmentBlockDecoder
    {
        IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk();
    }



    // ---------------[ SegmentBuilder ]---------------------------------------------------------



    class SegmentMemoryBuilder : ISortedSegment
    {
        // sortedlist perf: http://www.codeproject.com/KB/recipes/SplitArrayDictionary.aspx
        SortedList<RecordKey, RecordUpdate> items;

        // int approx_size = 0;
        // int num_deletions = 0;

        public SegmentMemoryBuilder() {
            items = new SortedList<RecordKey, RecordUpdate>();
        }

        public int RowCount {
            get { return this.items.Count; }
        }
          

        public void setRecord(RecordKey key, RecordUpdate value) {
            int index = items.IndexOfKey(key);
            if (index != -1) {
                items.RemoveAt(index);
            }
            items.Add(key, value);
        }

        public GetStatus getRecordUpdate(RecordKey key, out RecordUpdate update) {
            // TODO handle missing exception
            try {
                update = items[key];
                return GetStatus.PRESENT;
            } catch (KeyNotFoundException) {
                update = null;
                return GetStatus.MISSING;
            }
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk() {
            return items;
        }

        public void Dispose() {
            // pass
        }
    }

    // ---------------[ SortedSegmentIndex ]---------------------------------------------------------

    class SortedSegmentIndex
    {
        class _SegBlock
        {
            public long datastart;
            public long dataend;
            public short blocktype;
            // TODO: define the keyrange that this block covers

            public _SegBlock(short blocktype, long start, long end)
            {
                this.datastart = start;
                this.dataend = end;
                this.blocktype = blocktype;
            }
            public _SegBlock(BinaryReader rr) {
                datastart = rr.ReadInt64();
                dataend = rr.ReadInt64();
                blocktype = rr.ReadInt16();
            }
            public void Write(BinaryWriter wr) {
                wr.Write((long)datastart);
                wr.Write((long)dataend);
                wr.Write((long)blocktype);
            }
            
            override public String ToString() {
                return String.Format("({0}:{1},{2})",blocktype,datastart,dataend);
            }
        }
        List<_SegBlock> blocks;
        Stream fs; // used when we're in read-mode

        public SortedSegmentIndex() {
            blocks = new List<_SegBlock>();
        }
        public SortedSegmentIndex(Stream rr,Stream _fs) : this() {
            readFromStream(rr);
            this.fs = _fs;
        }

        public void addBlock(ISegmentBlockEncoder encoder, long startpos, long endpos) {
            blocks.Add(new _SegBlock((short)0, startpos, endpos));
        }

        public void writeToStream(Stream writer) {
            BinaryWriter wr = new BinaryWriter(writer);
            
            // write the number of segments in this block
            int length = blocks.Count;
            wr.Write((int)length); 
            foreach (_SegBlock block in blocks) {
                block.Write(wr);
            }
        }
        public void readFromStream(Stream reader) {
            BinaryReader rr = new BinaryReader(reader);

            // read the number of segments in the block
            int numsegments = rr.ReadInt32();
            for (int i=0;i<numsegments;i++) {
                _SegBlock block = new _SegBlock(rr);
                blocks.Add(block);
                Debug.WriteLine(block, "index reader");
               
            }
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk() {

            foreach (_SegBlock block in blocks) {
                // TODO: if the block is applicable to the scan
                ISegmentBlockDecoder decoder = new SegmentBlockBasicDecoder(fs,block.datastart,block.dataend);
                foreach(KeyValuePair<RecordKey,RecordUpdate> kvp in decoder.sortedWalk()) {
                    yield return kvp;
                }
            }
        }

    }

    // ---------------[ SegmentReader ]---------------------------------------------------------

    class SegmentReader : ISortedSegment
    {
        Stream fs;
        SortedSegmentIndex index;

        public SegmentReader(Stream _fs) {
            fs = _fs;
            
            // read the footer index size
            fs.Seek(-4, SeekOrigin.End);  // last 4 bytes of file
            byte[] lenbytes = new byte[4];
            int err = fs.Read(lenbytes, 0, 4);
            int indexlength = BitConverter.ToInt32(lenbytes, 0);

            // read the index
            fs.Seek(-(4 + indexlength), SeekOrigin.End); 
            byte[] indexdata = new byte[indexlength];
            fs.Read(indexdata, 0, indexlength);

            index = new SortedSegmentIndex(new MemoryStream(indexdata),_fs);

        }

        public GetStatus getRecordUpdate(RecordKey key, out RecordUpdate update) {
            // TODO: need to BINARY SEARCH!!!! for the key
            foreach (KeyValuePair<RecordKey,RecordUpdate> kvp in this.sortedWalk()) {
                if (kvp.Key.Equals(key)) {
                    update = kvp.Value;
                    return GetStatus.PRESENT;
                }
            }
            update = RecordUpdate.NoUpdate();
            return GetStatus.MISSING;

        }
        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk() {
            return index.sortedWalk();
        }

        public void Dispose() {
            if (fs != null) { fs.Close(); fs = null; }
        }
    }

    // ---------------------------[  SegmentWriter  ]--------------------------------

    class SegmentWriter
    {
        IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> enumeration;
        
        public SegmentWriter(IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> enumeration) {
            this.enumeration = enumeration;
            
        }

        public void writeToStream(Stream writer) {
            SortedSegmentIndex index = new SortedSegmentIndex();

            // start with the simple case of a "single basic datablock" for the whole segment
            ISegmentBlockEncoder encoder = new SegmentBlockBasicEncoder();
            encoder.setStream(writer);
            long startpos = writer.Position;
            foreach (KeyValuePair<RecordKey, RecordUpdate> kvp in enumeration) {
                encoder.add(kvp.Key, kvp.Value);
            }
            encoder.flush();
            long endpos = writer.Position;

            // add the single index entry
            index.addBlock(encoder, startpos, endpos);

            // write the index data
            long indexstartpos = writer.Position;
            index.writeToStream(writer);
            long indexendpos = writer.Position;
            int indexlength = (int)(indexendpos - indexstartpos);

            // write the fixed footer   
            byte[] indexlenbuf = BitConverter.GetBytes((int)indexlength);
            writer.Write(indexlenbuf, 0, indexlenbuf.Length);
        }
    }
    

}