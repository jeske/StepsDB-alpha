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

    // TODO: consider what this class is for long term. Right now it's just a needless wrapper
    //  around IScannableDictionary. However, we intend at some point for it to collect
    //  statistics which can help us predict which segment/block choices to make when we
    //  write out the segment.    
    //
    // Long term, we might want to have a prefix-compressed in-memory format. The current
    // format bloats when we have lots of very big keys.

    class SegmentMemoryBuilder : ISortedSegment , IScannable<RecordKey, RecordUpdate>
    {
        IScannableDictionary<RecordKey, RecordUpdate> items;

        // TODO: collect some statistics that help us inform the segment/block writing process
        // int approx_size = 0;
        // int num_deletions = 0;

        public SegmentMemoryBuilder() {
            // TODO: implement a type-instantiation registry, so clients can ask for us to
            //   instantiate a different implementation. 
            // ..sortedlist perf: http://www.codeproject.com/KB/recipes/SplitArrayDictionary.aspx
            // ..Our skiplist takes more space, but probably shreds those performance numbers,
            //   and it allows next/prev, which none of them offer.

            // items = new SortedDictionary<RecordKey, RecordUpdate>();
            items = new SkipList<RecordKey, RecordUpdate>();
        }

        public int RowCount {
            get { return this.items.Count; }
        }
          

        public void setRecord(RecordKey key, RecordUpdate value) {
            items[key] = value; // replace the existing value if it is there
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

        #region IScannableDictionary_mapping

        public KeyValuePair<RecordKey,RecordUpdate> FindNext(IComparable<RecordKey> keytest) {
            return this.items.FindNext(keytest);
        }
        public KeyValuePair<RecordKey, RecordUpdate> FindPrev(IComparable<RecordKey> keytest) {
            return this.items.FindPrev(keytest);
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanForward(IScanner<RecordKey> scanner) {            
            return this.items.scanForward(scanner);
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanBackward(IScanner<RecordKey> scanner) {
            return this.items.scanBackward(scanner);
        }


        #endregion

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
            // FIXME: this is a huge BUG!!! we either need to instantiate the Stream to be bounded to the
            //        valid regionmap metadata, or we need to change this!!
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