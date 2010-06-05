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
        // TODO: need IScannable<RecordKey, RecordUpdate>
        //    .. but consider adding it as "getScanner()" instead of directly inheriting/implementing
        GetStatus getRecordUpdate(RecordKey key, out RecordUpdate update);
        IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk();
    }

    public interface ISegmentBlockEncoder
    {
        void setStream(Stream output);
        void add(RecordKey key, RecordUpdate data);
        void flush();
    }

    public interface ISegmentBlockDecoder : IScannable<RecordKey, RecordUpdate>
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

    class SegmentMemoryBuilder : ISortedSegment, IScannable<RecordKey, RecordUpdate>
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

        public KeyValuePair<RecordKey,RecordUpdate> FindNext(IComparable<RecordKey> keytest,bool equal_ok) {
            return this.items.FindNext(keytest,equal_ok);
        }
        public KeyValuePair<RecordKey, RecordUpdate> FindPrev(IComparable<RecordKey> keytest,bool equal_ok) {
            return this.items.FindPrev(keytest,equal_ok);
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
    
    internal class SortedSegmentIndex: IScannable<RecordKey, RecordUpdate>
    {
        internal class _SegBlock : IEquatable<_SegBlock>
        {
            // TODO: how do ranges fit together? (how do we define inclusive/exclusive for the joint?)
            internal RecordKey lowest_key;    
            internal long datastart;
            internal long dataend;

            // TODO: should make a registry mapping shorts to GUIDs of block encoders
            internal short blocktype;    // 00 = special endblock lists the last key (inclusive)
            
            public _SegBlock( RecordKey start_key_inclusive, short blocktype, long datastartoffset, long dataendoffset) {
                this.lowest_key = start_key_inclusive;
                this.datastart = datastartoffset;
                this.dataend = dataendoffset;
                this.blocktype = blocktype;
            }
            public _SegBlock(BinaryReader rr) {
                Int32 coded_key_length = rr.ReadInt32();
                lowest_key = new RecordKey(rr.ReadBytes((int)coded_key_length));
                
                
                datastart = rr.ReadInt64();
                dataend = rr.ReadInt64();
                blocktype = rr.ReadInt16();
            }

            public void Write(BinaryWriter wr) {
                byte[] lowest_key_encoded = lowest_key.encode();
                wr.Write((Int32)lowest_key_encoded.Length);
                wr.Write((byte[])lowest_key_encoded);

                // TODO: switch this reading and writing to binstruct so we don't make mistakes
                wr.Write((Int64)datastart);
                wr.Write((Int64)dataend);
                wr.Write((Int16)blocktype);
            }
            
            override public String ToString() {
                return String.Format("({0}:{1}:{2},{3})",blocktype,lowest_key.ToString(),datastart,dataend);
            }

            public bool Equals(_SegBlock target) {
                return false;
            }

        } // end _SegBlock inner class 
                
        internal IScannableDictionary<RecordKey, _SegBlock> blocks;

        IRegion segmentRegion;        

        public SortedSegmentIndex() {
            // TODO: switch this to use a scannable array when we read back
            //  so we can avoid wasting the space and insertion time of a skiplist...
            blocks = new SkipList<RecordKey,_SegBlock>();
        }
        public SortedSegmentIndex(byte[] index_data,IRegion segmentRegion) : this() {
            this.segmentRegion = segmentRegion;
            readFromBytes(index_data);
        }

        public void addBlock(RecordKey start_key_inclusive, ISegmentBlockEncoder encoder, long startpos, long endpos) {
            blocks.Add(start_key_inclusive,new _SegBlock(start_key_inclusive,(short)0, startpos, endpos));
        }

        public void writeToStream(Stream writer) {
            BinaryWriter wr = new BinaryWriter(writer);
            
            // TODO: prefix compress the list of index RecordKeys
            //    .. NOTE that "prefix compress" for us does not mean eliminating the first n bytes of RecordKeys.encode() output,
            //    .. it means eliminating the first n bytes of EACH part of the record key, since it's hierarchially sorted

            // write the number of segments in this block
            int numblocks = blocks.Count;            

            wr.Write((Int32)numblocks); 
            foreach (KeyValuePair<RecordKey,_SegBlock> kvp in blocks) {
                _SegBlock block = kvp.Value;
                block.Write(wr);
            }
        }
        public void readFromBytes(byte[] index_data) {
            BinaryReader rr = new BinaryReader(new MemoryStream(index_data));

            // read the number of blocks in the Segment
            int numblocks = rr.ReadInt32();
            for (int i=0;i<numblocks;i++) {
                _SegBlock block = new _SegBlock(rr);
                blocks.Add(block.lowest_key,block);
                Debug.WriteLine(block, "index reader");
            }            
        }
        private ISegmentBlockDecoder openBlock(_SegBlock block) {
            // TODO, make this somehow get a threadsafe stream to hand to the basic block
            // decoder!!

            return new SegmentBlockBasicDecoder(
                segmentRegion.getNewBlockAccessor((int)block.datastart,
                (int)(block.dataend - block.datastart)));
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk() {

            foreach (KeyValuePair<RecordKey,_SegBlock> block_kvp in blocks) {
                _SegBlock block = block_kvp.Value;
                ISegmentBlockDecoder decoder = openBlock(block);
                    
                foreach(KeyValuePair<RecordKey,RecordUpdate> decode_kvp in decoder.sortedWalk()) {
                    yield return decode_kvp;
                }
            }
        }
        public KeyValuePair<RecordKey, RecordUpdate> FindNext(IComparable<RecordKey> keytest, bool equal_ok) {
            if (blocks.Count == 0) {
                System.Console.WriteLine("index has no blocks!");
                throw new KeyNotFoundException("SortedSegmentIndex: has no blocks in FindNext");
            }

            KeyValuePair<RecordKey, _SegBlock> blockkvp;
            try {
                blockkvp = blocks.FindPrev(keytest,false);
            } catch (KeyNotFoundException ex1) {
                // keytest is before any blocks, check the first block
                try {
                    blockkvp = blocks.FindNext(new ScanRange<RecordKey>.minKey(), true); // get the first block
                } catch (KeyNotFoundException ex2) {                   
                    throw new KeyNotFoundException("SortedSegmentIndex: INTERNAL ERROR in FindNext", ex2);
                }
            }
            _SegBlock block = blockkvp.Value;
            // instantiate the block
            ISegmentBlockDecoder decoder = openBlock(block);
            
            KeyValuePair<RecordKey, RecordUpdate> datakvp;
            try {
                datakvp = decoder.FindNext(keytest, equal_ok);
                return datakvp;
            }
            catch (KeyNotFoundException) {
                while (true) {
                    // the block above might not have had any records after keytest in it
                    // so give the next block(s) a shot if we have more
                    blockkvp = blocks.FindNext(blockkvp.Key, false);
                    block = blockkvp.Value;
                    decoder = openBlock(block);
                    try {
                        return decoder.FindNext(keytest, equal_ok);
                    }
                    catch (KeyNotFoundException) { }
                }

            }
        }
        public KeyValuePair<RecordKey, RecordUpdate> FindPrev(IComparable<RecordKey> keytest, bool equal_ok) {
            if (blocks.Count == 0) {
                System.Console.WriteLine("index has no blocks!");
                throw new KeyNotFoundException("SortedSegmentIndex: has no blocks in FindPrev");
            }
            KeyValuePair<RecordKey, _SegBlock> kvp;
            try {
                kvp = blocks.FindPrev(keytest, equal_ok);
            } catch (KeyNotFoundException ex1) {
                // if we don't have a block that starts before (or equal) to this key,
                // then we don't have a block that can have the key!
                throw new KeyNotFoundException("SegmentIndex.FindPrev (no block contains key " + keytest + ")", ex1);
            }

            _SegBlock block = kvp.Value;
            // instantiate the block
            ISegmentBlockDecoder decoder = openBlock(block);

            return decoder.FindPrev(keytest, equal_ok);
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanForward(IScanner<RecordKey> scanner) {
            IComparable<RecordKey> lowestKeyTest = null;
            IComparable<RecordKey> highestKeyTest = null;
            if (scanner != null) {
                lowestKeyTest = scanner.genLowestKeyTest();
                highestKeyTest = scanner.genHighestKeyTest();
            }
            
            KeyValuePair<RecordKey, RecordUpdate> cursor;
            try {
                cursor = FindNext(lowestKeyTest, true);
            } catch (KeyNotFoundException) {
                yield break;
            }

            while (true) {
                if (highestKeyTest.CompareTo(cursor.Key) >= 0) {
                    yield return cursor;
                } else {
                    yield break;
                }

                try {
                    cursor = FindNext(cursor.Key, false);
                } catch (KeyNotFoundException) {
                    yield break;
                }
            }
               
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanBackward(IScanner<RecordKey> scanner) {
            IComparable<RecordKey> lowestKeyTest = null;
            IComparable<RecordKey> highestKeyTest = null;
            if (scanner != null) {
                lowestKeyTest = scanner.genLowestKeyTest();
                highestKeyTest = scanner.genHighestKeyTest();
            }

            KeyValuePair<RecordKey, RecordUpdate> cursor;
            try {
                cursor = FindPrev(highestKeyTest, true);
            }
            catch (KeyNotFoundException) {
                yield break;
            }

            while (true) {
                if (lowestKeyTest.CompareTo(cursor.Key) <= 0) {
                    yield return cursor;
                } else {
                    yield break;
                }

                try {
                    cursor = FindPrev(cursor.Key, false);
                }
                catch (KeyNotFoundException) {
                    yield break;
                }

            }
        }



        internal long maxLengthAfterMicroBlockAdded(SegmentWriter.MicroBlockStream mb_writer) {
            // FIXME: this is a VERY slow way to compute this.

            MemoryStream test = new MemoryStream();
            int PAD_LENGTH = 1024;

            this.writeToStream(test);
            return (test.Length + 
                mb_writer.block_start_key.ToString().Length + 
                mb_writer.last_seen_key.ToString().Length + 
                PAD_LENGTH);
            
        }
    }

    // ---------------[ SegmentReader ]---------------------------------------------------------

    internal class SegmentReader : ISortedSegment, IScannable<RecordKey, RecordUpdate>
    {
        //         Stream fs;
        IRegion segmentRegion;
        SortedSegmentIndex index;

        public SegmentReader(IRegion segmentRegion) {
            this.segmentRegion = segmentRegion;
            Stream fs = segmentRegion.getNewAccessStream();
            
            // read the footer index size
            // FIXME: BUG BUG BUG!! using SeekOrigin.End is only valid here because our current RegionManager
            //        is handing us a file. We need to decide if future Regionmanagers are going to explicitly
            //        make "subregion" Streams, or whether we need to handle this differently.

            fs.Seek(-4, SeekOrigin.End);  // last 4 bytes of file
            byte[] lenbytes = new byte[4];
            int err = fs.Read(lenbytes, 0, 4);
            int indexlength = BitConverter.ToInt32(lenbytes, 0);

            // read the index bytes
            fs.Seek(-(4 + indexlength), SeekOrigin.End); 
            byte[] indexdata = new byte[indexlength];
            fs.Read(indexdata, 0, indexlength);
            
            // and push those bytes through the segmentindexread
            index = new SortedSegmentIndex(indexdata,segmentRegion);

        }

        public GetStatus getRecordUpdate(RecordKey key, out RecordUpdate update) {
            KeyValuePair<RecordKey, RecordUpdate> kvp;
            try {
                kvp = this.FindNext(key, true);
            } catch (KeyNotFoundException) {
                update = RecordUpdate.NoUpdate();
                return GetStatus.MISSING;
            }

            if (kvp.Key.Equals(key)) {
                update = kvp.Value;
                return GetStatus.PRESENT;
            }
            update = RecordUpdate.NoUpdate();
            return GetStatus.MISSING;
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk() {
            return index.sortedWalk();
        }

        // TODO: consider if we should have a method "getScanner()" to vend out someone that can do this
        // for us, (i.e. the index), so we don't have to proxy these calls.

        public KeyValuePair<RecordKey, RecordUpdate> FindNext(IComparable<RecordKey> keytest, bool equal_ok) {
            return index.FindNext(keytest, equal_ok);
        }
        public KeyValuePair<RecordKey, RecordUpdate> FindPrev(IComparable<RecordKey> keytest, bool equal_ok) {
            return index.FindPrev(keytest, equal_ok);
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanForward(IScanner<RecordKey> scanner) {
            return index.scanForward(scanner);
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanBackward(IScanner<RecordKey> scanner) {
            return index.scanBackward(scanner);
        }
        
        public void Dispose() {
            if (segmentRegion != null) { segmentRegion.Dispose(); segmentRegion = null; }
        }
    }

    // ---------------------------[  SegmentWriter  ]--------------------------------
    
    // SegmentWriterAdvisor tells the SegmentWriter when to split, and what type of block to use
    // TODO: figure out how we can get access to the source segment information about what is coming
    //       for lookahead.... Maybe we should stage blocks before we decide which format to put them
    //       in and add all the keys. (i.e. first find the end of the block, then go through again
    //       and format the block) -- sounds like lots of copying though.
    //
    // right now this is ULTRA simple, we just make sure only so many keys go into a block. Eventually
    // we should care more about bytes (keys or data), and eventually, we should start to care
    // about prefix compression efficiency.
    //
    // TODO: this is another class that the user should be able to suppily a new implemtation of in the context.

    class SegmentWriterAdvisor    
    {
        int keys_since_last_block = 0;
        int bytes_since_last_block = 0;
        static int RECOMMEND_MAX_KEYS_PER_MICROBLOCK = 200000; 
        static int RECOMMEND_MAX_BYTES_PER_MICROBLOCK = 64 * 1024;

        public SegmentWriterAdvisor() {
        }
        public void fyiAddedRecord(RecordKey key, RecordUpdate update) {
            this.keys_since_last_block++;
            this.bytes_since_last_block+= (key.ToString().Length + update.ToString().Length); // FIXME: this seems expensive!
        }
        public void fyiFinishedBlock() {
            this.keys_since_last_block = 0;
            this.bytes_since_last_block = 0;
        }
        public bool recommendsNewBlock() {
            if ((this.keys_since_last_block >= RECOMMEND_MAX_KEYS_PER_MICROBLOCK) ||
                (this.bytes_since_last_block >= RECOMMEND_MAX_BYTES_PER_MICROBLOCK)) {
                return true;
            } else {
                return false;
            }
        }
    }

    // ---------------------------[  SegmentWriter  ]--------------------------------

    class SegmentWriter
    {
        IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> enumeration;
        IEnumerator<KeyValuePair<RecordKey, RecordUpdate>> cursor;
        bool hasmore;
        MemoryStream carryoverMicroblock = null; // this is the Microblock we encoded that didn't fit into the destination
        
        // TODO: track input size, output size, number of blocks, 'wasted' space

        public SegmentWriter(IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> enumeration) {
            this.enumeration = enumeration;
            this.cursor = enumeration.GetEnumerator();
            this.hasmore = true;
        }

        public Boolean hasMoreData() {
            return this.hasmore || (this.carryoverMicroblock != null);
        }

        public class MicroBlockStream : System.IO.MemoryStream {
            public RecordKey block_start_key;
            public RecordKey last_seen_key;
            public ISegmentBlockEncoder encoder;
            public MicroBlockStream(RecordKey first_key, ISegmentBlockEncoder enc) : base() {
                block_start_key = first_key;
                this.encoder = enc;
            }
        }

        public void writeToStream(Stream writer) {
            SortedSegmentIndex index = new SortedSegmentIndex(); // the index for this destination block
            SegmentWriterAdvisor advisor = new SegmentWriterAdvisor();
            int num_microblocks = 0;
            int num_rows = 0;
            MicroBlockStream mb_writer = null;
            bool destination_full = false;


            long MIN_OUTPUT_LENGTH = 500 * 1024; // 500k min output block size for now!

            if (writer.Length < MIN_OUTPUT_LENGTH) {
                throw new Exception("SegmentWriter.writeToStream(Stream writer): handed writer with insufficient size");
            }

            //RecordKey block_start_key = null;
            //RecordKey last_seen_key = null;
            ISegmentBlockEncoder encoder = null;

            // see if we have a carryover microblock
            if (this.carryoverMicroblock != null) {
                // TODO, check to see if we have space
                long startpos = writer.Position;
                this.carryoverMicroblock.WriteTo(writer);
                long endpos = writer.Position;
                index.addBlock(mb_writer.block_start_key, mb_writer.encoder, startpos, endpos);
                this.carryoverMicroblock = null; // clear the microblock carryover
            }


            // Need to encode a new microblock, and see if it fits into the destination stream. 
            // We encode microblocks by adding 64kb of keys, then asking for the total size. (the advisor computes the 64k)

            this.hasmore = cursor.MoveNext();
            KeyValuePair<RecordKey, RecordUpdate> kvp;

            while (hasmore && !destination_full) {
                kvp = cursor.Current;
                if (encoder == null) {
                    encoder = new SegmentBlockBasicEncoder();
                    mb_writer = new MicroBlockStream(kvp.Key, encoder);
                    encoder.setStream(mb_writer);
                    //block_start_key = kvp.Key;


                    if ((num_microblocks % 2) == 0) {
                        System.Console.WriteLine("microblock {0} starting at row: {1}, key: {2}",
                            num_microblocks, num_rows, kvp.Key.ToString());
                    }
                    num_microblocks++;
                }
                // handle this row
                {
                    encoder.add(kvp.Key, kvp.Value);
                    mb_writer.last_seen_key = kvp.Key;
                    advisor.fyiAddedRecord(kvp.Key, kvp.Value);
                    num_rows++;
                }

                // move to next row
                this.hasmore = cursor.MoveNext();

                if ((!hasmore) || advisor.recommendsNewBlock()) {
                    advisor.fyiFinishedBlock();

                    encoder.flush(); encoder = null; // this will trigger reinit above                    

                    // we have a new microblock, now we need to decide if it will fit into the current destination
                    // stream, along with the index afterwords...

                    long space_left = writer.Length - writer.Position;

                    if ((mb_writer.Length + index.maxLengthAfterMicroBlockAdded(mb_writer)) < space_left) {
                        // yes, there is enough space to add the microblock
                        long startpos = writer.Position;
                        mb_writer.Position = 0;
                        mb_writer.WriteTo(writer);
                        long endpos = writer.Position;
                        index.addBlock(mb_writer.block_start_key, encoder, startpos, endpos);
                    } else {
                        // nope, there is not enough space to add the microblock
                        this.carryoverMicroblock = mb_writer;
                        destination_full = true;
                        System.Console.WriteLine("lastmicroblock {0} ending at row: {1}, key: {2}",
                            num_microblocks, num_rows, kvp.Key.ToString());
                    }
                }
            }


            // write the index data to the END of the output block
            {
                MemoryStream indexbytes = new MemoryStream();
                index.writeToStream(indexbytes);
                long indexlength = indexbytes.Length;
                // double check that the index fits


                writer.Position = writer.Length - (indexlength + 4);
                indexbytes.WriteTo(writer);

                // write the fixed footer
                byte[] indexlenbuf = BitConverter.GetBytes((int)indexlength);
                writer.Write(indexlenbuf, 0, indexlenbuf.Length);
            }

            if (this.carryoverMicroblock == null) {
                System.Console.WriteLine("segment write finished.  xx blocks, xx bytesin, xx bytesout");
            }

        }
    }
}

namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public class ZZ_TODO_SortedSegment_Stuff
    {
        [Test]
        public void T00_SegmentWriter() {
            Assert.Fail("multiple keys with same value will have a problem");
        }
    }
}