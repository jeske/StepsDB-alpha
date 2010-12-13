// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

namespace Bend {


    // ---------------[ SegmentBlockEncoderRecordOffsetList]------------------------------
    //
    // The Block contains directly encoded records, with addresses to record
    // starts packed at the end.
    //
    // * TODO * : add a record checksum, even a weak one... to prevent stupid mistakes!

    public class SegmentBlockEncoderRecordOffsetList : ISegmentBlockEncoder {
        Stream output;
        long pos_offset; // addr0 = (output.Position - pos_offset)
        List<RecordInfo> record_offsets;

        public struct RecordInfo {
            public int record_start_pos;
            public int key_len;
            public int data_len;
        }
        public void setStream(Stream output) {
            this.output = output;
            this.pos_offset = output.Position;
            this.record_offsets = new List<RecordInfo>();
        }

        public void add(RecordKey key, RecordUpdate data) {
            byte[] keybytes = key.encode();
            byte[] databytes = data.encode();

            RecordInfo ri;
            ri.record_start_pos = _curPos();
            ri.key_len = keybytes.Length;
            ri.data_len = databytes.Length;

            output.Write(keybytes, 0, keybytes.Length);
            output.Write(databytes, 0, databytes.Length);
            record_offsets.Add(ri);
        }

        private int _curPos() {
            return (int)(output.Position - this.pos_offset);
        }

        public void flush() {
            foreach (var ri in record_offsets) {
                Util.writeStruct(ri, output);
            }
            
            BinaryWriter w = new BinaryWriter(output);
            w.Write((System.Int32)record_offsets.Count);
            w.Flush();
        }
    }


     public class SegmentBlockDecoderRecordOffsetList : ISegmentBlockDecoder
    {
        BlockAccessor datastream; // keep in mind, we don't want to share this, since it has only one seek pointer
        int number_of_records;       

        // prepare to read from a stream
        public SegmentBlockDecoderRecordOffsetList(BlockAccessor datastream) {
            this.datastream = datastream;
                 
            if (this.datastream.Length == 0) {
                throw new Exception("SegmentBlockDecoderRecordOffsetList: handed empty stream");
            }

            // read the footer index size
            // FIXME: BUG BUG BUG!! using SeekOrigin.End is only valid here because our current RegionManager
            //        is handing us a file. We need to decide if future Regionmanagers are going to explicitly
            //        make "subregion" Streams, or whether we need to handle this differently.

            datastream.Seek(-4, SeekOrigin.End);  // last 4 bytes of block
            BinaryReader r = new BinaryReader(datastream);
            number_of_records = r.ReadInt32();
            Console.WriteLine("numrecords: " + number_of_records);
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk() {
            for (int x=0;x<this.number_of_records;x++) {
                SegmentBlockEncoderRecordOffsetList.RecordInfo ri = new SegmentBlockEncoderRecordOffsetList.RecordInfo();
                int position = -(4 + ((number_of_records-x) * Util.structSize(ref ri)));
                Console.WriteLine("pos : " + position);
                this.datastream.Seek(position,  SeekOrigin.End);
                ri = Util.readStruct<SegmentBlockEncoderRecordOffsetList.RecordInfo>(this.datastream);

                this.datastream.Seek(ri.record_start_pos, SeekOrigin.Begin);
                byte[] key_bytes = new byte[ri.key_len];
                this.datastream.Read(key_bytes, 0, ri.key_len);
                byte[] data_bytes = new byte[ri.data_len];
                this.datastream.Read(data_bytes, 0, ri.data_len);

                RecordKey key = new RecordKey(key_bytes);
                RecordUpdate update = RecordUpdate.FromEncodedData(data_bytes);
                yield return new KeyValuePair<RecordKey,RecordUpdate>(key, update);
            }

            // return this.scanForward(ScanRange<RecordKey>.All());
        }

        public KeyValuePair<RecordKey, RecordUpdate> FindNext(IComparable<RecordKey> keytest, bool equal_ok) {
            throw new Exception("not implemented");
        }

        public KeyValuePair<RecordKey, RecordUpdate> FindPrev(IComparable<RecordKey> keytest, bool equal_ok) {
            throw new Exception("not implemented");
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanForward(IScanner<RecordKey> scanner) {
            throw new Exception("not implemented");
        }
        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanBackward(IScanner<RecordKey> scanner) {
            throw new Exception("not implemented");
        }
     }

}


