// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

namespace Bend {  


// ---------------[ SegmentBlockEncoderBasic ]------------------------------
    //
    // 0x80 = End of Line Marker
    // 0x81 = Escape Character     (two byte sequence 0x81 <char> indicates a literal char)
    // 0x82 = Key/Value Separator
    //
    // Escape sequences, designed to avoid backtracking:
    //   0x81 0x01 -> 0x80
    //   0x81 0x02 -> 0x81
    //   0x81 0x03 -> 0x82
    // 

    // TODO: figure out how we can (a) advise that our scans will be slow when
    //    .. record size is large. (b) keep skiplist style fastscan-back pointers
    //    .. to jump back far and still find a record boundary

    class SegmentBlockBasicEncoder : ISegmentBlockEncoder
    {
        Stream output;

        internal static byte FIRST_SPECIAL = 0x80;
        internal static byte END_OF_LINE = 0x80;
        internal static byte ESCAPE_CHAR = 0x81;
        internal static byte KEY_VAL_SEP = 0x82;
        internal static byte ESCAPE_OFFSET = (byte)(FIRST_SPECIAL - 1);
        internal static byte LAST_SPECIAL = 0x82;
        
        // TODO: we need to make an Allocation-Is-Initialization Factory method, insetad of
        // this setStream call
        public void setStream(Stream output) {
            this.output = output;
        }

        public void add(RecordKey key, RecordUpdate data) {
            byte[] keybytes = key.encode();
            byte[] databytes = data.encode();

            writeEncoded(output,keybytes);
            output.WriteByte(KEY_VAL_SEP);
            writeEncoded(output,databytes);
            output.WriteByte(END_OF_LINE);
        }

       

        static void writeEncoded(Stream o, byte[] data) {
            int curstart = 0;
            int curend = 0;

            while (true) {
                if (curend == data.Length) {
                    // flush first
                    if (curstart != curend) {
                        o.Write(data, curstart, (curend - curstart));
                        curstart = curend;
                    }
                    break; // end while
                } else {
                    byte cur = data[curend];
                    if ((cur >= FIRST_SPECIAL && cur <= LAST_SPECIAL)) {
                        // flush first
                        if (curstart != curend) {
                            o.Write(data, curstart, (curend - curstart));
                            curstart = curend;
                        }
                        // write the escape char
                        o.WriteByte(ESCAPE_CHAR);
                        o.WriteByte((byte)(cur - ESCAPE_OFFSET));
                        curend++;
                        curstart = curend;
                    } else {
                        curend++;
                    }
                }
                
            }
        }

        public void flush() {
            // already flushed
        }

    }

    class SegmentBlockBasicDecoder : ISegmentBlockDecoder
    {
        Stream datastream; // keep in mind, we don't want to share this, since it has only one seek pointer
       
        // prepare to read from a stream
        public SegmentBlockBasicDecoder(Stream datastream) {
            this.datastream = datastream;
        }

        private static KeyValuePair<RecordKey, RecordUpdate> decodeRecordFromStream(Stream rs) {
            bool at_endmarker = false;
            // we are ASSUMING that the records starts right here in the stream, it better be true!
            // TODO: considering adding two separate record start/end markers
            // accumulate the key
            List<byte> keystr = new List<byte>();
            // StringBuilder keystr = new StringBuilder();
            bool keydone = false;
            while (rs.Position < rs.Length && !keydone) {
                byte c = (byte)rs.ReadByte();
                switch (c) {
                    case 0x80:   // end of line 
                        throw new Exception("reached end of line before keyvalue delimiter");
                    case 0x82:   // key value delimiter
                        keydone = true;
                        break;
                    case 0x81:
                        byte nc = (byte)rs.ReadByte();
                        byte unescaped = (byte)(nc + 0x7F);
                        if (unescaped < 0x80 || unescaped > 0x82) {
                            // throw new Exception("unhandled escape sequence");
                        }
                        keystr.Add(unescaped);
                        // keystr.Append((char)unescaped);
                        break;
                    default:
                        keystr.Add(c);
                        // keystr.Append((char)c);
                        break;
                }
            }
            if (!keydone) { throw new Exception("reached end of buffer before keydone!"); }
            // accumulate the value
            // TODO: switch this to use List<byte> instead of string builder!!!
            StringBuilder valuestr = new StringBuilder();
            bool valuedone = false;
            while (rs.Position < rs.Length && !valuedone) {
                at_endmarker = false;
                byte c = (byte)rs.ReadByte();
                switch (c) {
                    case 0x80:   // end of line 
                        valuedone = true;
                        at_endmarker = true;
                        break;
                    case 0x82:   // key value delimiter
                        throw new Exception("found keyvalue delimiter in value");
                    case 0x81:
                        throw new Exception("unhandled escape sequence");
                    default:
                        valuestr.Append((char)c);
                        break;
                }
            }

            RecordKey key = new RecordKey(keystr.ToArray());
            RecordUpdate value = RecordUpdate.FromEncodedData(valuestr.ToString());
            Debug.WriteLine("scanning " + key.ToString() + " : " + value.ToString());
            return new KeyValuePair<RecordKey, RecordUpdate>(key, value);
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk() {            
            Stream rs = datastream; // our read stream
            rs.Seek(0, SeekOrigin.Begin);
            bool at_endmarker = false;

            while (rs.Position < rs.Length) {

                // accumulate the key
                List<byte> keystr = new List<byte>();
                // StringBuilder keystr = new StringBuilder();
                bool keydone = false;
                while (rs.Position < rs.Length && !keydone) {
                    byte c = (byte)rs.ReadByte();
                    switch (c) {
                        case 0x80:   // end of line 
                            throw new Exception("reached end of line before keyvalue delimiter");
                        case 0x82:   // key value delimiter
                            keydone = true;
                            break;
                        case 0x81:
                            byte nc = (byte)rs.ReadByte();
                            byte unescaped = (byte)(nc + 0x7F);
                            if (unescaped < 0x80 || unescaped > 0x82) {
                                // throw new Exception("unhandled escape sequence");
                            }
                            keystr.Add(unescaped);
                            // keystr.Append((char)unescaped);
                            break;
                        default:
                            keystr.Add(c);
                            // keystr.Append((char)c);
                            break;
                    }
                }
                if (!keydone) { throw new Exception("reached end of buffer before keydone!"); }
                // accumulate the value
                // TODO: switch this to use List<byte> instead of string builder!!!
                StringBuilder valuestr = new StringBuilder();
                bool valuedone = false;
                while (rs.Position < rs.Length && !valuedone) {
                    at_endmarker = false;
                    byte c = (byte)rs.ReadByte();
                    switch (c) {
                        case 0x80:   // end of line 
                            valuedone = true;
                            at_endmarker = true;
                            break;
                        case 0x82:   // key value delimiter
                            throw new Exception("found keyvalue delimiter in value");
                        case 0x81:
                            throw new Exception("unhandled escape sequence");
                        default:
                            valuestr.Append((char)c);
                            break;
                    }
                }

                RecordKey key = new RecordKey(keystr.ToArray());
                RecordUpdate value = RecordUpdate.FromEncodedData(valuestr.ToString());
                Debug.WriteLine("scanning " + key.ToString() + " : " + value.ToString());
                yield return new KeyValuePair<RecordKey, RecordUpdate>(key, value);
            }


            if (rs.Position != rs.Length || !at_endmarker) {
                Debug.WriteLine("sortedWalk() did not finish at end marker");
            }
        }


        // ---------------------- IScannable --------------------------------------------------

        private enum mpss {  // MidpointSearchState
            BISECT,
            FIND_REC_FORWARD,
            HAVE_START
            
        }

        internal struct RecordLocator
        {
            internal KeyValuePair<RecordKey, RecordUpdate> record;
            internal int start_pos;
            internal int end_pos;
            internal bool have_record;

        }
        internal struct FindRecordResult {
            internal RecordLocator before_keytest;
            internal RecordLocator after_keytest;            
        }

        private static int FIND_RECORD_BUFSIZE = 500;
        // do a binary search, knowing we need to re-align to the record boundaries
        // everytime we pick a new cut point... this is efficient as long as our
        // records are reasonably small. For records that are large, we should
        // pick a different format to avoid all this scanning.

        private FindRecordResult _findRecord(IComparable<RecordKey> keytest) {
            // read state
            byte[] search_buffer = new byte[FIND_RECORD_BUFSIZE];
            int read_result_size, read_size;

            FindRecordResult result = default(FindRecordResult);
             
            // cursor state
            int startpos = 0;            
            int endpos = (int)this.datastream.Length;
            int midpoint_pos = 0xbeefed;  // distinct but uninitalized

            // start by decoding the first record in the block
            int eol_marker_pos = -1; 
            mpss state = mpss.HAVE_START;  
            
            switch (state) {
                case mpss.BISECT:
                    if (endpos == startpos) {
                        return result;                        
                    }
                    midpoint_pos = (endpos - startpos) / 2;
                    if (midpoint_pos > endpos) {
                        midpoint_pos = startpos;
                    }
                    goto case mpss.FIND_REC_FORWARD;
                case mpss.FIND_REC_FORWARD:
                    // scan forward until we find an END_OF_LINE record boundary
                    {                         
                        int cur_stream_pos = midpoint_pos;
                        do {
                            int bufpos = 0;
                            this.datastream.Seek(cur_stream_pos, SeekOrigin.Begin);
                            read_size = (int)Math.Min(search_buffer.Length,endpos-cur_stream_pos);
                            read_result_size = this.datastream.Read(search_buffer,0,search_buffer.Length);
                            
                            while (bufpos < read_result_size) {
                                if (search_buffer[bufpos] == SegmentBlockBasicEncoder.END_OF_LINE) {
                                    eol_marker_pos = cur_stream_pos + bufpos;
                                    goto case mpss.HAVE_START;
                                }
                                bufpos++;
                            }
                            cur_stream_pos += read_result_size;
                        } while (cur_stream_pos < endpos);
                    }
                    // ran out of datastream and didn't find END_OF_LINE! (yuck, we just scanned the whole tophalf)                        
                    // move to the beginning of the buffer and restart
                    endpos = midpoint_pos;
                    midpoint_pos = startpos;
                    goto case mpss.FIND_REC_FORWARD;
                    // fyi - we considered bisecting the first half, but if the records are so big relative
                    //  to our midpoint that there were none in the second half, it's because our records
                    //  are really large relative to the range we are scanning, so it's safer just to
                    //  start at the BEGINNING of the whole range.                                         
                case mpss.HAVE_START:
                    // we have a record_start_stream_pos, so decode the record and test against the comparison
                    {
                        RecordLocator rloc;
                        int record_start_pos = eol_marker_pos + 1;
                        if (record_start_pos >= endpos) {
                            // we found the end of the last record in the midpoint-region, but we don't
                            // know where the beginning is yet.                            
                            endpos = midpoint_pos;
                        }
                        
                        // decode the record
                        this.datastream.Seek(record_start_pos, SeekOrigin.Begin);
                        rloc.record = SegmentBlockBasicDecoder.decodeRecordFromStream(this.datastream);
                        rloc.start_pos = record_start_pos;
                        rloc.end_pos = (int)this.datastream.Position;
                        rloc.have_record = true;                        
                        
                        int compare_result = keytest.CompareTo(rloc.record.Key);
                        if (compare_result <= 0) {  // record is AFTER or EQUAL to keytest
                            result.after_keytest = rloc;
                            // we know this record is after us, so scan only before it
                            endpos = rloc.start_pos;
                        } else if (compare_result > 0) { // record is BEFORE keytest
                            result.before_keytest = rloc;
                            // we know this record is before us, so scan only after it
                            startpos = rloc.end_pos;
                        } 
                    }
                    goto case mpss.BISECT;
                default:
                    throw new Exception("unknown mpss state: " + state.ToString());
            } // end switch(mpss state)

        }

        public KeyValuePair<RecordKey, RecordUpdate> FindNext(IComparable<RecordKey> keytest) { 
            FindRecordResult result = _findRecord(keytest);
            if (result.after_keytest.have_record) {
                return result.after_keytest.record;
            } else {
                throw new KeyNotFoundException("SegmentBlockBasic: failed to find match : " + keytest.ToString());
            }            
        }
        public KeyValuePair<RecordKey, RecordUpdate> FindPrev(IComparable<RecordKey> keytest) {
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