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
    // 0x80 = End of Record Marker
    // 0x81 = Escape Character     (two byte sequence 0x81 <char> indicates a literal char)
    // 0x82 = Key/Value Separator
    //
    // Escape sequences, designed to avoid backtracking:
    //   0x81 0x01 -> 0x80
    //   0x81 0x02 -> 0x81
    //   0x81 0x03 -> 0x82
    // 
    // Example record:
    //
    //  username 0x82 jeske 0x80

    // * TODO * : add a record checksum, even a weak one... to prevent stupid mistakes!

    // TODO: figure out how we can (a) advise that our scans will be slow when
    //    .. record size is large. (b) keep skiplist style fastscan-back pointers
    //    .. to jump back far and still find a record boundary

    
    public class SegmentBlockBasicEncoder : ISegmentBlockEncoder
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

       

        private static void writeEncoded(Stream o, byte[] data) {
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

    public class SegmentBlockBasicDecoder : ISegmentBlockDecoder
    {
        BlockAccessor datastream; // keep in mind, we don't want to share this, since it has only one seek pointer
       
        // prepare to read from a stream
        public SegmentBlockBasicDecoder(BlockAccessor datastream) {
            this.datastream = datastream;
            if (this.datastream.Length == 0) {
                throw new Exception("SegmentBlockBasicDecoder: handed empty stream");
            }
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk() {
            return this.scanForward(ScanRange<RecordKey>.All());
        }


        private static KeyValuePair<RecordKey, RecordUpdate> _decodeRecordFromBlock(BlockAccessor rs) {            
            bool at_endmarker = false;
            
            // ..we are ASSUMING that the records starts right here in the stream, it better be true!
            // ..TODO: considering adding two separate record start/end markers

            // Accumulate the key.
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
            // Debug.WriteLine("scanning " + key.ToString() + " : " + value.ToString());
            return new KeyValuePair<RecordKey, RecordUpdate>(key, value);
        }



        // ---------------------- IScannable --------------------------------------------------

        private enum mpss {  // MidpointSearchState
            BISECT,
            FIND_REC_FORWARD,
            FIND_REC_FORWARD_RESTART,
            HAVE_START
            
        }

        internal struct RecordLocator
        {
            internal KeyValuePair<RecordKey, RecordUpdate> record;
            internal int start_pos;
            internal int after_end_pos;
            internal bool have_record;

        }
        internal struct FindRecordResult {
            internal RecordLocator before_keytest;
            internal RecordLocator after_keytest;            
        }
        private static int FIND_RECORD_BUFSIZE = 500;  // TODO: load this from configuration state
        private static int PREV_RECORD_SCANSIZE = 500; // TODO: load from config, or use avg record size




        // _nextRecord() - grab the next record
        // easy, since the last record end point stops right where we need to decode
        private void _nextRecord(ref RecordLocator rloc) {
            int new_start = rloc.after_end_pos;
            int datastream_length = (int)this.datastream.Length;

            if (!rloc.have_record) { 
                throw new Exception("_nextRecord() called with rloc.have_record==false");
            }

            if (new_start < datastream_length) {
                this.datastream.Seek(new_start, SeekOrigin.Begin);
                rloc.start_pos = new_start;
                rloc.record = SegmentBlockBasicDecoder._decodeRecordFromBlock(this.datastream);                
                rloc.after_end_pos = (int)this.datastream.Position;
                rloc.have_record = true;
                return;
            } else if (new_start == datastream_length) {
                // reached end
                rloc.have_record = false;
                rloc.record = default(KeyValuePair<RecordKey, RecordUpdate>);
                rloc.start_pos = 0;
                rloc.after_end_pos = 0;
                return;
            } else { 
                // if (new_start > datsstream_length)
                throw new Exception("error: _nextRecord called with rloc past end");
            }
        }

        // _prevRecord - scan back to the previous record
        // harder, since we have to scan backwards to find the start of the previous record
        // TODO: write a reverse fast strchr() clone
        private void _prevRecord(ref RecordLocator rloc) {
            byte[] search_buffer = new byte[PREV_RECORD_SCANSIZE];
            int read_result_size, read_size;

            if (!rloc.have_record) {
                throw new Exception("_prevRecord() called with rloc.have_record==false");
            }
            if (rloc.start_pos == 0) {
                // we can't go back before the beginning of our block, so if we reach it, we are done
                rloc.have_record = false;
                rloc.start_pos = 0;
                rloc.after_end_pos = 0;
                rloc.record = default(KeyValuePair<RecordKey,RecordUpdate>);
                return;
            }

            int check_up_to = rloc.start_pos - 1;  // stop short, or we'll try to redecode the same record we were handed
            int cur_stream_pos = Math.Max(0, rloc.start_pos - search_buffer.Length);
            while (cur_stream_pos >= 0) {            
                this.datastream.Seek(cur_stream_pos, SeekOrigin.Begin);
                read_size = (int)Math.Min(search_buffer.Length,check_up_to-cur_stream_pos);
                read_result_size = this.datastream.Read(search_buffer,0,read_size);

                int bufpos = read_result_size - 1;
                while (bufpos >= 0) {
                    
                    if (search_buffer[bufpos] == SegmentBlockBasicEncoder.END_OF_LINE) {
                        // decode record
                        int new_record_start = cur_stream_pos + bufpos + 1;
                       
                        this.datastream.Seek(new_record_start, SeekOrigin.Begin);
                        rloc.record = SegmentBlockBasicDecoder._decodeRecordFromBlock(this.datastream);
                        rloc.after_end_pos = (int) this.datastream.Position;
                        if (rloc.after_end_pos != rloc.start_pos) {
                            // if this decode didn't bring us to the start of the record we were
                            // ..handed, then something is WRONG!
                            throw new Exception("_prevRecord() INTERNAL scan error");
                        }
                        rloc.have_record = true;
                        rloc.start_pos = new_record_start;
                        return;                        
                    }
                    bufpos--;
                }
                // IF we got this far with cur_stream_pos == 0, we're at the start of the block!
                if (cur_stream_pos == 0) {
                    // TODO: use a crafty state switch to avoid the block duplicated here from above

                    // if we landed on the beginnng of the block, then decode the first record in the block
                    int new_record_start = 0;
                    this.datastream.Seek(new_record_start, SeekOrigin.Begin);
                    rloc.record = SegmentBlockBasicDecoder._decodeRecordFromBlock(this.datastream);
                    rloc.after_end_pos = (int)this.datastream.Position;
                    if (rloc.after_end_pos != rloc.start_pos) {
                        // if this decode didn't bring us to the start of the record we were
                        // ..handed, then something is WRONG!
                        throw new Exception("_prevRecord() INTERNAL scan error");
                    }
                    rloc.have_record = true;
                    rloc.start_pos = new_record_start;
                    return;
                }

                // backup to the next range                
                cur_stream_pos = Math.Max(0, cur_stream_pos - search_buffer.Length);
            };
            throw new Exception("_prevRecord() INTERNAL scan error, dropout");
        }

        // do a binary search, knowing we need to re-align to the record boundaries
        // everytime we pick a new cut point... this is efficient as long as our
        // records are reasonably small. For records that are large, we should
        // pick a different format to avoid all this scanning for startpoints. 

        private FindRecordResult _findRecord(IComparable<RecordKey> keytest, bool equal_is_after) {
            // read state
            byte[] search_buffer = new byte[FIND_RECORD_BUFSIZE];
            int read_result_size, read_size;
            int restart_count = 0;

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
                    {   // find out if we need to account for an odd-size split
                        int midpoint_pos_delta = endpos - startpos;
                        int midpoint_offset = 0xbeefed;
                        if ((midpoint_pos_delta & 1) == 1) {
                            midpoint_offset = (midpoint_pos_delta - 1) / 2;
                        } else {
                            midpoint_offset = midpoint_pos_delta / 2;
                        }
                        midpoint_pos = startpos + midpoint_offset;
                    }

                    if (midpoint_pos > endpos) {
                        midpoint_pos = startpos;
                    }
                    goto case mpss.FIND_REC_FORWARD;
                case mpss.FIND_REC_FORWARD:
                    restart_count = 0;
                    goto case mpss.FIND_REC_FORWARD_RESTART; // fall through
                case mpss.FIND_REC_FORWARD_RESTART:
                    restart_count++;
                    // scan forward until we find an END_OF_LINE record boundary
                    {                         
                        int cur_stream_pos = midpoint_pos;
                        do {
                            int bufpos = 0;
                            this.datastream.Seek(cur_stream_pos, SeekOrigin.Begin);
                            read_size = (int)Math.Min(search_buffer.Length,endpos-cur_stream_pos);
                            read_result_size = this.datastream.Read(search_buffer,0,read_size);
                            
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

                    if (restart_count > 1) {
                        throw new Exception("INTERNAL ERROR : SegmentBlockBasic._findRecord, restarted more than once!");
                    }
                    // ran out of datastream and didn't find END_OF_LINE!                         
                    // move to the beginning of the buffer and restart, 
                    midpoint_pos = startpos;
                    // but don't rescan that stuff we just saw between midpoint_pos and endpos
                    endpos = midpoint_pos+1;
                    goto case mpss.FIND_REC_FORWARD_RESTART;
                    // fyi - we considered bisecting the first half, but if the records are so big relative
                    //  to our midpoint that there were none in the second half, it's because our records
                    //  are really large relative to the range we are scanning, so it's safer just to
                    //  start at the BEGINNING of the whole range.                                         
                case mpss.HAVE_START:
                    // we have a record_start_stream_pos, so decode the record and test against the comparison
                    {
                        RecordLocator rloc;
                        int record_start_pos = eol_marker_pos + 1;
                        if (record_start_pos == endpos) {
                            // we found the end of the last record in the midpoint-region, 
                            // however, we're not sure where it starts, let's
                            // decode the record at the start of the region instead.
                            record_start_pos = startpos;
                        }
                        
                        // decode the record
                        this.datastream.Seek(record_start_pos, SeekOrigin.Begin);
                        rloc.record = SegmentBlockBasicDecoder._decodeRecordFromBlock(this.datastream);
                        rloc.start_pos = record_start_pos;
                        rloc.after_end_pos = (int)this.datastream.Position;
                        rloc.have_record = true;                        
                        
                        int compare_result = keytest.CompareTo(rloc.record.Key);
                        if (compare_result == 0) {
                            if (equal_is_after) {
                                compare_result = -1; // rec==keytest should be after
                            } else {
                                compare_result = 1;  // rec==keytest should be before
                            }

                        }
                        if (compare_result < 0) {  // record is AFTER keytest
                            result.after_keytest = rloc;
                            // we know this record is after us, so scan only before it
                            endpos = rloc.start_pos;
                        } else if (compare_result > 0) { // record is BEFORE keytest
                            result.before_keytest = rloc;
                            // we know this record is before us, so scan only after it
                            startpos = rloc.after_end_pos;
                        } 
                    }
                    goto case mpss.BISECT;
                default:
                    throw new Exception("unknown mpss state: " + state.ToString());
            } // end switch(mpss state)

        }

        public KeyValuePair<RecordKey, RecordUpdate> FindNext(IComparable<RecordKey> keytest, bool equal_ok) { 
            FindRecordResult result = _findRecord(keytest, equal_ok);
            if (result.after_keytest.have_record) {
                return result.after_keytest.record;
            } else {
                throw new KeyNotFoundException("SegmentBlockBasic: failed to find match : " + keytest.ToString());
            }            
        }
        public KeyValuePair<RecordKey, RecordUpdate> FindPrev(IComparable<RecordKey> keytest, bool equal_ok) {
            FindRecordResult result = _findRecord(keytest, !equal_ok);
            if (result.before_keytest.have_record) {
                return result.before_keytest.record;
            } else {
                throw new KeyNotFoundException("SegmentBlockBasic: failed to find match : " + keytest.ToString());
            }            
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanForward(IScanner<RecordKey> scanner) {
            IComparable<RecordKey> lowkeytest = scanner.genLowestKeyTest();
            IComparable<RecordKey> highkeytest = scanner.genHighestKeyTest();

            RecordLocator rloc = _findRecord(lowkeytest, true).after_keytest;
            
            while (rloc.have_record &&   // while we have a new record to test
                (highkeytest.CompareTo(rloc.record.Key) > 0) )  // and it's below the high key
            {
                if (scanner.MatchTo(rloc.record.Key)) {
                    yield return rloc.record;
                }
                _nextRecord(ref rloc);                  
            }
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> scanBackward(IScanner<RecordKey> scanner) {
            IComparable<RecordKey> lowkeytest = scanner.genLowestKeyTest();
            IComparable<RecordKey> highkeytest = scanner.genHighestKeyTest();

            RecordLocator rloc = _findRecord(highkeytest, true).before_keytest;

            while (rloc.have_record &&   // while we have a new record to test
                (lowkeytest.CompareTo(rloc.record.Key) < 0))  // and it's above the lowkey
            {
                if (scanner.MatchTo(rloc.record.Key)) {
                    yield return rloc.record;
                }
                _prevRecord(ref rloc);
            }
        }


    }

}