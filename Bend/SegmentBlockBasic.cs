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

    class SegmentBlockBasicEncoder : ISegmentBlockEncoder
    {
        Stream output;

        static byte FIRST_SPECIAL = 0x80;
        static byte END_OF_LINE = 0x80;
        static byte ESCAPE_CHAR = 0x81;
        static byte KEY_VAL_SEP = 0x82;
        static byte ESCAPE_OFFSET =  (byte) (FIRST_SPECIAL - 1);
        static byte LAST_SPECIAL = 0x82;
        

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
        Stream input; // keep in mind, we don't want to share this, since it has only one seek pointer
        long startoffset;
        long length;
        long endoffset;


        // prepare to read from a stream
        public SegmentBlockBasicDecoder(Stream datastream, long startoffset, long length) {
            input = datastream;
            this.startoffset = startoffset;
            this.length = length;
            this.endoffset = startoffset + length;
        }

        public IEnumerable<KeyValuePair<RecordKey, RecordUpdate>> sortedWalk() {
            BufferedStream bs = new BufferedStream(input);
            bs.Seek(startoffset, SeekOrigin.Begin);
            bool at_endmarker = false;

            while (bs.Position < endoffset) {

                // accumulate the key
                List<byte> keystr = new List<byte>();
                // StringBuilder keystr = new StringBuilder();
                bool keydone = false;
                while (bs.Position < endoffset && !keydone) {
                    byte c = (byte)bs.ReadByte();
                    switch (c) {
                        case 0x80:   // end of line 
                            throw new Exception("reached end of line before keyvalue delimiter");
                        case 0x82:   // key value delimiter
                            keydone = true;
                            break;
                        case 0x81:
                            byte nc = (byte)bs.ReadByte();
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

                // accumulate the value
                StringBuilder valuestr = new StringBuilder();
                bool valuedone = false;
                while (bs.Position < endoffset && !valuedone) {
                    at_endmarker = false;
                    byte c = (byte)bs.ReadByte();
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
                RecordUpdate value = new RecordUpdate(RecordUpdateTypes.FULL, valuestr.ToString());
                Debug.WriteLine("scanning " + key.ToString() + " : " + value.ToString());
                yield return new KeyValuePair<RecordKey, RecordUpdate>(key, value);
            }


            if (bs.Position != endoffset || !at_endmarker) {
                Debug.WriteLine("sortedWalk() did not finish at end marker");
            }
        }



    }

}