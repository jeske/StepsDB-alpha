// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

// This uses SharpZipLib...
//
// we should also make a version that uses LZO.NET, as it claims to be much faster
//   http://lzo-net.sourceforge.net/
// 


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

using ICSharpCode.SharpZipLib.Zip.Compression;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Zip.Compression.Streams;

namespace Bend
{

    // ---------------[ SegmentBlockCompressedEncoder ]--------------------
    // 
    // This nests another encoder inside itself. We'll start by nesting
    // BasicBlock, but we could nest any format.
    //
    // Our goal is to bound the uncompressed size, since we need to be able
    // to quickly uncompress the block. 

    // TODO: change this as a streaming gzip, instead of writing it all to a memory
    //     stream first and then shoving it through gzip second.

    class SegmentBlockCompressedEncoder : ISegmentBlockEncoder
    {
        ISegmentBlockEncoder subenc;
        MemoryStream uncompressed_output;
        Stream output = null;

        public SegmentBlockCompressedEncoder(ISegmentBlockEncoder nested_encoder) {
            this.subenc = nested_encoder;
        }

        public void setStream(Stream output) {
            this.output = output;
            uncompressed_output = new MemoryStream();
            this.subenc.setStream(uncompressed_output);
        }

        public void add(RecordKey key, RecordUpdate data) {
            subenc.add(key, data);
        }

        public void flush() {
            byte[] uncompressed_bytes = uncompressed_output.ToArray();
            
            // write uncompressed size to output
            new BinaryWriter(output).Write((UInt32)uncompressed_bytes.Length);

            // write the compressed data
            GZipOutputStream zipstream = new GZipOutputStream(this.output);
            zipstream.SetLevel(1); // 0 is no compression, 9 is best compression (slowest)
            zipstream.Write(uncompressed_bytes, 0, uncompressed_bytes.Length);
            zipstream.Finish();            
        }
        
    }


    class SegmentBlockCompressedDecodeStage
    {
        public static BlockAccessor decode(BlockAccessor block) {
            byte[] data = new byte[block.Length];
            if (block.Read(data,0,(int)block.Length) != block.Length) {
                throw new Exception("BlockAccessor partial read");
            }
            MemoryStream ms = new MemoryStream(data);
            BinaryReader reader = new BinaryReader(ms);
            UInt32 uncompressed_length = reader.ReadUInt32();
            byte[] uncompressed_data = new byte[uncompressed_length];
            GZipInputStream uncompressed_stream = new GZipInputStream(ms);

            if (uncompressed_stream.Read(uncompressed_data, 0, (int)uncompressed_length)
                != uncompressed_length) {
                throw new Exception("GZipInputStream partial read");
            }
                        
            return new BlockAccessor(uncompressed_data);
        }
    }

}


