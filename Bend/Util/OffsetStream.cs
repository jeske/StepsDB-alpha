// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.IO;



namespace Bend
{
    // this is a read-only stream, maybe we should be a StreamReader
    public class OffsetStream : Stream    
    {
        Stream ostream;
        long os_offset;
        long os_length;
        public OffsetStream(Stream originalStream, long offset,long length) {
            if (originalStream == null) {
                throw new Exception("null source stream for new OffsetStream()");
            }
            this.ostream = originalStream;
            this.os_offset = offset;
            this.os_length = length;
            this.Position = 0; // reset to beginning (?)
        }

        public OffsetStream(Stream originalStream, long offset) 
            : this (originalStream, offset, originalStream.Length - offset) { }

        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return true; } }
        public override bool CanWrite { get { return false; } }

        public override void Flush() { ostream.Flush(); }
        public override long Length { get { return os_length; } }

        public override long Position {
            get {
                return ostream.Position - os_offset;
            }
            set {
                ostream.Position = os_offset + value;
            }
        }

        public override void SetLength(long value) {
            throw new Exception("setlength not allowed on read-only OffsetStream");
        }
        public override long Seek(long offset, SeekOrigin origin) {
            long new_offset = ostream.Position + offset;

            switch (origin) {
                case SeekOrigin.Begin:
                    new_offset = os_offset + offset;
                    goto case SeekOrigin.Current;
                case SeekOrigin.End:
                    new_offset = os_offset + os_length + offset;
                    goto case SeekOrigin.Current;
                case SeekOrigin.Current:
                    if ((new_offset < os_offset) || (new_offset > (os_offset + os_length))) {
                        throw new Exception("OffsetStream.Seek() outside offset stream limits" + new_offset);
                    }
                    return ostream.Seek(new_offset, SeekOrigin.Begin) - os_offset;
                    break;
                default:
                    throw new Exception("unknown SeekOrigin value" + origin.ToString());
                    break;
            }
        }

        public override int Read(byte[] buffer, int offset, int count) {
            // TODO: we should rangecheck our current seekpointer against our offsets and count
            return ostream.Read(buffer, offset, count);
        }
        public override void Write(byte[] buffer, int offset, int count) {
            ostream.Write(buffer, offset,count);
        }


    }
}

namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    public partial class A00_UtilTest
    {
        [Test]
        public void T00_TestOffsetStream() {
            MemoryStream ms = new MemoryStream();
            byte[] testdata = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

            // write the testdata
            ms.Write(testdata, 0, testdata.Length);

            // read it back through my offset stream
            long O_LENGTH = 2;;
            long O_OFFSET = 2;
            OffsetStream os = new OffsetStream(ms, O_OFFSET, O_LENGTH);
            Assert.AreEqual(O_LENGTH, os.Length, "test .Length");

            Assert.AreEqual(0,os.Seek(0, SeekOrigin.Begin), "seek to beginning of my offset range");
            Assert.AreEqual(0, os.Position, "test .Position");

            Assert.AreEqual(2, os.ReadByte(), "read byte pos 2");
            Assert.AreEqual(3, os.ReadByte(), "read byte pos 3");

        }
    }
}