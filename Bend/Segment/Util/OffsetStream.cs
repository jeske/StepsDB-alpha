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
                long newpos = ostream.Position - os_offset;
                return newpos;
            }
            set {
                ostream.Position = os_offset + value;
            }
        }

        public override void SetLength(long value) {
            throw new Exception("setlength not allowed on read-only OffsetStream");
        }
        public override long Seek(long offset, SeekOrigin origin) {
            long new_offset = ostream.Position + offset; // SeekOrigin.Current

            switch (origin) {
                case SeekOrigin.Begin:
                    new_offset = os_offset + offset;
                    goto case SeekOrigin.Current;
                case SeekOrigin.End:
                    new_offset = os_offset + os_length + offset;
                    goto case SeekOrigin.Current;
                case SeekOrigin.Current:
                    if ((new_offset < os_offset) || (new_offset >= (os_offset + os_length))) {
                        throw new Exception(
                            "OffsetStream.Seek() outside offset stream limits" +
                            String.Format(", parms({0},{1})",offset,origin.ToString()) +
                            String.Format(", new_offset({0}) os_offset({1}) os_length({2})", new_offset, os_offset,os_length));
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
            int num_read = ostream.Read(buffer, offset, count);
            return num_read;
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
        public static byte[] testdata = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        public static long O_LENGTH = 2;
        public static long O_OFFSET = 4;


        public static void testStream(Stream os) {
            // read it back through my offset stream            
            Assert.AreEqual(O_LENGTH, os.Length, "test .Length");

            Assert.AreEqual(0, os.Seek(0, SeekOrigin.Begin), "seek to beginning of my offset range");
            Assert.AreEqual(0, os.Position, "test .Position");

            Assert.AreEqual(4, os.ReadByte(), "read byte pos 4");
            Assert.AreEqual(1, os.Position, "test .Position == 1");
            Assert.AreEqual(5, os.ReadByte(), "read byte pos 5");
            Assert.AreEqual(2, os.Position, "test .Position == 2");

            Assert.AreEqual(1, os.Seek(-1, SeekOrigin.Current), "seek to beginning of my offset range AGAIN");
            Assert.AreEqual(1, os.Position, "test .Position AGAIN");

            Assert.AreEqual(5, os.ReadByte(), "read byte pos 5 AGAIN");

        }

        [Test]
        public void T00_TestOffsetStream() {
            MemoryStream ms = new MemoryStream(testdata);            

            OffsetStream os = new OffsetStream(ms, O_OFFSET, O_LENGTH);
            testStream(os);

        }

    }

    [TestFixture]
    public class ZZ_TODO_OffsetStream_BufferedStream_Bug
    {

        [Test]
        public void T00_TestOffsetStreamWrappedInBufferedStream() {
            MemoryStream ms = new MemoryStream(A00_UtilTest.testdata);
            OffsetStream os = new OffsetStream(ms, A00_UtilTest.O_OFFSET, A00_UtilTest.O_LENGTH);

            BufferedStream bs = new BufferedStream(os);
            A00_UtilTest.testStream(bs);
        }
    }

}