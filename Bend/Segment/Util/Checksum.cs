// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;

using NUnit.Framework;

namespace Bend
{

    public static partial class Util
    {

        public sealed class Crc16
        {
            const ushort polynomial = 0xA001;
            ushort[] table = new ushort[256];

            public ushort ComputeChecksum(byte[] bytes) {
                ushort crc = 0;
                for (int i = 0; i < bytes.Length; i++) {
                    byte index = (byte)(crc ^ bytes[i]);
                    crc = (ushort)((crc >> 8) ^ table[index]);
                }
                return crc;
            }

            public byte[] ComputeChecksumBytes(byte[] bytes) {
                ushort crc = ComputeChecksum(bytes);
                return new byte[] { (byte)(crc >> 8), (byte)(crc & 0x00ff) };
            }

            public Crc16() {
                ushort value;
                ushort temp;
                for (ushort i = 0; i < table.Length; i++) {
                    value = 0;
                    temp = i;
                    for (byte j = 0; j < 8; j++) {
                        if (((value ^ temp) & 0x0001) != 0) {
                            value = (ushort)((value >> 1) ^ polynomial);
                        }
                        else {
                            value >>= 1;
                        }
                        temp >>= 1;
                    }
                    table[i] = value;
                }
            }
            public static Crc16 Instance {
                get {
                    return Nested.instance;
                }
            }
            class Nested
            {
                // explicit constructor to tell C# compiler not to mark as beforefieldinit
                static Nested() { }
                internal static readonly Crc16 instance = new Crc16();
            }
        }

    }

}
namespace BendTests {
    using Bend;

    public partial class A00_UtilTest
    {
        [Test]
        public void T00_Crc16() {
            byte[] testdata1 = { 0x80, 0x12, 0x14, 0x18 };
            byte[] testdata2 = { 0x80, 0x12, 0x14, 0x18 , 0x11, 0x99 };
            byte[] testdata3 = { 0x80, 0x12, 0x14, 0x18 };
            UInt16 chk1 = Util.Crc16.Instance.ComputeChecksum(testdata1);
            UInt16 chk2 = Util.Crc16.Instance.ComputeChecksum(testdata2);
            UInt16 chk3 = Util.Crc16.Instance.ComputeChecksum(testdata3);

            Assert.AreEqual(chk1, chk3, "checksums for the same data should be the same");
            Assert.AreNotEqual(chk1, chk2, "checksums for different data should be different");
        }
    }


}