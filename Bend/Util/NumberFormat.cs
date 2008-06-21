// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;

using NUnit.Framework;

namespace Bend
{

    // LSD - Lexographically Sortable ascii Decimal (length extended)
    //  10 -> 00010
    // 200 -> 00200
    
    class Lsd {

        public static byte[] numberToLsd(int num,int pad_to_digits) {
            List<byte> builder = new List<byte>();

            // build the LSBs
            int digit;
            while (num >= 10) {
                num = Math.DivRem(num, 10, out digit);
                builder.Insert(0,(byte)((int)'0' + digit));
            }
            builder.Insert(0,(byte)((int)'0' + num));
            int digit_count = builder.Count;
            if (digit_count > pad_to_digits) {
                throw new Exception(String.Format("number {0} too big to encode in {1} digits", num, pad_to_digits));
            }

            // zero pad the number
            while (digit_count < pad_to_digits) {
                builder.Insert(0, (byte)'0');
                digit_count++;
            }
            return builder.ToArray();
        }

        public static uint lsdToNumber(byte[] bytes) {
            int number = 0;
            int pos = 0;
            
            // skip zero pad
            while (pos < bytes.Length && bytes[pos] == '0') {
                pos++;
            }

            // multiply and grab the next digit
            while (pos < bytes.Length) {
                // verify char in range
                byte cur = bytes[pos];
                if (cur < '0' || cur > '9') { 
                    throw new Exception("invalid char in lsdToNumber: " + cur + " " + new String((char)cur,1)); 
                }
                number = number * 10;
                number = number + (cur - (int)'0');
                pos++;
            }
            return (uint)number;
        }
    }


    [TestFixture]
    public class TestLsd
    {
        public String ToHexString(byte[] bytes) {
            String str = "";
            foreach (byte b in bytes) {
                str = str + ((int)b).ToString() + " ";
            }
            return str;
        }
        [Test]
        public void TestEncodeDecode() {
            int[] testnumbers = { 0, 1, 2, 14, 23, 99, 105, 4020, 82925, 199292, 2992022 };
            
            int encode_length = 10;
            for (int i = 0; i < testnumbers.Length; i++) {
                byte[] encodednum = Lsd.numberToLsd(testnumbers[i], encode_length);
                Assert.AreEqual(encode_length,encodednum.Length, "encoded length not correct");
                Assert.AreEqual(testnumbers[i], Lsd.lsdToNumber(encodednum), "decode not equal {0}", ToHexString(encodednum));
            }
        }

        [Test]
        public void TestInvalidCharacter() {
            byte[] test = { (byte)'0', (byte)'1', (byte)':', (byte)'3' };
            bool err = false;
            try {
                uint num = Lsd.lsdToNumber(test);
            } catch {
                err = true;
            }
            Assert.AreEqual(true, err, "invalid character should throw exception");
        }
    }

}