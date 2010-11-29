using System;
using System.IO;

namespace Bend
{

    // ---------------[ SimpleEncoder ]------------------------------
    //
    // byte[] escape_list = { 0x80, 0x81, 0x82 }  // be careful, the order in the escape list matters!!
    // byte escape_char = 0x81;
    // 
    // SimpleEncoder enc = new SimpleEncoder(escape_char, escape_list);
    //
    // byte[] input = { 1, 2, 3, 4, 0x80, 0x82 };
    // byte[] result = enc.encode(input);          // 1,2,3,4,(0x81 0x00), 0x81, 0x02)
    // 
    // byte[] dresult = enc.decode(result);        // 1,2,3,4,0x80,0x82

    public class SimpleEncoder
    {
        byte[] escape_list;
        byte escape_char;

        public SimpleEncoder(byte[] escape_list, byte escape_char) {
            this.escape_list = escape_list;
            this.escape_char = escape_char;
        }

        private static int char_pos(byte ch, byte[] char_list) {
            for (int x = 0; x < char_list.Length; x++) {
                if (ch == char_list[x]) {
                    return x;
                }
            }
            return -1; // not found
        }

        public byte[] encode(byte[] input) {
            MemoryStream ms = new MemoryStream();
            foreach (byte ch in input) {
                int chpos = char_pos(ch,escape_list);
                if (chpos == -1) {
                    // output the char
                    ms.WriteByte(ch);
                    
                } else {
                    // output the escape char and the index
                    ms.WriteByte(escape_char);
                    ms.WriteByte((byte)chpos);
                }               
            }
            return ms.ToArray();
        }

        public byte[] decode(byte[] input) {
            MemoryStream ms = new MemoryStream();
            int curpos = 0;
            while (curpos < input.Length) {
                byte ch = input[curpos];
                if (ch != escape_char) {
                    ms.WriteByte(ch);
                } else {
                    curpos++;
                    if (!(curpos < input.Length)) { throw new Exception("SimpleEncoder.decode() length error"); }
                    int index = input[curpos];
                    if (index > escape_list.Length) { throw new Exception("SimpleEncoder.decode() escape error"); }
                    ms.WriteByte(escape_list[index]);
                }
                curpos++; // next 
            }
            return ms.ToArray();
        }

    }

}

namespace BendTests
{



    using NUnit.Framework;
    using Bend;

    
    public partial class A00_UtilTest
    {
        [Test]
        public void T00_SimpleEncoder() {
            byte CH_SLASH = 47;
            byte CH_PLUS = 43;

            byte[] input = { 1, 2, 3, 4, 47, 5, 6 };
            byte[] expected_output = { 1, 2, 3, 4, 43, 0, 5, 6 };
            byte[] escape_list = { 47, 43 };
            byte escape_char = 43;

            SimpleEncoder enc = new SimpleEncoder(escape_list, escape_char);
            byte[] output = enc.encode(input);

            Assert.AreEqual(output, expected_output, "encoded form is incorrect");


            byte[] decoded_version = enc.decode(output);
            Assert.AreEqual(input, decoded_version, "decoded version does not match original");

        }
        [Test]
        public void T01_SimpleEncoder_UseOfSpecials() {
            byte[] chars = { 92, 43, 0 };

            byte[] escape_list = { 47, 43 };
            byte escape_char = 43;

            SimpleEncoder enc = new SimpleEncoder(escape_list, escape_char);
            byte[] output = enc.encode(enc.encode(chars));

            byte[] decoded_version = enc.decode(enc.decode(output));
            Assert.AreEqual(chars, decoded_version, "binary decoded version does not match original");
        }
    }


}
