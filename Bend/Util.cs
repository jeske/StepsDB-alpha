using System;

#if false

public static class Helper
{

    // forum topic on this when I'm done
    // http://forums.msdn.microsoft.com/en-US/csharplanguage/thread/cfb98802-d1ec-4d46-b346-e46a8885daff/

    unsafe static public int FastScanForByte(byte[] data,byte b) {
        if (sizeof(uint) != 4) {
            throw new Exception("fast scan only works with 4-byte uint");
        }
        // pin down the byte buffer
        fixed (byte* startaddr = data) {
            byte* addr = startaddr;
            byte* endaddr = addr + data.Length;
                        
            // first test the unaligned initial bytes
            while (addr < endaddr && (((uint)addr & 0x03) != 0)) {
                if (*addr == b) {
                    return (int)((uint)addr - (uint)startaddr);
                }
                addr ++;
            }

            // now use fast aligned reads
            // http://sourceware.org/cgi-bin/cvsweb.cgi/~checkout~/libc/string/strchr.c?rev=1.1.2.2&content-type=text/plain&cvsroot=glibc
            /*  Bits 31, 24, 16, and 8 of this number are zero.  Call these bits
                the "holes."  Note that there is a hole just to the left of
                each byte, with an extra at the end:
    
                bits:  01111110 11111110 11111110 11111111
                bytes: AAAAAAAA BBBBBBBB CCCCCCCC DDDDDDDD

                The 1-bits make sure that carries propagate to the next 0-bit.
                The 0-bits provide holes for carries to fall into.  */

            uint magic_bits = 0x7efefeff;
            uint mask = (uint)(b | (b << 8));
            mask |= mask << 16;

            uint* aligned_addr    = (uint*) addr;
            uint* aligned_endaddr = (uint*) endaddr; // TODO:align this ?
            uint testval;
            while (aligned_addr < aligned_endaddr) {
                testval = *aligned_addr & mask;
                
                aligned_addr++;
            }
        }


        return -1; // NOT FOUND
    }

}

#endif