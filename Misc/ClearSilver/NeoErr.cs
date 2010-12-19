
using System;
using System.Runtime.InteropServices;
using System.Text;

// PInvoke Tutorial
// http://msdn.microsoft.com/en-us/library/aa288468(v=vs.71).aspx

namespace Clearsilver {


    // from Clearsilver neo_err.h
    [StructLayout(LayoutKind.Sequential, Pack = 1, CharSet=CharSet.Ansi)]
    internal unsafe struct _neo_err {
        internal int error;
        internal int err_stack;
        internal int flags;
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 256)]
        internal string desc;
        internal STR* file;
        internal STR* func;
        internal int lineno;
        /* internal use only */
        internal NEOERR* next;
    };

    [StructLayout(LayoutKind.Sequential, Pack = 1, CharSet = CharSet.Ansi)]
    internal unsafe struct STRING {
        public STR* buf;
        public int len;
        public int max;
    }


    internal class NeoErr {

        // Passing fixed size arrays
        // http://msdn.microsoft.com/en-us/library/s04yfy1s(v=vs.80).aspx
        // 
        // http://stackoverflow.com/questions/470135/how-do-i-marshal-cstring-via-p-invoke

        [DllImport("libneo")]        
        private static unsafe extern void nerr_error_string(NEOERR *err, STRING* info);
        [DllImport("libneo")]
        private static unsafe extern void nerr_error_traceback(NEOERR* err, STRING* info);

        // this free's the error chain
        [DllImport("libneo")]
        private static unsafe extern void nerr_ignore(NEOERR** err);

        internal unsafe static void hNE(NEOERR* err) {
            if ((IntPtr)err == (IntPtr)0) {
                return; // no error
            }
            // would be nice if we could get nerr_error_string to work...
            IntPtr buf = (IntPtr)0;
            byte[] empty_string = new byte[]{ 0 };
            string msg = null;
            STRING neo_string;
            try {            
             buf = Marshal.AllocHGlobal(4000);
             Marshal.Copy(empty_string, 0, buf, 1);
             neo_string.buf = (STR *)buf;
             neo_string.len = 0;
             neo_string.max = 4000;

             // nerr_error_traceback(err, &neo_string);
             nerr_error_string(err, &neo_string);
             msg = Marshal.PtrToStringAnsi(buf);
            } finally {
                if (buf != (IntPtr)0) {
                    Marshal.FreeHGlobal(buf);
                }
            }

            // get as much as we can out of the neoerr structure
            _neo_err info = (_neo_err)Marshal.PtrToStructure((IntPtr)err, typeof(_neo_err));
            string csfilename = Marshal.PtrToStringAnsi((IntPtr)info.file);
            
            string reason = String.Format("NeoErr: {0}", msg);

            // free the NEOERR structure
            nerr_ignore(&err);          

            // throw a real exception
            throw new Exception(reason);                
        }
    }

}