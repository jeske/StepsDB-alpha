
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
        internal STRING* file;
        internal STRING* func;
        internal int lineno;
        /* internal use only */
        internal NEOERR* next;
    };


    internal class NeoErr {

        // Passing fixed size arrays
        // http://msdn.microsoft.com/en-us/library/s04yfy1s(v=vs.80).aspx
        // 
        // http://stackoverflow.com/questions/470135/how-do-i-marshal-cstring-via-p-invoke

        [DllImport("libneo")]
        // [return: MarshalAs(UnmanagedType.LPStr)] 
        private static unsafe extern void nerr_error_string(NEOERR *err,
            // this is really a byte-buffer that nerr_error_String is going to write into! 
                string info);

        // this free's the error chain
        [DllImport("libneo")]
        private static unsafe extern void nerr_ignore(NEOERR** err);

        internal unsafe static void hNE(NEOERR* err) {
            if ((IntPtr)err == (IntPtr)0) {
                return; // no error
            }
            // would be nice if we could get nerr_error_string to work...


            // get as much as we can out of the neoerr structure
            _neo_err info = (_neo_err)Marshal.PtrToStructure((IntPtr)err, typeof(_neo_err));
            string csfilename = Marshal.PtrToStringAnsi((IntPtr)info.file);
            string reason = String.Format("NeoErr: {0} {1}:{2} {3}",
                info.error,
                csfilename, info.lineno,
                info.desc);

            // free the NEOERR structure
            nerr_ignore(&err);          

            // throw a real exception
            throw new Exception(reason);                
        }
    }

}