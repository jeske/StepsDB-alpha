
using System;
using System.Runtime.InteropServices;
using System.Text;

// PInvoke Tutorial
// http://msdn.microsoft.com/en-us/library/aa288468(v=vs.71).aspx

namespace Clearsilver {

    // these allocation functions should be used when we expect the Clearsilver C-dll
    // to take ownership of memory.... this assures the same allocator/decallocator is used

    internal class NeoUtil {
        [DllImport("libneo")]
        internal static extern IntPtr neo_malloc(int length);

        [DllImport("libneo")]
        internal static extern void neo_free(IntPtr buf);        

    }

}