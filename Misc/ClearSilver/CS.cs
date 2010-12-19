
using System;
using System.Runtime.InteropServices;




// PInvoke Tutorial
// http://msdn.microsoft.com/en-us/library/aa288468(v=vs.71).aspx

namespace Clearsilver {

// opaque types
internal unsafe struct HDF {};
internal unsafe struct STR {};
internal unsafe struct NEOERR {};


public unsafe class Hdf : IDisposable {

  [DllImport("libneo", EntryPoint="hdf_init")]
  private static extern unsafe NEOERR* hdf_init(HDF **foo);

  // NEOERR* hdf_set_value (HDF *hdf, char *name, char *value)
  [DllImport("libneo")]
  private static unsafe extern NEOERR* hdf_set_value(HDF *hdf,
       [MarshalAs(UnmanagedType.LPStr)] string name,
       [MarshalAs(UnmanagedType.LPStr)] string value);

  // char* hdf_get_value (HDF *hdf, char *name, char *defval)

  [DllImport("libneo")]
  // [return: MarshalAs(UnmanagedType.LPStr)] 
  private static unsafe extern STR* hdf_get_value(HDF *hdf,
       [MarshalAs(UnmanagedType.LPStr)] string name,
       [MarshalAs(UnmanagedType.LPStr)] string defval);

  // NEOERR* hdf_dump (HDF *hdf, char *prefix);

  [DllImport("libneo", EntryPoint="hdf_dump")]
  private static extern void hdf_dump( HDF *hdf, [MarshalAs(UnmanagedType.LPStr)] string prefix);

  // HDF* hdf_get_obj (HDF *hdf, char *name)

  [DllImport("libneo", EntryPoint="hdf_get_obj")]
  private static extern HDF* hdf_get_obj(HDF *hdf,  [MarshalAs(UnmanagedType.LPStr)] string name);

  [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
  private unsafe delegate NEOERR* HDFFILELOAD(void* ctx, HDF* hdf, STR* filename, STR **contents);
  // contents is a malloced copy of the file which the parser will own and free


  // -----------------------------------------------------------

    internal unsafe HDF *hdf_root;

    public Hdf() {
      fixed (HDF **hdf_ptr = &hdf_root) {          
	        hdf_init(hdf_ptr);            
      }

      // Console.WriteLine("Hdf.Hdf() hdf_root = {0}",(int)hdf_root);
    }

    // this is used by callbacks and other elements that get an HDF pointer
    internal unsafe Hdf(HDF* from_hdf) {
        hdf_root = from_hdf;
    }

    public void setValue(string name,string value) {        
         hdf_set_value(hdf_root,name,value);         
    }
    public string getValue(string name,string defvalue) {        
        STR *x = hdf_get_value(hdf_root,name,defvalue);
        // this allows us to marshall out the string value without freeing it
        string value = Marshal.PtrToStringAnsi((IntPtr)x);        
        return value;
    }

#if false
    public void test() {
       hdf_set_value(hdf_root,"b","1");
       // hdf_read_file(hdf_root,"test.hdf");
       // Console.WriteLine("b ", hdf_get_value(hdf_root,"b","5"));
       hdf_dump(hdf_root,null);

       // HDF *n = hdf_get_obj(hdf_root,"b");
       // Console.WriteLine("object name {0}", 
       // Marshal.PtrToStringAnsi((IntPtr)n->name));
    }
#endif

    // cleanup the unmanaged data when we are freed
    [DllImport("libneo")]
    extern static unsafe void hdf_destroy(HDF** hdf);
    private unsafe void hdfDestroy() {
        fixed (HDF** phdf = &hdf_root) {
            hdf_destroy(phdf);
        }
    }

    public void Dispose() {
        this.hdfDestroy();
    }

};

unsafe struct CSPARSE {};

public class CSTContext : IDisposable {
   unsafe CSPARSE *csp;
   unsafe public CSTContext(Hdf hdf) {
     fixed (CSPARSE **csp_ptr = &csp) {
       cs_init(csp_ptr, hdf.hdf_root);       
     }
     // Console.WriteLine("CSt.Cst() hdf_root = {0}", (int)hdf.hdf_root);
   } 

   [DllImport("libneo")]
   extern static unsafe NEOERR *cs_init (CSPARSE **parse, HDF *hdf);

   public unsafe void parseFile(string filename) {
       NeoErr.hNE(cs_parse_file(csp, filename));
   }

   [DllImport("libneo")]
   extern static unsafe NEOERR *cs_parse_file (CSPARSE *parse, 
       [MarshalAs(UnmanagedType.LPStr)] 
       string path);

   [DllImport("libneo")]
   extern static unsafe NEOERR *cs_parse_string (CSPARSE *parse,       
                    STR* buffer, 
                    int buf_len);

   public unsafe void parseString(string data) {       
       IntPtr buffer = Marshal.StringToHGlobalAnsi(data);
       NeoErr.hNE(cs_parse_string(csp, (STR*) buffer, data.Length));
   }

   //  NEOERR *cs_render (CSPARSE *parse, void *ctx, CSOUTFUNC cb);
   //  typedef NEOERR* (*CSOUTFUNC)(void *ctx, char *more_str_bytes);

   [DllImport("libneo")]
   extern static unsafe NEOERR *cs_render (CSPARSE *parse, void *ctx, 
           [MarshalAs(UnmanagedType.FunctionPtr)] CSOUTFUNC cb);

   // about calling convention Cdecl
   // http://www.gamedev.net/community/forums/topic.asp?topic_id=270670

   [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
   private unsafe delegate NEOERR* CSOUTFUNC(void* ctx, STR* more_bytes);

   // about GCHandle and pinning delegates
   // http://blogs.msdn.com/b/cbrumme/archive/2003/05/06/51385.aspx

   [DllImport("libneo")]
   extern static unsafe void cs_register_fileload(CSPARSE* csp, void* ctx,
           [MarshalAs(UnmanagedType.FunctionPtr)] CSFILELOAD cb);

   [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
   private unsafe delegate NEOERR* CSFILELOAD(void* ctx, HDF* hdf, STR* filename, STR** contents);
   // contents is a malloced copy of the file which clearsilver will own and free

   private class OutputBuilder {
      private string output = "";
       
      public unsafe NEOERR* handleOutput(void* ctx, STR* more_bytes) {
           // add the more_bytes to the current string buffer
          // Console.WriteLine("handleOutput called {0:X} {1:X}", (IntPtr)ctx, (IntPtr)more_bytes);          
          string data = Marshal.PtrToStringAnsi((IntPtr)more_bytes);
          // Console.WriteLine("datalen = {0}", data.Length);
          // Console.WriteLine("data: " + data);
          output += data;
           
          return null;
      }
      public string result() {
         return output;
      }
   }

   public delegate byte[] loadFileDelegate(Hdf hdf, string filename);
   loadFileDelegate cur_delegate;
   unsafe CSFILELOAD thunk_delegate;  // we have to hold onto the delegate to make sure the pinned thunk sticks around
   private unsafe NEOERR* csFileLoad(void* ctx, HDF* raw_hdf, STR* pFilename, STR** contents) {
       // Console.WriteLine("csFileLoad delegate called");
       IntPtr buf = IntPtr.Zero;
       try {
           Hdf hdf = new Hdf(raw_hdf);
           string filename = Marshal.PtrToStringAnsi((IntPtr)pFilename);
           byte[] data = cur_delegate(hdf, filename);
           byte[] end_null = new byte[] { 0 };
           buf = Marshal.AllocHGlobal(data.Length + 1); // +1 so we can force a null terminate
           Marshal.Copy(data, 0, buf, data.Length);
           Marshal.Copy(end_null, 0, buf + data.Length, 1); // write the end_null
           *contents = (STR*)buf;
       } catch (Exception e) {
           // Console.WriteLine("csFileLoad Thunk Exception + " + e);
           // should return a neo error
           if (buf != IntPtr.Zero) {
               Marshal.FreeHGlobal(buf);
           }
           return NeoErr.nERR(e.ToString());
       } 
       return (NEOERR*) IntPtr.Zero;
   }

   public unsafe void registerFileLoad(loadFileDelegate fn) {
       if (fn != null) {
           // set the fileload handler
           cur_delegate = fn;
           thunk_delegate = new CSFILELOAD(csFileLoad);

           cs_register_fileload(csp, null, thunk_delegate);
       } else {
           // clear the fileload handler
           cs_register_fileload(csp, null, null);
           cur_delegate = null;
           thunk_delegate = null;
       }
   }

   public unsafe string render() {
     OutputBuilder ob = new OutputBuilder();      
     NeoErr.hNE(cs_render(csp, null, new CSOUTFUNC(ob.handleOutput)));
     return ob.result();
   }

   [DllImport("libneo")]
   extern static unsafe void cs_destroy (CSPARSE **parse);
   private unsafe void csDestroy() {
       fixed (CSPARSE **pcsp = &csp) {
           cs_destroy(pcsp);
       }
   }

   public void Dispose() {
       this.csDestroy();
   }
};


} // namespace Clearsilver
