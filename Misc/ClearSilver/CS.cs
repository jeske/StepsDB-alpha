
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
       [MarshalAs(UnmanagedType.LPStr)] 
        string name,
       [MarshalAs(UnmanagedType.LPStr)] 
        string value);

  // char* hdf_get_value (HDF *hdf, char *name, char *defval)

  [DllImport("libneo")]
  // [return: MarshalAs(UnmanagedType.LPStr)] 
  private static unsafe extern STR* hdf_get_value(HDF *hdf,
       [MarshalAs(UnmanagedType.LPStr)] 
        string name,
       [MarshalAs(UnmanagedType.LPStr)] 
        string defval);

  // NEOERR* hdf_dump (HDF *hdf, char *prefix);

  [DllImport("libneo", EntryPoint="hdf_dump")]
  private static extern void hdf_dump(
       HDF *hdf,
       [MarshalAs(UnmanagedType.LPStr)]
         string prefix);

  // HDF* hdf_get_obj (HDF *hdf, char *name)

  [DllImport("libneo", EntryPoint="hdf_get_obj")]
  private static extern HDF* hdf_get_obj(
     HDF *hdf, 
       [MarshalAs(UnmanagedType.LPStr)]
     string name);


  // -----------------------------------------------------------

    internal unsafe HDF *hdf_root;

    public Hdf() {
      fixed (HDF **hdf_ptr = &hdf_root) {          
	        hdf_init(hdf_ptr);            
      }

      Console.WriteLine("Hdf.Hdf() hdf_root = {0}",(int)hdf_root);
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
     Console.WriteLine("CSt.Cst() hdf_root = {0}", (int)hdf.hdf_root);
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
   extern static unsafe NEOERR *cs_render (CSPARSE *parse, 
           void *ctx, 
           [MarshalAs(UnmanagedType.FunctionPtr)]
           CSOUTFUNC cb);

   // http://www.gamedev.net/community/forums/topic.asp?topic_id=270670


   [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
   private unsafe delegate NEOERR* CSOUTFUNC(void* ctx, STR* more_bytes);

   private class OutputBuilder {
      private string output = "";
       
      public unsafe NEOERR* handleOutput(void* ctx, STR* more_bytes) {
           // add the more_bytes to the current string buffer
          Console.WriteLine("handleOutput called {0:X} {1:X}", (IntPtr)ctx, (IntPtr)more_bytes);
          
          string data = Marshal.PtrToStringAnsi((IntPtr)more_bytes);
          Console.WriteLine("datalen = {0}", data.Length);
          Console.WriteLine("data: " + data);

          output += data;
           
          return null;
      }
      public string result() {
         return output;
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
