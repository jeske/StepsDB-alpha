using System;
using System.Runtime.InteropServices;

using Clearsilver;

public class CSTest {
   public static int Main(string[] argv) {
     
      Hdf h = new Hdf();

      h.setValue("foo.1","1");
      h.setValue("foo.2","2");
      Console.WriteLine("foo.2 = {0}", h.getValue("foo.2","def"));

      CSTContext cs = new CSTContext(h);
      
      Console.WriteLine("parsing file");
      cs.parseFile("test.cst");

      Console.WriteLine("render file");
      Console.WriteLine(cs.render());
      return 0;
   }
  


}
