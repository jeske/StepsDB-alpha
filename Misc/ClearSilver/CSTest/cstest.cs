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
      h.setValue("hdf.loadpaths.0", ".");
      cs.parseFile("test.cst");

      // cs.parseString(" foo.1 = <?cs var:foo.1 ?> ");
      // cs.parseString("this is a big tesT............ this is a big tesT............ this is a big tesT............ this is a big tesT............ this is a big tesT............ this is a big tesT............ this is a big tesT............ this is a big tesT............ .");

      Console.WriteLine("render file");
      Console.WriteLine(cs.render());
      return 0;
   }
  


}
