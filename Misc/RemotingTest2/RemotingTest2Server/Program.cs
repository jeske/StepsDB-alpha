using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;



// Reflection.Emit to create proxies
// http://dcooney.com/ViewEntry.aspx?ID=422

// .NET remoting a custom proxy
// http://www.csharphelp.com/2006/12/net-remoting-part4-a-custom-proxy/


// custom proxy generation using RealProxy
// http://blog.ngommans.ca/index.php?/archives/31-Custom-Proxy-Generation-using-RealProxy.html

namespace ConsoleApplication1 {
    public interface IServer {
        IEnumerable<string> doSomething(string data);
    }

    public class MyServer : IServer {
        public IEnumerable<string> doSomething(string data) {
            Console.WriteLine("MyServer.doSomething({0})", data);
            for (int x = 0; x < 5; x++) {
                Console.WriteLine("Yield {0}:{1}", x, data);
                yield return String.Format("executed:{0}:{1}", x, data);
            }            
        }
    }    
      

    class Program {
        static void Main(string[] args) {

            // instantiate my proxy
            MyProxy proxy = new MyProxy(typeof(IServer), new MyServer());
            IServer srvr = (IServer)proxy.GetTransparentProxy();

            // use the server
            foreach (var val in srvr.doSomething("aaa")) {
                Console.WriteLine("got: {0}", val);
            }
        }
    }
}
