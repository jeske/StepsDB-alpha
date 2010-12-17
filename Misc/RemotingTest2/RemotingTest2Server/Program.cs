using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Reflection;

using System.Runtime.Remoting.Proxies;
using System.Runtime.Remoting.Messaging;


// Reflection.Emit to create proxies
// http://dcooney.com/ViewEntry.aspx?ID=422

// .NET remoting a custom proxy
// http://www.csharphelp.com/2006/12/net-remoting-part4-a-custom-proxy/


// custom proxy generation using RealProxy
// http://blog.ngommans.ca/index.php?/archives/31-Custom-Proxy-Generation-using-RealProxy.html

namespace ConsoleApplication1 {
    public interface IServer {
        string doSomething(string data);
    }

    public class MyProxy : RealProxy {
        Object proxyTarget;
        public MyProxy(Type iftype, Object obj) : base(iftype) {
            proxyTarget = obj;
        }
        public override IMessage Invoke(IMessage message) {
            IMessage result = null;

            IMethodCallMessage methodCall = message as IMethodCallMessage;
            MethodInfo method = methodCall.MethodBase as MethodInfo;

            // Invoke 
            if (result == null) {
                if (proxyTarget != null) {
                    Console.WriteLine("proxy going to invoke: {0}", method.Name);
                    object callResult = method.Invoke(proxyTarget, methodCall.InArgs);
                    Console.WriteLine("proxy done Invoking: {0}", method.Name);
                    LogicalCallContext context = methodCall.LogicalCallContext;
                    result = new ReturnMessage(callResult, null, 0, context, message as IMethodCallMessage);                    
                } else {
                    NotSupportedException exception = new NotSupportedException("proxyTarget is not defined");
                    result = new ReturnMessage(exception, message as IMethodCallMessage);
                }
            }
            return result;
        }
    }

    public class MyServer : IServer {
        public string doSomething(string data) {
            Console.WriteLine("MyServer.doSomething({0})", data);
            return String.Format("executed:{0}", data);
        }
    }

    class Program {
        static void Main(string[] args) {

            MyProxy proxy = new MyProxy(typeof(IServer), new MyServer());
            IServer srvr = (IServer)proxy.GetTransparentProxy();
            srvr.doSomething("aaa");
        }
    }
}
