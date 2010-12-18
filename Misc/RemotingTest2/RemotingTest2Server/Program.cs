using System;
using System.Collections.Generic;
using System.Collections;
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
    public class EnumeratorServerBatch<T> : IDisposable {
        IEnumerator<T> target;
        public EnumeratorServerBatch(IEnumerator<T> actual_enumerator) {
            target = actual_enumerator;
        }

        public List<T> getBatch() {
            Console.WriteLine("server getbatch");
            List<T> next_batch = new List<T>();
            int count = 0;
            while (target.MoveNext()) {
                next_batch.Add(target.Current);
                count++;
                if (count > 10) { break; }
            }
            return next_batch;
        }
        public void Reset() {
            target.Reset();
        }
        public void Dispose() {
            target.Dispose();
            target = null;
        }

    }

    public class EnumeratorClientBatch<T> : IEnumerator<T> {
        T cur;
        List<T> batch;
        EnumeratorServerBatch<T> batch_target;
        public EnumeratorClientBatch(EnumeratorServerBatch<T> target) {
            batch_target = target;
        }
        object IEnumerator.Current { get { return cur; } }
        public T Current { get { return cur; } }        
        public bool MoveNext() {
            if (batch == null || batch.Count() == 0) {
                batch = batch_target.getBatch();
                if (batch.Count() == 0) {
                    return false;
                }
            }

            cur = batch[0]; batch.RemoveAt(0);
            return true;
        }
        public void Reset() {
            batch = new List<T>();
            batch_target.Reset();
        }
        public void Dispose() {
            batch = null;
            batch_target.Dispose();
            batch_target = null;
        }
    }

    public class MyProxy : RealProxy {
        Object proxyTarget;
        public MyProxy(Type iftype, Object obj) : base(iftype) {
            proxyTarget = obj;
            Console.WriteLine("new MyProxy for Type: {0}  Obj:{1}", iftype, obj);
        }
        public override IMessage Invoke(IMessage message) {
            IMessage result = null;

            IMethodCallMessage methodCall = message as IMethodCallMessage;
            MethodInfo method = methodCall.MethodBase as MethodInfo;

            // Invoke 
            if (result == null) {
                if (proxyTarget != null) {
                    Console.WriteLine("proxy going to invoke: {0}", method.Name);                    
                    object callResult;
                    object actualresult;
                    bool make_proxy = true;

                    if (method.ReturnType.IsInterface) {
                        actualresult = method.Invoke(proxyTarget, methodCall.InArgs);

                        if (method.ReturnType.IsGenericType) {
                            // Console.WriteLine("** return value is generic type: {0}", method.ReturnType.GetGenericTypeDefinition());
                            if (method.ReturnType.GetGenericTypeDefinition() == (typeof(IEnumerator<>))) {
                                Console.WriteLine("** method returning IEnumerator<>, making BatchProxy");                           
                                Type[] args = method.ReturnType.GetGenericArguments();

                                Type srvbatchtype = typeof(EnumeratorServerBatch<>).MakeGenericType(args);
                                object srv = Activator.CreateInstance(srvbatchtype, actualresult);

                                Type clbatchtype = typeof(EnumeratorClientBatch<>).MakeGenericType(args);
                                object client = Activator.CreateInstance(clbatchtype, srv);
                                make_proxy = false;
                                actualresult = client;
                            } 
                        }

                        if (make_proxy) {
                            var newproxy = new MyProxy(method.ReturnType, actualresult);
                            callResult = newproxy.GetTransparentProxy();
                        } else {
                            callResult = actualresult;  
                        }
                    } else {                        
                    callResult = method.Invoke(proxyTarget, methodCall.InArgs);
                    }

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

    

    class Program {
        static void Main(string[] args) {

            MyProxy proxy = new MyProxy(typeof(IServer), new MyServer());
            IServer srvr = (IServer)proxy.GetTransparentProxy();
            foreach (var val in srvr.doSomething("aaa")) {
                Console.WriteLine("got: {0}", val);
            }
        }
    }
}
