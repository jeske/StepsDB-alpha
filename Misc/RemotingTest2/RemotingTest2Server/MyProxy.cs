using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


using System.Reflection;

using System.Runtime.Remoting.Proxies;
using System.Runtime.Remoting.Messaging;


namespace ConsoleApplication1 {
    public class MyProxy : RealProxy {
        Object proxyTarget;
        public MyProxy(Type iftype, Object obj)
            : base(iftype) {
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
}
