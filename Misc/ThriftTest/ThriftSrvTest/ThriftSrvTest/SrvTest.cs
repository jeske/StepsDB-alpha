using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Server;


/* 
 * http://incubator.apache.org/thrift/
  
http://www.markhneedham.com/blog/2008/08/29/c-thrift-examples/
  
http://skorage.org/2009/03/08/simple-thrift-tutorial/
 */

namespace ThriftSrvTest
{
    public class UserStorageImpl : UserStorage.Iface
    {
        public void store(UserProfile user) {
            Console.WriteLine(user.Name);
        }

        public UserProfile retrieve(int uid) {
            return new UserProfile{Name="Tom"};
        }
    }

    class Program
    {
        static void Main(string[] args) {
             TServerSocket serverTransport = new TServerSocket(7911);


             UserStorage.Processor processor = new UserStorage.Processor(new UserStorageImpl());
             // var protFactory = new TBinaryProtocol.Factory(true, true);
             var server = new TThreadPoolServer(processor, serverTransport);
             Console.WriteLine("Starting server on port 7911 ...");
             server.Serve();
        }
    }
}
