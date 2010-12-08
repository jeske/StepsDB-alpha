using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Server;

namespace ThriftClientTest
{
    class ThirftClient
    {
        static void Main(string[] args) {
            var transport = new TSocket("localhost", 7911);
            var protocol = new TBinaryProtocol(transport);
            var client = new UserStorage.Client(protocol);

            transport.Open();
            var userdata = new UserProfile();
            userdata.Name = "Jeske";

            client.store(userdata);
            transport.Close();
        }
    }
}
