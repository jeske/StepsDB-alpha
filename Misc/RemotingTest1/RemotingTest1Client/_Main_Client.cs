using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Security.Permissions;

using RemotingTest1Srv;

// http://aviadezra.blogspot.com/2008/06/code-snippets-remoting-client-server_19.html

namespace RemotingTest1Client {
    class _Main_Client {
        static void Main(string[] args) {

            // Create the channel.
            TcpChannel clientChannel = new TcpChannel();

            // Register the channel.
            ChannelServices.RegisterChannel(clientChannel, false);

            IServer proxy = (IServer) Activator.GetObject(typeof(RemotingTest1Srv.IServer),
                "tcp://localhost:8082/MyServerUri");


            Console.WriteLine("server returned: {0}", proxy.doSomething("aaaa"));
        }
    }
}
