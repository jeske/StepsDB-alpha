using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;




namespace RemotingTest1Srv {

    public interface IServer {
        string doSomething(string test);        
    }

    public class MyServer : IServer  {
        public string doSomething(string test) {
            Console.WriteLine("Server received: {0}", test);
            return String.Format("Server did: {0}", test);
        }
    }


    class _Main_Srv {
        static void Main(string[] args) {
            var serverChannel = new TcpChannel(8082);
            ChannelServices.RegisterChannel(serverChannel);
            RemotingConfiguration.ApplicationName = "TestServer";
            RemotingConfiguration.RegisterWellKnownServiceType(
                typeof(IServer), "MyServerUri", WellKnownObjectMode.SingleCall);

            // Parse the channel's URI.
            string[] urls = serverChannel.GetUrlsForUri("MyServerUri");
            if (urls.Length > 0) {
                string objectUrl = urls[0];
                string objectUri;
                string channelUri = serverChannel.Parse(objectUrl, out objectUri);
                Console.WriteLine("The object URL is {0}.", objectUrl);
                Console.WriteLine("The object URI is {0}.", objectUri);
                Console.WriteLine("The channel URI is {0}.", channelUri);
            }


            Console.WriteLine("press a key to exit");
            Console.ReadLine();
        }
    }
}
