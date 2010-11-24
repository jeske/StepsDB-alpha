using System;
using System.Net;
using System.Net.Sockets;
using System.Collections;

namespace LumiSoft.Net.ICMP
{
	/// <summary>
	/// ICMP type.
	/// </summary>
	public enum ICMP_Type
	{
		/// <summary>
		/// Echo rely.
		/// </summary>
		EchoReply = 0,

		/// <summary>
		/// Time to live exceeded reply.
		/// </summary>
		TimeExceeded = 11,

		/// <summary>
		/// Echo.
		/// </summary>
		Echo = 8,
	}

	/// <summary>
	/// Echo reply message.
	/// </summary>
	public class EchoMessage
	{
		private string m_IP   = "";
		private int    m_TTL  = 0;
		private int    m_Time = 0;
		
		/// <summary>
		/// 
		/// </summary>
		/// <param name="ip"></param>
		/// <param name="ttl"></param>
		/// <param name="time"></param>
		public EchoMessage(string ip,int ttl,int time)
		{
			m_IP   = ip;
			m_TTL  = ttl;
			m_Time = time;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public string ToStringEx()
		{
			return "TTL=" + m_TTL + "\tTime=" + m_Time + "ms" + "\tIP=" + m_IP;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="messages"></param>
		/// <returns></returns>
		public static string ToStringEx(EchoMessage[] messages)
		{
			string retVal = "";

			foreach(EchoMessage m in messages){
				retVal += m.ToStringEx() + "\r\n";
			}

			return retVal;
		}
	}
	
	/// <summary>
	/// Icmp utils.
	/// </summary>
	public class Icmp
	{
	//	public Icmp()
	//	{			
	//	}


		#region function Trace

		/// <summary>
		/// Traces specified ip.
		/// </summary>
		/// <param name="destIP"></param>
		/// <returns></returns>
		public static EchoMessage[] Trace(string destIP)
		{
			ArrayList retVal = new ArrayList();

			//Create Raw ICMP Socket 
			Socket s = new Socket(AddressFamily.InterNetwork,SocketType.Raw,ProtocolType.Icmp);
			
			IPEndPoint ipdest = new IPEndPoint(System.Net.IPAddress.Parse(destIP),80);			
			EndPoint endpoint = (EndPoint)(new IPEndPoint(System.Net.Dns.GetHostByName(System.Net.Dns.GetHostName()).AddressList[0],80));
													
			ushort id = (ushort)DateTime.Now.Millisecond;
			byte[] ByteSend= CreatePacket(id);

			int continuesNoReply = 0;
			//send requests with increasing number of TTL
			for(int ittl=1;ittl<=30; ittl++){
				byte[] ByteRecv = new byte[256];
				
				try
				{
					//Socket options to set TTL and Timeouts 
					s.SetSocketOption(SocketOptionLevel.IP,SocketOptionName.IpTimeToLive      ,ittl);
					s.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.SendTimeout   ,4000); 
					s.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.ReceiveTimeout,4000); 

					//Get current time
					DateTime startTime = DateTime.Now;

					//Send Request
					s.SendTo(ByteSend,ByteSend.Length,SocketFlags.None,ipdest);
				
					//Receive				
					s.ReceiveFrom(ByteRecv,ByteRecv.Length,SocketFlags.None,ref endpoint);

					//Calculate time required
					TimeSpan ts = DateTime.Now - startTime;
					retVal.Add(new EchoMessage(((IPEndPoint)endpoint).Address.ToString(),ittl,ts.Milliseconds));

					// Endpoint reached
					if(ByteRecv[20] == (byte)ICMP_Type.EchoReply){
						break;
					}
					
					// Un wanted reply
					if(ByteRecv[20] != (byte)ICMP_Type.TimeExceeded){
						throw new Exception("UnKnown error !");
					}

					continuesNoReply = 0;
				}
				catch{
					//ToDo: Handle recive/send timeouts
					continuesNoReply++;
				}

				// If there is 3 continues no reply, consider that destination host won't accept ping.
				if(continuesNoReply >= 3){
					break;
				}
			}

			EchoMessage[] val = new EchoMessage[retVal.Count];
			retVal.CopyTo(val);
			return val;
		}

		#endregion

	//	public static void Ping(string destIP)
	//	{
	//	}


		#region function CreatePacket

		private static byte[] CreatePacket(ushort id)
		{
			/*Rfc 792  Echo or Echo Reply Message
			  0               8              16              24
			 +---------------+---------------+---------------+---------------+
			 |     Type      |     Code      |           Checksum            |
			 +---------------+---------------+---------------+---------------+
			 |           ID Number           |            Sequence Number    |
			 +---------------+---------------+---------------+---------------+
			 |     Data...        
			 +---------------+---------------+---------------+---------------+
			*/

			byte[] packet = new byte[8 + 2];
			packet[0] = (byte)ICMP_Type.Echo; // Type
			packet[1] = 0;  // Code
			packet[2] = 0;  // Checksum
			packet[3] = 0;  // Checksum
			packet[4] = 0;  // ID
			packet[5] = 0;  // ID
			packet[6] = 0;  // Sequence
			packet[7] = 0;  // Sequence

			// Set id
			Array.Copy(BitConverter.GetBytes(id), 0, packet, 4, 2);

			// Fill data 2 byte data
			for(int i=0;i<2;i++){
				packet[i + 8] = (byte)'x'; // Data
			}
			
			//calculate checksum
			int checkSum = 0;
			for(int i= 0;i<packet.Length;i+= 2){ 
				checkSum += Convert.ToInt32(BitConverter.ToUInt16(packet,i));
			}

			//The checksum is the 16-bit ones's complement of the one's
			//complement sum of the ICMP message starting with the ICMP Type.
			checkSum  = (checkSum & 0xffff);
			Array.Copy(BitConverter.GetBytes((ushort)~checkSum),0,packet,2,2);

			return packet;
		}

		#endregion
	}
}
