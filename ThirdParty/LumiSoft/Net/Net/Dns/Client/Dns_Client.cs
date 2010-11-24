using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Collections;
using System.Text;
using System.Diagnostics;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// Dns client.
	/// </summary>
	/// <example>
	/// <code>
	/// // Set dns servers
	/// Dns_Client.DnsServers = new string[]{"194.126.115.18"};
	/// 
	/// Dns_Client dns = Dns_Client();
	/// 
	/// // Get MX records.
	/// DnsServerResponse resp = dns.Query("lumisoft.ee",QTYPE.MX);
	/// if(resp.ConnectionOk &amp;&amp; resp.ResponseCode == RCODE.NO_ERROR){
	///		MX_Record[] mxRecords = resp.GetMXRecords();
	///		
	///		// Do your stuff
	///	}
	///	else{
	///		// Handle error there, for more exact error info see RCODE 
	///	}	 
	/// 
	/// </code>
	/// </example>
	public class Dns_Client
	{
		private static string[] m_DnsServers  = null;
		private static bool     m_UseDnsCache = true;
		private static int      m_ID          = 100;

		/// <summary>
		/// Static constructor.
		/// </summary>
		static Dns_Client()
		{
			// Try to get system dns servers
			try{
				string[] retVal = new string[]{"",""};

				ProcessStartInfo pInfo = new ProcessStartInfo("ipconfig","/all");
				pInfo.RedirectStandardOutput = true;
				pInfo.UseShellExecute = false;
				pInfo.CreateNoWindow = true;
				
				Process p = Process.Start(pInfo);
				StreamReader r = p.StandardOutput;
				
				bool secondary = false;
				string line = r.ReadLine();
				while(line != null){
					if(line.Trim().ToLower().StartsWith("dns servers")){
						// Primary dns server
						retVal[0] = line.Substring(line.IndexOf(':') + 1).Trim();

						line = r.ReadLine();
						if(line == null){
							break;
						}
						line = r.ReadLine();
						if(line == null){
							break;
						}
						
						// There is secondary dns server						
						if(line.Trim().IndexOf(":") == -1){
							secondary = true;
							retVal[1] = line.Trim();
						}
						break;
					}
					line = r.ReadLine();
				}
			
				if(secondary){
					m_DnsServers = retVal;
				}
				else{
					m_DnsServers = new string[]{retVal[0]};
				}
			}
			catch{
			}
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public Dns_Client()
		{
		}


		#region method Query

		/// <summary>
		/// Queries server with specified query.
		/// </summary>
		/// <param name="queryText">Query text. It depends on queryType.</param>
		/// <param name="queryType">Query type.</param>
		/// <returns></returns>
		public DnsServerResponse Query(string queryText,QTYPE queryType)
		{
			if(queryType == QTYPE.PTR){
				string ip = queryText;

				// See if IP is ok.
				IPAddress ipA = IPAddress.Parse(ip);		
				queryText = "";

				// IPv6
				if(ipA.AddressFamily == AddressFamily.InterNetworkV6){
					// 4321:0:1:2:3:4:567:89ab
					// would be
					// b.a.9.8.7.6.5.0.4.0.0.0.3.0.0.0.2.0.0.0.1.0.0.0.0.0.0.0.1.2.3.4.IP6.ARPA
					
					char[] ipChars = ip.Replace(":","").ToCharArray();
					for(int i=ipChars.Length - 1;i>-1;i--){
						queryText += ipChars[i] + ".";
					}
					queryText += "IP6.ARPA";
				}
				// IPv4
				else{
					// 213.35.221.186
					// would be
					// 186.221.35.213.in-addr.arpa

					string[] ipParts = ip.Split('.');
					//--- Reverse IP ----------
					for(int i=3;i>-1;i--){
						queryText += ipParts[i] + ".";
					}
					queryText += "in-addr.arpa";
				}
			}

			return QueryServer(queryText,queryType,1);
		}

		#endregion

		#region method Resolve

		/// <summary>
		/// Resolves a DNS host name or IP to IPAddress[].
		/// </summary>
		/// <param name="hostName_IP">Host name or IP address.</param>
		/// <returns></returns>
		public static IPAddress[] Resolve(string hostName_IP)
		{
			// If hostName_IP is IP
			try{
				return new IPAddress[]{IPAddress.Parse(hostName_IP)};
			}
			catch{
			}

			// This is probably NetBios name
			if(hostName_IP.IndexOf(".") == -1){
				return System.Net.Dns.Resolve(hostName_IP).AddressList;
			}
			else{
				// hostName_IP must be host name, try to resolve it's IP
				Dns_Client dns = new Dns_Client();
				DnsServerResponse resp = dns.Query(hostName_IP,QTYPE.A);
				if(resp.ResponseCode == RCODE.NO_ERROR){
					A_Record[] records = resp.GetARecords();
					IPAddress[] retVal = new IPAddress[records.Length];
					for(int i=0;i<records.Length;i++){
						retVal[i] = IPAddress.Parse(records[i].IP);
					}

					return retVal;
				}
				else{
					throw new Exception(resp.ResponseCode.ToString());
				}
			}
		}

		#endregion

		
		#region method ParseARecord

		private A_Record ParseARecord(byte[] reply,ref int offset,int rdLength,int ttl)
		{
			// IPv4 = byte byte byte byte

			byte[] ip = new byte[rdLength];
			Array.Copy(reply,offset,ip,0,rdLength);

			return new A_Record(ip[0] + "." + ip[1] + "." + ip[2] + "." + ip[3],ttl);	
		}

		#endregion

		#region method ParseNSRecord

		private NS_Record ParseNSRecord(byte[] reply,ref int offset,int rdLength,int ttl)
		{
			// Name server name

			string name = "";			
			if(GetQName(reply,ref offset,ref name)){			
				return new NS_Record(name,ttl);
			}

			return null;
		}

		#endregion

		#region method ParseCNAMERecord

		private CNAME_Record ParseCNAMERecord(byte[] reply,ref int offset,int rdLength,int ttl)
		{
			// Alias

			string name = "";			
			if(GetQName(reply,ref offset,ref name)){			
				return new CNAME_Record(name,ttl);
			}

			return null;
		}

		#endregion

		#region method ParseSOARecord

		private SOA_Record ParseSOARecord(byte[] reply,ref int offset,int rdLength,int ttl)
		{	
			/* RFC 1035 3.3.13. SOA RDATA format

				+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
				/                     MNAME                     /
				/                                               /
				+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
				/                     RNAME                     /
				+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
				|                    SERIAL                     |
				|                                               |
				+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
				|                    REFRESH                    |
				|                                               |
				+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
				|                     RETRY                     |
				|                                               |
				+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
				|                    EXPIRE                     |
				|                                               |
				+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
				|                    MINIMUM                    |
				|                                               |
				+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

			where:

			MNAME           The <domain-name> of the name server that was the
							original or primary source of data for this zone.

			RNAME           A <domain-name> which specifies the mailbox of the
							person responsible for this zone.

			SERIAL          The unsigned 32 bit version number of the original copy
							of the zone.  Zone transfers preserve this value.  This
							value wraps and should be compared using sequence space
							arithmetic.

			REFRESH         A 32 bit time interval before the zone should be
							refreshed.

			RETRY           A 32 bit time interval that should elapse before a
							failed refresh should be retried.

			EXPIRE          A 32 bit time value that specifies the upper limit on
							the time interval that can elapse before the zone is no
							longer authoritative.
							
			MINIMUM         The unsigned 32 bit minimum TTL field that should be
							exported with any RR from this zone.
			*/

			//---- Parse record -------------------------------------------------------------//
			// MNAME
			string nameserver = "";			
			GetQName(reply,ref offset,ref nameserver);

			// RNAME
			string adminMailBox = "";			
			GetQName(reply,ref offset,ref adminMailBox);
			char[] adminMailBoxAr = adminMailBox.ToCharArray();
			for(int i=0;i<adminMailBoxAr.Length;i++){			
				if(adminMailBoxAr[i] == '.'){
					adminMailBoxAr[i] = '@';
					break;
				}
			}
			adminMailBox = new string(adminMailBoxAr);

			// SERIAL
			long serial = reply[offset++] << 24 | reply[offset++] << 16 | reply[offset++] << 8 | reply[offset++];

			// REFRESH
			long refresh = reply[offset++] << 24 | reply[offset++] << 16 | reply[offset++] << 8 | reply[offset++];

			// RETRY
			long retry = reply[offset++] << 24 | reply[offset++] << 16 | reply[offset++] << 8 | reply[offset++];

			// EXPIRE
			long expire = reply[offset++] << 24 | reply[offset++] << 16 | reply[offset++] << 8 | reply[offset++];

			// MINIMUM
			long minimum = reply[offset++] << 24 | reply[offset++] << 16 | reply[offset++] << 8 | reply[offset++];
			//--------------------------------------------------------------------------------//

			return new SOA_Record(nameserver,adminMailBox,serial,refresh,retry,expire,minimum,ttl);
		}

		#endregion

		#region method ParsePTRRecord

		private PTR_Record ParsePTRRecord(byte[] reply,ref int offset,int rdLength,int ttl)
		{
			string name = "";
			GetQName(reply,ref offset,ref name);

			return new PTR_Record(name,ttl);	
		}

		#endregion

		#region method ParseHINFORecord

		private HINFO_Record ParseHINFORecord(byte[] reply,ref int offset,int rdLength,int ttl)
		{
			/* RFC 1035 3.3.2. HINFO RDATA format

			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			/                      CPU                      /
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			/                       OS                      /
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			
			CPU     A <character-string> which specifies the CPU type.

			OS      A <character-string> which specifies the operating
					system type.
					
					Standard values for CPU and OS can be found in [RFC-1010].

			*/

			// CPU
			string cpu = "";
			GetQName(reply,ref offset,ref cpu);

			// OS
			string os = "";
			GetQName(reply,ref offset,ref os);

			return new HINFO_Record(cpu,os,ttl);
		}

		#endregion
		
		#region method ParseMxRecord

		/// <summary>
		/// Parses MX record.
		/// </summary>
		/// <param name="reply"></param>
		/// <param name="offset"></param>
		/// <param name="ttl">TTL(time to live) value in seconds.</param>
		/// <returns>Returns null, if failed.</returns>
		private MX_Record ParseMxRecord(byte[] reply,ref int offset,int ttl)
		{
			/* RFC 1035	3.3.9. MX RDATA format

			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                  PREFERENCE                   |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			/                   EXCHANGE                    /
			/                                               /
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

			where:

			PREFERENCE      
				A 16 bit integer which specifies the preference given to
				this RR among others at the same owner.  Lower values
                are preferred.

			EXCHANGE 
			    A <domain-name> which specifies a host willing to act as
                a mail exchange for the owner name. 
			*/

			int pref = reply[offset++] << 8 | reply[offset++];
		
			string name = "";			
			if(GetQName(reply,ref offset,ref name)){
				return new MX_Record(pref,name,ttl);
			}

			return null;
		}

		#endregion

		#region method ParseTXTRecord

		private TXT_Record ParseTXTRecord(byte[] reply,ref int offset,int rdLength,int ttl)
		{
			// just text

			byte[] text = new byte[rdLength];
			Array.Copy(reply,offset,text,0,rdLength);

			return new TXT_Record(System.Text.Encoding.Default.GetString(text).Trim(),ttl);
		}

		#endregion

		#region method ParseAAAARecord

		private A_Record ParseAAAARecord(byte[] reply,ref int offset,int rdLength,int ttl)
		{
			// IPv6 = 16xbyte

			byte[] ip = new byte[rdLength];
			Array.Copy(reply,offset,ip,0,rdLength);

			// Covert byte array to IPv6
			// ip = 8 x 2byte blocks in HEX
			string ipStr = "";
			if(rdLength == 16){
				for(int i=1;i<16;i+=2){
					long a = (ip[i-1] << 8) | ip[i];
					ipStr += a.ToString("x");

					if(i < 15){
						ipStr += ":";
					}
				}
			}
	
			return new A_Record(ipStr,ttl);	
		}

		#endregion


		#region method QueryServer

		/// <summary>
		/// Sends query to server.
		/// </summary>
		/// <param name="qname">Query text.</param>
		/// <param name="qtype">Query type.</param>
		/// <param name="qclass">Query class.</param>
		/// <returns></returns>
		private DnsServerResponse QueryServer(string qname,QTYPE qtype,int qclass)
		{	
			if(m_DnsServers == null || m_DnsServers.Length == 0){
				throw new Exception("Dns server isn't specified !!!");
			}

			// See if query is in cache
			if(m_UseDnsCache){
				DnsServerResponse resopnse = DnsCache.GetFromCache(qname,(int)qtype);
				if(resopnse != null){
					return resopnse;
				}
			}

			int queryID = Dns_Client.ID;
			byte[] query = CreateQuery(queryID,qname,qtype,qclass);

			int helper = 0;
			for(int i=0;i<(m_DnsServers.Length * 2);i++){				
				if(helper >= m_DnsServers.Length){
					helper = 0;
				}

				try{
					Socket udpClient = new Socket(AddressFamily.InterNetwork,SocketType.Dgram,ProtocolType.Udp);
					udpClient.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.ReceiveTimeout,10000);
					udpClient.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.SendTimeout,10000);
					udpClient.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.NoDelay,1);
			
					udpClient.Connect(new IPEndPoint(IPAddress.Parse(m_DnsServers[helper]),53));

					//send query
					udpClient.Send(query);

					byte[] retVal = new byte[512];
					int countRecieved = udpClient.Receive(retVal);

					udpClient.Close();

					// If reply is ok, return it
					DnsServerResponse serverResponse = ParseQuery(retVal,queryID);
				
					// Cache query
					if(m_UseDnsCache && serverResponse.ResponseCode == RCODE.NO_ERROR){
						DnsCache.AddToCache(qname,(int)qtype,serverResponse);
					}

					return serverResponse;
				}
				catch{					
				}

				helper++;
			}

			// If we reach so far, we probably won't get connection to dsn server
			return new DnsServerResponse(false,RCODE.SERVER_FAILURE,null,null,null);
		}

		#endregion

		#region method CreateQuery

		/// <summary>
		/// Creates new query.
		/// </summary>
		/// <param name="ID">Query ID.</param>
		/// <param name="qname">Query text.</param>
		/// <param name="qtype">Query type.</param>
		/// <param name="qclass">Query class.</param>
		/// <returns></returns>
		private byte[] CreateQuery(int ID,string qname,QTYPE qtype,int qclass)
		{
			byte[] query = new byte[512];

			//---- Create header --------------------------------------------//
			// Header is first 12 bytes of query

			/* 4.1.1. Header section format
										  1  1  1  1  1  1
			0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                      ID                       |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|QR|   Opcode  |AA|TC|RD|RA|   Z    |   RCODE   |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                    QDCOUNT                    |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                    ANCOUNT                    |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                    NSCOUNT                    |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                    ARCOUNT                    |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			
			QR  A one bit field that specifies whether this message is a
                query (0), or a response (1).
				
			OPCODE          A four bit field that specifies kind of query in this
                message.  This value is set by the originator of a query
                and copied into the response.  The values are:

                0               a standard query (QUERY)

                1               an inverse query (IQUERY)

                2               a server status request (STATUS)
				
			*/

			//--------- Header part -----------------------------------//
			query[0]  = (byte) (ID >> 8); query[1]  = (byte) (ID & 0xFF);
			query[2]  = (byte) 1;         query[3]  = (byte) 0;
			query[4]  = (byte) 0;         query[5]  = (byte) 1;
			query[6]  = (byte) 0;         query[7]  = (byte) 0;
			query[8]  = (byte) 0;         query[9]  = (byte) 0;
			query[10] = (byte) 0;         query[11] = (byte) 0;
			//---------------------------------------------------------//

			//---- End of header --------------------------------------------//


			//----Create query ------------------------------------//

			/* 	Rfc 1035 4.1.2. Question section format
											  1  1  1  1  1  1
			0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                                               |
			/                     QNAME                     /
			/                                               /
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                     QTYPE                     |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                     QCLASS                    |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			
			QNAME
				a domain name represented as a sequence of labels, where
				each label consists of a length octet followed by that
				number of octets.  The domain name terminates with the
				zero length octet for the null label of the root.  Note
				that this field may be an odd number of octets; no
				padding is used.
			*/
			string[] labels = qname.Split(new char[] {'.'});
			int position = 12;
					
			// Copy all domain parts(labels) to query
			// eg. lumisoft.ee = 2 labels, lumisoft and ee.
			// format = label.length + label(bytes)
			foreach(string label in labels){
				// add label lenght to query
				query[position++] = (byte)(label.Length); 

				// convert label string to byte array
				byte[] b = Encoding.ASCII.GetBytes(label);
				b.CopyTo(query,position);

				// Move position by label length
				position += b.Length;
			}

			// Terminate domain (see note above)
			query[position++] = (byte) 0; 
			
			// Set QTYPE 
			query[position++] = (byte) 0;
			query[position++] = (byte)qtype;
				
			// Set QCLASS
			query[position++] = (byte) 0;
			query[position++] = (byte)qclass;
			//-------------------------------------------------------//
			
			return query;
		}

		#endregion

		#region method GetQName

		private bool GetQName(byte[] reply,ref int offset,ref string name)
		{				
			try{
				// Do while not terminator
				while(reply[offset] != 0){
					
					// Check if it's pointer(In pointer first two bits always 1)
					bool isPointer = ((reply[offset] & 0xC0) == 0xC0);
					
					// If pointer
					if(isPointer){
						// Pointer location number is 2 bytes long
						// 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7  # byte 2 # 0 | 1 | 2 | | 3 | 4 | 5 | 6 | 7
						// empty | < ---- pointer location number --------------------------------->
						int pStart = ((reply[offset] & 0x3F) << 8) | (reply[++offset]);
						offset++;						
						return GetQName(reply,ref pStart,ref name);
					}
					else{
						// label length (length = 8Bit and first 2 bits always 0)
						// 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7
						// empty | lablel length in bytes 
						int labelLength = (reply[offset] & 0x3F);
						offset++;
						
						// Copy label into name 
						name += Encoding.ASCII.GetString(reply,offset,labelLength);
						offset += labelLength;
					}
									
					// If the next char isn't terminator,
					// label continues - add dot between two labels
					if (reply[offset] != 0){
						name += ".";
					}					
				}

				// Move offset by terminator length
				offset++;

				return true;
			}
			catch{
				return false;
			}
		}

		#endregion

		#region method ParseQuery

		/// <summary>
		/// Parses query.
		/// </summary>
		/// <param name="reply">Dns server reply.</param>
		/// <param name="queryID">Query id of sent query.</param>
		/// <returns></returns>
		private DnsServerResponse ParseQuery(byte[] reply,int queryID)
		{	
			//--- Parse headers ------------------------------------//

			/* RFC 1035 4.1.1. Header section format
			 
											1  1  1  1  1  1
			  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
			 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			 |                      ID                       |
			 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			 |QR|   Opcode  |AA|TC|RD|RA|   Z    |   RCODE   |
			 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			 |                    QDCOUNT                    |
			 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			 |                    ANCOUNT                    |
			 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			 |                    NSCOUNT                    |
			 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			 |                    ARCOUNT                    |
			 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			 
			QDCOUNT
				an unsigned 16 bit integer specifying the number of
				entries in the question section.

			ANCOUNT
				an unsigned 16 bit integer specifying the number of
				resource records in the answer section.
				
			NSCOUNT
			    an unsigned 16 bit integer specifying the number of name
                server resource records in the authority records section.

			ARCOUNT
			    an unsigned 16 bit integer specifying the number of
                resource records in the additional records section.
				
			*/
		
			// Get reply code
			int    id                     = (reply[0]  << 8 | reply[1]);
			OPCODE opcode                 = (OPCODE)((reply[2] >> 3) & 15);
			RCODE  replyCode              = (RCODE)(reply[3]  & 15);	
			int    queryCount             = (reply[4]  << 8 | reply[5]);
			int    answerCount            = (reply[6]  << 8 | reply[7]);
			int    authoritiveAnswerCount = (reply[8]  << 8 | reply[9]);
			int    additionalAnswerCount  = (reply[10] << 8 | reply[11]);
			//---- End of headers ---------------------------------//

			// Check that it's query what we want
			if(queryID != id){
				throw new Exception("This isn't query with ID what we expected");
			}
		
			int pos = 12;

			//----- Parse question part ------------//
			for(int q=0;q<queryCount;q++){
				string dummy = "";
				GetQName(reply,ref pos,ref dummy);
				//qtype + qclass
				pos += 4;
			}
			//--------------------------------------//
			

			// 1) parse answers
			// 2) parse authoritive answers
			// 3) parse additional answers
			ArrayList answers = ParseAnswers(reply,answerCount,ref pos);
			ArrayList authoritiveAnswers = ParseAnswers(reply,authoritiveAnswerCount,ref pos);
			ArrayList additionalAnswers = ParseAnswers(reply,additionalAnswerCount,ref pos);

			return new DnsServerResponse(true,replyCode,answers,authoritiveAnswers,additionalAnswers);
		}

		#endregion

		#region method ParseAnswers

		/// <summary>
		/// Parses specified count of answers from query.
		/// </summary>
		/// <param name="reply">Server returned query.</param>
		/// <param name="answerCount">Number of answers to parse.</param>
		/// <param name="offset">Position from where to start parsing answers.</param>
		/// <returns></returns>
		private ArrayList ParseAnswers(byte[] reply,int answerCount,ref int offset)
		{
			/* RFC 1035 4.1.3. Resource record format
			 
										   1  1  1  1  1  1
			 0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                                               |
			/                                               /
			/                      NAME                     /
			|                                               |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                      TYPE                     |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                     CLASS                     |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                      TTL                      |
			|                                               |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			|                   RDLENGTH                    |
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--|
			/                     RDATA                     /
			/                                               /
			+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
			*/

			ArrayList answers = new ArrayList();
			//---- Start parsing aswers ------------------------------------------------------------------//
			for(int i=0;i<answerCount;i++){	
				string name = "";
				if(!GetQName(reply,ref offset,ref name)){
					throw new Exception("Error parisng anser");
				}

				int type     = reply[offset++] << 8  | reply[offset++];
				int rdClass  = reply[offset++] << 8  | reply[offset++];
				int ttl      = reply[offset++] << 24 | reply[offset++] << 16 | reply[offset++] << 8  | reply[offset++];
				int rdLength = reply[offset++] << 8  | reply[offset++];

				object answerObj = null;
				switch((QTYPE)type)
				{
					case QTYPE.A:
						answerObj = ParseARecord(reply,ref offset,rdLength,ttl);
						offset += rdLength;		
						break;

					case QTYPE.NS:
						answerObj = ParseNSRecord(reply,ref offset,rdLength,ttl);
					//	pos += rdLength;		
						break;

					case QTYPE.CNAME:
						answerObj = ParseCNAMERecord(reply,ref offset,rdLength,ttl);
					//	offset += rdLength;		
						break;

					case QTYPE.SOA:
						answerObj = ParseSOARecord(reply,ref offset,rdLength,ttl);
					//	offset += rdLength;		
						break;

					case QTYPE.PTR:
						answerObj = ParsePTRRecord(reply,ref offset,rdLength,ttl);							
					//	offset += rdLength;
						break;

					case QTYPE.HINFO:
						answerObj = ParseHINFORecord(reply,ref offset,rdLength,ttl);							
					//	offset += rdLength;
						break;

					case QTYPE.MX:
						answerObj = ParseMxRecord(reply,ref offset,ttl);
						break;

					case QTYPE.TXT:
						answerObj = ParseTXTRecord(reply,ref offset,rdLength,ttl);
						offset += rdLength;
						break;

					case QTYPE.AAAA:
						answerObj = ParseAAAARecord(reply,ref offset,rdLength,ttl);
						offset += rdLength;		
						break;

					default:
						answerObj = "dummy"; // Dummy place holder for now
						offset += rdLength;
						break;
				}
			
				// Add answer to answer list
				answers.Add(answerObj);
			}

			return answers;
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets or sets dns servers.
		/// </summary>
		public static string[] DnsServers
		{
			get{ return m_DnsServers; }

			set{ m_DnsServers = value; }
		}

		/// <summary>
		/// Gets or sets if to use dns caching.
		/// </summary>
		public static bool UseDnsCache
		{
			get{ return m_UseDnsCache; }

			set{ m_UseDnsCache = value; }
		}

		/// <summary>
		/// Get next query ID.
		/// </summary>
		internal static int ID
		{
			get{
				if(m_ID >= 65535){
					m_ID = 100;
				}
				return m_ID++; 
			}
		}

		#endregion

	}
}
