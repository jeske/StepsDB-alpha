using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;

using LumiSoft.Net;
using LumiSoft.Net.Dns.Client;
using LumiSoft.Net.Mime;

namespace LumiSoft.Net.SMTP.Client
{
	/// <summary>
	/// Is called when asynchronous command had completed.
	/// </summary>
	public delegate void CommadCompleted(SocketCallBackResult result,Exception exception);

	/// <summary>
	/// SMTP client.
	/// </summary>
	public class SmtpClientEx : IDisposable
	{
		private BufferedSocket m_pSocket          = null;
		private SocketLogger   m_pLogger          = null;
		private bool           m_Connected        = false;
		private bool           m_Supports_Size    = false;
		private bool           m_Supports_Bdat    = false;
		private bool           m_Supports_Login   = false;
		private bool           m_Supports_CramMd5 = false;
		private string[]       m_pDnsServers      = null;
		private DateTime       m_LastDataTime;

		/// <summary>
		/// Occurs when SMTP session has finished and session log is available.
		/// </summary>
		public event LogEventHandler SessionLog = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public SmtpClientEx()
		{
			m_LastDataTime = DateTime.Now;
		}

		#region method Dispose

		/// <summary>
		/// Cleasns up resources and disconnect smtp client if open.
		/// </summary>
		public void Dispose()
		{
			try{
				Disconnect();				
			}
			catch{
			}	
		}

		#endregion


		#region Events handling

		#region method m_pSocket_Activity

		/// <summary>
		/// Is called when socket has sent or recieved data.
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>
		private void m_pSocket_Activity(object sender, EventArgs e)
		{
			m_LastDataTime = DateTime.Now;
		}

		#endregion

		#endregion


		#region method Connect

		/// <summary>
		/// Connects to sepcified host.
		/// </summary>
		/// <param name="host">Host name or IP address.</param>
		/// <param name="port">Port where to connect.</param>
		public void Connect(string host,int port)
		{
			Connect(null,host,port);
		}

		/// <summary>
		/// Connects to sepcified host.
		/// </summary>
		/// <param name="localEndpoint">Sets local endpoint. Pass null, to use default.</param>
		/// <param name="host">Host name or IP address.</param>
		/// <param name="port">Port where to connect.</param>
		public void Connect(IPEndPoint localEndpoint,string host,int port)
		{	
			Socket s = new Socket(AddressFamily.InterNetwork,SocketType.Stream,ProtocolType.IP);
			s.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.ReceiveTimeout,30000);
			s.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.NoDelay,1);
			
			m_pSocket = new BufferedSocket(s);
			if(localEndpoint != null){
				m_pSocket.Bind(localEndpoint);
			}
			m_pSocket.Activity += new EventHandler(m_pSocket_Activity);
	
			if(SessionLog != null){
				m_pLogger = new SocketLogger(s,SessionLog);
				m_pLogger.SessionID = Guid.NewGuid().ToString();
				m_pSocket.Logger = m_pLogger;
			}
		
			if(host.IndexOf("@") == -1){
				m_pSocket.Connect(new IPEndPoint(Dns_Client.Resolve(host)[0],port));					
			}
			else{
				//---- Parse e-domain -------------------------------//
				string domain = host;

				// eg. Ivx <ivx@lumisoft.ee>
				if(domain.IndexOf("<") > -1 && domain.IndexOf(">") > -1){
					domain = domain.Substring(domain.IndexOf("<")+1,domain.IndexOf(">") - domain.IndexOf("<")-1);
				}

				if(domain.IndexOf("@") > -1){
					domain = domain.Substring(domain.LastIndexOf("@") + 1);
				}

				if(domain.Trim().Length == 0){
					if(m_pLogger != null){
						m_pLogger.AddTextEntry("Destination address '" + host + "' is invalid, aborting !");
					}
					throw new Exception("Destination address '" + host + "' is invalid, aborting !");
				}

				//--- Get MX record -------------------------------------------//
				Dns_Client dns = new Dns_Client();
				Dns_Client.DnsServers = m_pDnsServers;
				DnsServerResponse dnsResponse = dns.Query(domain,QTYPE.MX);

				switch(dnsResponse.ResponseCode)
				{
					case RCODE.NO_ERROR:
						MX_Record[] mxRecords = dnsResponse.GetMXRecords();

						// Try all available hosts by MX preference order, if can't connect specified host.
						foreach(MX_Record mx in mxRecords){
							try{
								if(m_pLogger != null){
									m_pLogger.AddTextEntry("Connecting with mx record to: " + mx.Host);
								}
								m_pSocket.Connect(new IPEndPoint(Dns_Client.Resolve(mx.Host)[0],port));
								break;
							}
							catch{ // Just skip and let for to try next host.									
								if(m_pLogger != null){
									m_pLogger.AddTextEntry("Failed connect to: " + mx.Host);
								}
							}
						}

						// None of MX didn't connect
						if(mxRecords.Length > 0 && !m_pSocket.Connected){
							throw new Exception("Destination email server is down");
						}

						/* Rfc 2821 5
						 If no MX records are found, but an A RR is found, the A RR is treated as
						 if it was associated with an implicit MX RR, with a preference of 0,
						 pointing to that host.
						*/
						if(mxRecords.Length == 0){
							// Try to connect with A record
							IPAddress[] ipEntry = null;
							try{
								if(m_pLogger != null){
									m_pLogger.AddTextEntry("No mx record, trying to get A record for: " + domain);
								}
								ipEntry = Dns_Client.Resolve(domain);								
							}
							catch{
								if(m_pLogger != null){
									m_pLogger.AddTextEntry("Invalid domain,no MX or A record: " + domain);
								}
								throw new Exception("Invalid domain,no MX or A record: " + domain);
							}

							try{
								if(m_pLogger != null){
									m_pLogger.AddTextEntry("Connecting with A record to:" + domain);
								}
								m_pSocket.Connect(new IPEndPoint(ipEntry[0],port));
							}
							catch{
								if(m_pLogger != null){
									m_pLogger.AddTextEntry("Failed connect to:" + domain);
								}
								throw new Exception("Destination email server is down");
							}
						}
						break;

					case RCODE.NAME_ERROR:
						if(m_pLogger != null){
							m_pLogger.AddTextEntry("Invalid domain,no MX or A record: " + domain);
						}
						throw new Exception("Invalid domain,no MX or A record: " + domain);

					case RCODE.SERVER_FAILURE:
						if(m_pLogger != null){
							m_pLogger.AddTextEntry("Dns server unvailable.");
						}
						throw new Exception("Dns server unvailable.");
					}					
			}
					
			/*
			 * Notes: Greeting may be single or multiline response.
			 *		
			 * Examples:
			 *		220<SP>SMTP server ready<CRLF> 
			 * 
			 *		220-SMTP server ready<CRLF>
			 *		220-Addtitional text<CRLF>
			 *		220<SP>final row<CRLF>
			 * 
			*/

			// Read server response
			string responseLine = m_pSocket.ReadLine(1000);
			while(!responseLine.StartsWith("220 ")){
				// If lisne won't start with 220, then its error response
				if(!responseLine.StartsWith("220")){
					throw new Exception(responseLine);
				}

				responseLine = m_pSocket.ReadLine(1000);
			}

			m_Connected = true;
		}

		#endregion

		#region method BeginConnect

		/// <summary>
		/// Starts connection to specified host.
		/// </summary>
		/// <param name="host">Host name or IP address.</param>
		/// <param name="port">Port where to connect.</param>
		/// <param name="callback">Callback to be called if connect ends.</param>
		public void BeginConnect(string host,int port,CommadCompleted callback)
		{
			BeginConnect(null,host,port,callback);
		}

		/// <summary>
		/// Starts connection to specified host.
		/// </summary>
		/// <param name="localEndpoint">Sets local endpoint. Pass null, to use default.</param>
		/// <param name="host">Host name or IP address.</param>
		/// <param name="port">Port where to connect.</param>
		/// <param name="callback">Callback to be called if connect ends.</param>
		public void BeginConnect(IPEndPoint localEndpoint,string host,int port,CommadCompleted callback)
		{
			ThreadPool.QueueUserWorkItem(new WaitCallback(this.BeginConnect_workerThread),new object[]{localEndpoint,host,port,callback});
		}

		#endregion

		#region method BeginConnect_workerThread

		/// <summary>
		/// Is called from ThreadPool Thread. This method just call synchrounous Connect.
		/// </summary>
		/// <param name="tag"></param>
		private void BeginConnect_workerThread(object tag)
		{
			CommadCompleted callback = (CommadCompleted)((object[])tag)[3];

			try{
				IPEndPoint localEndpoint = (IPEndPoint)((object[])tag)[0];
				string     host          = (string)((object[])tag)[1];
				int        port          = (int)((object[])tag)[2];
			
				Connect(localEndpoint,host,port);

				// Connect completed susscessfully, call callback method.
				callback(SocketCallBackResult.Ok,null);
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion


/*		/// <summary>
		/// Starts disconnecting SMTP client.
		/// </summary>
		public void BeginDisconnect()
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}
		}*/

		#region method Disconnect

		/// <summary>
		/// Disconnects smtp client from server.
		/// </summary>
		public void Disconnect()
		{
            try{		
				if(m_pSocket != null && m_pSocket.Connected){
					m_pSocket.SendLine("QUIT");

					m_pSocket.Shutdown(SocketShutdown.Both);
				}
			}
			catch{	
			}

			m_pSocket = null;
			m_Connected = false;
			m_Supports_Size = false;
			m_Supports_Bdat = false;
			m_Supports_Login = false;
			m_Supports_CramMd5 = false;

			if(m_pLogger != null){
				m_pLogger.Flush();
				m_pLogger = null;
			}
		}

		#endregion


		#region method Ehlo
		
		/// <summary>
		/// Does EHLO command. If server don't support EHLO, tries HELO.
		/// </summary>
		/// <param name="hostName">Host name which is reported to SMTP server.</param>
		public void Ehlo(string hostName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}

			/* Rfc 2821 4.1.1.1 EHLO
			 * Syntax: "EHLO" SP Domain CRLF
			*/

			if(hostName.Length == 0){
				hostName = System.Net.Dns.GetHostName();
			}

			// Send EHLO command to server
			m_pSocket.SendLine("EHLO " + hostName);
			
			string responseLine = m_pSocket.ReadLine();
			// Response line must start with 250 or otherwise it's error response,
			// try HELO
			if(!responseLine.StartsWith("250")){
				// Send HELO command to server
				m_pSocket.SendLine("HELO " + hostName);

				responseLine = m_pSocket.ReadLine();
				// HELO failed, return error
				if(!responseLine.StartsWith("250")){
					throw new Exception(responseLine);
				}
			}

			/* RFC 2821 4.1.1.1 EHLO
				*	Examples:
				*		250-domain<SP>free_text<CRLF>
				*       250-EHLO_keyword<CRLF>
				*		250<SP>EHLO_keyword<CRLF>
				* 
				*		250<SP> specifies that last EHLO response line.
			*/

			while(!responseLine.StartsWith("250 ")){
				//---- Store supported ESMTP features --------------------//
				if(responseLine.ToLower().IndexOf("size") > -1){
					m_Supports_Size = true;
				}
				else if(responseLine.ToLower().IndexOf("chunking") > -1){
					m_Supports_Bdat = true;
				}
				else if(responseLine.ToLower().IndexOf("cram-md5") > -1){
					m_Supports_CramMd5 = true;
				}
				else if(responseLine.ToLower().IndexOf("login") > -1){
					m_Supports_Login = true;
				}
				//--------------------------------------------------------//

				// Read next EHLO response line
				responseLine = m_pSocket.ReadLine();
			}			
		}

		#endregion

		#region method BeginEhlo

		/// <summary>
		/// Begins EHLO command.
		/// </summary>
		/// <param name="hostName">Host name which is reported to SMTP server.</param>
		/// <param name="callback">Callback to be called if command ends.</param>
		public void BeginEhlo(string hostName,CommadCompleted callback)
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}

			/* Rfc 2821 4.1.1.1 EHLO
			 * Syntax: "EHLO" SP Domain CRLF
			*/

			if(hostName.Length == 0){
				hostName = System.Net.Dns.GetHostName();
			}

			// Start sending EHLO command to server
			m_pSocket.BeginSendLine("EHLO " + hostName,new object[]{hostName,callback},new SocketCallBack(this.OnEhloSendFinished));			
		}

		#endregion

		#region method OnEhloSendFinished

		/// <summary>
		/// Is called when smtp client has finished EHLO command sending.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnEhloSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[1]);

			try{
				if(result == SocketCallBackResult.Ok){
					// Begin reading server EHLO command response
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{((object[])tag)[0],callback,ms},new SocketCallBack(this.OnEhloReadServerResponseFinished));
				}
				else{ 
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnEhloReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading EHLO command server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnEhloReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[1]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[2])).ToArray());

					/* RFC 2821 4.1.1.1 EHLO
					*	Examples:
					*		250-domain<SP>free_text<CRLF>
					*       250-EHLO_keyword<CRLF>
					*		250<SP>EHLO_keyword<CRLF>
					* 
					* 250<SP> specifies that last EHLO response line.
					*/

					// Response line must start with 250 or otherwise it's error response
					if(!responseLine.StartsWith("250")){
						// Server isn't required to support EHLO, try HELO
						string hostName = (string)(((object[])tag)[0]);
						m_pSocket.BeginSendLine("HELO " + hostName,callback,new SocketCallBack(this.OnHeloSendFinished));					
					}
					else{
						//---- Store supported ESMTP features --------------------//
						if(responseLine.ToLower().IndexOf("size") > -1){
							m_Supports_Size = true;
						}
						else if(responseLine.ToLower().IndexOf("chunking") > -1){
							m_Supports_Bdat = true;
						}
						else if(responseLine.ToLower().IndexOf("cram-md5") > -1){
							m_Supports_CramMd5 = true;
						}
						else if(responseLine.ToLower().IndexOf("login") > -1){
							m_Supports_Login = true;
						}
						//--------------------------------------------------------//

						// This isn't last EHLO response line
						if(!responseLine.StartsWith("250 ")){
							MemoryStream ms = new MemoryStream();
							m_pSocket.BeginReadLine(ms,1000,new object[]{(((object[])tag)[0]),callback,ms},new SocketCallBack(this.OnEhloReadServerResponseFinished));
						}
						else{
							// EHLO completed susscessfully, call callback method.
							callback(SocketCallBackResult.Ok,null);
						}
					}
				}
				else{ 
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion


		#region method OnHeloSendFinished

		/// <summary>
		/// Is called when smtp client has finished HELO command sending.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnHeloSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)tag;

			try{
				if(result == SocketCallBackResult.Ok){
					// Begin reading server HELO command response
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{callback,ms},new SocketCallBack(this.OnHeloReadServerResponseFinished));
				}
				else{ 
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnHeloReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading EHLO command server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnHeloReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[0]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[1])).ToArray());

					/* RFC 2821 4.1.1.1 HELO
					*	Examples:
					*		250<SP>domain<SP>free_text<CRLF>
					* 
					*/

					// Response line must start with 250 or otherwise it's error response
					if(!responseLine.StartsWith("250")){
						throw new Exception(responseLine);
					}
					else{
						// EHLO completed susscessfully, call callback method.
						callback(SocketCallBackResult.Ok,null);
					}		
				}
				else{ 
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion


		#region method Authenticate

		/// <summary>
		/// Does AUTH command.
		/// </summary>
		/// <param name="userName">Uesr name.</param>
		/// <param name="password">Password.</param>
		public void Authenticate(string userName,string password)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!(m_Supports_CramMd5 || m_Supports_Login)){
				throw new Exception("Authentication isn't supported.");
			}

			/* LOGIN
			 * Example:
			 *	C: AUTH<SP>LOGIN<CRLF>
			 *	S: 334<SP>base64(USERNAME)<CRLF>   // USERNAME is string constant
			 *	C: base64(username)<CRLF>
			 *  S: 334<SP>base64(PASSWORD)<CRLF>   // PASSWORD is string constant
			 *  C: base64(password)<CRLF>
			 *	S: 235 Ok<CRLF>
			*/ 

			/* Cram-M5
			   Example:
					C: AUTH<SP>CRAM-MD5<CRLF>
					S: 334<SP>base64(md5_calculation_hash)<CRLF>
					C: base64(username<SP>password_hash)<CRLF>
					S: 235 Ok<CRLF>
			*/

			if(m_Supports_CramMd5){
				m_pSocket.SendLine("AUTH CRAM-MD5");

				string responseLine = m_pSocket.ReadLine();
				// Response line must start with 334 or otherwise it's error response
				if(!responseLine.StartsWith("334")){
					throw new Exception(responseLine);
				}

				string md5HashKey = System.Text.Encoding.ASCII.GetString(Convert.FromBase64String(responseLine.Split(' ')[1]));
								
				HMACMD5 kMd5 = new HMACMD5(System.Text.Encoding.ASCII.GetBytes(password));
				byte[] md5HashByte = kMd5.ComputeHash(System.Text.Encoding.ASCII.GetBytes(md5HashKey));
				string hashedPwd = BitConverter.ToString(md5HashByte).ToLower().Replace("-","");
				
				m_pSocket.SendLine(Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(userName + " " + hashedPwd)));

				responseLine = m_pSocket.ReadLine();
				// Response line must start with 235 or otherwise it's error response
				if(!responseLine.StartsWith("235")){
					throw new Exception(responseLine);
				}
			}
			else if(m_Supports_Login){
				m_pSocket.SendLine("AUTH LOGIN");

				string responseLine = m_pSocket.ReadLine();
				// Response line must start with 334 or otherwise it's error response
				if(!responseLine.StartsWith("334")){
					throw new Exception(responseLine);
				}

				// Send user name to server
				m_pSocket.SendLine(Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(userName)));

				responseLine = m_pSocket.ReadLine();
				// Response line must start with 334 or otherwise it's error response
				if(!responseLine.StartsWith("334")){
					throw new Exception(responseLine);
				}

				// Send password to server
				m_pSocket.SendLine(Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(password)));

				responseLine = m_pSocket.ReadLine();
				// Response line must start with 235 or otherwise it's error response
				if(!responseLine.StartsWith("235")){
					throw new Exception(responseLine);
				}
			}
		}

		#endregion

		#region method BeginAuthenticate

		/// <summary>
		/// Begins authenticate.
		/// </summary>
		/// <param name="userName">Uesr name.</param>
		/// <param name="password">Password.</param> 
		/// <param name="callback">Callback to be called if command ends.</param>
		public void BeginAuthenticate(string userName,string password,CommadCompleted callback)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!(m_Supports_CramMd5 || m_Supports_Login)){
				throw new Exception("Authentication isn't supported.");
			}

			/* LOGIN
			 * Example:
			 *	C: AUTH<SP>LOGIN<CRLF>
			 *	S: 334<SP>base64(USERNAME)<CRLF>   // USERNAME is string constant
			 *	C: base64(username)<CRLF>
			 *  S: 334<SP>base64(PASSWORD)<CRLF>   // PASSWORD is string constant
			 *  C: base64(password)<CRLF>
			 *	S: 235 Ok<CRLF>
			*/ 

			/* Cram-M5
			   Example:
					C: AUTH<SP>CRAM-MD5<CRLF>
					S: 334<SP>base64(md5_calculation_hash)<CRLF>
					C: base64(username<SP>password_hash)<CRLF>
					S: 235 Ok<CRLF>
			*/

			if(m_Supports_CramMd5){
				m_pSocket.BeginSendLine("AUTH CRAM-MD5",new object[]{userName,password,callback},new SocketCallBack(this.OnAuthCramMd5SendFinished));
			}
			else if(m_Supports_Login){
				m_pSocket.BeginSendLine("AUTH LOGIN",new object[]{userName,password,callback},new SocketCallBack(this.OnAuthLoginSendFinished));
			}
		}

		#endregion

		#region method OnAuthCramMd5SendFinished

		/// <summary>
		/// Is called when smtp client has finished AUTH CRAM-MD5 command sending.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnAuthCramMd5SendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[2]);

			try{
				if(result == SocketCallBackResult.Ok){
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{(((object[])tag)[0]),(((object[])tag)[1]),callback,ms},new SocketCallBack(this.OnAuthCramMd5ReadServerResponseFinished));
				}
				else{					
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnAuthCramMd5ReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading AUTH CRAM-MD% server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param> 
		private void OnAuthCramMd5ReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[2]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[3])).ToArray());

					// Response line must start with 334 or otherwise it's error response
					if(!responseLine.StartsWith("334")){
						throw new Exception(responseLine);
					}
					else{
						string md5HashKey = System.Text.Encoding.ASCII.GetString(Convert.FromBase64String(responseLine.Split(' ')[1]));
					
						string userName = (string)(((object[])tag)[0]);
						string password = (string)(((object[])tag)[1]);

						HMACMD5 kMd5 = new HMACMD5(System.Text.Encoding.ASCII.GetBytes(password));
						byte[] md5HashByte = kMd5.ComputeHash(System.Text.Encoding.ASCII.GetBytes(md5HashKey));
						string hashedPwd = BitConverter.ToString(md5HashByte).ToLower().Replace("-","");

						// Start sending user name to server
						m_pSocket.BeginSendLine(Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(userName + " " + hashedPwd)),callback,new SocketCallBack(this.OnAuthCramMd5UserPwdSendFinished));
					}
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnAuthCramMd5UserPwdSendFinished

		/// <summary>
		/// Is called when smtp client has finished sending username and password to smtp server.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnAuthCramMd5UserPwdSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)tag;

			try{
				if(result == SocketCallBackResult.Ok){
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{callback,ms},new SocketCallBack(this.OnAuthCramMd5UserPwdReadServerResponseFinished));
				}
				else{					
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnAuthCramMd5UserPwdReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading user name and password send server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param> 
		private void OnAuthCramMd5UserPwdReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[0]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[1])).ToArray());

					// Response line must start with 235 or otherwise it's error response
					if(!responseLine.StartsWith("235")){
						throw new Exception(responseLine);
					}
					else{
						// AUTH CRAM-MD5 completed susscessfully, call callback method.
						callback(SocketCallBackResult.Ok,null);
					}
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion


		#region method OnAuthLoginSendFinished

		/// <summary>
		/// Is called when smtp client has finished AUTH LOGIN command sending.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnAuthLoginSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[2]);

			try{
				if(result == SocketCallBackResult.Ok){
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{(((object[])tag)[0]),(((object[])tag)[1]),callback,ms},new SocketCallBack(this.OnAuthLoginReadServerResponseFinished));
				}
				else{					
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnAuthLoginReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading MAIL FROM: command server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param> 
		private void OnAuthLoginReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[2]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[3])).ToArray());

					// Response line must start with 334 or otherwise it's error response
					if(!responseLine.StartsWith("334")){
						throw new Exception(responseLine);
					}
					else{
						string userName = (string)(((object[])tag)[0]);

						// Start sending user name to server
						m_pSocket.BeginSendLine(Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(userName)),new object[]{(((object[])tag)[1]),callback},new SocketCallBack(this.OnAuthLoginUserSendFinished));
					}
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnAuthLoginUserSendFinished

		/// <summary>
		/// Is called when smtp client has finished sending user name to SMTP server.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnAuthLoginUserSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[1]);

			try{
				if(result == SocketCallBackResult.Ok){
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{(((object[])tag)[0]),callback,ms},new SocketCallBack(this.OnAuthLoginUserReadServerResponseFinished));
				}
				else{					
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnAuthLoginUserReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading AUTH LOGIN user send server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param> 
		private void OnAuthLoginUserReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[1]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[2])).ToArray());

					// Response line must start with 334 or otherwise it's error response
					if(!responseLine.StartsWith("334")){
						throw new Exception(responseLine);
					}
					else{
						string password = (string)(((object[])tag)[0]);

						// Start sending password to server
						m_pSocket.BeginSendLine(Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(password)),new object[]{(((object[])tag)[1]),callback},new SocketCallBack(this.OnAuthLoginPasswordSendFinished));
					}
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnAuthLoginPasswordSendFinished

		/// <summary>
		/// Is called when smtp client has finished sending password to SMTP server.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnAuthLoginPasswordSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[1]);

			try{
				if(result == SocketCallBackResult.Ok){
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{callback,ms},new SocketCallBack(this.OnAuthLoginPwdReadServerResponseFinished));
				}
				else{					
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnAuthLoginPwdReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading AUTH LOGIN password send server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param> 
		private void OnAuthLoginPwdReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[0]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[1])).ToArray());

					// Response line must start with 235 or otherwise it's error response
					if(!responseLine.StartsWith("235")){
						throw new Exception(responseLine);
					}
					else{
						// AUTH LOGIN completed susscessfully, call callback method.
						callback(SocketCallBackResult.Ok,null);
					}
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion


		#region method SetSender

		/// <summary>
		/// Does MAIL FROM: command.
		/// </summary>
		/// <param name="senderEmail">Sender email address what is reported to smtp server</param>
		/// <param name="messageSize">Message size in bytes or -1 if message size isn't known.</param>
		public void SetSender(string senderEmail,long messageSize)
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}

			/* RFC 2821 4.1.1.2 MAIL
			 *  Examples:
			 *		MAIL FROM:<ivx@lumisoft.ee>
			 * 
			 * RFC 1870 adds optional SIZE keyword support.
			 * SIZE keyword may only be used if it's reported in EHLO command response.
			 *	Examples:
			 *		MAIL FROM:<ivx@lumisoft.ee> SIZE=1000
			*/

			if(m_Supports_Size && messageSize > -1){
				m_pSocket.SendLine("MAIL FROM:<" + senderEmail + "> SIZE=" + messageSize.ToString());
			}
			else{
				m_pSocket.SendLine("MAIL FROM:<" + senderEmail + ">");
			}

			string responseLine = m_pSocket.ReadLine();
			// Response line must start with 250 or otherwise it's error response
			if(!responseLine.StartsWith("250")){
				throw new Exception(responseLine);
			}
		}

		#endregion

		#region method BeginSetSender
		
		/// <summary>
		/// Begin setting sender.
		/// </summary>
		/// <param name="senderEmail">Sender email address what is reported to smtp server.</param>
		/// <param name="messageSize">Message size in bytes or -1 if message size isn't known.</param>
		/// <param name="callback">Callback to be called if command ends.</param>
		public void BeginSetSender(string senderEmail,long messageSize,CommadCompleted callback)
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}

			/* RFC 2821 4.1.1.2 MAIL
			 *  Examples:
			 *		MAIL FROM:<ivx@lumisoft.ee>
			 * 
			 * RFC 1870 adds optional SIZE keyword support.
			 * SIZE keyword may only be used if it's reported in EHLO command response.
			 *	Examples:
			 *		MAIL FROM:<ivx@lumisoft.ee> SIZE=1000
			*/

			if(m_Supports_Size && messageSize > -1){
				m_pSocket.BeginSendLine("MAIL FROM:<" + senderEmail + "> SIZE=" + messageSize.ToString(),callback,new SocketCallBack(this.OnMailSendFinished));
			}
			else{
				m_pSocket.BeginSendLine("MAIL FROM:<" + senderEmail + ">",callback,new SocketCallBack(this.OnMailSendFinished));
			}
		}

		#endregion
		
		#region method OnMailSendFinished

		/// <summary>
		/// Is called when smtp client has finished MAIL FROM: command sending.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnMailSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)tag;

			try{
				if(result == SocketCallBackResult.Ok){
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{callback,ms},new SocketCallBack(this.OnMailReadServerResponseFinished));
				}
				else{					
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnMailReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading MAIL FROM: command server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param> 
		private void OnMailReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[0]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[1])).ToArray());

					// Response line must start with 250 or otherwise it's error response
					if(!responseLine.StartsWith("250")){
						throw new Exception(responseLine);
					}
					else{
						// MAIL FROM: completed susscessfully, call callback method.
						callback(SocketCallBackResult.Ok,null);
					}
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion


		#region method AddRecipient

		/// <summary>
		/// Does RCPT TO: command.
		/// </summary>
		/// <param name="recipientEmail">Recipient email address.</param>
		public void AddRecipient(string recipientEmail)
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}

			/* RFC 2821 4.1.1.2 RCPT
			 *  Examples:
			 *		RCPT TO:<ivx@lumisoft.ee>
			*/

			m_pSocket.SendLine("RCPT TO:<" + recipientEmail + ">");

			string responseLine = m_pSocket.ReadLine();
			// Response line must start with 250 or otherwise it's error response
			if(!responseLine.StartsWith("250")){
				throw new Exception(responseLine);
			}
		}

		#endregion

		#region method BeginAddRecipient

		/// <summary>
		/// Begin adding recipient.
		/// </summary>
		/// <param name="recipientEmail">Recipient email address.</param>
		/// <param name="callback">Callback to be called if command ends.</param>
		public void BeginAddRecipient(string recipientEmail,CommadCompleted callback)
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}

			/* RFC 2821 4.1.1.2 RCPT
			 *  Examples:
			 *		RCPT TO:<ivx@lumisoft.ee>
			*/

			m_pSocket.BeginSendLine("RCPT TO:<" + recipientEmail + ">",callback,new SocketCallBack(this.OnRcptSendFinished));
		}

		#endregion

		#region method OnRcptSendFinished

		/// <summary>
		/// Is called when smtp client has finished RCPT TO: command sending.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnRcptSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)tag;

			try{
				if(result == SocketCallBackResult.Ok){
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{callback,ms},new SocketCallBack(this.OnRcptReadServerResponseFinished));	
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnRcptReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading RCPT TO: command server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnRcptReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[0]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[1])).ToArray());
			
					// Response line must start with 250 or otherwise it's error response
					if(!responseLine.StartsWith("250")){
						throw new Exception(responseLine);
					}
					else{
						// RCPT TO: completed susscessfully, call callback method.
						callback(SocketCallBackResult.Ok,null);
					}					
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion


		#region method SendMessage

		/// <summary>
		/// Sends message to server. NOTE: Message sending starts from message stream current posision.
		/// </summary>
		/// <param name="message">Message what will be sent to server. NOTE: Message sending starts from message stream current posision.</param>
		public void SendMessage(Stream message)
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}

			/* RFC 2821 4.1.1.4 DATA
			 * Notes:
			 *		Message must be period handled for DATA command. This meas if message line starts with .,
			 *		additional .(period) must be added.
			 *		Message send is ended with <CRLF>.<CRLF>.
			 *	Examples:
			 *		C: DATA<CRLF>
			 *		S: 354 Start sending message, end with <crlf>.<crlf><CRLF>
			 *		C: send_message
			 *		C: <CRLF>.<CRLF>
			*/

			/* RFC 3030 BDAT
			 *	Syntax:BDAT<SP>message_size_in_bytes<SP>LAST<CRLF>
			 *	
			 *	Exapmle:
			 *		C: BDAT 1000 LAST<CRLF>
			 *		C: send_1000_byte_message
			 *		S: 250 OK<CRLF>
			 * 
			*/

			if(m_Supports_Bdat){
				m_pSocket.SendLine("BDAT " + (message.Length - message.Position) + " LAST");

				m_pSocket.SendData(message);

				string responseLine = m_pSocket.ReadLine();
				// Response line must start with 250 or otherwise it's error response
				if(!responseLine.StartsWith("250")){
					throw new Exception(responseLine);
				}
			}
			else{
				// Do period handling
				MemoryStream ms = Core.DoPeriodHandling(message,true,true);

				m_pSocket.SendLine("DATA");
		
				string responseLine = m_pSocket.ReadLine();
				// Response line must start with 334 or otherwise it's error response
				if(!responseLine.StartsWith("354")){
					throw new Exception(responseLine);
				}

				m_pSocket.SendData(ms);

				responseLine = m_pSocket.ReadLine();
				// Response line must start with 250 or otherwise it's error response
				if(!responseLine.StartsWith("250")){
					throw new Exception(responseLine);
				}
			}
		}

		#endregion

		#region method BeginSendMessage

		/// <summary>
		/// Starts sending message.
		/// </summary>
		/// <param name="message">Message what will be sent to server. NOTE: Message sending starts from message stream current posision.</param>
		/// <param name="callback">Callback to be called if command ends.</param>
		public void BeginSendMessage(Stream message,CommadCompleted callback)
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}

			/* RFC 2821 4.1.1.4 DATA
			 * Notes:
			 *		Message must be period handled for DATA command. This meas if message line starts with .,
			 *		additional .(period) must be added.
			 *		Message send is ended with <CRLF>.<CRLF>.
			 *	Examples:
			 *		C: DATA<CRLF>
			 *		S: 354 Start sending message, end with <crlf>.<crlf><CRLF>
			 *		C: send_message
			 *		C: <CRLF>.<CRLF>
			*/

			/* RFC 3030 BDAT
			 *	Syntax:BDAT<SP>message_size_in_bytes<SP>LAST<CRLF>
			 *	
			 *	Exapmle:
			 *		C: BDAT 1000 LAST<CRLF>
			 *		C: send_1000_byte_message
			 *		S: 250 OK<CRLF>
			 * 
			*/

			if(m_Supports_Bdat){
				m_pSocket.BeginSendLine("BDAT " + (message.Length - message.Position) + " LAST",new object[]{message,callback},new SocketCallBack(this.OnBdatSendFinished));
			}
			else{
				// Do period handling
				MemoryStream ms = Core.DoPeriodHandling(message,true,true);

				m_pSocket.BeginSendLine("DATA",new object[]{message,callback},new SocketCallBack(this.OnDataSendFinished));				
			}
		}

		#endregion

		#region method OnBdatSendFinished

		/// <summary>
		/// Is called when smtp client has finished BDAT command sending.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnBdatSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[1]);

			try{
				if(result == SocketCallBackResult.Ok){
					// BDAT command successfully sent to SMTP server, start sending DATA.
					m_pSocket.BeginSendData((Stream)(((object[])tag)[0]),callback,new SocketCallBack(this.OnBdatDataSendFinished));	
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnBdatDataSendFinished

		/// <summary>
		/// Is called when smtp client has finished sending BDAT message data to smtp server.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnBdatDataSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)tag;

			try{
				if(result == SocketCallBackResult.Ok){
					// BDAT message data successfully sent to SMTP server, start reading server response
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{callback,ms},new SocketCallBack(this.OnBdatReadServerResponseFinished));	
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnBdatReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading BDAT: command server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnBdatReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[0]);

			try{
				if(result == SocketCallBackResult.Ok){					
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[1])).ToArray());
			
					// Response line must start with 250 or otherwise it's error response
					if(!responseLine.StartsWith("250")){
						throw new Exception(responseLine);
					}
					else{
						// BDAT: completed susscessfully, call callback method.
						callback(SocketCallBackResult.Ok,null);
					}					
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion


		#region method OnDataSendFinished

		/// <summary>
		/// Is called when smtp client has finished DATA command sending.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnDataSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[1]);

			try{
				if(result == SocketCallBackResult.Ok){
					// DATA command has sent to SMTP server, start reading server response.
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{(Stream)(((object[])tag)[0]),callback,ms},new SocketCallBack(this.OnDataReadServerResponseFinished));	
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnDataReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading DATA command server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnDataReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[1]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[2])).ToArray());

					// Response line must start with 334 or otherwise it's error response
					if(!responseLine.StartsWith("354")){
						throw new Exception(responseLine);
					}
					else{
						Stream message = (Stream)(((object[])tag)[0]);
						message.Seek(0,SeekOrigin.End);

						// Terminate message <CRLF>.<CRLF>
						message.Write(new byte[]{(byte)'\r',(byte)'\n',(byte)'.',(byte)'\r',(byte)'\n'},0,5);

						message.Seek(0,SeekOrigin.Begin);
					
						// Start sending message to smtp server
						m_pSocket.BeginSendData(message,callback,new SocketCallBack(this.OnDataMessageSendFinished));	
					}				
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnDataMessageSendFinished

		/// <summary>
		/// Is called when smtp client has sending MESSAGE to smtp server.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnDataMessageSendFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)tag;

			try{
				if(result == SocketCallBackResult.Ok){
					// Message has successfully sent to smtp server, start reading server response
					MemoryStream ms = new MemoryStream();
					m_pSocket.BeginReadLine(ms,1000,new object[]{callback,ms},new SocketCallBack(this.OnDataMessageSendReadServerResponseFinished));	
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion

		#region method OnDataMessageSendReadServerResponseFinished

		/// <summary>
		/// Is called when smtp client has finished reading MESSAGE send smtp server response line.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void OnDataMessageSendReadServerResponseFinished(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			CommadCompleted callback = (CommadCompleted)(((object[])tag)[0]);

			try{
				if(result == SocketCallBackResult.Ok){
					string responseLine = System.Text.Encoding.ASCII.GetString(((MemoryStream)(((object[])tag)[1])).ToArray());

					// Response line must start with 250 or otherwise it's error response
					if(!responseLine.StartsWith("250")){
						throw new Exception(responseLine);
					}
					else{
						// DATA: completed susscessfully, call callback method.
						callback(SocketCallBackResult.Ok,null);
					}
				}
				else{
					HandleSocketError(result,exception);
				}
			}
			catch(Exception x){
				// Pass exception to callback method
				callback(SocketCallBackResult.Exception,x);
			}
		}

		#endregion


		#region method Reset

		/// <summary>
		/// Send RSET command to SMTP server, resets SMTP session.
		/// </summary>
		public void Reset()
		{
			if(!m_Connected){
				throw new Exception("You must connect first");
			}


			m_pSocket.SendLine("RSET");

			string responseLine = m_pSocket.ReadLine();
			if(!responseLine.StartsWith("250")){
				throw new Exception(responseLine);
			}
		}

		#endregion


		#region method HandleSocketError

		/// <summary>
		/// Handles socket errors.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="x"></param>
		private void HandleSocketError(SocketCallBackResult result,Exception x)
		{
			if(result == SocketCallBackResult.Exception){
				throw x;
			}
			else{
				throw new Exception(result.ToString());
			}
		}

		#endregion


		#region static method QuickSendSmartHost

		/// <summary>
		/// Sends specified message to specified smart host. NOTE: Message sending starts from message stream current posision.
		/// </summary>
		/// <param name="smartHost">Smarthost name or IP.</param>
		/// <param name="port">SMTP port number. Normally this is 25.</param>
		/// <param name="hostName">Host name reported to SMTP server.</param>
		/// <param name="from">From address reported to SMTP server.</param>
		/// <param name="to">Message Recipients.</param>
		/// <param name="messageStream">Message stream. NOTE: Message sending starts from message stream current posision.</param>
		public static void QuickSendSmartHost(string smartHost,int port,string hostName,string from,string[] to,Stream messageStream)
		{
			using(SmtpClientEx smtp = new SmtpClientEx()){
				smtp.Connect(smartHost,port);

				smtp.Ehlo(hostName);				
				smtp.SetSender(MailboxAddress.Parse(from).EmailAddress,messageStream.Length - messageStream.Position);
				foreach(string t in to){
					smtp.AddRecipient(MailboxAddress.Parse(t).EmailAddress);
				}

				smtp.SendMessage(messageStream);
			}
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets local endpoint. Returns null if smtp client isn't connected.
		/// </summary>
		public EndPoint LocalEndpoint
		{
			get{
				if(m_pSocket != null){
					return m_pSocket.LocalEndPoint; 
				}
				else{
					return null;
				}
			}
		}

		/// <summary>
		/// Gets remote endpoint. Returns null if smtp client isn't connected.
		/// </summary>
		public EndPoint RemoteEndPoint
		{
			get{
				if(m_pSocket != null){
					return m_pSocket.RemoteEndPoint; 
				}
				else{
					return null;
				}
			}
		}

		/// <summary>
		/// Gets or sets dns servers.
		/// </summary>
		public string[] DnsServers
		{
			get{ return m_pDnsServers; }

			set{ m_pDnsServers = value; }
		}

		/// <summary>
		/// Gets if smtp client is connected.
		/// </summary>
		public bool Connected
		{
			get{ return m_Connected; }
		}

		/// <summary>
		/// Gets when was last activity.
		/// </summary>
		public DateTime LastDataTime
		{
			get{ return m_LastDataTime; }
		}

		#endregion		
	}
}
