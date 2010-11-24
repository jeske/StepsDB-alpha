using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Collections;
using System.Security.Cryptography;

namespace LumiSoft.Net.POP3.Client
{
	/// <summary>
	/// POP3 Client.
	/// </summary>
	/// <example>
	/// <code>
	/// 
	/// /*
	///  To make this code to work, you need to import following namespaces:
	///  using LumiSoft.Net.Mime;
	///  using LumiSoft.Net.POP3.Client; 
	///  */
	/// 
	/// using(POP3_Client c = new POP3_Client()){
	///		c.Connect("ivx",110);
	///		c.Authenticate("test","test",true);
	///		
	///		POP3_MessagesInfo mInf = c.GetMessagesInfo();
	///		
	///		// Get first message if there is any
	///		if(mInf.Count > 0){
	///			byte[] messageData = c.GetMessage(mInf.Messages[0].MessageNumber);
	///		
	///			// Do your suff
	///			
	///			// Parse message
	///			MimeParser m = MimeParser(messageData);
	///			string from = m.From;
	///			string subject = m.Subject;			
	///			// ... 
	///		}		
	///	}
	/// </code>
	/// </example>
	public class POP3_Client : IDisposable
	{
		private BufferedSocket m_pSocket       = null;
		private SocketLogger   m_pLogger       = null;
		private bool           m_Connected     = false;
		private bool           m_Authenticated = false;
		private string         m_ApopHashKey   = "";
		private bool           m_LogCmds       = false;

		/// <summary>
		/// Occurs when POP3 session has finished and session log is available.
		/// </summary>
		public event LogEventHandler SessionLog = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public POP3_Client()
		{				
		}

		#region function Dispose

		/// <summary>
		/// Clean up any resources being used.
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


		#region function Connect

		/// <summary>
		/// Connects to specified host.
		/// </summary>
		/// <param name="host">Host name.</param>
		/// <param name="port">Port number.</param>
		public void Connect(string host,int port)
		{
			if(!m_Connected){
				Socket s = new Socket(AddressFamily.InterNetwork,SocketType.Stream,ProtocolType.Tcp);
				IPEndPoint ipdest = new IPEndPoint(System.Net.Dns.Resolve(host).AddressList[0],port);
				s.Connect(ipdest);
				s.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.NoDelay,1);

				m_pSocket = new BufferedSocket(s);

				if(m_LogCmds && SessionLog != null){
					m_pLogger = new SocketLogger(s,SessionLog);
					m_pLogger.SessionID = Guid.NewGuid().ToString();
					m_pSocket.Logger = m_pLogger;
				}

				// Set connected flag
				m_Connected = true;

				string reply = m_pSocket.ReadLine();
				if(reply.StartsWith("+OK")){
					// Try to read APOP hash key, if supports APOP
					if(reply.IndexOf("<") > -1 && reply.IndexOf(">") > -1){
						m_ApopHashKey = reply.Substring(reply.LastIndexOf("<"),reply.LastIndexOf(">") - reply.LastIndexOf("<") + 1);
					}
				}				
			}
		}

		#endregion

		#region function Disconnect

		/// <summary>
		/// Closes connection to POP3 server.
		/// </summary>
		public void Disconnect()
		{
			try{
				if(m_pSocket != null){
					// Send QUIT
					m_pSocket.SendLine("QUIT");			

					m_pSocket.Shutdown(SocketShutdown.Both);					
				}
			}
			catch{
			}

			if(m_pLogger != null){
				m_pLogger.Flush();
			}
			m_pLogger = null;

			m_pSocket       = null;
			m_Connected     = false;			
			m_Authenticated = false;
		}

		#endregion

		#region function Authenticate

		/// <summary>
		/// Authenticates user.
		/// </summary>
		/// <param name="userName">User login name.</param>
		/// <param name="password">Password.</param>
		/// <param name="tryApop"> If true and POP3 server supports APOP, then APOP is used, otherwise normal login used.</param>
		public void Authenticate(string userName,string password,bool tryApop)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(m_Authenticated){
				throw new Exception("You are already authenticated !");
			}

			// Supports APOP, use it
			if(tryApop && m_ApopHashKey.Length > 0){
				//--- Compute md5 hash -----------------------------------------------//
				byte[] data = System.Text.Encoding.ASCII.GetBytes(m_ApopHashKey + password);
			
				MD5 md5 = new MD5CryptoServiceProvider();			
				byte[] hash = md5.ComputeHash(data);

				string hexHash = BitConverter.ToString(hash).ToLower().Replace("-","");
				//---------------------------------------------------------------------//

				m_pSocket.SendLine("APOP " + userName + " " + hexHash);

				string reply = m_pSocket.ReadLine();
				if(reply.StartsWith("+OK")){
					m_Authenticated = true;
				}
				else{
					throw new Exception("Server returned:" + reply);
				}
			}
			else{ // Use normal LOGIN, don't support APOP 
				m_pSocket.SendLine("USER " + userName);

				string reply = m_pSocket.ReadLine();
				if(reply.StartsWith("+OK")){
					m_pSocket.SendLine("PASS " + password);

					reply = m_pSocket.ReadLine();
					if(reply.StartsWith("+OK")){
						m_Authenticated = true;
					}
					else{
						throw new Exception("Server returned:" + reply);
					}
				}
				else{
					throw new Exception("Server returned:" + reply);
				}				
			}
		}

		#endregion


		#region function GetMessagesInfo

		/// <summary>
		/// Gets messages info.
		/// </summary>
		public POP3_MessagesInfo GetMessagesInfo()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			POP3_MessagesInfo messagesInfo = new POP3_MessagesInfo();

			// Before getting list get UIDL list, then we make full message info (UID,Nr,Size).
			Hashtable uidlList = GetUidlList();

			m_pSocket.SendLine("LIST");

			/* NOTE: If reply is +OK, this is multiline respone and is terminated with '.'.
			Examples:
				C: LIST
				S: +OK 2 messages (320 octets)
				S: 1 120				
				S: 2 200
				S: .
				...
				C: LIST 3
				S: -ERR no such message, only 2 messages in maildrop
			*/

			// Read first line of reply, check if it's ok
			string line = m_pSocket.ReadLine();
			if(line.StartsWith("+OK")){
				// Read lines while get only '.' on line itshelf.
				while(true){
					line = m_pSocket.ReadLine();

					// End of data
					if(line.Trim() == "."){
						break;
					}
					else{
						string[] param = line.Trim().Split(new char[]{' '});
						int  nr   = Convert.ToInt32(param[0]);
						long size = Convert.ToInt64(param[1]);

						messagesInfo.Add(uidlList[nr].ToString(),nr,size);
					}
				}
			}
			else{
				throw new Exception("Server returned:" + line);
			}

			return messagesInfo;
		}

		#endregion

		#region function GetUidlList

		/// <summary>
		/// Gets uid listing.
		/// </summary>
		/// <returns>Returns Hashtable containing uidl listing. Key column contains message NR and value contains message UID.</returns>
		public Hashtable GetUidlList()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			Hashtable retVal = new Hashtable();

			m_pSocket.SendLine("UIDL");

			/* NOTE: If reply is +OK, this is multiline respone and is terminated with '.'.
			Examples:
				C: UIDL
				S: +OK
				S: 1 whqtswO00WBw418f9t5JxYwZ
				S: 2 QhdPYR:00WBw1Ph7x7
				S: .
				...
				C: UIDL 3
				S: -ERR no such message
			*/

			// Read first line of reply, check if it's ok
			string line = m_pSocket.ReadLine();
			if(line.StartsWith("+OK")){
				// Read lines while get only '.' on line itshelf.				
				while(true){
					line = m_pSocket.ReadLine();

					// End of data
					if(line.Trim() == "."){
						break;
					}
					else{
						string[] param = line.Trim().Split(new char[]{' '});
						int    nr  = Convert.ToInt32(param[0]);
						string uid = param[1];

						retVal.Add(nr,uid);
					}
				}
			}
			else{
				throw new Exception("Server returned:" + line);
			}

			return retVal;
		}

		#endregion

		#region function GetMessage
/*
		/// <summary>
		/// Transfers specified message to specified socket. This is good for example transfering message from remote POP3 server to POP3 client.
		/// </summary>
		/// <param name="nr">Message number.</param>
		/// <param name="socket">Socket where to store message.</param>
		public void GetMessage(int nr,Socket socket)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			Core.SendLine(m_pSocket,"RETR " + nr.ToString());

			// Read first line of reply, check if it's ok
			string line = Core.ReadLine(m_pSocket);
			if(line.StartsWith("+OK")){
				NetworkStream readStream  = new NetworkStream(m_pSocket);
				NetworkStream storeStream = new NetworkStream(socket);

				byte[] crlf = new byte[]{(byte)'\r',(byte)'\n'};
				StreamLineReader reader = new StreamLineReader(readStream);
				byte[] lineData = reader.ReadLine();
				while(lineData != null){
					// End of message reached
					if(lineData.Length == 1 && lineData[0] == '.'){
						return;
					}

					storeStream.Write(lineData,0,lineData.Length);
					storeStream.Write(crlf,0,crlf.Length);
					lineData = reader.ReadLine();
				}
			}
			else{
				throw new Exception("Server returned:" + line);
			}
		}
*/
		/// <summary>
		/// Gets specified message.
		/// </summary>
		/// <param name="nr">Message number.</param>
		public byte[] GetMessage(int nr)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("RETR " + nr.ToString());

			// Read first line of reply, check if it's ok
			string line = m_pSocket.ReadLine();
			if(line.StartsWith("+OK")){
				MemoryStream strm = new MemoryStream();
				ReadReplyCode code = m_pSocket.ReadData(strm,100000000,"\r\n.\r\n",".\r\n");
				if(code != ReadReplyCode.Ok){
					throw new Exception("Error:" + code.ToString());
				}
				return Core.DoPeriodHandling(strm,false).ToArray();
			}
			else{
				throw new Exception("Server returned:" + line);
			}
		}

		#endregion

		#region function DeleteMessage

		/// <summary>
		/// Deletes specified message
		/// </summary>
		/// <param name="messageNr">Message number.</param>
		public void DeleteMessage(int messageNr)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("DELE " + messageNr.ToString());

			// Read first line of reply, check if it's ok
			string line = m_pSocket.ReadLine();
			if(!line.StartsWith("+OK")){
				throw new Exception("Server returned:" + line);
			}
		}

		#endregion

		#region function GetTopOfMessage

		/// <summary>
		/// Gets top lines of message.
		/// </summary>
		/// <param name="nr">Message number which top lines to get.</param>
		/// <param name="nLines">Number of lines to get.</param>
		public byte[] GetTopOfMessage(int nr,int nLines)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}
			

			m_pSocket.SendLine("TOP " + nr.ToString() + " " + nLines.ToString());

			// Read first line of reply, check if it's ok
			string line = m_pSocket.ReadLine();
			if(line.StartsWith("+OK")){
				MemoryStream strm = new MemoryStream();
				ReadReplyCode code = m_pSocket.ReadData(strm,100000000,"\r\n.\r\n",".\r\n");
				if(code != ReadReplyCode.Ok){
					throw new Exception("Error:" + code.ToString());
				}
				return Core.DoPeriodHandling(strm,false).ToArray();
			}
			else{
				throw new Exception("Server returned:" + line);
			}
		}

		#endregion

		#region function Reset

		/// <summary>
		/// Resets session.
		/// </summary>
		public void Reset()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("RSET");

			// Read first line of reply, check if it's ok
			string line = m_pSocket.ReadLine();
			if(!line.StartsWith("+OK")){
				throw new Exception("Server returned:" + line);
			}
		}

		#endregion


		#region properties Implementation

		/// <summary>
		/// Gets or sets if to log commands.
		/// </summary>
		public bool LogCommands
		{
			get{ return m_LogCmds;	}

			set{ m_LogCmds = value; }
		}

		#endregion

	}
}
