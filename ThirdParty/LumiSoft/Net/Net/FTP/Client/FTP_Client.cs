using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Data;

namespace LumiSoft.Net.FTP.Client
{
	/// <summary>
	/// Transfer mode.
	/// </summary>
	internal enum TransferMode
	{
		/// <summary>
		/// ASCII transfer mode.
		/// </summary>
		Ascii = 0,
		/// <summary>
		/// Binary transfer mode. 
		/// </summary>
		Binary = 1,
	}

	/// <summary>
	/// Ftp client.
	/// </summary>
	public class FTP_Client : IDisposable
	{
		private BufferedSocket m_pSocket       = null;
		private bool           m_Connected     = false;
		private bool           m_Authenticated = false;
		private bool           m_Passive       = true;

		/// <summary>
		/// Default connection.
		/// </summary>
		public FTP_Client()
		{			
		}

		#region function Dispose

		/// <summary>
		/// Clears resources and closes connection if open.
		/// </summary>
		public void Dispose()
		{
			Disconnect();
		}

		#endregion


		#region function Connect

		/// <summary>
		/// Connects to specified host.
		/// </summary>
		/// <param name="host">Host name.</param>
		/// <param name="port">Port.</param>
		public void Connect(string host,int port)
		{
			Socket s = new Socket(AddressFamily.InterNetwork,SocketType.Stream,ProtocolType.Tcp);
			IPEndPoint ipdest = new IPEndPoint(System.Net.Dns.Resolve(host).AddressList[0],port);
			s.Connect(ipdest);
			s.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.NoDelay,1);

			m_pSocket = new BufferedSocket(s);
			
			string reply = m_pSocket.ReadLine();
			while(!reply.StartsWith("220 ")){
				reply = m_pSocket.ReadLine();
			}

			m_Connected = true;
		}

		#endregion

		#region function Disconnect

		/// <summary>
		/// Disconnects from active host.
		/// </summary>
		public void Disconnect()
		{
			if(m_pSocket != null){
				if(m_pSocket.Connected){
					// Send QUIT
					m_pSocket.SendLine("QUIT");
				}

		//		m_pSocket.Close();
				m_pSocket = null;
			}

			m_Connected     = false;
			m_Authenticated = false;
		}

		#endregion

		#region function Authenticate

		/// <summary>
		/// Authenticates user.
		/// </summary>
		/// <param name="userName">User name.</param>
		/// <param name="password">Password.</param>
		public void Authenticate(string userName,string password)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(m_Authenticated){
				throw new Exception("You are already authenticated !");
			}

			m_pSocket.SendLine("USER " + userName);

			string reply = m_pSocket.ReadLine();
			if(reply.StartsWith("331")){
				m_pSocket.SendLine("PASS " + password);

				/* FTP server may give multiline reply here
				/  For example:
				/	230-User someuser has group access to:  someuser
				/	230 OK. Current restricted directory is /
				*/
				reply = m_pSocket.ReadLine();
				while(!reply.StartsWith("230 ")){
					if(!reply.StartsWith("230")){
						throw new Exception(reply);
					}

					reply = m_pSocket.ReadLine();
				}

				m_Authenticated = true;
			}
			else{
				throw new Exception(reply);
			}
		}

		#endregion

		
		#region function SetCurrentDir

		/// <summary>
		/// Sets current directory.
		/// </summary>
		/// <param name="dir">Directory.</param>
		public void SetCurrentDir(string dir)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("CWD " + dir);

			string reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("250")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion


		#region function GetList
		
		/// <summary>
		/// Gets directory listing.
		/// </summary>
		/// <returns>Returns DataSet(DirInfo DataTable) with directory listing info.</returns>
		public DataSet GetList()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			// Set transfer mode
			this.SetTransferMode(TransferMode.Ascii);

			Socket socket = null;

			MemoryStream storeStream = new MemoryStream();
			string       reply       = "";

			try{
				if(m_Passive){
					socket = GetDataConnection(-1);

					// Send LIST command
					m_pSocket.SendLine("LIST");

					reply = m_pSocket.ReadLine();
					if(!(reply.StartsWith("125") || reply.StartsWith("150"))){
						throw new Exception(reply);
					}
				}
				else{
					int port = this.Port();

					// Send LIST command
					m_pSocket.SendLine("LIST");

					reply = m_pSocket.ReadLine();
					if(!(reply.StartsWith("125") || reply.StartsWith("150"))){
						throw new Exception(reply);
					}

					socket = GetDataConnection(port);
				}

				int count = 1;
				while(count > 0){
					byte[] data  = new Byte[4000];
					count = socket.Receive(data,data.Length,SocketFlags.None);
					storeStream.Write(data,0,count);
				}

				if(socket != null){
					socket.Shutdown(SocketShutdown.Both);
					socket.Close();
				}
			}
			catch(Exception x){
				if(socket != null){
					socket.Shutdown(SocketShutdown.Both);
					socket.Close();
				}

				throw x;
			}
	
			// Get "226 Transfer Complete" response
			/* FTP server may give multiline reply here
			/  For example:
			/	226-Options: -l
			/	226 6 matches total
			*/
			reply = m_pSocket.ReadLine();
			while(!reply.StartsWith("226 ")){
				if(!reply.StartsWith("226")){
					throw new Exception(reply);
				}

				reply = m_pSocket.ReadLine();
			}

			return ParseDirListing(System.Text.Encoding.Default.GetString(storeStream.ToArray()));
		}

		#endregion

		#region function CreateDir

		/// <summary>
		/// Creates directory.
		/// </summary>
		/// <param name="dir">Directory name.</param>
		public void CreateDir(string dir)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("MKD " + dir);

			string reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("257")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region function RenameDir

		/// <summary>
		/// Renames directory.
		/// </summary>
		/// <param name="oldDir">Name of directory which to rename.</param>
		/// <param name="newDir">New directory name.</param>
		public void RenameDir(string oldDir,string newDir)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("RNFR " + oldDir);

			string reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("350")){
				throw new Exception("Server returned:" + reply);
			}

			m_pSocket.SendLine("RNTO " + newDir);

			reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("250")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region function DeleteDir

		/// <summary>
		/// Deletes directory.
		/// </summary>
		/// <param name="dir">Name of directory which to delete.</param>
		public void DeleteDir(string dir)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			// ToDo: delete all sub directories and files

			m_pSocket.SendLine("RMD " + dir);

			string reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("250")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion


		#region function ReceiveFile

		/// <summary>
		/// Recieves specified file from server.
		/// </summary>
		/// <param name="fileName">File name of file which to receive.</param>
		/// <param name="putFileName">File path+name which to store.</param>
		public void ReceiveFile(string fileName,string putFileName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			using(FileStream fs = File.Create(putFileName)){
				ReceiveFile(fileName,fs);
			}
		}

		/// <summary>
		/// Recieves specified file from server.
		/// </summary>
		/// <param name="fileName">File name of file which to receive.</param>
		/// <param name="storeStream">Stream where to store file.</param>
		public void ReceiveFile(string fileName,Stream storeStream)
		{
			
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			// Set transfer mode
			this.SetTransferMode(TransferMode.Binary);

			Socket socket = null;

			string reply = "";

			try{
				if(m_Passive){
					socket = GetDataConnection(-1);

					// Send RETR command
					m_pSocket.SendLine("RETR " + fileName);

					reply = m_pSocket.ReadLine();
					if(!(reply.StartsWith("125") || reply.StartsWith("150"))){
						throw new Exception(reply);
					}
				}
				else{
					int port = this.Port();

					// Send RETR command
					m_pSocket.SendLine("RETR " + fileName);

					reply = m_pSocket.ReadLine();
					if(!(reply.StartsWith("125") || reply.StartsWith("150"))){
						throw new Exception(reply);
					}

					socket = GetDataConnection(port);
				}

				int count = 1;
				while(count > 0){
					byte[] data  = new byte[4000];
					count = socket.Receive(data,data.Length,SocketFlags.None);
					storeStream.Write(data,0,count);
				}
			
				if(socket != null){
					socket.Shutdown(SocketShutdown.Both);
					socket.Close();
				}
			}
			catch(Exception x){
				if(socket != null){
					socket.Shutdown(SocketShutdown.Both);
					socket.Close();
				}

				throw x;
			}

			// Get "226 Transfer Complete" response
			/* FTP server may give multiline reply here
			/  For example:
			/	226-File successfully transferred
			/	226 0.002 seconds (measured here), 199.65 Mbytes per second 339163 bytes received in 00:00 (8.11 MB/s)
			*/
			reply = m_pSocket.ReadLine();
			while(!reply.StartsWith("226 ")){
				if(!reply.StartsWith("226")){
					throw new Exception(reply);
				}

				reply = m_pSocket.ReadLine();
			}

			// Get "226 Transfer Complete" response
		/*	reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("226")){
				throw new Exception(reply);
			}*/
		}

		#endregion

		#region function StoreFile

		/// <summary>
		/// Stores specified file to server.
		/// </summary>
		/// <param name="getFileName">File path+name which to store in server.</param>
		public void StoreFile(string getFileName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			using(FileStream fs = File.OpenRead(getFileName)){
				StoreFile(fs,Path.GetFileName(getFileName));
			}
		}

		/// <summary>
		/// Stores specified file to server.
		/// </summary>
		/// <param name="getStream">Stream from where to gets file.</param>
		/// <param name="fileName">File name to store in server.</param>
		public void StoreFile(Stream getStream,string fileName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			// Set transfer mode
			this.SetTransferMode(TransferMode.Binary);

			Socket socket = null;

			string reply = "";

			try{
				if(m_Passive){
					socket = GetDataConnection(-1);

					// Send STOR command
					m_pSocket.SendLine("STOR " + fileName);

					reply = m_pSocket.ReadLine();
					if(!(reply.StartsWith("125") || reply.StartsWith("150"))){
						throw new Exception(reply);
					}
				}
				else{
					int port = this.Port();

					// Send STOR command
					m_pSocket.SendLine("STOR " + fileName);

					reply = m_pSocket.ReadLine();
					if(!(reply.StartsWith("125") || reply.StartsWith("150"))){
						throw new Exception(reply);
					}

					socket = GetDataConnection(port);
				}

				int count = 1;
				while(count > 0){
					byte[] data  = new Byte[4000];
					count = getStream.Read(data,0,data.Length);
					socket.Send(data,0,count,SocketFlags.None);
				}

				if(socket != null){
					socket.Shutdown(SocketShutdown.Both);
					socket.Close();
				}
			}
			catch(Exception x){
				if(socket != null){
					socket.Shutdown(SocketShutdown.Both);
					socket.Close();
				}

				throw x;
			}

			// Get "226 Transfer Complete" response
			/* FTP server may give multiline reply here
			/  For example:
			/	226-File successfully transferred
			/	226 0.016 seconds (measured here), 6.64 Mbytes per second 110309 bytes sent in 00:00 (2.58 MB/s)
			*/
			reply = m_pSocket.ReadLine();
			while(!reply.StartsWith("226 ")){
				if(!reply.StartsWith("226")){
					throw new Exception(reply);
				}

				reply = m_pSocket.ReadLine();
			}

			// Get "226 Transfer Complete" response
		/*	reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("226")){
				throw new Exception(reply);
			}*/
		}

		#endregion
		
		#region function DeleteFile

		/// <summary>
		/// Deletes specified file or directory.
		/// </summary>
		/// <param name="file">File name.</param>
		public void DeleteFile(string file)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("DELE " + file);

			string reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("250")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region function RenameFile

		/// <summary>
		/// Renames specified file or directory.
		/// </summary>
		/// <param name="oldFileName">File name of file what to rename.</param>
		/// <param name="newFileName">New file name.</param>
		public void RenameFile(string oldFileName,string newFileName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("RNFR " + oldFileName);

			string reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("350")){
				throw new Exception("Server returned:" + reply);
			}

			m_pSocket.SendLine("RNTO " + newFileName);

			reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("250")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		
		#region function Port
        
		private int Port()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			/*
			 Syntax:{PORT ipPart1,ipPart1,ipPart1,ipPart1,portPart1,portPart1<CRLF>}
			
			<host-port> ::= <host-number>,<port-number>
            <host-number> ::= <number>,<number>,<number>,<number>
            <port-number> ::= <number>,<number>
            <number> ::= any decimal integer 1 through 255
			*/
		
			
			IPHostEntry ipThis = System.Net.Dns.GetHostByName(System.Net.Dns.GetHostName());
			Random r = new Random();
			int port = 0;
			bool found = false;
			// we will try all IP addresses assigned to this machine
			// the first one that the remote machine likes will be chosen
			for(int tryCount=0;!found && tryCount<20;tryCount++) {
				for(int i=0;i<ipThis.AddressList.Length;i++){
					string ip = ipThis.AddressList[i].ToString().Replace(".",",");
					int p1 = r.Next(100);
					int p2 = r.Next(100);

					port = (p1 << 8) | p2;
			
					m_pSocket.SendLine("PORT " + ip + "," + p1.ToString() + "," + p2.ToString());

					string reply = m_pSocket.ReadLine();
					if(reply.StartsWith("200")){
						found = true;
					break;
					}
				}
			}

			if(!found){
				throw new Exception("No suitable port found");
			}

			return port;
		}

		#endregion

		#region function SetTransferMode

		/// <summary>
		/// Sets transfer mode.
		/// </summary>
		/// <param name="mode">Transfer mode.</param>
		private void SetTransferMode(TransferMode mode)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}

			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			switch(mode)
			{
				case TransferMode.Ascii:
					m_pSocket.SendLine("TYPE A");
					break;

				case TransferMode.Binary:
					m_pSocket.SendLine("TYPE I");
					break;
			}			

			string reply = m_pSocket.ReadLine();
			if(!reply.StartsWith("200")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		
		#region method ParseDirListing

		/// <summary>
		/// Parses server returned directory listing.
		/// </summary>
		/// <param name="list"></param>
		/// <returns></returns>
		private DataSet ParseDirListing(string list)
		{
			DataSet   ds = new DataSet();
			DataTable dt = ds.Tables.Add("DirInfo");
			dt.Columns.Add("Name");
			dt.Columns.Add("Date",typeof(DateTime));
			dt.Columns.Add("Size",typeof(long));
			dt.Columns.Add("IsDirectory",typeof(bool));

			// Remove continues spaces
			while(list.IndexOf("  ") > -1){
				list = list.Replace("  "," ");
			}

			string[] entries = list.Replace("\r\n","\n").Split('\n');
			foreach(string entry in entries){
				if(entry.Length > 0){
					string[] entryParts = entry.Split(' ');
					
					DateTime date  = DateTime.Today;
					long     size  = 0;
					bool     isDir = false;
					string   name  = "";

					bool winListing = false;
					try{
						string[] dateFormats = new string[]{
							"MM-dd-yy hh:mmtt",
							"M-d-yy h:mmtt",
						};
						date = DateTime.ParseExact(entryParts[0] + " " + entryParts[1],dateFormats,System.Globalization.CultureInfo.InvariantCulture,System.Globalization.DateTimeStyles.None);
						winListing = true;
					}
					catch{
					}

					// Windows listing
					if(winListing){
						string[] dateFormats = new string[]{
						   "MM-dd-yy hh:mmtt",
						   "M-d-yy h:mmtt",
						   "MM-dd-yy hh:mm",
						   "M-d-yy h:mm"
						};
						// Date
						date = DateTime.ParseExact(entryParts[0] + " " + entryParts[1],dateFormats,System.Globalization.CultureInfo.InvariantCulture,System.Globalization.DateTimeStyles.None);

						// This block is <DIR> or file size
						if(entryParts[2].ToUpper().IndexOf("DIR") > -1){
							isDir = true;
						}
						else{
							size = Convert.ToInt64(entryParts[2]);
						}

						// Name. Name starts from 3 to ... (if contains <SP> in name)
						for(int i=3;i<entryParts.Length;i++){
							name += entryParts[i] + " ";
						}
						name = name.Trim();						
					}
					// Unix listing
					else{
						string[] dateFormats = new string[]{
							"MMM dd HH:mm",
							"MMM dd H:mm",
							"MMM d HH:mm",
							"MMM d H:mm",
							"MMM dd yyyy",
							"MMM d yyyy",
						};
						// Date
						date = DateTime.ParseExact(entryParts[5] + " " + entryParts[6] + " " + entryParts[7],dateFormats,System.Globalization.CultureInfo.InvariantCulture,System.Globalization.DateTimeStyles.None);

						// IsDir
						if(entryParts[0].ToUpper().StartsWith("D")){
							isDir = true;
						}

						// Size
						size = Convert.ToInt64(entryParts[4]);

						// Name. Name starts from 8 to ... (if contains <SP> in name)
						for(int i=8;i<entryParts.Length;i++){
							name += entryParts[i] + " ";
						}
						name = name.Trim();	
					}

					dt.Rows.Add(new object[]{name,date,size,isDir});
				}
			}

			return ds;
		}

		#endregion

		#region method GetDataConnection

		private Socket GetDataConnection(int portA)
		{
			// Passive mode
			if(m_Passive){
				// Send PASV command
				m_pSocket.SendLine("PASV");
				
				// Get 227 Entering Passive Mode (192,168,1,10,1,10)
				string reply = m_pSocket.ReadLine();
				if(!reply.StartsWith("227")){
					throw new Exception(reply);
				}
				
				// Parse IP and port
				reply = reply.Substring(reply.IndexOf("(") + 1,reply.IndexOf(")") - reply.IndexOf("(") - 1);
				string[] parts = reply.Split(',');
				
				string ip   = parts[0] + "." + parts[1] + "." + parts[2] + "." + parts[3];
				int    port = (Convert.ToInt32(parts[4]) << 8) | Convert.ToInt32(parts[5]);
				
				Socket socket = new Socket(AddressFamily.InterNetwork,SocketType.Stream,ProtocolType.Tcp);
			//	socket.Connect(new IPEndPoint(System.Net.Dns.GetHostByAddress(ip).AddressList[0],port));
				socket.Connect(new IPEndPoint(IPAddress.Parse(ip),port));

				return socket;
			}
			// Active mode
			else{
				TcpListener conn = new TcpListener(IPAddress.Any,portA);
				conn.Start();

				//--- Wait ftp server connection -----------------------------//			
				long startTime = DateTime.Now.Ticks;
				// Wait ftp server to connect
				while(!conn.Pending()){
					System.Threading.Thread.Sleep(50);

					// Time out after 30 seconds
					if((DateTime.Now.Ticks - startTime) / 10000 > 20000){
						throw new Exception("Ftp server didn't respond !");
					}
				}
				//-----------------------------------------------------------//
				
				Socket connectedFtpServer = conn.AcceptSocket();

				// Stop listening
				conn.Stop();
				
				return connectedFtpServer;
			}
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets data connection mode.
		/// Passive - client connects to ftp server.
		/// Active  - ftp server connects to client.
		/// </summary>
		public bool PassiveMode
		{
			get{ return m_Passive; }

			set{ m_Passive = value; }
		}

		#endregion

	}
}
