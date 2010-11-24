using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Collections;
using System.Security.Cryptography;

using LumiSoft.Net.IMAP.Server;

namespace LumiSoft.Net.IMAP.Client
{
	/// <summary>
	/// IMAP client.
	/// </summary>
	/// <example>
	/// <code>
	/// using(IMAP_Client c = new IMAP_Client()){
	///		c.Connect("ivx",143);
	///		c.Authenticate("test","test");
	///				
	///		c.SelectFolder("Inbox");
	///		
	///		// Get messages header here
	///		IMAP_FetchItem msgsInfo = c.FetchMessages(1,-1,false,true,true);
	///		
	///		// Do your suff
	///	}
	/// </code>
	/// </example>
	public class IMAP_Client : IDisposable
	{
		private BufferedSocket m_pSocket        = null;
		private bool   m_Connected      = false;
		private bool   m_Authenticated  = false;
		private string m_SelectedFolder = "";
		private int    m_MsgCount       = 0;
		private int    m_NewMsgCount    = 0;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public IMAP_Client()
		{			
		}

		#region method Dispose

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		public void Dispose()
		{
			Disconnect();
		}

		#endregion


		#region method Connect

		/// <summary>
		/// Connects to IMAP server.
		/// </summary>		
		/// <param name="host">Host name.</param>
		/// <param name="port">Port number.</param>
		public void Connect(string host,int port)
		{
			if(!m_Connected){
			//	m_pSocket = new Socket(AddressFamily.InterNetwork,SocketType.Stream,ProtocolType.Tcp);
			//	IPEndPoint ipdest = new IPEndPoint(System.Net.Dns.Resolve(host).AddressList[0],port);
			//	m_pSocket.Connect(ipdest);

				Socket s = new Socket(AddressFamily.InterNetwork,SocketType.Stream,ProtocolType.Tcp);
				IPEndPoint ipdest = new IPEndPoint(System.Net.Dns.Resolve(host).AddressList[0],port);
				s.Connect(ipdest);
				s.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.NoDelay,1);

				m_pSocket = new BufferedSocket(s);

				string reply = m_pSocket.ReadLine();
				reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

				if(!reply.ToUpper().StartsWith("OK")){
					m_pSocket.Close();
					m_pSocket = null;
					throw new Exception("Server returned:" + reply);
				}

				m_Connected = true;
			}
		}

		#endregion

		#region method Disconnect

		/// <summary>
		/// Disconnects from IMAP server.
		/// </summary>
		public void Disconnect()
		{
			if(m_pSocket != null && m_pSocket.Connected){
				// Send QUIT
				m_pSocket.SendLine("a1 LOGOUT");

			//	m_pSocket.Close();
				m_pSocket = null;
			}

			m_Connected     = false;
			m_Authenticated = false;
		}

		#endregion

		#region method Authenticate

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

			m_pSocket.SendLine("a1 LOGIN \"" + userName +  "\" \"" + password + "\"");

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(reply.ToUpper().StartsWith("OK")){
				m_Authenticated = true;
			}
			else{
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion


		#region method CreateFolder

		/// <summary>
		/// Creates specified folder.
		/// </summary>
		/// <param name="folderName">Folder name. Eg. test, Inbox/SomeSubFolder. NOTE: use GetFolderSeparator() to get right folder separator.</param>
		public void CreateFolder(string folderName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("a1 CREATE \"" + EncodeUtf7(folderName) + "\"");

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region method DeleteFolder

		/// <summary>
		/// Deletes specified folder.
		/// </summary>
		/// <param name="folderName">Folder name.</param>
		public void DeleteFolder(string folderName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("a1 DELETE \"" + EncodeUtf7(folderName) + "\"");

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region method RenameFolder

		/// <summary>
		/// Renames specified folder.
		/// </summary>
		/// <param name="sourceFolderName">Source folder name.</param>
		/// <param name="destinationFolderName">Destination folder name.</param>
		public void RenameFolder(string sourceFolderName,string destinationFolderName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("a1 RENAME \"" + EncodeUtf7(sourceFolderName) + "\" \"" + EncodeUtf7(destinationFolderName) + "\"");

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region method GetFolders

		/// <summary>
		///  Gets all available folders.
		/// </summary>
		/// <returns></returns>
		public string[] GetFolders()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			ArrayList list = new ArrayList();

			m_pSocket.SendLine("a1 LIST \"\" \"*\"");

			// Must get lines with * and cmdTag + OK or cmdTag BAD/NO
			string reply = m_pSocket.ReadLine();			
			if(reply.StartsWith("*")){
				// Read multiline response
				while(reply.StartsWith("*")){
					// don't show not selectable folders
					if(reply.ToLower().IndexOf("\\noselect") == -1){
						reply = reply.Substring(reply.IndexOf(")") + 1).Trim(); // Remove * LIST(..)
						reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Folder separator

						// Folder name between ""
						if(reply.IndexOf("\"") > -1){
							list.Add(DecodeUtf7(reply.Substring(reply.IndexOf("\"") + 1,reply.Length - reply.IndexOf("\"") - 2)));
						}
						else{
							list.Add(DecodeUtf7(reply.Trim()));
						}
					}

					reply = m_pSocket.ReadLine();
				}
			}
			
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}

			string[] retVal = new string[list.Count];
			list.CopyTo(retVal);

            return retVal;
		}

		#endregion

		#region method GetSubscribedFolders

		/// <summary>
		/// Gets all subscribed folders.
		/// </summary>
		/// <returns></returns>
		public string[] GetSubscribedFolders()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			ArrayList list = new ArrayList();

			m_pSocket.SendLine("a1 LSUB \"\" \"*\"");

			// Must get lines with * and cmdTag + OK or cmdTag BAD/NO
			string reply = m_pSocket.ReadLine();			
			if(reply.StartsWith("*")){
				// Read multiline response
				while(reply.StartsWith("*")){
					//
					string folder = reply.Substring(reply.LastIndexOf(" ")).Trim().Replace("\"","");
					list.Add(DecodeUtf7(folder));

					reply = m_pSocket.ReadLine();
				}
			}
			
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}

			string[] retVal = new string[list.Count];
			list.CopyTo(retVal);

            return retVal;
		}

		#endregion

		#region method SubscribeFolder

		/// <summary>
		/// Subscribes specified folder.
		/// </summary>
		/// <param name="folderName">Folder name.</param>
		public void SubscribeFolder(string folderName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("a1 SUBSCRIBE \"" + EncodeUtf7(folderName) + "\"");

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region method UnSubscribeFolder

		/// <summary>
		/// UnSubscribes specified folder.
		/// </summary>
		/// <param name="folderName">Folder name,</param>
		public void UnSubscribeFolder(string folderName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("a1 UNSUBSCRIBE \"" + EncodeUtf7(folderName) + "\"");

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region method SelectFolder

		/// <summary>
		/// Selects specified folder.
		/// </summary>
		/// <param name="folderName">Folder name.</param>
		public void SelectFolder(string folderName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("a1 SELECT \"" + EncodeUtf7(folderName) + "\"");

			// Must get lines with * and cmdTag + OK or cmdTag BAD/NO
			string reply = m_pSocket.ReadLine();		
			if(reply.StartsWith("*")){
				// Read multiline response
				while(reply.StartsWith("*")){
					// Get rid of *
					reply = reply.Substring(1).Trim();

					if(reply.ToUpper().IndexOf("EXISTS") > -1 && reply.ToUpper().IndexOf("FLAGS") == -1){
						m_MsgCount = Convert.ToInt32(reply.Substring(0,reply.IndexOf(" ")).Trim());
					}
					else if(reply.ToUpper().IndexOf("RECENT") > -1 && reply.ToUpper().IndexOf("FLAGS") == -1){
						m_NewMsgCount = Convert.ToInt32(reply.Substring(0,reply.IndexOf(" ")).Trim());
					}
					
					reply = m_pSocket.ReadLine();
				}
			}
			
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}

			m_SelectedFolder = folderName;
		}

		#endregion


		/// <summary>
		/// TODO:
		/// </summary>
		/// <param name="folderName"></param>
		public void GetFolderACL(string folderName)
		{
			throw new Exception("TODO:");
		}


		#region method SetFolderACL

		/// <summary>
		/// Sets specified user ACL permissions for specified folder.
		/// </summary>
		/// <param name="folderName">Folder name which ACL to set.</param>
		/// <param name="userName">User name who's ACL to set.</param>
		/// <param name="acl">ACL permissions to set.</param>
		public void SetFolderACL(string folderName,string userName,IMAP_ACL_Flags acl)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("a1 SETACL \"" + EncodeUtf7(folderName) + "\" \"" + userName + "\" " + IMAP_Utils.ACL_to_String(acl));

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region method DeleteFolderACL

		/// <summary>
		/// Deletes specifieed user access to specified folder.
		/// </summary>
		/// <param name="folderName">Folder which ACL to remove.</param>
		/// <param name="userName">User name who's ACL to remove.</param>
		public void DeleteFolderACL(string folderName,string userName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("a1 DELETEACL \"" + EncodeUtf7(folderName) + "\" \"" + userName + "\"");

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region method GetFolderMyrights

		/// <summary>
		/// Gets myrights to specified folder.
		/// </summary>
		/// <param name="folderName"></param>
		/// <returns></returns>
		public IMAP_ACL_Flags GetFolderMyrights(string folderName)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			IMAP_ACL_Flags aclFlags = IMAP_ACL_Flags.None;

			m_pSocket.SendLine("a1 MYRIGHTS \"" + EncodeUtf7(folderName) + "\"");

			// Must get lines with * and cmdTag + OK or cmdTag BAD/NO
			string reply = m_pSocket.ReadLine();		
			if(reply.StartsWith("*")){
				// Read multiline response
				while(reply.StartsWith("*")){
					// Get rid of *
					reply = reply.Substring(1).Trim();

					if(reply.ToUpper().IndexOf("MYRIGHTS") > -1){
						aclFlags = IMAP_Utils.ACL_From_String(reply.Substring(0,reply.IndexOf(" ")).Trim());
					}
					
					reply = m_pSocket.ReadLine();
				}
			}
			
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}

			return aclFlags;
		}

		#endregion

		
		#region method CopyMessages

		/// <summary>
		/// Makes copy of messages to specified folder.
		/// </summary>
		/// <param name="startMsgNo">Start message number.</param>
		/// <param name="endMsgNo">End message number. -1 = last.</param>
		/// <param name="destFolder">Folder where to cpoy messages.</param>
		/// <param name="uidCopy">Specifies if startMsgNo and endMsgNo is message UIDs.</param>
		public void CopyMessages(int startMsgNo,int endMsgNo,string destFolder,bool uidCopy)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}
			if(m_SelectedFolder.Length == 0){
				throw new Exception("You must select folder first !");
			}
			
			string endMsg = endMsgNo.ToString();
			if(endMsgNo < 1){
				endMsg = "*";
			}
			string uidC = "";
			if(uidCopy){
				uidC = "UID ";
			}

			m_pSocket.SendLine("a1 " + uidC + "COPY " + startMsgNo + ":" + endMsg  + " \"" + EncodeUtf7(destFolder) + "\"");

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region method MoveMessages

		/// <summary>
		/// Moves messages to specified folder.
		/// </summary>
		/// <param name="startMsgNo">Start message number.</param>
		/// <param name="endMsgNo">End message number. -1 = last.</param>
		/// <param name="destFolder">Folder where to cpoy messages.</param>
		/// <param name="uidMove">Specifies if startMsgNo and endMsgNo is message UIDs.</param>
		public void MoveMessages(int startMsgNo,int endMsgNo,string destFolder,bool uidMove)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}
			if(m_SelectedFolder.Length == 0){
				throw new Exception("You must select folder first !");
			}

			CopyMessages(startMsgNo,endMsgNo,destFolder,uidMove);
			DeleteMessages(startMsgNo,endMsgNo,uidMove);
		}

		#endregion

		#region method DeleteMessages

		/// <summary>
		/// Deletes specified messages.
		/// </summary>
		/// <param name="startMsgNo">Start message number.</param>
		/// <param name="endMsgNo">End message number. -1 = last.</param>
		/// <param name="uidDelete">Specifies if startMsgNo and endMsgNo is message UIDs.</param>
		public void DeleteMessages(int startMsgNo,int endMsgNo,bool uidDelete)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}
			if(m_SelectedFolder.Length == 0){
				throw new Exception("You must select folder first !");
			}

			string endMsg = endMsgNo.ToString();
			if(endMsgNo < 1){
				endMsg = "*";
			}
			string uidD = "";
			if(uidDelete){
				uidD = "UID ";
			}

			// 1) Set deleted flag
			// 2) Delete messages with EXPUNGE command

			m_pSocket.SendLine("a1 " + uidD + "STORE " + startMsgNo + ":" + endMsg  + " +FLAGS.SILENT (\\Deleted)");

			string reply = m_pSocket.ReadLine();
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}

			m_pSocket.SendLine("a1 EXPUNGE");

			reply = m_pSocket.ReadLine();

			// Read multiline response, just skip these lines
			while(reply.StartsWith("*")){
				reply = m_pSocket.ReadLine();
			}

			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion

		#region method StoreMessage

		/// <summary>
		/// Stores message to specified folder.
		/// </summary>
		/// <param name="folderName">Folder where to store message.</param>
		/// <param name="data">Message data which to store.</param>
		public void StoreMessage(string folderName,byte[] data)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			m_pSocket.SendLine("a1 APPEND \"" + EncodeUtf7(folderName) + "\" (\\Seen) {" + data.Length + "}");

			// ToDo: server may return status response there.

			// must get reply with starting +
			string reply = m_pSocket.ReadLine();
			if(reply.StartsWith("+")){
				// Send message
                m_pSocket.Send(data);

				// Why must send this ??? 
				m_pSocket.Send(new byte[]{(byte)'\r',(byte)'\n'});

				// Read store result
				reply = m_pSocket.ReadLine();
				reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove command tag

				if(!reply.ToUpper().StartsWith("OK")){
					throw new Exception("Server returned:" + reply);
				}
			}
			else{
				throw new Exception("Server returned:" + reply);
			}			
		}

		#endregion

		#region method FetchMessages

		/// <summary>
		/// Fetches messages headers or full messages data.
		/// </summary>
		/// <param name="startMsgNo">Start message number.</param>
		/// <param name="endMsgNo">End message number. -1 = last.</param>
		/// <param name="uidFetch">Specifies if startMsgNo and endMsgNo is message UIDs.</param>
		/// <param name="headersOnly">If true message headers are retrieved, otherwise full message retrieved.</param>
		/// <param name="setSeenFlag">If true message seen flag is setted.</param>
		/// <returns></returns>
		public IMAP_FetchItem[] FetchMessages(int startMsgNo,int endMsgNo,bool uidFetch,bool headersOnly,bool setSeenFlag)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}
			if(m_SelectedFolder.Length == 0){
				throw new Exception("You must select folder first !");
			}

			ArrayList fetchItems = new ArrayList();
			
			string endMsg = endMsgNo.ToString();
			if(endMsgNo < 1){
				endMsg = "*";
			}
			string headers = "";
			if(headersOnly){
				headers = "HEADER";
			}
			string uidF = "";
			if(uidFetch){
				uidF = "UID ";
			}
			string seenFl = "";
			if(!setSeenFlag){
				seenFl = ".PEEK";
			}

			m_pSocket.SendLine("a1 " + uidF + "FETCH " + startMsgNo + ":" + endMsg  + " (UID RFC822.SIZE FLAGS BODY" + seenFl + "[" + headers + "])");

			// Must get lines with * and cmdTag + OK or cmdTag BAD/NO
			string reply = m_pSocket.ReadLine();			
	//		if(reply.StartsWith("*")){
				// Read multiline response
				while(reply.StartsWith("*")){
					// Fetch may return status response there, skip them					
					if(IsStatusResponse(reply)){
						continue;
					}

					// Get rid of * 1 FETCH  and parse params. Reply:* 1 FETCH (UID 12 BODY[] ...)
					reply = reply.Substring(reply.IndexOf("FETCH (") + 7);

					int    uid        = 0;
					int    size       = 0;
					byte[] data       = null;
					bool   isNewMsg   = true;
					bool   isAnswered = false;

					// Loop fetch result fields
				//	for(int i=0;i<4;i++){
					while(reply.Length > 0){
						// UID field
						if(reply.ToUpper().StartsWith("UID")){
							reply = reply.Substring(3).Trim(); // Remove UID word from reply
							if(reply.IndexOf(" ") > -1){
								uid   = Convert.ToInt32(reply.Substring(0,reply.IndexOf(" ")));
								reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove UID value from reply
							}
							else{ // Last param, no ending ' '
								uid   = Convert.ToInt32(reply.Substring(0));
								reply = "";
							}
						}
						// RFC822.SIZE field
						else if(reply.ToUpper().StartsWith("RFC822.SIZE")){
							reply = reply.Substring(11).Trim(); // Remove RFC822.SIZE word from reply
							if(reply.IndexOf(" ") > -1){
								size  = Convert.ToInt32(reply.Substring(0,reply.IndexOf(" ")));
								reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove RFC822.SIZE value from reply
							}
							else{
								// Last param, no ending ' '
								size  = Convert.ToInt32(reply.Substring(0));
								reply = "";
							}
						}
						// BODY.PEEK field
						else if(reply.ToUpper().StartsWith("BODY")){
							// Get data. Find {dataLen}
							int dataLen = Convert.ToInt32(reply.Substring(reply.IndexOf("{") + 1,reply.IndexOf("}") - reply.IndexOf("{") - 1));

							MemoryStream storeStrm = new MemoryStream(dataLen);
							ReadReplyCode code = m_pSocket.ReadData(storeStrm,dataLen,true);
							if(code != ReadReplyCode.Ok){
								throw new Exception("Read data:" + code.ToString());
							}

							data = storeStrm.ToArray();

							// Read last fetch line, can be ')' or some params')'.
							reply = m_pSocket.ReadLine().Trim();
							if(!reply.EndsWith(")")){
								throw new Exception("UnExpected fetch end ! value:" + reply);
							}
							else{
								reply = reply.Substring(0,reply.Length - 1).Trim(); // Remove ')' from reply
							}
						}
						// FLAGS field
						else if(reply.ToUpper().StartsWith("FLAGS")){
							if(reply.ToUpper().IndexOf("\\SEEN") > -1){
								isNewMsg = false;
							}
							if(reply.ToUpper().IndexOf("\\ANSWERED") > -1){
								isAnswered = true;
							}

							reply = reply.Substring(reply.IndexOf(")") + 1).Trim(); // Remove FLAGS value from reply
						}
						else{
							throw new Exception("Not supported fetch reply");
						}
					}

					fetchItems.Add(new IMAP_FetchItem(uid,size,data,headersOnly,isNewMsg,isAnswered));

					reply = m_pSocket.ReadLine();
				}
	//		}
			
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				if(!reply.ToUpper().StartsWith("NO")){
					throw new Exception("Server returned:" + reply);
				}
			}

			IMAP_FetchItem[] retVal = new IMAP_FetchItem[fetchItems.Count];
			fetchItems.CopyTo(retVal);

			return retVal;
		}

		#endregion

		#region method StoreMessageFlags

		/// <summary>
		/// Stores message folgs to sepcified messages range.
		/// </summary>
		/// <param name="startMsgNo">Start message number.</param>
		/// <param name="endMsgNo">End message number.</param>
		/// <param name="uidStore">Sepcifies if message numbers are message UID numbers.</param>
		/// <param name="msgFlags">Message flags to store.</param>
		public void StoreMessageFlags(int startMsgNo,int endMsgNo,bool uidStore,IMAP_MessageFlags msgFlags)
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}
			if(m_SelectedFolder.Length == 0){
				throw new Exception("You must select folder first !");
			}

			if(uidStore){
				m_pSocket.SendLine("a1 UID STORE " + startMsgNo + ":" + endMsgNo + " FLAGS (" + IMAP_Utils.MessageFlagsToString(msgFlags) + ")");
			}
			else{
				m_pSocket.SendLine("a1 STORE " + startMsgNo + ":" + endMsgNo + " FLAGS (" + IMAP_Utils.MessageFlagsToString(msgFlags) + ")");
			}

			// Must get lines with * and cmdTag + OK or cmdTag BAD/NO
			string reply = m_pSocket.ReadLine();		
			if(reply.StartsWith("*")){
				// Read multiline response
				while(reply.StartsWith("*")){
					// Get rid of *
					reply = reply.Substring(1).Trim();
										
					reply = m_pSocket.ReadLine();
				}
			}
			
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				throw new Exception("Server returned:" + reply);
			}
		}

		#endregion


		#region method GetMessagesTotalSize

		/// <summary>
		/// Gets messages total size in selected folder.
		/// </summary>
		/// <returns></returns>
		public int GetMessagesTotalSize()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}
			if(m_SelectedFolder.Length == 0){
				throw new Exception("You must select folder first !");
			}

			int totalSize = 0;
			
			m_pSocket.SendLine("a1 FETCH 1:* (RFC822.SIZE)");

			// Must get lines with * and cmdTag + OK or cmdTag BAD/NO
			string reply = m_pSocket.ReadLine();			
			if(reply.StartsWith("*")){
				// Read multiline response
				while(reply.StartsWith("*")){
					// Get rid of * 1 FETCH  and parse params. Reply:* 1 FETCH (UID 12 BODY[] ...)
					reply = reply.Substring(reply.IndexOf("FETCH (") + 7);
					
					// RFC822.SIZE field
					if(reply.ToUpper().StartsWith("RFC822.SIZE")){
						reply = reply.Substring(11).Trim(); // Remove RFC822.SIZE word from reply
						
						totalSize += Convert.ToInt32(reply.Substring(0,reply.Length - 1).Trim()); // Remove ending ')'						
					}					

					reply = m_pSocket.ReadLine();
				}
			}
			
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				if(!reply.ToUpper().StartsWith("NO")){
					throw new Exception("Server returned:" + reply);
				}
			}

			return totalSize;
		}

		#endregion

		#region method GetUnseenMessagesCount

		/// <summary>
		/// Gets unseen messages count in selected folder.
		/// </summary>
		/// <returns></returns>
		public int GetUnseenMessagesCount()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}
			if(m_SelectedFolder.Length == 0){
				throw new Exception("You must select folder first !");
			}

			int count = 0;
			
			m_pSocket.SendLine("a1 FETCH 1:* (FLAGS)");

			// Must get lines with * and cmdTag + OK or cmdTag BAD/NO
			string reply = m_pSocket.ReadLine();			
			if(reply.StartsWith("*")){
				// Read multiline response
				while(reply.StartsWith("*")){
					// Get rid of * 1 FETCH  and parse params. Reply:* 1 FETCH (UID 12 BODY[] ...)
					reply = reply.Substring(reply.IndexOf("FETCH (") + 7);
					
					if(reply.ToUpper().IndexOf("\\SEEN") == -1){
						count++;
					}

					reply = m_pSocket.ReadLine();
				}
			}
			
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				if(!reply.ToUpper().StartsWith("NO")){
					throw new Exception("Server returned:" + reply);
				}
			}

			return count;
		}

		#endregion

		#region method GetFolderSeparator

		/// <summary>
		/// Gets IMAP server folder separator char.
		/// </summary>
		/// <returns></returns>
		public string GetFolderSeparator()
		{
			if(!m_Connected){
				throw new Exception("You must connect first !");
			}
			if(!m_Authenticated){
				throw new Exception("You must authenticate first !");
			}

			string folderSeparator = "";

			m_pSocket.SendLine("a1 LIST \"\" \"\"");

			// Must get lines with * and cmdTag + OK or cmdTag BAD/NO
			string reply = m_pSocket.ReadLine();			
			if(reply.StartsWith("*")){
				// Read multiline response
				while(reply.StartsWith("*")){
					reply = reply.Substring(reply.IndexOf(")") + 1).Trim(); // Remove * LIST(..)

					// get folder separator
					folderSeparator = reply.Substring(0,reply.IndexOf(" ")).Trim();

					reply = m_pSocket.ReadLine();
				}
			}
			
			reply = reply.Substring(reply.IndexOf(" ")).Trim(); // Remove Cmd tag

			if(!reply.ToUpper().StartsWith("OK")){
				if(!reply.ToUpper().StartsWith("NO")){
					throw new Exception("Server returned:" + reply);
				}
			}

			reply = reply.Substring(reply.IndexOf(")") + 1).Trim(); // Remove * LIST(..)


			return folderSeparator.Replace("\"","");
		}

		#endregion


		#region method DecodeUtf7

		private string DecodeUtf7(string str)
		{
			str = str.Replace("&","+");
			return System.Text.Encoding.UTF7.GetString(System.Text.Encoding.ASCII.GetBytes(str));
		}

		#endregion

		#region method EncodeUtf7

		private string EncodeUtf7(string str)
		{
			return System.Text.Encoding.Default.GetString(System.Text.Encoding.UTF7.GetBytes(str)).Replace("+","&");
		}

		#endregion

		#region method IsStatusResponse

		private bool IsStatusResponse(string line)
		{
			// * 1 EXISTS
			// * 1 RECENT
			if(line.ToLower().IndexOf("exists") > -1){
				return true;
			}
			if(line.ToLower().IndexOf("recent") > -1 && line.ToLower().IndexOf("flags") == -1){
				return true;
			}

			return false;
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets selected folder.
		/// </summary>
		public string SelectedFolder
		{
			get{ return m_SelectedFolder; }
		}

		/// <summary>
		/// Gets numbers of recent(not accessed messages) in selected folder.
		/// </summary>
		public int RecentMessagesCount
		{
			get{ return m_NewMsgCount; }
		}

		/// <summary>
		/// Gets numbers of messages in selected folder.
		/// </summary>
		public int MessagesCount
		{
			get{ return m_MsgCount; }
		}

		#endregion

	}
}
