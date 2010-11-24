using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Collections;
using System.Text;
using LumiSoft.Net;
using LumiSoft.Net.IMAP;
using LumiSoft.Net.Mime;
using LumiSoft.Net.AUTH;

namespace LumiSoft.Net.IMAP.Server
{	
	/// <summary>
	/// IMAP session.
	/// </summary>
	public class IMAP_Session : ISocketServerSession
	{
		private BufferedSocket m_pSocket           = null;    // Referance to client Socket.
		private IMAP_Server    m_pServer           = null;    // Referance to SMTP server.
		private string         m_SessionID         = "";      // Holds session ID.
		private string         m_UserName          = "";      // Holds loggedIn UserName.
		private string         m_SelectedMailbox   = "";
		private IMAP_Messages  m_Messages          = null;
		private bool           m_Authenticated     = false;   // Holds authentication flag.
		private int            m_BadCmdCount       = 0;       // Holds number of bad commands.
		private DateTime       m_SessionStartTime;
		private DateTime       m_LastDataTime;
		private object         m_Tag               = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="clientSocket">Referance to socket.</param>
		/// <param name="server">Referance to IMAP server.</param>
		/// <param name="logWriter">Log writer.</param>
		internal IMAP_Session(Socket clientSocket,IMAP_Server server,SocketLogger logWriter)
		{
			m_pSocket = new BufferedSocket(clientSocket);
			m_pServer = server;

			m_SessionID        = Guid.NewGuid().ToString();			
			m_SessionStartTime = DateTime.Now;
			m_LastDataTime     = DateTime.Now;

			if(m_pServer.LogCommands){
				m_pSocket.Logger = logWriter;
				m_pSocket.Logger.SessionID = m_SessionID;
			}			

			m_pSocket.SetSocketOption(SocketOptionLevel.Socket,SocketOptionName.NoDelay,1);
			m_pSocket.Activity += new EventHandler(OnSocketActivity);

			// Start session proccessing
			StartSession();
		}

		#region method StartSession

		/// <summary>
		/// Starts session.
		/// </summary>
		private void StartSession()
		{
			// Add session to session list
			m_pServer.AddSession(this);
	
			try{
				// Check if ip is allowed to connect this computer
				if(m_pServer.OnValidate_IpAddress(this.LocalEndPoint,this.RemoteEndPoint)){
					// Notify that server is ready
					m_pSocket.SendLine("* OK " + m_pServer.HostName + " IMAP Server ready");

					BeginRecieveCmd();
				}
				else{
					EndSession();
				}
			}
			catch(Exception x){
				OnError(x);
			}
		}

		#endregion

		#region method EndSession

		/// <summary>
		/// Ends session, closes socket.
		/// </summary>
		private void EndSession()
		{			
			try{
				// Write logs to log file, if needed
				if(m_pServer.LogCommands){
					m_pSocket.Logger.Flush();
				}

				if(m_pSocket != null){
					m_pSocket.Shutdown(SocketShutdown.Both);
					m_pSocket.Close();
					m_pSocket = null;
				}
			}
			catch{ // We don't need to check errors here, because they only may be Socket closing errors.
			}
			finally{
				m_pServer.RemoveSession(this);
			}
		}

		#endregion


		#region method OnSessionTimeout

		/// <summary>
		/// Is called by server when session has timed out.
		/// </summary>
		public void OnSessionTimeout()
		{
			try{
				m_pSocket.SendLine("* BAD Session timeout, closing transmission channel");
			}
			catch{
			}

			EndSession();
		}

		#endregion

		#region method OnSocketActivity

		/// <summary>
		/// Is called if there was some activity on socket, some data sended or received.
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>		
		private void OnSocketActivity(object sender,EventArgs e)
		{
			m_LastDataTime = DateTime.Now;
		}

		#endregion

		#region method OnError

		/// <summary>
		/// Is called when error occures.
		/// </summary>
		/// <param name="x"></param>
		private void OnError(Exception x)
		{
			try{
				if(x is SocketException){
					SocketException xs = (SocketException)x;

					// Client disconnected without shutting down
					if(xs.ErrorCode == 10054 || xs.ErrorCode == 10053){
						if(m_pServer.LogCommands){
						//	m_pLogWriter.AddEntry("Client aborted/disconnected",this.SessionID,this.RemoteEndPoint.Address.ToString(),"C");
							m_pSocket.Logger.AddTextEntry("Client aborted/disconnected");
						}

						EndSession();

						// Exception handled, return
						return;
					}
				}

				m_pServer.OnSysError("",x);
			}
			catch(Exception ex){
				m_pServer.OnSysError("",ex);
			}
		}

		#endregion


		#region method BeginRecieveCmd
		
		/// <summary>
		/// Starts recieveing command.
		/// </summary>
		private void BeginRecieveCmd()
		{
			MemoryStream strm = new MemoryStream();
			m_pSocket.BeginReadLine(strm,1024,strm,new SocketCallBack(this.EndRecieveCmd));
		}

		#endregion

		#region method EndRecieveCmd

		/// <summary>
		/// Is called if command is recieved.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void EndRecieveCmd(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			try{
				switch(result)
				{
					case SocketCallBackResult.Ok:
						MemoryStream strm = (MemoryStream)tag;

						string cmdLine = System.Text.Encoding.Default.GetString(strm.ToArray());

					//	if(m_pServer.LogCommands){
					//		m_pLogWriter.AddEntry(cmdLine + "<CRLF>",this.SessionID,this.RemoteEndPoint.Address.ToString(),"C");
					//	}

						// Exceute command
						if(SwitchCommand(cmdLine)){
							// Session end, close session
							EndSession();
						}
						break;

					case SocketCallBackResult.LengthExceeded:
						m_pSocket.SendLine("* BAD Line too long.");

						BeginRecieveCmd();
						break;

					case SocketCallBackResult.SocketClosed:
						EndSession();
						break;

					case SocketCallBackResult.Exception:
						OnError(exception);
						break;
				}
			}
			catch(Exception x){
				 OnError(x);
			}
		}

		#endregion

		
		#region function SwitchCommand

		/// <summary>
		/// Executes IMAP command.
		/// </summary>
		/// <param name="IMAP_commandTxt">Original command text.</param>
		/// <returns>Returns true if must end session(command loop).</returns>
		private bool SwitchCommand(string IMAP_commandTxt)
		{
			// Parse commandTag + comand + args
			// eg. C:a100 SELECT INBOX<CRLF>
			// a100   - commandTag
			// SELECT - command
			// INBOX  - arg

			//---- Parse command --------------------------------------------------//
			string[] cmdParts = IMAP_commandTxt.TrimStart().Split(new char[]{' '});
			// For bad command, just return empty cmdTag and command name
			if(cmdParts.Length < 2){
				cmdParts = new string[]{"",""};
			}
			string commandTag = cmdParts[0].Trim().Trim();
			string command    = cmdParts[1].ToUpper().Trim();
			string argsText   = Core.GetArgsText(IMAP_commandTxt,cmdParts[0] + " " + cmdParts[1]);
			//---------------------------------------------------------------------//

			bool getNextCmd = true;

			switch(command){
				//--- Non-Authenticated State
				case "AUTHENTICATE":
					Authenticate(commandTag,argsText);
					break;

				case "LOGIN":
					LogIn(commandTag,argsText);
					break;
				//--- End of non-Authenticated

				//--- Authenticated State
				case "SELECT":
					Select(commandTag,argsText);
					break;

				case "EXAMINE":
					Examine(commandTag,argsText);
					break;

				case "CREATE":
					Create(commandTag,argsText);
					break;

				case "DELETE":
					Delete(commandTag,argsText);
					break;

				case "RENAME":
					Rename(commandTag,argsText);
					break;

				case "SUBSCRIBE":
					Suscribe(commandTag,argsText);
					break;

				case "UNSUBSCRIBE":
					UnSuscribe(commandTag,argsText);
					break;

				case "LIST":
					List(commandTag,argsText);
					break;

				case "LSUB":
					LSub(commandTag,argsText);
					break;

				case "STATUS":
					Status(commandTag,argsText);
					break;

				case "APPEND":
					getNextCmd = BeginAppendCmd(commandTag,argsText);
					break;

				case "NAMESPACE":
					Namespace(commandTag,argsText);
					break;

				case "GETACL":
					GETACL(commandTag,argsText);
					break;

				case "SETACL":
					SETACL(commandTag,argsText);
					break;

				case "DELETEACL":
					DELETEACL(commandTag,argsText);
					break;

				case "LISTRIGHTS":
					LISTRIGHTS(commandTag,argsText);
					break;

				case "MYRIGHTS":
					MYRIGHTS(commandTag,argsText);
					break;
				//--- End of Authenticated

				//--- Selected State
				case "CHECK":
					Check(commandTag);
					break;

				case "CLOSE":
					Close(commandTag);
					break;

				case "EXPUNGE":	
					Expunge(commandTag);
					break;

				case "SEARCH":
					Search(commandTag,argsText,false);
					break;

				case "FETCH":
					Fetch(commandTag,argsText,false);
					break;

				case "STORE":
					Store(commandTag,argsText,false);
					break;

				case "COPY":
					Copy(commandTag,argsText,false);
					break;

				case "UID":	
					Uid(commandTag,argsText);
					break;
				//--- End of Selected 

				//--- Any State
				case "CAPABILITY":
					Capability(commandTag);
					break;

				case "NOOP":
					Noop(commandTag);
					break;

				case "LOGOUT":
					LogOut(commandTag);
					return true;
				//--- End of Any
										
				default:					
					m_pSocket.SendLine(commandTag + " BAD command unrecognized");

					//---- Check that maximum bad commands count isn't exceeded ---------------//
					if(m_BadCmdCount > m_pServer.MaxBadCommands-1){
						m_pSocket.SendLine("* BAD Too many bad commands, closing transmission channel");
						return true;
					}
					m_BadCmdCount++;
					//-------------------------------------------------------------------------//
					break;				
			}

			if(getNextCmd){
				BeginRecieveCmd();
			}
			
			return false;
		}

		#endregion


		//--- Non-Authenticated State ------

		#region function Authenticate

		private void Authenticate(string cmdTag,string argsText)
		{
			/* Rfc 3501 6.2.2.  AUTHENTICATE Command

				Arguments:  authentication mechanism name

				Responses:  continuation data can be requested

				Result:     OK  - authenticate completed, now in authenticated state
							NO  - authenticate failure: unsupported authentication
								  mechanism, credentials rejected
							BAD - command unknown or arguments invalid,
								  authentication exchange cancelled
			*/
			if(m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO AUTH you are already logged in");
                return;
			}

			string userName = "";
		//	string password = "";
			AuthUser_EventArgs aArgs = null;

			switch(argsText.ToUpper())
			{
				case "CRAM-MD5":

					#region CRAM-MDD5 authentication

					/* Cram-M5
					C: A0001 AUTH CRAM-MD5
					S: + <md5_calculation_hash_in_base64>
					C: base64(decoded:username password_hash)
					S: A0001 OK CRAM authentication successful
					*/
					
					string md5Hash = "<" + Guid.NewGuid().ToString().ToLower() + ">";
					m_pSocket.SendLine("+ " + Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(md5Hash)));

					string reply = m_pSocket.ReadLine();								
					reply = System.Text.Encoding.Default.GetString(Convert.FromBase64String(reply));
					string[] replyArgs = reply.Split(' ');
					userName = replyArgs[0];
					
					aArgs = m_pServer.OnAuthUser(this,userName,replyArgs[1],md5Hash,AuthType.CRAM_MD5);
					if(aArgs.Validated){
						m_pSocket.SendLine(cmdTag + " OK Authentication successful.");
						m_Authenticated = true;
						m_UserName = userName;
					}
					else{
						m_pSocket.SendLine(cmdTag + " NO Authentication failed");
					}

					#endregion

					break;

				case "DIGEST-MD5":
					
					#region DIGEST-MD5 authentication

					/* RFC 2831 AUTH DIGEST-MD5
					 * 
					 * Example:
					 * 
					 * C: AUTH DIGEST-MD5
					 * S: + base64(realm="elwood.innosoft.com",nonce="OA6MG9tEQGm2hh",qop="auth",algorithm=md5-sess)
					 * C: base64(username="chris",realm="elwood.innosoft.com",nonce="OA6MG9tEQGm2hh",
					 *    nc=00000001,cnonce="OA6MHXh6VqTrRk",digest-uri="imap/elwood.innosoft.com",
                     *    response=d388dad90d4bbd760a152321f2143af7,qop=auth)
					 * S: + base64(rspauth=ea40f60335c427b5527b84dbabcdfffd)
					 * C:
					 * S: A0001 OK Authentication successful.
					*/

					string realm = m_pServer.HostName;
					string nonce = AuthHelper.GenerateNonce();

					m_pSocket.SendLine("+ " + AuthHelper.Base64en(AuthHelper.Create_Digest_Md5_ServerResponse(realm,nonce)));

					string clientResponse = AuthHelper.Base64de(m_pSocket.ReadLine());					
					// Check that realm and nonce in client response are same as we specified
					if(clientResponse.IndexOf("realm=\"" + realm + "\"") > - 1 && clientResponse.IndexOf("nonce=\"" + nonce + "\"") > - 1){
						// Parse user name and password compare value
				//		string userName  = "";
						string passwData = "";
						string cnonce = ""; 
						foreach(string clntRespParam in clientResponse.Split(',')){
							if(clntRespParam.StartsWith("username=")){
								userName = clntRespParam.Split(new char[]{'='},2)[1].Replace("\"","");
							}
							else if(clntRespParam.StartsWith("response=")){
								passwData = clntRespParam.Split(new char[]{'='},2)[1];
							}							
							else if(clntRespParam.StartsWith("cnonce=")){
								cnonce = clntRespParam.Split(new char[]{'='},2)[1].Replace("\"","");
							}
						}

						aArgs = m_pServer.OnAuthUser(this,userName,passwData,clientResponse,AuthType.DIGEST_MD5);
						if(aArgs.Validated){
							// Send server computed password hash
							m_pSocket.SendLine("+ " + AuthHelper.Base64en("rspauth=" + aArgs.ReturnData));
					
							// We must got empty line here
							clientResponse = m_pSocket.ReadLine();
							if(clientResponse == ""){
								m_pSocket.SendLine(cmdTag + " OK Authentication successful.");
							}
							else{
								m_pSocket.SendLine(cmdTag + " NO Authentication failed");
							}
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO Authentication failed");
						}
					}
					else{
						m_pSocket.SendLine(cmdTag + " NO Authentication failed");
					}
				
					#endregion

					break;

				default:
					m_pSocket.SendLine(cmdTag + " NO unsupported authentication mechanism");
					break;
			}
			
		}

		#endregion

		#region function LogIn

		private void LogIn(string cmdTag,string argsText)
		{
			/* RFC 3501 6.2.3 LOGIN Command
			  
				Arguments:  user name
							password

				Responses:  no specific responses for this command

				Result:     OK  - login completed, now in authenticated state
							NO  - login failure: user name or password rejected
							BAD - command unknown or arguments invalid
			   
				The LOGIN command identifies the client to the server and carries
				the plaintext password authenticating this user.
			
				Example: C: a001 LOGIN SMITH SESAME
					     S: a001 OK LOGIN completed
						 
						 //----
						 C: a001 LOGIN "SMITH" "SESAME"
						 S: a001 OK LOGIN completed
			   
			*/
			if(m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO LOGIN you are already logged in");
                return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 2){
				m_pSocket.SendLine(cmdTag + " BAD Invalid arguments");
				return;
			}

			string userName = args[0];
			string password = args[1];

			AuthUser_EventArgs aArgs = m_pServer.OnAuthUser(this,userName,password,"",AuthType.Plain);
			if(aArgs.Validated){
				m_pSocket.SendLine(cmdTag + " OK LOGIN completed");
				m_UserName      = userName;
				m_Authenticated = true;
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO LOGIN failed");
			}
		}

		#endregion

		//--- End of non-Authenticated State 


		//--- Authenticated State ------

		#region function Select

		private void Select(string cmdTag,string argsText)
		{
			/* Rfc 3501 6.3.1 SELECT Command
			 
				Arguments:  mailbox name

				Responses:  REQUIRED untagged responses: FLAGS, EXISTS, RECENT
							REQUIRED OK untagged responses:  UNSEEN,  PERMANENTFLAGS,
							UIDNEXT, UIDVALIDITY

				Result:     OK - select completed, now in selected state
							NO - select failure, now in authenticated state: no
									such mailbox, can't access mailbox
							BAD - command unknown or arguments invalid
							
				The SELECT command selects a mailbox so that messages in the
				mailbox can be accessed.  Before returning an OK to the client,
				the server MUST send the following untagged data to the client.
				Note that earlier versions of this protocol only required the
				FLAGS, EXISTS, and RECENT untagged data; consequently, client
				implementations SHOULD implement default behavior for missing data
				as discussed with the individual item.

					FLAGS       Defined flags in the mailbox.  See the description
								of the FLAGS response for more detail.

					<n> EXISTS  The number of messages in the mailbox.  See the
								description of the EXISTS response for more detail.

					<n> RECENT  The number of messages with the \Recent flag set.
								See the description of the RECENT response for more
								detail.

					OK [UNSEEN <n>]
								The message sequence number of the first unseen
								message in the mailbox.  If this is missing, the
								client can not make any assumptions about the first
								unseen message in the mailbox, and needs to issue a
								SEARCH command if it wants to find it.

					OK [PERMANENTFLAGS (<list of flags>)]
								A list of message flags that the client can change
								permanently.  If this is missing, the client should
								assume that all flags can be changed permanently.

					OK [UIDNEXT <n>]
								The next unique identifier value.  Refer to section
								2.3.1.1 for more information.  If this is missing,
								the client can not make any assumptions about the
								next unique identifier value.
								
					OK [UIDVALIDITY <n>]
                     The unique identifier validity value.  Refer to
                     section 2.3.1.1 for more information.  If this is
                     missing, the server does not support unique
                     identifiers.

				Only one mailbox can be selected at a time in a connection;
				simultaneous access to multiple mailboxes requires multiple
				connections.  The SELECT command automatically deselects any
				currently selected mailbox before attempting the new selection.
				Consequently, if a mailbox is selected and a SELECT command that
				fails is attempted, no mailbox is selected.

				If the client is permitted to modify the mailbox, the server
				SHOULD prefix the text of the tagged OK response with the
				"[READ-WRITE]" response code.

				If the client is not permitted to modify the mailbox but is
				permitted read access, the mailbox is selected as read-only, and
				the server MUST prefix the text of the tagged OK response to
				SELECT with the "[READ-ONLY]" response code.  Read-only access
				through SELECT differs from the EXAMINE command in that certain
				read-only mailboxes MAY permit the change of permanent state on a
				per-user (as opposed to global) basis.  Netnews messages marked in
				a server-based .newsrc file are an example of such per-user
				permanent state that can be modified with read-only mailboxes.

				Example:    C: A142 SELECT INBOX
							S: * 172 EXISTS
							S: * 1 RECENT
							S: * OK [UNSEEN 12] Message 12 is first unseen
							S: * OK [UIDVALIDITY 3857529045] UIDs valid
							S: * OK [UIDNEXT 4392] Predicted next UID
							S: * FLAGS (\Answered \Flagged \Deleted \Seen \Draft)
							S: * OK [PERMANENTFLAGS (\Deleted \Seen \*)] Limited
							S: A142 OK [READ-WRITE] SELECT completed
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 1){
				m_pSocket.SendLine(cmdTag + " BAD SELECT invalid arguments");
				return;
			}

			IMAP_Messages messages = m_pServer.OnGetMessagesInfo(this,args[0]);
			if(messages.ErrorText == null){
				m_Messages = messages;
				m_SelectedMailbox = args[0];

				string response = "";
				response += "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n";
				response += "* " + m_Messages.Count + " EXISTS\r\n";
				response += "* " + m_Messages.RecentCount + " RECENT\r\n";
				response += "* OK [UNSEEN " + m_Messages.FirstUnseen + "] Message " + m_Messages.FirstUnseen + " is first unseen\r\n";
				response += "* OK [UIDVALIDITY " + m_Messages.MailboxUID + "] UIDs valid\r\n";
				response += "* OK [UIDNEXT " + m_Messages.UID_Next + "] Predicted next UID\r\n";
				response += "* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)] Available permanent flags\r\n";
				response += cmdTag + " OK [" + (m_Messages.ReadOnly ? "READ-ONLY" : "READ-WRITE") + "] SELECT Completed\r\n";

				m_pSocket.SendData(response);
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO " + messages.ErrorText);
			}
		}

		#endregion

		#region function Examine

		private void Examine(string cmdTag,string argsText)
		{
			/* Rfc 3501 6.3.2 EXAMINE Command
			
				Arguments:  mailbox name

				Responses:  REQUIRED untagged responses: FLAGS, EXISTS, RECENT
							REQUIRED OK untagged responses:  UNSEEN,  PERMANENTFLAGS,
							UIDNEXT, UIDVALIDITY

				Result:     OK - examine completed, now in selected state
							NO - examine failure, now in authenticated state: no
									such mailbox, can't access mailbox
							BAD - command unknown or arguments invalid

				The EXAMINE command is identical to SELECT and returns the same
				output; however, the selected mailbox is identified as read-only.
				No changes to the permanent state of the mailbox, including
				per-user state, are permitted; in particular, EXAMINE MUST NOT
				cause messages to lose the \Recent flag.

				The text of the tagged OK response to the EXAMINE command MUST
				begin with the "[READ-ONLY]" response code.

				Example:    C: A932 EXAMINE blurdybloop
							S: * 17 EXISTS
							S: * 2 RECENT
							S: * OK [UNSEEN 8] Message 8 is first unseen
							S: * OK [UIDVALIDITY 3857529045] UIDs valid
							S: * OK [UIDNEXT 4392] Predicted next UID
							S: * FLAGS (\Answered \Flagged \Deleted \Seen \Draft)
							S: * OK [PERMANENTFLAGS ()] No permanent flags permitted
							S: A932 OK [READ-ONLY] EXAMINE completed
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 1){
				m_pSocket.SendLine(cmdTag + " BAD EXAMINE invalid arguments");
				return;
			}

			IMAP_Messages messages = m_pServer.OnGetMessagesInfo(this,args[0]);
			if(messages.ErrorText == null){
				messages.ReadOnly = true;
				m_Messages = messages;
				m_SelectedMailbox = argsText;

				string response = "";
				response += "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n";
				response += "* " + m_Messages.Count + " EXISTS\r\n";
				response += "* " + m_Messages.RecentCount + " RECENT\r\n";
				response += "* OK [UNSEEN " + m_Messages.FirstUnseen + "] Message " + m_Messages.FirstUnseen + " is first unseen\r\n";
				response += "* OK [UIDVALIDITY " + m_Messages.MailboxUID + "] UIDs valid\r\n";
				response += "* OK [UIDNEXT " + m_Messages.UID_Next + "] Predicted next UID\r\n";
				response += "* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)] Available permanent falgs\r\n";
				response += cmdTag + " OK [READ-ONLY] EXAMINE Completed\r\n";

				m_pSocket.SendData(response);
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO " + messages.ErrorText);
			}
		}

		#endregion

		#region function Create

		private void Create(string cmdTag,string argsText)
		{
			/* RFC 3501 6.3.3
				
				Arguments:  mailbox name

				Responses:  no specific responses for this command

				Result:     OK - create completed
							NO - create failure: can't create mailbox with that name
							BAD - command unknown or arguments invalid
			   
				The CREATE command creates a mailbox with the given name.  An OK
				response is returned only if a new mailbox with that name has been
				created.  It is an error to attempt to create INBOX or a mailbox
				with a name that refers to an extant mailbox.  Any error in
				creation will return a tagged NO response.

				If the mailbox name is suffixed with the server's hierarchy
				separator character (as returned from the server by a LIST
				command), this is a declaration that the client intends to create
				mailbox names under this name in the hierarchy.  Server
				implementations that do not require this declaration MUST ignore
				it.

				If the server's hierarchy separator character appears elsewhere in
				the name, the server SHOULD create any superior hierarchical names
				that are needed for the CREATE command to complete successfully.
				In other words, an attempt to create "foo/bar/zap" on a server in
				which "/" is the hierarchy separator character SHOULD create foo/
				and foo/bar/ if they do not already exist.

				If a new mailbox is created with the same name as a mailbox which
				was deleted, its unique identifiers MUST be greater than any
				unique identifiers used in the previous incarnation of the mailbox
				UNLESS the new incarnation has a different unique identifier
				validity value.  See the description of the UID command for more
				detail.
				
				Example:    C: A003 CREATE owatagusiam/
							S: A003 OK CREATE completed
							C: A004 CREATE owatagusiam/blurdybloop
							S: A004 OK CREATE completed
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 1){
				m_pSocket.SendLine(cmdTag + " BAD CREATE invalid arguments");
				return;
			}

			string errorText = m_pServer.OnCreateMailbox(this,args[0]);
			if(errorText == null){
				m_pSocket.SendLine(cmdTag + " OK CREATE completed");
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO " + errorText);
			}
		}

		#endregion

		#region function Delete

		private void Delete(string cmdTag,string argsText)
		{
			/* RFC 3501 6.3.4 DELETE Command
			
				Arguments:  mailbox name

				Responses:  no specific responses for this command

				Result:     OK - create completed
							NO - create failure: can't create mailbox with that name
							BAD - command unknown or arguments invalid
			   
				The DELETE command permanently removes the mailbox with the given
				name.  A tagged OK response is returned only if the mailbox has
				been deleted.  It is an error to attempt to delete INBOX or a
				mailbox name that does not exist.

				The DELETE command MUST NOT remove inferior hierarchical names.
				For example, if a mailbox "foo" has an inferior "foo.bar"
				(assuming "." is the hierarchy delimiter character), removing
				"foo" MUST NOT remove "foo.bar".  It is an error to attempt to
				delete a name that has inferior hierarchical names and also has
				the \Noselect mailbox name attribute (see the description of the
				LIST response for more details).

				It is permitted to delete a name that has inferior hierarchical
				names and does not have the \Noselect mailbox name attribute.  In
				this case, all messages in that mailbox are removed, and the name
				will acquire the \Noselect mailbox name attribute.

				The value of the highest-used unique identifier of the deleted
				mailbox MUST be preserved so that a new mailbox created with the
				same name will not reuse the identifiers of the former
				incarnation, UNLESS the new incarnation has a different unique
				identifier validity value.  See the description of the UID command
				for more detail.
				
				Examples:   C: A682 LIST "" *
							S: * LIST () "/" blurdybloop
							S: * LIST (\Noselect) "/" foo
							S: * LIST () "/" foo/bar
							S: A682 OK LIST completed
							C: A683 DELETE blurdybloop
							S: A683 OK DELETE completed
							C: A684 DELETE foo
							S: A684 NO Name "foo" has inferior hierarchical names
							C: A685 DELETE foo/bar
							S: A685 OK DELETE Completed
							C: A686 LIST "" *
							S: * LIST (\Noselect) "/" foo
							S: A686 OK LIST completed
							C: A687 DELETE foo
							S: A687 OK DELETE Completed
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 1){
				m_pSocket.SendLine(cmdTag + " BAD DELETE invalid arguments");
				return;
			}
			
			string errorText = m_pServer.OnDeleteMailbox(this,args[0]);
			if(errorText == null){
				m_pSocket.SendLine(cmdTag + " OK DELETE Completed");
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO " + errorText);
			}			
		}

		#endregion

		#region function Rename

		private void Rename(string cmdTag,string argsText)
		{
			/* RFC 3501 6.3.5 RENAME Command
			  
				Arguments:  existing mailbox name
							new mailbox name

				Responses:  no specific responses for this command

				Result:     OK - rename completed
							NO - rename failure: can't rename mailbox with that name,
								 can't rename to mailbox with that name
							BAD - command unknown or arguments invalid
			   
				The RENAME command changes the name of a mailbox.  A tagged OK
				response is returned only if the mailbox has been renamed.  It is		  
				an error to attempt to rename from a mailbox name that does not
				exist or to a mailbox name that already exists.  Any error in
				renaming will return a tagged NO response.

				If the name has inferior hierarchical names, then the inferior
				hierarchical names MUST also be renamed.  For example, a rename of
				"foo" to "zap" will rename "foo/bar" (assuming "/" is the
				hierarchy delimiter character) to "zap/bar".

				The value of the highest-used unique identifier of the old mailbox
				name MUST be preserved so that a new mailbox created with the same
				name will not reuse the identifiers of the former incarnation,
				UNLESS the new incarnation has a different unique identifier
				validity value.  See the description of the UID command for more
				detail.

				Renaming INBOX is permitted, and has special behavior.  It moves
				all messages in INBOX to a new mailbox with the given name,
				leaving INBOX empty.  If the server implementation supports
				inferior hierarchical names of INBOX, these are unaffected by a
				rename of INBOX.
				
				Examples:   C: A682 LIST "" *
							S: * LIST () "/" blurdybloop
							S: * LIST (\Noselect) "/" foo
							S: * LIST () "/" foo/bar
							S: A682 OK LIST completed
							C: A683 RENAME blurdybloop sarasoop
							S: A683 OK RENAME completed
							C: A684 RENAME foo zowie
							S: A684 OK RENAME Completed
							C: A685 LIST "" *
							S: * LIST () "/" sarasoop
							S: * LIST (\Noselect) "/" zowie
							S: * LIST () "/" zowie/bar
							S: A685 OK LIST completed
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 2){
				m_pSocket.SendLine(cmdTag + " BAD RENAME invalid arguments");
				return;
			}

			string mailbox    = args[0];
			string newMailbox = args[1];
			
			string errorText = m_pServer.OnRenameMailbox(this,mailbox,newMailbox);
			if(errorText == null){
				m_pSocket.SendLine(cmdTag + " OK RENAME Completed");
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO " + errorText);
			}		
		}

		#endregion

		#region function Suscribe

		private void Suscribe(string cmdTag,string argsText)
		{
			/* RFC 3501 6.3.6 SUBSCRIBE Commmand
				
				Arguments:  mailbox

				Responses:  no specific responses for this command

				Result:     OK - subscribe completed
							NO - subscribe failure: can't subscribe to that name
							BAD - command unknown or arguments invalid
			   
				The SUBSCRIBE command adds the specified mailbox name to the
				server's set of "active" or "subscribed" mailboxes as returned by
				the LSUB command.  This command returns a tagged OK response only
				if the subscription is successful.

				A server MAY validate the mailbox argument to SUBSCRIBE to verify
				that it exists.  However, it MUST NOT unilaterally remove an
				existing mailbox name from the subscription list even if a mailbox
				by that name no longer exists.

				Note: this requirement is because some server sites may routinely
				remove a mailbox with a well-known name (e.g.  "system-alerts")
				after its contents expire, with the intention of recreating it
				when new contents are appropriate.

				Example:    C: A002 SUBSCRIBE #news.comp.mail.mime
							S: A002 OK SUBSCRIBE completed
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 1){
				m_pSocket.SendLine(cmdTag + " BAD SUBSCRIBE invalid arguments");
				return;
			}

			string errorText = m_pServer.OnSubscribeMailbox(this,args[0]);
			if(errorText == null){
				m_pSocket.SendLine(cmdTag + " OK SUBSCRIBE completed");
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO " + errorText);
			}			
		}

		#endregion

		#region function UnSuscribe

		private void UnSuscribe(string cmdTag,string argsText)
		{
			/* RFC 3501 6.3.7 UNSUBSCRIBE Command
				
				Arguments:  mailbox

				Responses:  no specific responses for this command

				Result:     OK - subscribe completed
							NO - subscribe failure: can't subscribe to that name
							BAD - command unknown or arguments invalid
			   
				The UNSUBSCRIBE command removes the specified mailbox name from
				the server's set of "active" or "subscribed" mailboxes as returned
				by the LSUB command.  This command returns a tagged OK response
				only if the unsubscription is successful.

				Example:    C: A002 UNSUBSCRIBE #news.comp.mail.mime
							S: A002 OK UNSUBSCRIBE completed
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 1){
				m_pSocket.SendLine(cmdTag + " BAD UNSUBSCRIBE invalid arguments");
				return;
			}

			string errorText = m_pServer.OnUnSubscribeMailbox(this,args[0]);
			if(errorText == null){
				m_pSocket.SendLine(cmdTag + " OK UNSUBSCRIBE completed");
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO " + errorText);
			}			
		}

		#endregion

		#region function List

		private void List(string cmdTag,string argsText)
		{
			/* Rc 3501 6.3.8 LIST Command
			 
				Arguments:  reference name
							mailbox name with possible wildcards

				Responses:  untagged responses: LIST

				Result:     OK - list completed
							NO - list failure: can't list that reference or name
							BAD - command unknown or arguments invalid
							
				The LIST command returns a subset of names from the complete set
				of all names available to the client.  Zero or more untagged LIST
				replies are returned, containing the name attributes, hierarchy
				delimiter, and name; see the description of the LIST reply for
				more detail.
				
				An empty ("" string) reference name argument indicates that the
				mailbox name is interpreted as by SELECT. The returned mailbox
				names MUST match the supplied mailbox name pattern.  A non-empty
				reference name argument is the name of a mailbox or a level of
				mailbox hierarchy, and indicates a context in which the mailbox
				name is interpreted in an implementation-defined manner.
				
				An empty ("" string) mailbox name argument is a special request to
				return the hierarchy delimiter and the root name of the name given
				in the reference.  The value returned as the root MAY be null if
				the reference is non-rooted or is null.  In all cases, the
				hierarchy delimiter is returned.  This permits a client to get the
				hierarchy delimiter even when no mailboxes by that name currently
				exist.
				
				The character "*" is a wildcard, and matches zero or more
				characters at this position.  The character "%" is similar to "*",
				but it does not match a hierarchy delimiter.  If the "%" wildcard
				is the last character of a mailbox name argument, matching levels
				of hierarchy are also returned.
			    
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args  = ParseParams(argsText);
			if(args.Length != 2){
				m_pSocket.SendLine(cmdTag + " BAD LIST invalid arguments");
				return;
			}

			string refName = args[0];
			string mailbox = args[1];

			string reply = "";

			// Domain separator wanted
			if(mailbox.Length == 0){
				reply += "* LIST (\\Noselect) \"/\" \"\"\r\n";
			}
			// List mailboxes
			else{				
				IMAP_Folders mailboxes = m_pServer.OnGetMailboxes(this,refName,mailbox);
				foreach(IMAP_Folder mBox in mailboxes.Folders){
					if(mBox.Selectable){
						reply += "* LIST () \"/\" \"" + mBox.Folder + "\" \r\n";
					}
					else{
						reply += "* LIST (\\Noselect) \"/\" \"" + mBox.Folder + "\" \r\n";
					}
				}
			}

			reply += cmdTag + " OK LIST Completed\r\n";
			m_pSocket.SendData(reply);
		}

		#endregion

		#region function LSub

		private void LSub(string cmdTag,string argsText)
		{
			/* RFC 3501 6.3.9 LSUB Command
			 
				Arguments:  reference name
							mailbox name with possible wildcards

				Responses:  untagged responses: LSUB

				Result:     OK - lsub completed
							NO - lsub failure: can't list that reference or name
							BAD - command unknown or arguments invalid
				   
				The LSUB command returns a subset of names from the set of names
				that the user has declared as being "active" or "subscribed".
				Zero or more untagged LSUB replies are returned.  The arguments to
				LSUB are in the same form as those for LIST.

				The returned untagged LSUB response MAY contain different mailbox
				flags from a LIST untagged response.  If this should happen, the
				flags in the untagged LIST are considered more authoritative.

				A special situation occurs when using LSUB with the % wildcard.
				Consider what happens if "foo/bar" (with a hierarchy delimiter of
				"/") is subscribed but "foo" is not.  A "%" wildcard to LSUB must
				return foo, not foo/bar, in the LSUB response, and it MUST be
				flagged with the \Noselect attribute.

				The server MUST NOT unilaterally remove an existing mailbox name
				from the subscription list even if a mailbox by that name no
				longer exists.

				Example:    C: A002 LSUB "#news." "comp.mail.*"
							S: * LSUB () "." #news.comp.mail.mime
							S: * LSUB () "." #news.comp.mail.misc
							S: A002 OK LSUB completed
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args  = ParseParams(argsText);
			if(args.Length != 2){
				m_pSocket.SendLine(cmdTag + " BAD LSUB invalid arguments");
				return;
			}

			string refName = args[0];
			string mailbox = args[1];
			
			string reply = "";

			IMAP_Folders mailboxes = m_pServer.OnGetSubscribedMailboxes(this,refName,mailbox);
			foreach(IMAP_Folder mBox in mailboxes.Folders){
				reply += "* LSUB () \"/\" \"" + mBox.Folder + "\" \r\n";
			}

			reply += cmdTag + " OK LSUB Completed\r\n";
			m_pSocket.SendData(reply);
		}

		#endregion

		#region function Status

		private void Status(string cmdTag,string argsText)
		{
			/* RFC 3501 6.3.10 STATUS Command
			
				Arguments:  mailbox name
							status data item names

				Responses:  untagged responses: STATUS

				Result:     OK - status completed
							NO - status failure: no status for that name
							BAD - command unknown or arguments invalid
			   
				The STATUS command requests the status of the indicated mailbox.
				It does not change the currently selected mailbox, nor does it
				affect the state of any messages in the queried mailbox (in
				particular, STATUS MUST NOT cause messages to lose the \Recent
				flag).

				The STATUS command provides an alternative to opening a second
				IMAP4rev1 connection and doing an EXAMINE command on a mailbox to
				query that mailbox's status without deselecting the current
				mailbox in the first IMAP4rev1 connection.

				Unlike the LIST command, the STATUS command is not guaranteed to
				be fast in its response.  In some implementations, the server is
				obliged to open the mailbox read-only internally to obtain certain
				status information.  Also unlike the LIST command, the STATUS
				command does not accept wildcards.

				The currently defined status data items that can be requested are:

				MESSAGES
					The number of messages in the mailbox.

				RECENT
					The number of messages with the \Recent flag set.

				UIDNEXT
					The next unique identifier value of the mailbox.  Refer to
					section 2.3.1.1 for more information.

				UIDVALIDITY
					The unique identifier validity value of the mailbox.  Refer to
					section 2.3.1.1 for more information.

				UNSEEN
					The number of messages which do not have the \Seen flag set.


				Example:    C: A042 STATUS blurdybloop (UIDNEXT MESSAGES)
							S: * STATUS blurdybloop (MESSAGES 231 UIDNEXT 44292)
							S: A042 OK STATUS completed
				  
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 2){
				m_pSocket.SendLine(cmdTag + " BAD STATUS invalid arguments");
				return;
			}

			string mailbox     = args[0];
			string wantedItems = args[1].ToUpper();

			// See wanted items are valid.
			if(wantedItems.Replace("MESSAGES","").Replace("RECENT","").Replace("UIDNEXT","").Replace("UIDVALIDITY","").Replace("UNSEEN","").Trim().Length > 0){
				m_pSocket.SendLine(cmdTag + " BAD STATUS invalid arguments");
				return;
			}

			IMAP_Messages messages = m_pServer.OnGetMessagesInfo(this,mailbox);
			if(messages.ErrorText == null){
				string itemsReply = "";
				if(wantedItems.IndexOf("MESSAGES") > -1){
					itemsReply += " MESSAGES " + messages.Count;
				}
				if(wantedItems.IndexOf("RECENT") > -1){
					itemsReply += " RECENT " + messages.RecentCount;
				}
				if(wantedItems.IndexOf("UNSEEN") > -1){
					itemsReply += " UNSEEN " + messages.UnSeenCount;
				}			
				if(wantedItems.IndexOf("UIDVALIDITY") > -1){
					itemsReply += " UIDVALIDITY " + messages.MailboxUID;
				}
				if(wantedItems.IndexOf("UIDNEXT") > -1){
					itemsReply += " UIDNEXT " + messages.UID_Next;
				}
				itemsReply = itemsReply.Trim();

				m_pSocket.SendLine("* STATUS " + messages.Mailbox + " (" + itemsReply + ")");
				m_pSocket.SendLine(cmdTag + " OK STATUS completed");
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO " + messages.ErrorText);
			}
		}

		#endregion

		#region function Append

		#region method BeginAppendCmd

		/// <summary>
		/// Returns true if command ended syncronously.
		/// </summary>
		private bool BeginAppendCmd(string cmdTag,string argsText)
		{
			/* Rfc 3501 6.3.11 APPEND Command
			
				Arguments:  mailbox name
							OPTIONAL flag parenthesized list
							OPTIONAL date/time string
							message literal

				Responses:  no specific responses for this command

				Result:     OK - append completed
							NO - append error: can't append to that mailbox, error
									in flags or date/time or message text
							BAD - command unknown or arguments invalid

				The APPEND command appends the literal argument as a new message
				to the end of the specified destination mailbox.  This argument
				SHOULD be in the format of an [RFC-2822] message.  8-bit
				characters are permitted in the message.  A server implementation
				that is unable to preserve 8-bit data properly MUST be able to
				reversibly convert 8-bit APPEND data to 7-bit using a [MIME-IMB]
				content transfer encoding.
					
				If a flag parenthesized list is specified, the flags SHOULD be set
				in the resulting message; otherwise, the flag list of the
				resulting message is set to empty by default.  In either case, the
				Recent flag is also set.

				If a date-time is specified, the internal date SHOULD be set in
				the resulting message; otherwise, the internal date of the
				resulting message is set to the current date and time by default.

				If the append is unsuccessful for any reason, the mailbox MUST be
				restored to its state before the APPEND attempt; no partial
				appending is permitted.

				If the destination mailbox does not exist, a server MUST return an
				error, and MUST NOT automatically create the mailbox.  Unless it
				is certain that the destination mailbox can not be created, the
				server MUST send the response code "[TRYCREATE]" as the prefix of
				the text of the tagged NO response.  This gives a hint to the
				client that it can attempt a CREATE command and retry the APPEND
				if the CREATE is successful.
					
				Example:    C: A003 APPEND saved-messages (\Seen) {310}
							S: + Ready for literal data
							C: Date: Mon, 7 Feb 1994 21:52:25 -0800 (PST)
							C: From: Fred Foobar <foobar@Blurdybloop.COM>
							C: Subject: afternoon meeting
							C: To: mooch@owatagu.siam.edu
							C: Message-Id: <B27397-0100000@Blurdybloop.COM>
							C: MIME-Version: 1.0
							C: Content-Type: TEXT/PLAIN; CHARSET=US-ASCII
							C:
							C: Hello Joe, do you think we can meet at 3:30 tomorrow?
							C:
							S: A003 OK APPEND completed
					
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return true;
			}

			string[] args = ParseParams(argsText);
			if(args.Length < 2 || args.Length > 4){
				m_pSocket.SendLine(cmdTag + " BAD APPEND Invalid arguments");
				return true;
			}

			string            mailbox = args[0];			
			IMAP_MessageFlags mFlags  = 0;
			DateTime          date    = DateTime.Now;
			long              msgLen  = Convert.ToInt64(args[args.Length - 1].Replace("{","").Replace("}",""));

			if(args.Length == 4){
				//--- Parse flags, see if valid ----------------
				string flags = args[1].ToUpper();
				if(flags.Replace("\\ANSWERED","").Replace("\\FLAGGED","").Replace("\\DELETED","").Replace("\\SEEN","").Replace("\\DRAFT","").Trim().Length > 0){
					m_pSocket.SendLine(cmdTag + " BAD arguments invalid");
					return false;
				}
				
				mFlags = ParseMessageFalgs(flags);				
				date = MimeUtils.ParseDate(args[2]);
			}
			else if(args.Length == 3){
				// See if date or flags specified, try date first
				try{
					date = MimeUtils.ParseDate(args[1]);
				}
				catch{
					//--- Parse flags, see if valid ----------------
					string flags = args[1].ToUpper();
					if(flags.Replace("\\ANSWERED","").Replace("\\FLAGGED","").Replace("\\DELETED","").Replace("\\SEEN","").Replace("\\DRAFT","").Trim().Length > 0){
						m_pSocket.SendLine(cmdTag + " BAD arguments invalid");
						return false;
					}
			
					mFlags = ParseMessageFalgs(flags);
				}
			}

			// Request data
			m_pSocket.SendLine("+ Ready for literal data");

			MemoryStream strm = new MemoryStream();
			Hashtable param = new Hashtable();
			param.Add("cmdTag",cmdTag);
			param.Add("mailbox",mailbox);
			param.Add("mFlags",mFlags);
			param.Add("date",date);
			param.Add("strm",strm);

			// Begin recieving data                      Why needed msgLen+2 ??? 
			m_pSocket.BeginReadData(strm,msgLen + 2,50000000,param,new SocketCallBack(this.EndAppendCmd));

			return false;
		}

		#endregion

		#region method EndAppendCmd

		/// <summary>
		/// Is called when DATA command is finnished.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void EndAppendCmd(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			try{
			//	if(m_pServer.LogCommands){
			//		m_pLogWriter.AddEntry("big binary " + count.ToString() + " bytes",this.SessionID,this.RemoteEndPoint.Address.ToString(),"S");
			//	}

				switch(result)
				{
					case SocketCallBackResult.Ok:
						Hashtable         param   = (Hashtable)tag;
						string            cmdTag  = (string)param["cmdTag"];
						string            mailbox = (string)param["mailbox"];
						IMAP_MessageFlags mFlags  = (IMAP_MessageFlags)param["mFlags"];
						DateTime          date    = (DateTime)param["date"];
						MemoryStream      strm    = (MemoryStream)param["strm"];
						
						IMAP_Message msg = new IMAP_Message(null,"",0,mFlags,0,date);
						string errotText = m_pServer.OnStoreMessage(this,mailbox,msg,strm.ToArray());
						if(errotText == null){
							m_pSocket.SendLine(cmdTag + " OK APPEND completed, recieved " + strm.Length + " bytes");
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO " + errotText);
						}												
						break;

					case SocketCallBackResult.LengthExceeded:
					//	SendData("552 Requested mail action aborted: exceeded storage allocation\r\n");

					//	BeginRecieveCmd();
						break;

					case SocketCallBackResult.SocketClosed:
						EndSession();
						return;

					case SocketCallBackResult.Exception:
						OnError(exception);
						return;
				}

				// Command completed ok, get next command
				BeginRecieveCmd();
			}
			catch(Exception x){
				OnError(x);
			}
		}

		#endregion

		#endregion

		#region method Namespace

		private void Namespace(string cmdTag,string argsText)
		{
			/* Rfc 2342
				Example: < A server that contains a Personal Namespace and a single Shared
						Namespace. >

						C: A001 NAMESPACE
						S: * NAMESPACE (("" "/")) NIL (("Public Folders/" "/"))
						S: A001 OK NAMESPACE command completed	  
			*/

			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			SharedRootFolders_EventArgs eArgs = m_pServer.OnGetSharedRootFolders(this);
			string publicRootFolders = "NIL";
			if(eArgs.PublicRootFolders != null && eArgs.PublicRootFolders.Length > 0){
				publicRootFolders = "(";
				foreach(string publicRootFolder in eArgs.PublicRootFolders){
					publicRootFolders += "(\"" + publicRootFolder + "/\" \"/\")";
				}
				publicRootFolders += ")";
			}
			string sharedRootFolders = "NIL";
			if(eArgs.SharedRootFolders != null && eArgs.SharedRootFolders.Length > 0){
				sharedRootFolders = "(";
				foreach(string sharedRootFolder in eArgs.SharedRootFolders){
					sharedRootFolders += "(\"" + sharedRootFolder + "/\" \"/\")";
				}
				sharedRootFolders += ")";
			}
			
			string response  = "* NAMESPACE ((\"\" \"/\")) " + sharedRootFolders + " " + publicRootFolders + "\r\n";
			       response += cmdTag + " OK CHECK completed";

			m_pSocket.SendLine(response);
		}

		#endregion


		#region method GETACL

		private void GETACL(string cmdTag,string argsText)
		{
			/* RFC 2086 4.3. GETACL
				Arguments:  mailbox name

				Data:       untagged responses: ACL

				Result:     OK - getacl completed
							NO - getacl failure: can't get acl
							BAD - command unknown or arguments invalid

					The GETACL command returns the access control list for mailbox in
					an untagged ACL reply.

				Example:    C: A002 GETACL INBOX
							S: * ACL INBOX Fred rwipslda
							S: A002 OK Getacl complete
							
							.... Multiple users
							S: * ACL INBOX Fred rwipslda test rwipslda
							
							.... No acl flags for Fred
							S: * ACL INBOX Fred "" test rwipslda
									
			*/

			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 1){
				m_pSocket.SendLine(cmdTag + " BAD GETACL invalid arguments. Syntax: GETACL<SP>FolderName<CRLF>");
				return;
			}

			IMAP_GETACL_eArgs eArgs = m_pServer.OnGetFolderACL(this,IMAP_Utils.NormalizeFolder(args[0]));
			if(eArgs.ErrorText.Length > 0){
				m_pSocket.SendLine(cmdTag + " NO GETACL " + eArgs.ErrorText);
			}
			else{
				string response = "";
				if(eArgs.ACL.Count > 0){
					response += "* ACL \"" + args[0] + "\"";
					foreach(DictionaryEntry ent in eArgs.ACL){
						string aclFalgs = IMAP_Utils.ACL_to_String((IMAP_ACL_Flags)ent.Value);
						if(aclFalgs.Length == 0){
							aclFalgs = "\"\"";
						}
						response += " \"" + ent.Key + "\" " + aclFalgs;
					}
					response += "\r\n";
				}
				response += cmdTag + " OK Getacl complete\r\n";

				m_pSocket.SendData(response);
			}
		}

		#endregion

		#region method SETACL

		private void SETACL(string cmdTag,string argsText)
		{
			/* RFC 2086 4.1. SETACL
				Arguments:  mailbox name
							authentication identifier
							access right modification

				Data:       no specific data for this command

				Result:     OK - setacl completed
							NO - setacl failure: can't set acl
							BAD - command unknown or arguments invalid

					The SETACL command changes the access control list on the
					specified mailbox so that the specified identifier is granted
					permissions as specified in the third argument.

					The third argument is a string containing an optional plus ("+")
					or minus ("-") prefix, followed by zero or more rights characters.
					If the string starts with a plus, the following rights are added
					to any existing rights for the identifier.  If the string starts
					with a minus, the following rights are removed from any existing
					rights for the identifier.  If the string does not start with a
					plus or minus, the rights replace any existing rights for the
					identifier.
			*/

			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 3){
				m_pSocket.SendLine(cmdTag + " BAD GETACL invalid arguments. Syntax: SETACL<SP>FolderName<SP>UserName<SP>ACL_Flags<CRLF>");
				return;
			}

			string aclFlags = args[2];
			IMAP_Flags_SetType setType = IMAP_Flags_SetType.Replace;
			if(aclFlags.StartsWith("+")){
				setType = IMAP_Flags_SetType.Add;
			}
			else if(aclFlags.StartsWith("-")){
				setType = IMAP_Flags_SetType.Remove;
			}

			IMAP_SETACL_eArgs eArgs = m_pServer.OnSetFolderACL(this,IMAP_Utils.NormalizeFolder(args[0]),args[1],setType,IMAP_Utils.ACL_From_String(aclFlags));
			if(eArgs.ErrorText.Length > 0){
				m_pSocket.SendLine(cmdTag + " NO SETACL " + eArgs.ErrorText);
			}
			else{
				m_pSocket.SendLine(cmdTag + " OK SETACL completed");
			}
		}

		#endregion

		#region method DELETEACL

		private void DELETEACL(string cmdTag,string argsText)
		{
			/* RFC 2086 4.2. DELETEACL
				Arguments:  mailbox name
							authentication identifier

				Data:       no specific data for this command

				Result:     OK - deleteacl completed
							NO - deleteacl failure: can't delete acl
							BAD - command unknown or arguments invalid

					The DELETEACL command removes any <identifier,rights> pair for the
					specified identifier from the access control list for the specified
					mailbox.
					
				Example:    C: A002 DELETEACL INBOX test
							S: A002 OK DELETEACL completed
			*/

			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 2){
				m_pSocket.SendLine(cmdTag + " BAD GETACL invalid arguments. Syntax: DELETEACL<SP>FolderName<SP>UserName<CRLF>");
				return;
			}

			IMAP_DELETEACL_eArgs eArgs = m_pServer.OnDeleteFolderACL(this,IMAP_Utils.NormalizeFolder(args[0]),args[1]);
			if(eArgs.ErrorText.Length > 0){
				m_pSocket.SendLine(cmdTag + " NO DELETEACL " + eArgs.ErrorText);
			}
			else{
				m_pSocket.SendLine(cmdTag + " OK DELETEACL completed");
			}
		}

		#endregion

		#region method LISTRIGHTS

		private void LISTRIGHTS(string cmdTag,string argsText)
		{
			/* RFC 2086 4.4. LISTRIGHTS
				Arguments:  mailbox name
				authentication identifier

				Data:       untagged responses: LISTRIGHTS

				Result:     OK - listrights completed
							NO - listrights failure: can't get rights list
							BAD - command unknown or arguments invalid

					The LISTRIGHTS command takes a mailbox name and an identifier and
					returns information about what rights may be granted to the identifier
					in the ACL for the mailbox.
					
				Example:    C: a001 LISTRIGHTS ~/Mail/saved smith
							S: * LISTRIGHTS ~/Mail/saved "smith" la r swicd
							S: a001 OK Listrights completed


							C: a005 LISTRIGHTS archive.imap anyone
							S: * LISTRIGHTS archive.imap "anyone" "" l r s w i p c d a
							0 1 2 3 4 5 6 7 8 9
			   
			*/

			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 2){
				m_pSocket.SendLine(cmdTag + " BAD GETACL invalid arguments. Syntax: LISTRIGHTS<SP>FolderName<SP>UserName<CRLF>");
				return;
			}
			
			string response = "* LISTRIGHTS " + "\"" + args[0] + "\" \"" + args[1] + "\" l r s w i p c d a\r\n";
			response += cmdTag + " OK MYRIGHTS Completed\r\n";

			m_pSocket.SendData(response);
		}

		#endregion

		#region method MYRIGHTS

		private void MYRIGHTS(string cmdTag,string argsText)
		{
			/* RFC 2086 4.5. MYRIGHTS
				Arguments:  mailbox name

				Data:       untagged responses: MYRIGHTS

				Result:     OK - myrights completed
							NO - myrights failure: can't get rights
							BAD - command unknown or arguments invalid

					The MYRIGHTS command returns the set of rights that the user has
					to mailbox in an untagged MYRIGHTS reply.

				Example:    C: A003 MYRIGHTS INBOX
							S: * MYRIGHTS INBOX rwipslda
							S: A003 OK Myrights complete
							
			*/

			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 1){
				m_pSocket.SendLine(cmdTag + " BAD GETACL invalid arguments. Syntax: MYRIGHTS<SP>FolderName<CRLF>");
				return;
			}

			IMAP_GetUserACL_eArgs eArgs = m_pServer.OnGetUserACL(this,IMAP_Utils.NormalizeFolder(args[0]),this.UserName);
			if(eArgs.ErrorText.Length > 0){
				m_pSocket.SendLine(cmdTag + " NO MYRIGHTS " + eArgs.ErrorText);
			}
			else{
				string aclFlags = IMAP_Utils.ACL_to_String(eArgs.ACL);
				if(aclFlags.Length == 0){
					aclFlags = "\"\"";
				}
				string response = "* MYRIGHTS " + "\"" + args[0] + "\" " + aclFlags + "\r\n";
				response += cmdTag + " OK MYRIGHTS Completed\r\n";

				m_pSocket.SendData(response);
			}
		}

		#endregion

		//--- End of Authenticated State 


		//--- Selected State ------

		#region function Check

		private void Check(string cmdTag)
		{
			/* RFC 3501 6.4.1 CHECK Command
			
				Arguments:  none

				Responses:  no specific responses for this command

				Result:     OK - check completed
							BAD - command unknown or arguments invalid
			   
				The CHECK command requests a checkpoint of the currently selected
				mailbox.  A checkpoint refers to any implementation-dependent
				housekeeping associated with the mailbox (e.g. resolving the
				server's in-memory state of the mailbox with the state on its
				disk) that is not normally executed as part of each command.  A
				checkpoint MAY take a non-instantaneous amount of real time to
				complete.  If a server implementation has no such housekeeping
				considerations, CHECK is equivalent to NOOP.

				There is no guarantee that an EXISTS untagged response will happen
				as a result of CHECK.  NOOP, not CHECK, SHOULD be used for new
				mail polling.
				
				Example:    C: FXXZ CHECK
							S: FXXZ OK CHECK Completed
			   
			*/
			if(!m_Authenticated)
			{
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}
			if(m_SelectedMailbox.Length == 0){
				m_pSocket.SendLine(cmdTag + " NO Select mailbox first !");
				return;
			}
			
			m_pSocket.SendLine(cmdTag + " OK CHECK completed");
		}

		#endregion

		#region function Close

		private void Close(string cmdTag)
		{
			/* RFC 3501 6.4.2 CLOSE Command
			
				Arguments:  none

				Responses:  no specific responses for this command

				Result:     OK - close completed, now in authenticated state
							BAD - command unknown or arguments invalid
			   
				The CLOSE command permanently removes from the currently selected
				mailbox all messages that have the \Deleted flag set, and returns
				to authenticated state from selected state.  No untagged EXPUNGE
				responses are sent.

				No messages are removed, and no error is given, if the mailbox is
				selected by an EXAMINE command or is otherwise selected read-only.

				Even if a mailbox is selected, a SELECT, EXAMINE, or LOGOUT
				command MAY be issued without previously issuing a CLOSE command.
				The SELECT, EXAMINE, and LOGOUT commands implicitly close the
				currently selected mailbox without doing an expunge.  However,
				when many messages are deleted, a CLOSE-LOGOUT or CLOSE-SELECT
				sequence is considerably faster than an EXPUNGE-LOGOUT or
				EXPUNGE-SELECT because no untagged EXPUNGE responses (which the
				client would probably ignore) are sent.

				Example:    C: A341 CLOSE
							S: A341 OK CLOSE completed
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}			
			if(m_SelectedMailbox.Length == 0){
				m_pSocket.SendLine(cmdTag + " NO Select mailbox first !");
				return;
			}

			if(!m_Messages.ReadOnly){
				IMAP_Message[] messages = m_Messages.GetDeleteMessages();
				foreach(IMAP_Message msg in messages){
					m_pServer.OnDeleteMessage(this,msg);
				}
			}
			
			m_SelectedMailbox = "";
			m_Messages        = null;

			m_pSocket.SendLine(cmdTag + " OK CLOSE completed");
		}

		#endregion

		#region function Expunge

		private void Expunge(string cmdTag)
		{
			/* RFC 3501 6.4.3 EXPUNGE Command
			
				Arguments:  none

				Responses:  untagged responses: EXPUNGE

				Result:     OK - expunge completed
							NO - expunge failure: can't expunge (e.g., permission
									denied)
							BAD - command unknown or arguments invalid

					The EXPUNGE command permanently removes all messages that have the
					\Deleted flag set from the currently selected mailbox.  Before
					returning an OK to the client, an untagged EXPUNGE response is
					sent for each message that is removed.
					
				The EXPUNGE response reports that the specified message sequence
			    number has been permanently removed from the mailbox.  The message
				sequence number for each successive message in the mailbox is
				IMMEDIATELY DECREMENTED BY 1, and this decrement is reflected in
				message sequence numbers in subsequent responses (including other
				untagged EXPUNGE responses).


				Example:    C: A202 EXPUNGE
							S: * 3 EXPUNGE
							S: * 3 EXPUNGE
							S: * 5 EXPUNGE
							S: * 8 EXPUNGE
							S: A202 OK EXPUNGE completed

						Note: In this example, messages 3, 4, 7, and 11 had the
						\Deleted flag set.  See the description of the EXPUNGE
						response for further explanation.
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}
			if(m_SelectedMailbox.Length == 0){
				m_pSocket.SendLine(cmdTag + " NO Select mailbox first !");
				return;
			}
			
			IMAP_Message[] messages = m_Messages.GetDeleteMessages();
			for(int i=0;i<messages.Length;i++){
				IMAP_Message msg = messages[i]; 

				string errorText = m_pServer.OnDeleteMessage(this,msg);
				if(errorText == null){
					m_pSocket.SendLine("* " + msg.MessageNo + " EXPUNGE");

					m_Messages.RemoveMessage(msg);
				}
				else{
					m_pSocket.SendLine(cmdTag + " NO " + errorText);
					return;
				}
			}

			// Refresh m_Messages to reflect deletion
			m_Messages = m_pServer.OnGetMessagesInfo(this,this.SelectedMailbox);

			m_pSocket.SendLine(cmdTag + " OK EXPUNGE completed");
		}

		#endregion
//
		#region function Search

		private void Search(string cmdTag,string argsText,bool uidSearch)
		{
			/* Rfc 3501 6.4.4 SEARCH Command
			
				Arguments:  OPTIONAL [CHARSET] specification
							searching criteria (one or more)

				Responses:  REQUIRED untagged response: SEARCH

				Result:     OK - search completed
							NO - search error: can't search that [CHARSET] or
									criteria
							BAD - command unknown or arguments invalid
							
				The SEARCH command searches the mailbox for messages that match
				the given searching criteria.  Searching criteria consist of one
				or more search keys.  The untagged SEARCH response from the server
				contains a listing of message sequence numbers corresponding to
				those messages that match the searching criteria.
				
				When multiple keys are specified, the result is the intersection
				(AND function) of all the messages that match those keys.  For
				example, the criteria DELETED FROM "SMITH" SINCE 1-Feb-1994 refers
				to all deleted messages from Smith that were placed in the mailbox
				since February 1, 1994.  A search key can also be a parenthesized
				list of one or more search keys (e.g., for use with the OR and NOT
				keys).

				Server implementations MAY exclude [MIME-IMB] body parts with
				terminal content media types other than TEXT and MESSAGE from
				consideration in SEARCH matching.

				The OPTIONAL [CHARSET] specification consists of the word
				"CHARSET" followed by a registered [CHARSET].  It indicates the
				[CHARSET] of the strings that appear in the search criteria.
				[MIME-IMB] content transfer encodings, and [MIME-HDRS] strings in
				[RFC-2822]/[MIME-IMB] headers, MUST be decoded before comparing
				text in a [CHARSET] other than US-ASCII.  US-ASCII MUST be
				supported; other [CHARSET]s MAY be supported.

				If the server does not support the specified [CHARSET], it MUST
				return a tagged NO response (not a BAD).  This response SHOULD
				contain the BADCHARSET response code, which MAY list the
				[CHARSET]s supported by the server.

				In all search keys that use strings, a message matches the key if
				the string is a substring of the field.  The matching is
				case-insensitive.

				The defined search keys are as follows.  Refer to the Formal
				Syntax section for the precise syntactic definitions of the
				arguments.

				<sequence set>
					Messages with message sequence numbers corresponding to the
					specified message sequence number set.

				ALL
					All messages in the mailbox; the default initial key for
					ANDing.

				ANSWERED
					Messages with the \Answered flag set.
					
				BCC <string>
					Messages that contain the specified string in the envelope
					structure's BCC field.

				BEFORE <date>
					Messages whose internal date (disregarding time and timezone)
					is earlier than the specified date.

				BODY <string>
					Messages that contain the specified string in the body of the
					message.

				CC <string>
					Messages that contain the specified string in the envelope
					structure's CC field.

				DELETED
					Messages with the \Deleted flag set.

				DRAFT
					Messages with the \Draft flag set.

				FLAGGED
					Messages with the \Flagged flag set.

				FROM <string>
					Messages that contain the specified string in the envelope
					structure's FROM field.

				HEADER <field-name> <string>
					Messages that have a header with the specified field-name (as
					defined in [RFC-2822]) and that contains the specified string
					in the text of the header (what comes after the colon).  If the
					string to search is zero-length, this matches all messages that
					have a header line with the specified field-name regardless of
					the contents.

				KEYWORD <flag>
					Messages with the specified keyword flag set.

				LARGER <n>
					Messages with an [RFC-2822] size larger than the specified
					number of octets.

				NEW
					Messages that have the \Recent flag set but not the \Seen flag.
					This is functionally equivalent to "(RECENT UNSEEN)".
					
				NOT <search-key>
					Messages that do not match the specified search key.

				OLD
					Messages that do not have the \Recent flag set.  This is
					functionally equivalent to "NOT RECENT" (as opposed to "NOT
					NEW").

				ON <date>
					Messages whose internal date (disregarding time and timezone)
					is within the specified date.

				OR <search-key1> <search-key2>
					Messages that match either search key.

				RECENT
					Messages that have the \Recent flag set.

				SEEN
					Messages that have the \Seen flag set.

				SENTBEFORE <date>
					Messages whose [RFC-2822] Date: header (disregarding time and
					timezone) is earlier than the specified date.

				SENTON <date>
					Messages whose [RFC-2822] Date: header (disregarding time and
					timezone) is within the specified date.

				SENTSINCE <date>
					Messages whose [RFC-2822] Date: header (disregarding time and
					timezone) is within or later than the specified date.

				SINCE <date>
					Messages whose internal date (disregarding time and timezone)
					is within or later than the specified date.

				SMALLER <n>
					Messages with an [RFC-2822] size smaller than the specified
					number of octets.
					
				SUBJECT <string>
					Messages that contain the specified string in the envelope
					structure's SUBJECT field.

				TEXT <string>
					Messages that contain the specified string in the header or
					body of the message.

				TO <string>
					Messages that contain the specified string in the envelope
					structure's TO field.

				UID <sequence set>
					Messages with unique identifiers corresponding to the specified
					unique identifier set.  Sequence set ranges are permitted.

				UNANSWERED
					Messages that do not have the \Answered flag set.

				UNDELETED
					Messages that do not have the \Deleted flag set.

				UNDRAFT
					Messages that do not have the \Draft flag set.

				UNFLAGGED
					Messages that do not have the \Flagged flag set.

				UNKEYWORD <flag>
					Messages that do not have the specified keyword flag set.

				UNSEEN
					Messages that do not have the \Seen flag set.
					
					Example:   
						C: A282 SEARCH FLAGGED SINCE 1-Feb-1994 NOT FROM "Smith"
						S: * SEARCH 2 84 882
						S: A282 OK SEARCH completed
						C: A283 SEARCH TEXT "string not in mailbox"
						S: * SEARCH
						S: A283 OK SEARCH completed
						C: A284 SEARCH CHARSET UTF-8 TEXT {6}
						S: + Continue                             ### THIS IS UNDUCUMENTED !!!
						C: XXXXXX
						S: * SEARCH 43
						S: A284 OK SEARCH completed

				Note: Since this document is restricted to 7-bit ASCII
				text, it is not possible to show actual UTF-8 data.  The
				"XXXXXX" is a placeholder for what would be 6 octets of
				8-bit data in an actual transaction.
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}
			if(m_SelectedMailbox.Length == 0){
				m_pSocket.SendLine(cmdTag + " NO Select mailbox first !");
				return;
			}

			// Get Optional charset, if specified
			string charset = "ASCII";
			// CHARSET charset
			if(argsText.ToUpper().StartsWith("CHARSET")){
				// Remove CHARSET from argsText
				argsText = argsText.Substring(7).Trim();

				string charsetValueString = IMAP_Utils.ParseQuotedParam(ref argsText);//argsText.Split(' ')[0];

				try{
					System.Text.Encoding.GetEncoding(charsetValueString);

					charset = charsetValueString;
				}
				catch{
					m_pSocket.SendLine(cmdTag + " NO [BADCHARSET UTF-8] " + charsetValueString + " is not supported");
					return;
				}

				// Remove charset value from args text
			//	argsText = argsText.Substring(charset.Length).Trim();
			}

			// Parse search keys
			Hashtable searchKeys = new Hashtable();
			while(argsText.Length > 0){
				// ALL
				if(argsText.ToUpper().StartsWith("ALL")){
					// Remove ALL from argsText
					argsText = argsText.Substring(3).Trim();

					// Just eat this keyword
				}
				// ANSWERED
				else if(argsText.ToUpper().StartsWith("ANSWERED")){
					// Remove ANSWERED from argsText
					argsText = argsText.Substring(8).Trim();

					// Move to KEYWORD <flag>
					searchKeys.Add("KEYWORD","ANSWERED");
				}
				// BCC <string> or {<string>.Length} if charset specified
				else if(argsText.ToUpper().StartsWith("BCC")){
					// Remove BCC from argsText
					argsText = argsText.Substring(3).Trim();

					string paramValue = "";
					if(!argsText.StartsWith("{")){
						try{
							paramValue = IMAP_Utils.ParseQuotedParam(ref argsText);
						}
						catch(Exception x){
							m_pSocket.SendLine(cmdTag + " NO Invalid BCC <string> value: " + x.Message);
							return;
						}
					}
					else{
						// get string length, it must be between {}
						long count = 0;
						if(argsText.StartsWith("{") && argsText.IndexOf('}',1) > -1){
							try{
								count = Convert.ToInt64(argsText.Substring(1,argsText.IndexOf('}',1) - 1));
							}
							catch{
								m_pSocket.SendLine(cmdTag + " NO Invalid BCC {x} value");
								return;
							}
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO Invalid BCC value");
							return;
						}

						// Notify connected client that it can continue sending cmdline
						m_pSocket.SendLine("+ Continue");
		
						// Read string from socket
						MemoryStream ms = new MemoryStream();
						ReadReplyCode result = m_pSocket.ReadData(ms,count,true);
						if(result != ReadReplyCode.Ok){
							throw new Exception(result.ToString());
						}

						paramValue = System.Text.Encoding.GetEncoding(charset).GetString(ms.ToArray());

						// Get next args text
						argsText = m_pSocket.ReadLine();
					}
					
					// Move to HEADER <field-name> <string>
					searchKeys.Add("HEADER",new string[]{"BCC",paramValue});
				}
				// BEFORE <date>
				else if(argsText.ToUpper().StartsWith("BEFORE")){
					// Remove BEFORE from argsText
					argsText = argsText.Substring(6).Trim();

					string dateValueString = argsText.Split(' ')[0];
					try{
						DateTime dateValue = _SearchHelper.ParseDate(dateValueString);

						searchKeys.Add("BEFORE",dateValue);
					}
					catch{
						m_pSocket.SendLine(cmdTag + " NO Invalid BEFORE <date> value");
						return;
					}
					
					// Remove date value from argsText
					argsText = argsText.Substring(dateValueString.Length).Trim();
				}
				// BODY <string> or {<string>.Length} if charset specified
				else if(argsText.ToUpper().StartsWith("BODY")){
					// Remove BODY from argsText
					argsText = argsText.Substring(4).Trim();

					string paramValue = "";
					if(!argsText.StartsWith("{")){
						try{
							paramValue = IMAP_Utils.ParseQuotedParam(ref argsText);
						}
						catch(Exception x){
							m_pSocket.SendLine(cmdTag + " NO Invalid BODY <string> value: " + x.Message);
							return;
						}
					}
					else{
						// get string length, it must be between {}
						long count = 0;
						if(argsText.StartsWith("{") && argsText.IndexOf('}',1) > -1){
							try{
								count = Convert.ToInt64(argsText.Substring(1,argsText.IndexOf('}',1) - 1));
							}
							catch{
								m_pSocket.SendLine(cmdTag + " NO Invalid BODY {x} value");
								return;
							}
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO Invalid BODY value");
							return;
						}

						// Notify connected client that it can continue sending cmdline
						m_pSocket.SendLine("+ Continue");
		
						// Read string from socket
						MemoryStream ms = new MemoryStream();
						ReadReplyCode result = m_pSocket.ReadData(ms,count,true);
						if(result != ReadReplyCode.Ok){
							throw new Exception(result.ToString());
						}

						paramValue = System.Text.Encoding.GetEncoding(charset).GetString(ms.ToArray());

						// Get next args text
						argsText = m_pSocket.ReadLine();
					}

					searchKeys.Add("BODY",paramValue);
				}
				// CC <string> or {<string>.Length} if charset specified
				else if(argsText.ToUpper().StartsWith("CC")){
					// Remove CC from argsText
					argsText = argsText.Substring(2).Trim();

					string paramValue = "";
					if(!argsText.StartsWith("{")){
						try{
							paramValue = IMAP_Utils.ParseQuotedParam(ref argsText);
						}
						catch(Exception x){
							m_pSocket.SendLine(cmdTag + " NO Invalid CC <string> value: " + x.Message);
							return;
						}
					}
					else{
						// get string length, it must be between {}
						long count = 0;
						if(argsText.StartsWith("{") && argsText.IndexOf('}',1) > -1){
							try{
								count = Convert.ToInt64(argsText.Substring(1,argsText.IndexOf('}',1) - 1));
							}
							catch{
								m_pSocket.SendLine(cmdTag + " NO Invalid CC {x} value");
								return;
							}
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO Invalid CC value");
							return;
						}

						// Notify connected client that it can continue sending cmdline
						m_pSocket.SendLine("+ Continue");
		
						// Read string from socket
						MemoryStream ms = new MemoryStream();
						ReadReplyCode result = m_pSocket.ReadData(ms,count,true);
						if(result != ReadReplyCode.Ok){
							throw new Exception(result.ToString());
						}

						paramValue = System.Text.Encoding.GetEncoding(charset).GetString(ms.ToArray());

						// Get next args text
						argsText = m_pSocket.ReadLine();
					}

					// Move to HEADER <field-name> <string>
					searchKeys.Add("HEADER",new string[]{"CC",paramValue});
				}
				// DELETED
				else if(argsText.ToUpper().StartsWith("DELETED")){
					// Remove DELETED from argsText
					argsText = argsText.Substring(7).Trim();

					// Move to KEYWORD <flag>
					searchKeys.Add("KEYWORD","DELETED");
				}
				// DRAFT
				else if(argsText.ToUpper().StartsWith("DRAFT")){
					// Remove DRAFT from argsText
					argsText = argsText.Substring(5).Trim();

					// Move to KEYWORD <flag>
					searchKeys.Add("KEYWORD","DRAFT");
				}
				// FLAGGED
				else if(argsText.ToUpper().StartsWith("FLAGGED")){
					// Remove FLAGGED from argsText
					argsText = argsText.Substring(7).Trim();

					// Move to KEYWORD <flag>
					searchKeys.Add("KEYWORD","FLAGGED");
				}
				// FROM <string> or {<string>.Length} if charset specified
				else if(argsText.ToUpper().StartsWith("FROM")){
					// Remove FROM from argsText
					argsText = argsText.Substring(4).Trim();

					string paramValue = "";
					if(!argsText.StartsWith("{")){
						try{
							paramValue = IMAP_Utils.ParseQuotedParam(ref argsText);
						}
						catch(Exception x){
							m_pSocket.SendLine(cmdTag + " NO Invalid FROM <string> value: " + x.Message);
							return;
						}
					}
					else{
						// get string length, it must be between {}
						long count = 0;
						if(argsText.StartsWith("{") && argsText.IndexOf('}',1) > -1){
							try{
								count = Convert.ToInt64(argsText.Substring(1,argsText.IndexOf('}',1) - 1));
							}
							catch{
								m_pSocket.SendLine(cmdTag + " NO Invalid FROM {x} value");
								return;
							}
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO Invalid FROM value");
							return;
						}

						// Notify connected client that it can continue sending cmdline
						m_pSocket.SendLine("+ Continue");
		
						// Read string from socket
						MemoryStream ms = new MemoryStream();
						ReadReplyCode result = m_pSocket.ReadData(ms,count,true);
						if(result != ReadReplyCode.Ok){
							throw new Exception(result.ToString());
						}

						paramValue = System.Text.Encoding.GetEncoding(charset).GetString(ms.ToArray());

						// Get next args text
						argsText = m_pSocket.ReadLine();
					}

					// Move to HEADER <field-name> <string>
					searchKeys.Add("HEADER",new string[]{"FROM",paramValue});
				}
				// HEADER <field-name> <string> or {<string>.Length} if charset specified
				else if(argsText.ToUpper().StartsWith("HEADER")){
					// Remove HEADER from argsText
					argsText = argsText.Substring(6).Trim();

					// Get field name
					string fieldName = IMAP_Utils.ParseQuotedParam(ref argsText);

					string paramValue = "";
					if(!argsText.StartsWith("{")){
						try{
							paramValue = IMAP_Utils.ParseQuotedParam(ref argsText);
						}
						catch(Exception x){
							m_pSocket.SendLine(cmdTag + " NO Invalid HEADER <string> value: " + x.Message);
							return;
						}
					}
					else{
						// get string length, it must be between {}						
						long count = 0;
						if(argsText.StartsWith("{") && argsText.IndexOf('}') > -1){
							try{
								count = Convert.ToInt64(argsText.Substring(1,argsText.IndexOf('}') - 1));
							}
							catch{
								m_pSocket.SendLine(cmdTag + " NO Invalid HEADER {x} value");
								return;
							}
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO Invalid HEADER value");
							return;
						}
						
						// Notify connected client that it can continue sending cmdline
						m_pSocket.SendLine("+ Continue");
		
						// Read string from socket
						MemoryStream ms = new MemoryStream();
						ReadReplyCode result = m_pSocket.ReadData(ms,count,true);
						if(result != ReadReplyCode.Ok){
							throw new Exception(result.ToString());
						}

						paramValue = System.Text.Encoding.GetEncoding(charset).GetString(ms.ToArray());

						// Get next args text
						argsText = m_pSocket.ReadLine();
					}

					searchKeys.Add("HEADER",new string[]{fieldName,paramValue});
				}
				// KEYWORD <flag>
				else if(argsText.ToUpper().StartsWith("KEYWORD")){
					// Remove KEYWORD from argsText
					argsText = argsText.Substring(7).Trim();

					// Get flag value
					string flagValue = argsText.Split(' ')[0];

					searchKeys.Add("KEYWORD",flagValue);

					// Remove flag value from argsText
					argsText = argsText.Substring(flagValue.Length).Trim();
				}
				// LARGER <n>
				else if(argsText.ToUpper().StartsWith("LARGER")){
					// Remove LARGER from argsText
					argsText = argsText.Substring(6).Trim();

					long lagerValue = 0;
					try{
						lagerValue = Convert.ToInt64(argsText.Split(' ')[0]);
					}
					catch{
					}

					searchKeys.Add("LARGER",lagerValue);

					// Remove flag value from argsText
					argsText = argsText.Substring(lagerValue.ToString().Length).Trim();
				}
				// NEW
				else if(argsText.ToUpper().StartsWith("NEW")){
					// Remove NEW from argsText
					argsText = argsText.Substring(3).Trim();

					// Move to KEYWORD <flag>
					searchKeys.Add("KEYWORD","RECENT");
				}
				// NOT <search-key>
				else if(argsText.ToUpper().StartsWith("NOT")){
					// Remove NOT from argsText
					argsText = argsText.Substring(3).Trim();

					m_pSocket.SendLine(cmdTag + " NO NOT search key isn't supported at moment");
					return;
				}
				// OLD
				else if(argsText.ToUpper().StartsWith("OLD")){
					// Remove OLD from argsText
					argsText = argsText.Substring(3).Trim();

					// Move to KEYWORD <flag>
					searchKeys.Add("UNKEYWORD","RECENT");
				}
				// ON <date>
				else if(argsText.ToUpper().StartsWith("ON")){
					// Remove ON from argsText
					argsText = argsText.Substring(2).Trim();

					string dateValueString = argsText.Split(' ')[0];
					try{
						DateTime dateValue = _SearchHelper.ParseDate(dateValueString);

						searchKeys.Add("ON",dateValue);
					}
					catch{
						m_pSocket.SendLine(cmdTag + " NO Invalid ON <date> value");
						return;
					}
					
					// Remove date value from argsText
					argsText = argsText.Substring(dateValueString.Length).Trim();
				}
				// OR <search-key1> <search-key2>
				else if(argsText.ToUpper().StartsWith("OR")){
					// Remove OR from argsText
					argsText = argsText.Substring(2).Trim();

					m_pSocket.SendLine(cmdTag + " NO OR search key isn't supported at moment");
					return;
				}
				// RECENT
				else if(argsText.ToUpper().StartsWith("RECENT")){
					// Remove RECENT from argsText
					argsText = argsText.Substring(6).Trim();

					// Move to KEYWORD <flag>
					searchKeys.Add("KEYWORD","RECENT");
				}
				// SEEN
				else if(argsText.ToUpper().StartsWith("SEEN")){
					// Remove SEEN from argsText
					argsText = argsText.Substring(4).Trim();

					// Move to KEYWORD <flag>
					searchKeys.Add("KEYWORD","SEEN");
				}
				// SENTBEFORE <date>
				else if(argsText.ToUpper().StartsWith("SENTBEFORE")){
					// Remove SENTBEFORE from argsText
					argsText = argsText.Substring(10).Trim();

					string dateValueString = argsText.Split(' ')[0];
					try{
						DateTime dateValue = _SearchHelper.ParseDate(dateValueString);

						searchKeys.Add("SENTBEFORE",dateValue);
					}
					catch{
						m_pSocket.SendLine(cmdTag + " NO Invalid SENTBEFORE <date> value");
						return;
					}
					
					// Remove date value from argsText
					argsText = argsText.Substring(dateValueString.Length).Trim();
				}
				// SENTON <date>
				else if(argsText.ToUpper().StartsWith("SENTON")){
					// Remove SENTON from argsText
					argsText = argsText.Substring(6).Trim();

					string dateValueString = argsText.Split(' ')[0];
					try{
						DateTime dateValue = _SearchHelper.ParseDate(dateValueString);

						searchKeys.Add("SENTON",dateValue);
					}
					catch{
						m_pSocket.SendLine(cmdTag + " NO Invalid SENTON <date> value");
						return;
					}
					
					// Remove date value from argsText
					argsText = argsText.Substring(dateValueString.Length).Trim();
				}
				// SENTSINCE <date>
				else if(argsText.ToUpper().StartsWith("SENTSINCE")){
					// Remove SENTSINCE from argsText
					argsText = argsText.Substring(9).Trim();

					string dateValueString = argsText.Split(' ')[0];
					try{
						DateTime dateValue = _SearchHelper.ParseDate(dateValueString);

						searchKeys.Add("SENTSINCE",dateValue);
					}
					catch{
						m_pSocket.SendLine(cmdTag + " NO Invalid SENTSINCE <date> value");
						return;
					}
					
					// Remove date value from argsText
					argsText = argsText.Substring(dateValueString.Length).Trim();
				}
				// SINCE <date>
				else if(argsText.ToUpper().StartsWith("SINCE")){
					// Remove SINCE from argsText
					argsText = argsText.Substring(5).Trim();

					string dateValueString = argsText.Split(' ')[0];
					try{
						DateTime dateValue = _SearchHelper.ParseDate(dateValueString);

						searchKeys.Add("SINCE",dateValue);
					}
					catch{
						m_pSocket.SendLine(cmdTag + " NO Invalid SINCE <date> value");
						return;
					}
					
					// Remove date value from argsText
					argsText = argsText.Substring(dateValueString.Length).Trim();
				}
				// SMALLER <n>
				else if(argsText.ToUpper().StartsWith("SMALLER")){
					// Remove SMALLER from argsText
					argsText = argsText.Substring(7).Trim();

					long smallerValue = 0;
					try{
						smallerValue = Convert.ToInt64(argsText.Split(' ')[0]);
					}
					catch{
					}

					searchKeys.Add("SMALLER",smallerValue);

					// Remove flag value from argsText
					argsText = argsText.Substring(smallerValue.ToString().Length).Trim();
				}
				// SUBJECT <string> or {<string>.Length} if charset specified
				else if(argsText.ToUpper().StartsWith("SUBJECT")){
					// Remove SUBJECT from argsText
					argsText = argsText.Substring(7).Trim();

					string paramValue = "";
					if(!argsText.StartsWith("{")){
						try{
							paramValue = IMAP_Utils.ParseQuotedParam(ref argsText);
						}
						catch(Exception x){
							m_pSocket.SendLine(cmdTag + " NO Invalid SUBJECT <string> value: " + x.Message);
							return;
						}
					}
					else{
						// get string length, it must be between {}
						long count = 0;
						if(argsText.StartsWith("{") && argsText.IndexOf('}',1) > -1){
							try{
								count = Convert.ToInt64(argsText.Substring(1,argsText.IndexOf('}',1) - 1));
							}
							catch{
								m_pSocket.SendLine(cmdTag + " NO Invalid SUBJECT {x} value");
								return;
							}
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO Invalid SUBJECT value");
							return;
						}

						// Notify connected client that it can continue sending cmdline
						m_pSocket.SendLine("+ Continue");
		
						// Read string from socket
						MemoryStream ms = new MemoryStream();
						ReadReplyCode result = m_pSocket.ReadData(ms,count,true);
						if(result != ReadReplyCode.Ok){
							throw new Exception(result.ToString());
						}

						paramValue = System.Text.Encoding.GetEncoding(charset).GetString(ms.ToArray());

						// Get next args text
						argsText = m_pSocket.ReadLine();
					}
				
					// Move to HEADER <field-name> <string>
					searchKeys.Add("HEADER",new string[]{"SUBJECT",paramValue});
				}
				// TEXT <string> or {<string>.Length} if charset specified
				else if(argsText.ToUpper().StartsWith("TEXT")){
					// Remove TEXT from argsText
					argsText = argsText.Substring(4).Trim();

					string paramValue = "";
					if(!argsText.StartsWith("{")){
						try{
							paramValue = IMAP_Utils.ParseQuotedParam(ref argsText);
						}
						catch(Exception x){
							m_pSocket.SendLine(cmdTag + " NO Invalid TEXT <string> value: " + x.Message);
							return;
						}
					}
					else{
						// get string length, it must be between {}
						long count = 0;
						if(argsText.StartsWith("{") && argsText.IndexOf('}',1) > -1){
							try{
								count = Convert.ToInt64(argsText.Substring(1,argsText.IndexOf('}',1) - 1));
							}
							catch{
								m_pSocket.SendLine(cmdTag + " NO Invalid TEXT {x} value");
								return;
							}
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO Invalid TEXT value");
							return;
						}

						// Notify connected client that it can continue sending cmdline
						m_pSocket.SendLine("+ Continue");
		
						// Read string from socket
						MemoryStream ms = new MemoryStream();
						ReadReplyCode result = m_pSocket.ReadData(ms,count,true);
						if(result != ReadReplyCode.Ok){
							throw new Exception(result.ToString());
						}

						paramValue = System.Text.Encoding.GetEncoding(charset).GetString(ms.ToArray());

						// Get next args text
						argsText = m_pSocket.ReadLine();
					}

					searchKeys.Add("TEXT",paramValue);
				}
				// TO <string> or {<string>.Length} if charset specified
				else if(argsText.ToUpper().StartsWith("TO")){
					// Remove TO from argsText
					argsText = argsText.Substring(2).Trim();

					string paramValue = "";
					if(!argsText.StartsWith("{")){
						try{
							paramValue = IMAP_Utils.ParseQuotedParam(ref argsText);
						}
						catch(Exception x){
							m_pSocket.SendLine(cmdTag + " NO Invalid TO <string> value: " + x.Message);
							return;
						}
					}
					else{
						// get string lenght, it must be between {}
						long count = 0;
						if(argsText.StartsWith("{") && argsText.IndexOf('}',1) > -1){
							try{
								count = Convert.ToInt64(argsText.Substring(1,argsText.IndexOf('}',1) - 1));
							}
							catch{
								m_pSocket.SendLine(cmdTag + " NO Invalid TO {x} value");
								return;
							}
						}
						else{
							m_pSocket.SendLine(cmdTag + " NO Invalid TO value");
							return;
						}

						// Notify connected client that it can continue sending cmdline
						m_pSocket.SendLine("+ Continue");
		
						// Read string from socket
						MemoryStream ms = new MemoryStream();
						ReadReplyCode result = m_pSocket.ReadData(ms,count,true);
						if(result != ReadReplyCode.Ok){
							throw new Exception(result.ToString());
						}

						paramValue = System.Text.Encoding.GetEncoding(charset).GetString(ms.ToArray());

						// Get next args text
						argsText = m_pSocket.ReadLine();
					}

					// Move to HEADER <field-name> <string>
					searchKeys.Add("HEADER",new string[]{"TO",paramValue});
				}
				// UID <sequence set>
				else if(argsText.ToUpper().StartsWith("UID")){
					// Remove UID from argsText
					argsText = argsText.Substring(3).Trim();

					string uidValue = argsText.Split(' ')[0];

					searchKeys.Add("UID",uidValue);

					// Remove uid value from argsText
					argsText = argsText.Substring(uidValue.Length).Trim();
				}
				// UNANSWERED
				else if(argsText.ToUpper().StartsWith("UNANSWERED")){
					// Remove UNANSWERED from argsText
					argsText = argsText.Substring(10).Trim();

					// Move to UNKEYWORD <flag>
					searchKeys.Add("UNKEYWORD","ANSWERED");
				}
				// UNDELETED
				else if(argsText.ToUpper().StartsWith("UNDELETED")){
					// Remove UNDELETED from argsText
					argsText = argsText.Substring(9).Trim();

					// Move to UNKEYWORD <flag>
					searchKeys.Add("UNKEYWORD","DELETED");
				}
				// UNDRAFT
				else if(argsText.ToUpper().StartsWith("UNDRAFT")){
					// Remove UNDRAFT from argsText
					argsText = argsText.Substring(7).Trim();

					// Move to UNKEYWORD <flag>
					searchKeys.Add("UNKEYWORD","DRAFT");
				}
				// UNFLAGGED
				else if(argsText.ToUpper().StartsWith("UNFLAGGED")){
					// Remove UNFLAGGED from argsText
					argsText = argsText.Substring(9).Trim();

					// Move to UNKEYWORD <flag>
					searchKeys.Add("UNKEYWORD","FLAGGED");
				}
				// UNKEYWORD <flag>
				else if(argsText.ToUpper().StartsWith("UNKEYWORD")){
				}
				// UNSEEN
				else if(argsText.ToUpper().StartsWith("UNSEEN")){
					// Remove UNSEEN from argsText
					argsText = argsText.Substring(6).Trim();

					// Move to UNKEYWORD <flag>
					searchKeys.Add("UNKEYWORD","SEEN");
				}
				// This is is unkown search key
				else{
					m_pSocket.SendLine(cmdTag + " NO Invalid search key " + argsText.Split(' ')[0]);
					return;
				}
			}

			// Just loop messages headers or full messages (depends on search type)
			string searchResponse = "* SEARCH";
			for(int i=0;i<m_Messages.Count;i++){
				IMAP_Message msg = m_Messages[i];

				byte[]     msgData     = null;
				
				Message_EventArgs eArgs = m_pServer.OnGetMessage(this,msg,false);
				msgData = eArgs.MessageData;

				LumiSoft.Net.Mime.Mime parser = LumiSoft.Net.Mime.Mime.Parse(msgData);
				
				bool matches = true;
				foreach(DictionaryEntry ent in searchKeys){
					matches = _SearchHelper.MatchSearchKey(ent.Key.ToString(),ent.Value,msg,parser);
					if(!matches){
						matches = false;
						break;
					}
				}
				if(!matches){
					continue;
				}

				// If we reached so far, then message matches search criteria
				if(uidSearch){
					searchResponse += " " + msg.MessageUID.ToString();
				}
				else{
					searchResponse += " " + msg.MessageNo.ToString();
				}
			}
			
			searchResponse += "\r\n";
			searchResponse += cmdTag + " OK SEARCH completed\r\n";

			m_pSocket.SendData(searchResponse);
		}

		#endregion

		#region function Fetch

		private void Fetch(string cmdTag,string argsText,bool uidFetch)
		{
			/* Rfc 3501 6.4.5 FETCH Command
			
				Arguments:  message set
							message data item names

				Responses:  untagged responses: FETCH

				Result:     OK - fetch completed
							NO - fetch error: can't fetch that data
							BAD - command unknown or arguments invalid

				The FETCH command retrieves data associated with a message in the
				mailbox.  The data items to be fetched can be either a single atom
				or a parenthesized list.
				
			Most data items, identified in the formal syntax under the
			msg-att-static rule, are static and MUST NOT change for any
			particular message.  Other data items, identified in the formal
			syntax under the msg-att-dynamic rule, MAY change, either as a
			result of a STORE command or due to external events.

				For example, if a client receives an ENVELOPE for a
				message when it already knows the envelope, it can
				safely ignore the newly transmitted envelope.

			There are three macros which specify commonly-used sets of data
			items, and can be used instead of data items.  A macro must be
			used by itself, and not in conjunction with other macros or data
			items.
			
			ALL
				Macro equivalent to: (FLAGS INTERNALDATE RFC822.SIZE ENVELOPE)

			FAST
				Macro equivalent to: (FLAGS INTERNALDATE RFC822.SIZE)

			FULL
				Macro equivalent to: (FLAGS INTERNALDATE RFC822.SIZE ENVELOPE
				BODY)

			The currently defined data items that can be fetched are:

			BODY
				Non-extensible form of BODYSTRUCTURE.

			BODY[<section>]<<partial>>
				The text of a particular body section.  The section
				specification is a set of zero or more part specifiers
				delimited by periods.  A part specifier is either a part number
				or one of the following: HEADER, HEADER.FIELDS,
				HEADER.FIELDS.NOT, MIME, and TEXT.  An empty section
				specification refers to the entire message, including the
				header.

				Every message has at least one part number.  Non-[MIME-IMB]
				messages, and non-multipart [MIME-IMB] messages with no
				encapsulated message, only have a part 1.

				Multipart messages are assigned consecutive part numbers, as
				they occur in the message.  If a particular part is of type
				message or multipart, its parts MUST be indicated by a period
				followed by the part number within that nested multipart part.

				A part of type MESSAGE/RFC822 also has nested part numbers,
				referring to parts of the MESSAGE part's body.

				The HEADER, HEADER.FIELDS, HEADER.FIELDS.NOT, and TEXT part
				specifiers can be the sole part specifier or can be prefixed by
				one or more numeric part specifiers, provided that the numeric
				part specifier refers to a part of type MESSAGE/RFC822.  The
				MIME part specifier MUST be prefixed by one or more numeric
				part specifiers.

				The HEADER, HEADER.FIELDS, and HEADER.FIELDS.NOT part
				specifiers refer to the [RFC-2822] header of the message or of
				an encapsulated [MIME-IMT] MESSAGE/RFC822 message.
				HEADER.FIELDS and HEADER.FIELDS.NOT are followed by a list of
				field-name (as defined in [RFC-2822]) names, and return a
				subset of the header.  The subset returned by HEADER.FIELDS
				contains only those header fields with a field-name that
				matches one of the names in the list; similarly, the subset
				returned by HEADER.FIELDS.NOT contains only the header fields
				with a non-matching field-name.  The field-matching is
				case-insensitive but otherwise exact.  Subsetting does not
				exclude the [RFC-2822] delimiting blank line between the header
				and the body; the blank line is included in all header fetches,
				except in the case of a message which has no body and no blank
				line.

				The MIME part specifier refers to the [MIME-IMB] header for
				this part.

				The TEXT part specifier refers to the text body of the message,
				omitting the [RFC-2822] header.

					Here is an example of a complex message with some of its
					part specifiers:

					HEADER     ([RFC-2822] header of the message)
					TEXT       ([RFC-2822] text body of the message) MULTIPART/MIXED
					1          TEXT/PLAIN
					2          APPLICATION/OCTET-STREAM
					3          MESSAGE/RFC822
					3.HEADER   ([RFC-2822] header of the message)
					3.TEXT     ([RFC-2822] text body of the message) MULTIPART/MIXED
					3.1        TEXT/PLAIN
					3.2        APPLICATION/OCTET-STREAM
					4          MULTIPART/MIXED
					4.1        IMAGE/GIF
					4.1.MIME   ([MIME-IMB] header for the IMAGE/GIF)
					4.2        MESSAGE/RFC822
					4.2.HEADER ([RFC-2822] header of the message)
					4.2.TEXT   ([RFC-2822] text body of the message) MULTIPART/MIXED
					4.2.1      TEXT/PLAIN
					4.2.2      MULTIPART/ALTERNATIVE
					4.2.2.1    TEXT/PLAIN
					4.2.2.2    TEXT/RICHTEXT


				It is possible to fetch a substring of the designated text.
				This is done by appending an open angle bracket ("<"), the
				octet position of the first desired octet, a period, the
				maximum number of octets desired, and a close angle bracket
				(">") to the part specifier.  If the starting octet is beyond
				the end of the text, an empty string is returned.
				
				Any partial fetch that attempts to read beyond the end of the
				text is truncated as appropriate.  A partial fetch that starts
				at octet 0 is returned as a partial fetch, even if this
				truncation happened.

					Note: This means that BODY[]<0.2048> of a 1500-octet message
					will return BODY[]<0> with a literal of size 1500, not
					BODY[].

					Note: A substring fetch of a HEADER.FIELDS or
					HEADER.FIELDS.NOT part specifier is calculated after
					subsetting the header.

				The \Seen flag is implicitly set; if this causes the flags to
				change, they SHOULD be included as part of the FETCH responses.

			BODY.PEEK[<section>]<<partial>>
				An alternate form of BODY[<section>] that does not implicitly
				set the \Seen flag.

			BODYSTRUCTURE
				The [MIME-IMB] body structure of the message.  This is computed
				by the server by parsing the [MIME-IMB] header fields in the
				[RFC-2822] header and [MIME-IMB] headers.

			ENVELOPE
				The envelope structure of the message.  This is computed by the
				server by parsing the [RFC-2822] header into the component
				parts, defaulting various fields as necessary.

			FLAGS
				The flags that are set for this message.

			INTERNALDATE
				The internal date of the message.

			RFC822
				Functionally equivalent to BODY[], differing in the syntax of
				the resulting untagged FETCH data (RFC822 is returned).

			RFC822.HEADER
				Functionally equivalent to BODY.PEEK[HEADER], differing in the
				syntax of the resulting untagged FETCH data (RFC822.HEADER is
				returned).

			RFC822.SIZE
				The [RFC-2822] size of the message.
				
			RFC822.TEXT
				Functionally equivalent to BODY[TEXT], differing in the syntax
				of the resulting untagged FETCH data (RFC822.TEXT is returned).

			UID
				The unique identifier for the message.


			Example:    C: A654 FETCH 2:4 (FLAGS BODY[HEADER.FIELDS (DATE FROM)])
						S: * 2 FETCH ....
						S: * 3 FETCH ....
						S: * 4 FETCH ....
						S: A654 OK FETCH completed
	  
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}
			if(m_SelectedMailbox.Length == 0){
				m_pSocket.SendLine(cmdTag + " NO Select mailbox first !");
				return;
			}

			#region Parse parameters

			string[] args = ParseParams(argsText);		
			if(args.Length != 2){
				m_pSocket.SendLine(cmdTag + " BAD Invalid arguments");
				return;
			}

			ArrayList seq_set = ParseMsgNumbersFromSequenceSet(args[0].Trim(),uidFetch);
		
			// Replace macros
			string fetchItems = args[1].ToUpper();
			       fetchItems = fetchItems.Replace("ALL" ,"FLAGS INTERNALDATE RFC822.SIZE ENVELOPE");
				   fetchItems = fetchItems.Replace("FAST","FLAGS INTERNALDATE RFC822.SIZE");
				   fetchItems = fetchItems.Replace("FULL","FLAGS INTERNALDATE RFC822.SIZE ENVELOPE BODY");
		
			// If UID FETCH and no UID, we must implicity add it, it's required 
			if(uidFetch && fetchItems.ToUpper().IndexOf("UID") == -1){
				fetchItems += " UID";
			}

			// ToDo: ??? start parm parsing from left to end in while loop while params parsed or bad param found
			
			bool headersNeeded = false;
			bool fullMsgNeeded = false;

			// Parse,validate requested fetch items
			Hashtable items = new Hashtable();
			if(fetchItems.IndexOf("UID") > -1){
				items.Add("UID","");
				fetchItems = fetchItems.Replace("UID","");
			}
			if(fetchItems.IndexOf("RFC822.TEXT") > -1){
				fullMsgNeeded = true;
				items.Add("RFC822.TEXT","");
				fetchItems = fetchItems.Replace("RFC822.TEXT","");
			}
			if(fetchItems.IndexOf("RFC822.SIZE") > -1){
				items.Add("RFC822.SIZE","");
				fetchItems = fetchItems.Replace("RFC822.SIZE","");
			}
			if(fetchItems.IndexOf("RFC822.HEADER") > -1){
				headersNeeded = true;
				items.Add("RFC822.HEADER","");
				fetchItems = fetchItems.Replace("RFC822.HEADER","");
			}
			if(fetchItems.IndexOf("RFC822") > -1){
				fullMsgNeeded = true;
				items.Add("RFC822","");
				fetchItems = fetchItems.Replace("RFC822","");
			}
			if(fetchItems.IndexOf("INTERNALDATE") > -1){
				items.Add("INTERNALDATE","");
				fetchItems = fetchItems.Replace("INTERNALDATE","");
			}
			if(fetchItems.IndexOf("FLAGS") > -1){
				items.Add("FLAGS","");
				fetchItems = fetchItems.Replace("FLAGS","");
			}
			if(fetchItems.IndexOf("ENVELOPE") > -1){
				headersNeeded = true;
				items.Add("ENVELOPE","");
				fetchItems = fetchItems.Replace("ENVELOPE","");
			}
			if(fetchItems.IndexOf("BODYSTRUCTURE") > -1){
				fullMsgNeeded = true;
				items.Add("BODYSTRUCTURE","");
				fetchItems = fetchItems.Replace("BODYSTRUCTURE","");
			}
			if(fetchItems.IndexOf("BODY.PEEK[") > -1){
				int start = fetchItems.IndexOf("BODY.PEEK[") + 10;
				string val = fetchItems.Substring(start,fetchItems.IndexOf("]",start) - start).ToUpper().Trim();

				// Remove BODY.PEEK[...] from remaining args string.
				fetchItems = fetchItems.Substring(fetchItems.IndexOf("]") + 1);
				
				//--- See if partial fetch. For example: BODY.PEEK[]<0.100>
				long startPosition = 0;
				long maxLength     = long.MaxValue;
				bool partial       = false;
				if(fetchItems.StartsWith("<")){
					// We got partial fetch, need to get data between <>
					string partialArgs = fetchItems.Substring(1,fetchItems.IndexOf(">") - 1);
					string[] pArgs = partialArgs.Split('.');
					startPosition = Convert.ToInt64(pArgs[0]);
					maxLength     = Convert.ToInt64(pArgs[1]);

					// Remove partial fetch args(<...>) from remaining args string.
					fetchItems = fetchItems.Substring(fetchItems.IndexOf(">") + 1);

					partial = true;
				}
				//----------------------------------------------------------------

				// We must support only:
				// ""                   - full message
				// TEXT                 - message text
				// HEADER               - message header
				// HEADER.FIELDS        - requested message header fields
				// HEADER.FIELDS.NOT    - message header fields except requested
				// number of mime entry - Example: BODY[1];BODY[1.1];BODY[1.1.x. ...]

				if(val.Length > 0){
					string[] fArgs = ParseParams(val);
					
					// Specified number of mime entry requested
			//		if(fArgs.Length == 1 && Core.IsNumber(fArgs[0])){
			//			fullMsgNeeded = true;
			//			items.Add("BODY.PEEK[NUMBER]",new object[]{partial,startPosition,maxLength,Convert.ToInt32(fArgs[0])});
			//		}
			//		else{
						switch(fArgs[0].ToUpper())
						{
							case "TEXT":
								fullMsgNeeded = true;
								items.Add("BODY.PEEK[TEXT]",new object[]{partial,startPosition,maxLength});
								break;

							case "HEADER":
								headersNeeded = true;
								items.Add("BODY.PEEK[HEADER]",new object[]{partial,startPosition,maxLength});
								break;

							case "HEADER.FIELDS":
								if(fArgs.Length == 2){
									headersNeeded = true;
									items.Add("BODY.PEEK[HEADER.FIELDS]",new object[]{partial,startPosition,maxLength,fArgs[1]});
								}
								
								break;

							case "HEADER.FIELDS.NOT":
								if(fArgs.Length == 2){
									headersNeeded = true;
									items.Add("BODY.PEEK[HEADER.FIELDS.NOT]",new object[]{partial,startPosition,maxLength,fArgs[1]});
								}
								break;

							default:
								// This must be number of mime entry
								fullMsgNeeded = true;
								items.Add("BODY.PEEK[NUMBER]",new object[]{partial,startPosition,maxLength,fArgs[0]});
								break;

							//	m_pSocket.SendLine(cmdTag + " BAD Invalid fetch-items argument");
							//	return;
						}
			//		}
				}
				else{
					fullMsgNeeded = true;
					items.Add("BODY.PEEK[]",new object[]{partial,startPosition,maxLength});
				}				
			}
			// BODY[<section>]<<partial>>
			if(fetchItems.IndexOf("BODY[") > -1){
				int start = fetchItems.IndexOf("BODY[") + 5;
				string val = fetchItems.Substring(start,fetchItems.IndexOf("]",start) - start).ToUpper().Trim();
				
				// Remove BODY[...] from remaining args string.
				fetchItems = fetchItems.Substring(fetchItems.IndexOf("]") + 1);
				
				//--- See if partial fetch. For example: BODY[]<0.100>
				long startPosition = 0;
				long maxLength     = long.MaxValue;
				bool partial       = false;
				if(fetchItems.StartsWith("<")){
					// We got partial fetch, need to get data between <>
					string partialArgs = fetchItems.Substring(1,fetchItems.IndexOf(">") - 1);
					string[] pArgs = partialArgs.Split('.');
					startPosition = Convert.ToInt64(pArgs[0]);
					maxLength     = Convert.ToInt64(pArgs[1]);

					// Remove partial fetch args(<...>) from remaining args string.
					fetchItems = fetchItems.Substring(fetchItems.IndexOf(">") + 1);

					partial = true;
				}
				//----------------------------------------------------------------

				// We must support only:
				// ""                   - full message
				// TEXT                 - message text
				// HEADER               - message header
				// HEADER.FIELDS        - requested message header fields
				// HEADER.FIELDS.NOT    - message header fields except requested
				// number of mime entry - Example: BODY[1];BODY[1.1];BODY[1.1.x. ...]

				if(val.Length > 0){
					string[] fArgs = ParseParams(val);
				
					// Specified number of mime entry requested
			//		if(fArgs.Length == 1 && Core.IsNumber(fArgs[0])){
			//			fullMsgNeeded = true;
			//			items.Add("BODY[NUMBER]",new object[]{partial,startPosition,maxLength,Convert.ToInt32(fArgs[0])});
			//		}
			//		else{
						switch(fArgs[0].ToUpper())
						{
							case "TEXT":
								fullMsgNeeded = true;
								items.Add("BODY[TEXT]",new object[]{partial,startPosition,maxLength});
								break;

							case "HEADER":
								headersNeeded = true;
								items.Add("BODY[HEADER]",new object[]{partial,startPosition,maxLength});
								break;

							case "HEADER.FIELDS":
								if(fArgs.Length == 2){
									headersNeeded = true;
									items.Add("BODY[HEADER.FIELDS]",new object[]{partial,startPosition,maxLength,fArgs[1]});
								}								
								break;

							case "HEADER.FIELDS.NOT":
								if(fArgs.Length == 2){
									headersNeeded = true;
									items.Add("BODY[HEADER.FIELDS.NOT]",new object[]{partial,startPosition,maxLength,fArgs[1]});
								}
								break;

							default:
								// This must be number of mime entry
								fullMsgNeeded = true;
								items.Add("BODY[NUMBER]",new object[]{partial,startPosition,maxLength,fArgs[0]});
								break;

							//	m_pSocket.SendLine(cmdTag + " BAD Invalid fetch-items argument");
							//	return;
						}
			//		}
				}
				else{
					fullMsgNeeded = true;
					items.Add("BODY[]",new object[]{partial,startPosition,maxLength});
				}
			}
			if(fetchItems.IndexOf("BODY") > -1){
				fullMsgNeeded = true;
				items.Add("BODY","");
				fetchItems = fetchItems.Replace("BODY","");
			}

			// If length != 0, then contains invalid fetch items
			if(fetchItems.Trim().Length > 0){
				m_pSocket.SendLine(cmdTag + " BAD Invalid fetch-items argument");
				return;
			}

			#endregion

			// ToDo: 
			// The server should respond with a tagged BAD response to a command that uses a message
            // sequence number greater than the number of messages in the selected mailbox.  This
            // includes "*" if the selected mailbox is empty.
		//	if(m_Messages.Count == 0 || ){
		//		SendData(cmdTag + " BAD Sequence number greater than the number of messages in the selected mailbox !\r\n");
		//		return;
		//	}

			// ToDo: Move to all parts to MimeParse where possible, this avoid multiple decodings

			for(int i=0;i<m_Messages.Count;i++){
				//
				if(seq_set.Contains(i + 1)){
					IMAP_Message msg = m_Messages[i];
				
					byte[] buf = null;
					MemoryStream reply = new MemoryStream();
					// Write fetch start data "* msgNo FETCH ("
					buf = Encoding.ASCII.GetBytes("* " + msg.MessageNo + " FETCH (");
					reply.Write(buf,0,buf.Length);

					byte[]     msgData     = null;
					byte[]     msgHeadData = null;
					LumiSoft.Net.Mime.Mime parser = null;
					// Check if header or data is neccessary. Eg. if only UID wanted, don't get message at all.
					if(fullMsgNeeded || headersNeeded){
						Message_EventArgs eArgs = m_pServer.OnGetMessage(this,msg,!fullMsgNeeded);
						msgData = eArgs.MessageData;

						// Headers needed parse headers from msgData
						// Is that needed ???
						if(headersNeeded){							
							string headers = MimeUtils.ParseHeaders(new MemoryStream(msgData));
							msgHeadData = Encoding.Default.GetBytes(headers + "\r\n"); // blank line is included in all header fetches
						}

						parser = LumiSoft.Net.Mime.Mime.Parse(msgData);
					}
					
					IMAP_MessageFlags msgFlagsOr = msg.Flags;
					// Construct reply here, based on requested fetch items
					int nCount = 0;
					foreach(string fetchItem in items.Keys){
						object[] partArgs      = null;
						bool     partial       = false;
						long     startPosition = 0;
						long     maxLength     = 0;
						long     lengthToSend  = 0;
						string   partNumber    = "";
						string   headerFields  = "";

						switch(fetchItem)
						{
							case "UID":
								buf = Encoding.ASCII.GetBytes("UID " + msg.MessageUID);
								reply.Write(buf,0,buf.Length);
								break;

							case "RFC822.TEXT":
								// Sets \seen flag
								msg.SetFlags(msg.Flags | IMAP_MessageFlags.Seen);
								
								// RFC822.TEXT {size}
								// msg text
								byte[] f11Data = System.Text.Encoding.ASCII.GetBytes(parser.BodyText);
									
								buf = Encoding.ASCII.GetBytes("RFC822.TEXT {" + f11Data.Length + "}\r\n");
								reply.Write(buf,0,buf.Length);

								reply.Write(f11Data,0,f11Data.Length);
								break;

							case "RFC822.SIZE":
								// "RFC822.SIZE size
								buf = Encoding.ASCII.GetBytes("RFC822.SIZE " + msg.Size);
								reply.Write(buf,0,buf.Length);
								break;

							case "RFC822.HEADER":
								// RFC822.HEADER {size}
								// msg header data							
								buf = Encoding.ASCII.GetBytes("RFC822.HEADER {" + msgHeadData.Length + "}\r\n");
								reply.Write(buf,0,buf.Length);
								reply.Write(msgHeadData,0,msgHeadData.Length);								
								break;

							case "RFC822":
								// Sets \seen flag
								msg.SetFlags(msg.Flags | IMAP_MessageFlags.Seen);

								// RFC822 {size}
								// msg data
								buf = Encoding.ASCII.GetBytes("RFC822 {" + msgData.Length + "}\r\n");
								reply.Write(buf,0,buf.Length);
								reply.Write(msgData,0,msgData.Length);
								break;

							case "INTERNALDATE":
								// INTERNALDATE "date"
								buf = Encoding.ASCII.GetBytes("INTERNALDATE \"" + msg.Date.ToString("r",System.Globalization.DateTimeFormatInfo.InvariantInfo) + "\"");
								reply.Write(buf,0,buf.Length);														
								break;

							case "FLAGS":
								buf = Encoding.ASCII.GetBytes("FLAGS (" + msg.FlagsToString() + ")");
								reply.Write(buf,0,buf.Length);
								break;

							case "ENVELOPE":
								buf = Encoding.ASCII.GetBytes(FetchHelper.ConstructEnvelope(parser));
								reply.Write(buf,0,buf.Length);
								break;

							case "BODYSTRUCTURE":
								// BODYSTRUCTURE ()
								buf = Encoding.ASCII.GetBytes(FetchHelper.ConstructBodyStructure(parser,true));
								reply.Write(buf,0,buf.Length);
								break;

							case "BODY.PEEK[]":
								// BODY[] {size}
								// msg header data
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								
								lengthToSend = msgData.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}
								if(lengthToSend < 0){
									lengthToSend = 0;
								}

								if(partial){
									buf = Encoding.ASCII.GetBytes("BODY[]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(msgData,(int)startPosition,(int)lengthToSend);
								break;

							case "BODY.PEEK[HEADER]":
								// BODY[HEADER] {size}
								// msg header data
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								
								lengthToSend = msgHeadData.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}
								if(lengthToSend < 0){
									lengthToSend = 0;
								}

								if(partial){
									buf = Encoding.ASCII.GetBytes("BODY[HEADER]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[HEADER] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(msgHeadData,(int)startPosition,(int)lengthToSend);
								break;

							case "BODY.PEEK[HEADER.FIELDS]":
								// BODY[HEADER.FIELDS ()] {size}
								// msg header data
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								headerFields  = (string)partArgs[3];

								byte[] fData = Encoding.ASCII.GetBytes(FetchHelper.ParseHeaderFields(headerFields,msgHeadData));

								lengthToSend = fData.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}
								if(lengthToSend < 0){
									lengthToSend = 0;
								}
								
								if(partial){
									buf = Encoding.ASCII.GetBytes("BODY[HEADER.FIELDS (" + headerFields + ")]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[HEADER.FIELDS (" + headerFields + ")] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(fData,(int)startPosition,(int)lengthToSend);														
								break;

							case "BODY.PEEK[HEADER.FIELDS.NOT]":
								// BODY[HEADER.FIELDS.NOT ()] {size}
								// msg header data
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								headerFields  = (string)partArgs[3];

								byte[] f1Data = Encoding.ASCII.GetBytes(FetchHelper.ParseHeaderFieldsNot(headerFields,msgHeadData));

								lengthToSend = f1Data.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}
								if(lengthToSend < 0){
									lengthToSend = 0;
								}
								
								if(partial){
									buf = Encoding.ASCII.GetBytes("BODY[HEADER.FIELDS.NOT (" + headerFields + ")]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[HEADER.FIELDS.NOT (" + headerFields + ")] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(f1Data,(int)startPosition,(int)lengthToSend);
								break;

							case "BODY.PEEK[TEXT]":
								// BODY[TEXT] {size}
								// msg text
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];

								byte[] f111Data = Encoding.ASCII.GetBytes(parser.BodyText);

								lengthToSend = f111Data.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}
								if(lengthToSend < 0){
									lengthToSend = 0;
								}
								
								if(partial){	
									buf = Encoding.ASCII.GetBytes("BODY[TEXT]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[TEXT] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(f111Data,(int)startPosition,(int)lengthToSend);
								break;

							case "BODY.PEEK[NUMBER]":
								// BODY[no.] {size}
								// mime part
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								partNumber    = partArgs[3].ToString();

								byte[] b1113Data = FetchHelper.ParseMimeEntry(parser,partNumber);
																									
								if(b1113Data != null){
									lengthToSend = b1113Data.Length - startPosition;
									if(lengthToSend > maxLength){
										lengthToSend = maxLength;
									}
									if(lengthToSend < 0){
										lengthToSend = 0;
									}
									
									if(partial){
										buf = Encoding.ASCII.GetBytes("BODY[" + partNumber + "]<" + startPosition + "> {" + lengthToSend + "}\r\n");
									}
									else{
										buf = Encoding.ASCII.GetBytes("BODY[" + partNumber + "] {" + lengthToSend + "}\r\n");
									}
									reply.Write(buf,0,buf.Length);
									reply.Write(b1113Data,(int)startPosition,(int)lengthToSend);
								}
								else{
									// BODY[no.] NIL
									buf = Encoding.ASCII.GetBytes("BODY[" + partNumber + "] NIL");
									reply.Write(buf,0,buf.Length);
								}
								break;

							case "BODY[]":
								// Sets \seen flag
								msg.SetFlags(msg.Flags | IMAP_MessageFlags.Seen);

								// BODY[] {size}
								// msg header data
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								
								lengthToSend = msgData.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}
								if(lengthToSend < 0){
									lengthToSend = 0;
								}

								if(partial){
									buf = Encoding.ASCII.GetBytes("BODY[]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(msgData,(int)startPosition,(int)lengthToSend);							
								break;

							case "BODY[HEADER]":
								// Sets \seen flag
								msg.SetFlags(msg.Flags | IMAP_MessageFlags.Seen);

								// BODY[HEADER] {size}
								// msg header data
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								
								lengthToSend = msgHeadData.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}
								if(lengthToSend < 0){
									lengthToSend = 0;
								}

								if(partial){
									buf = Encoding.ASCII.GetBytes("BODY[HEADER]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[HEADER] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(msgHeadData,(int)startPosition,(int)lengthToSend);
								break;

							case "BODY[HEADER.FIELDS]":
								// Sets \seen flag
								msg.SetFlags(msg.Flags | IMAP_MessageFlags.Seen);

								// BODY[HEADER.FIELDS ()] {size}
								// msg header data
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								headerFields  = (string)partArgs[3];

								byte[] bData = Encoding.ASCII.GetBytes(FetchHelper.ParseHeaderFields(headerFields,msgHeadData));

								lengthToSend = bData.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}
								if(lengthToSend < 0){
									lengthToSend = 0;
								}
								
								if(partial){
									buf = Encoding.ASCII.GetBytes("BODY[HEADER.FIELDS (" + headerFields + ")]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[HEADER.FIELDS (" + headerFields + ")] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(bData,(int)startPosition,(int)lengthToSend);
								break;

							case "BODY[HEADER.FIELDS.NOT]":
								// Sets \seen flag
								msg.SetFlags(msg.Flags | IMAP_MessageFlags.Seen);

								// BODY[HEADER.FIELDS.NOT ()] {size}
								// msg header data
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								headerFields  = (string)partArgs[3];

								byte[] f2Data = Encoding.ASCII.GetBytes(FetchHelper.ParseHeaderFieldsNot(headerFields,msgHeadData));

								lengthToSend = f2Data.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}
								if(lengthToSend < 0){
									lengthToSend = 0;
								}
									
								if(partial){
									buf = Encoding.ASCII.GetBytes("BODY[HEADER.FIELDS.NOT (" + headerFields + ")]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[HEADER.FIELDS.NOT (" + headerFields + ")] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(f2Data,(int)startPosition,(int)lengthToSend);
								break;

							case "BODY[TEXT]":
								// Sets \seen flag
								msg.SetFlags(msg.Flags | IMAP_MessageFlags.Seen);

								// BODY[TEXT] {size}
								// msg text									
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];

								byte[] f1111Data = Encoding.ASCII.GetBytes(parser.BodyText);

								lengthToSend = f1111Data.Length - startPosition;
								if(lengthToSend > maxLength){
									lengthToSend = maxLength;
								}

								if(partial){
									buf = Encoding.ASCII.GetBytes("BODY[TEXT]<" + startPosition + "> {" + lengthToSend + "}\r\n");
								}
								else{
									buf = Encoding.ASCII.GetBytes("BODY[TEXT] {" + lengthToSend + "}\r\n");
								}
								reply.Write(buf,0,buf.Length);
								reply.Write(f1111Data,(int)startPosition,(int)lengthToSend);
								break;

							case "BODY[NUMBER]":
								// Sets \seen flag
								msg.SetFlags(msg.Flags | IMAP_MessageFlags.Seen);

								// BODY[no.] {size}
								// mime part
								partArgs = (object[])items[fetchItem];
								partial       = (bool)partArgs[0];
								startPosition = (long)partArgs[1];
								maxLength     = (long)partArgs[2];
								partNumber    = partArgs[3].ToString();

								byte[] b113Data = FetchHelper.ParseMimeEntry(parser,partNumber);
																									
								if(b113Data != null){
									lengthToSend = b113Data.Length - startPosition;
									if(lengthToSend > maxLength){
										lengthToSend = maxLength;
									}
									if(lengthToSend < 0){
										lengthToSend = 0;
									}

									if(partial){
										buf = Encoding.ASCII.GetBytes("BODY[" + partNumber + "]<" + startPosition + "> {" + lengthToSend + "}\r\n");
									}
									else{
										buf = Encoding.ASCII.GetBytes("BODY[" + partNumber + "] {" + lengthToSend + "}\r\n");
									}
									reply.Write(buf,0,buf.Length);
									reply.Write(b113Data,(int)startPosition,(int)lengthToSend);
								}
								else{									
									// BODY[no.] NIL
									buf = Encoding.ASCII.GetBytes("BODY[" + partNumber + "] NIL");
									reply.Write(buf,0,buf.Length);
								}
								break;

							case "BODY":
								// Sets \seen flag

								// BODY ()
								buf = Encoding.ASCII.GetBytes(FetchHelper.ConstructBodyStructure(parser,false));
								reply.Write(buf,0,buf.Length);
								break;
						}

						nCount++;

						// Write fetch item separator data " "
						// We don't write it for last item
						if(nCount < items.Count){						
							buf = Encoding.ASCII.GetBytes(" ");
							reply.Write(buf,0,buf.Length);
						}
					}

					// Write fetch end data ")"
					buf = Encoding.ASCII.GetBytes(")\r\n");
					reply.Write(buf,0,buf.Length);

					// Send fetch reply to client
					reply.Position = 0;
					m_pSocket.SendData(reply);


					// Set message flags here if required or changed
					if(((int)IMAP_MessageFlags.Recent & (int)msg.Flags) != 0 || msgFlagsOr != msg.Flags){
						msg.SetFlags(msg.Flags & ~IMAP_MessageFlags.Recent);

						m_pServer.OnStoreMessageFlags(this,msg);
					}
				}
				
			}

			m_pSocket.SendLine(cmdTag + " OK FETCH completed");
		}

		#endregion

		#region function Store

		private void Store(string cmdTag,string argsText,bool uidStore)
		{
			/* Rfc 3501 6.4.6 STORE Command
				
				Arguments:  message set
							message data item name
							value for message data item

				Responses:  untagged responses: FETCH

				Result:     OK - store completed
							NO - store error: can't store that data
							BAD - command unknown or arguments invalid
							
				The STORE command alters data associated with a message in the
				mailbox.  Normally, STORE will return the updated value of the
				data with an untagged FETCH response.  A suffix of ".SILENT" in
				the data item name prevents the untagged FETCH, and the server
				SHOULD assume that the client has determined the updated value
				itself or does not care about the updated value.
				
				Note: regardless of whether or not the ".SILENT" suffix was
					used, the server SHOULD send an untagged FETCH response if a
					change to a message's flags from an external source is
					observed.  The intent is that the status of the flags is
					determinate without a race condition.

				The currently defined data items that can be stored are:

				FLAGS <flag list>
					Replace the flags for the message (other than \Recent) with the
					argument.  The new value of the flags is returned as if a FETCH
					of those flags was done.

				FLAGS.SILENT <flag list>
					Equivalent to FLAGS, but without returning a new value.

				+FLAGS <flag list>
					Add the argument to the flags for the message.  The new value
					of the flags is returned as if a FETCH of those flags was done.

				+FLAGS.SILENT <flag list>
					Equivalent to +FLAGS, but without returning a new value.

				-FLAGS <flag list>
					Remove the argument from the flags for the message.  The new
					value of the flags is returned as if a FETCH of those flags was
					done.

				-FLAGS.SILENT <flag list>
					Equivalent to -FLAGS, but without returning a new value.
		 

				Example:    C: A003 STORE 2:4 +FLAGS (\Deleted)
							S: * 2 FETCH FLAGS (\Deleted \Seen)
							S: * 3 FETCH FLAGS (\Deleted)
							S: * 4 FETCH FLAGS (\Deleted \Flagged \Seen)
							S: A003 OK STORE completed
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}
			if(m_SelectedMailbox.Length == 0){
				m_pSocket.SendLine(cmdTag + " NO Select mailbox first !");
				return;
			}
			if(m_Messages.ReadOnly){
				m_pSocket.SendLine(cmdTag + " NO Mailbox is read-only");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 3){
				m_pSocket.SendLine(cmdTag + " BAD STORE invalid arguments");
				return;
			}

			ArrayList seq_set = ParseMsgNumbersFromSequenceSet(args[0].Trim(),uidStore);

			//--- Parse Flags behaviour ---------------//
			string flagsAction = "";
			bool   silent      = false;
			string flagsType = args[1].ToUpper();
			switch(flagsType)
			{
				case "FLAGS":
					flagsAction = "REPLACE";
					break;

				case "FLAGS.SILENT":
					flagsAction = "REPLACE";
					silent = true;
					break;

				case "+FLAGS":
					flagsAction = "ADD";
					break;

				case "+FLAGS.SILENT":
					flagsAction = "ADD";
					silent = true;
					break;

				case "-FLAGS":
					flagsAction = "REMOVE";
					break;

				case "-FLAGS.SILENT":
					flagsAction = "REMOVE";	
					silent = true;
					break;

				default:
					m_pSocket.SendLine(cmdTag + " BAD arguments invalid");
					return;
			}
			//-------------------------------------------//

			//--- Parse flags, see if valid ----------------
			string flags = args[2].ToUpper();
			if(flags.Replace("\\ANSWERED","").Replace("\\FLAGGED","").Replace("\\DELETED","").Replace("\\SEEN","").Replace("\\DRAFT","").Trim().Length > 0){
				m_pSocket.SendLine(cmdTag + " BAD arguments invalid");
				return;
			}

			IMAP_MessageFlags mFlags = ParseMessageFalgs(flags);

			// Call OnStoreMessageFlags for each message in sequence set
			// Calulate new flags(using old message flags + new flags) for message 
			// and request to store all flags to message, don't specify if add, remove or replace falgs.
			
			for(int i=0;i<m_Messages.Count;i++){
				//
				if(seq_set.Contains(i + 1)){
					IMAP_Message msg = m_Messages[i];
					
					// Calculate new flags and set to msg
					switch(flagsAction)
					{
						case "REPLACE":
							msg.SetFlags(mFlags);
							break;

						case "ADD":
							msg.SetFlags(msg.Flags | mFlags);
							break;

						case "REMOVE":
							msg.SetFlags(msg.Flags & ~mFlags);
							break;
					}

					// ToDo: see if flags changed, if not don't call OnStoreMessageFlags

					string errorText = m_pServer.OnStoreMessageFlags(this,msg);
					if(errorText == null){
						if(!silent){ // Silent doesn't reply untagged lines
							if(!uidStore){
								m_pSocket.SendLine("* " + (i + 1) + " FETCH FLAGS (" + msg.FlagsToString() + ")");
							}
							// Called from UID command, need to add UID response
							else{
								m_pSocket.SendLine("* " + (i + 1) + " FETCH (FLAGS (" + msg.FlagsToString() + ") UID " + msg.MessageUID + "))");
							}
						}
					}
					else{
						m_pSocket.SendLine(cmdTag + " NO " + errorText);
						return;
					}
				}
			}

			m_pSocket.SendLine(cmdTag + " OK STORE completed");
		}

		#endregion

		#region function Copy

		private void Copy(string cmdTag,string argsText,bool uidCopy)
		{
			/* RFC 3501 6.4.7 COPY Command
			
				Arguments:  message set
							mailbox name

				Responses:  no specific responses for this command

				Result:     OK - copy completed
							NO - copy error: can't copy those messages or to that
									name
							BAD - command unknown or arguments invalid
			   
				The COPY command copies the specified message(s) to the end of the
				specified destination mailbox.  The flags and internal date of the
				message(s) SHOULD be preserved in the copy.

				If the destination mailbox does not exist, a server SHOULD return
				an error.  It SHOULD NOT automatically create the mailbox.  Unless
				it is certain that the destination mailbox can not be created, the
				server MUST send the response code "[TRYCREATE]" as the prefix of
				the text of the tagged NO response.  This gives a hint to the
				client that it can attempt a CREATE command and retry the COPY if
				
				If the COPY command is unsuccessful for any reason, server
				implementations MUST restore the destination mailbox to its state
				before the COPY attempt.

				Example:    C: A003 COPY 2:4 MEETING
							S: A003 OK COPY completed
			   
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}
			if(m_SelectedMailbox.Length == 0){
				m_pSocket.SendLine(cmdTag + " NO Select mailbox first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length != 2){
				m_pSocket.SendLine(cmdTag + " BAD Invalid arguments");
				return;
			}

			ArrayList seq_set = ParseMsgNumbersFromSequenceSet(args[0].Trim(),uidCopy);

			string errorText = "";
			for(int i=0;i<m_Messages.Count;i++){
				//
				if(seq_set.Contains(i + 1)){			
					IMAP_Message msg = m_Messages[i];

					errorText = m_pServer.OnCopyMessage(this,msg,args[1]);
					if(errorText != null){
						break; // Errors return error text, don't try to copy other messages
					}
				}
			}

			if(errorText == null){
				m_pSocket.SendLine(cmdTag + " OK COPY completed");
			}
			else{
				m_pSocket.SendLine(cmdTag + " NO " + errorText);
			}
		}

		#endregion

		#region function Uid

		private void Uid(string cmdTag,string argsText)
		{
			/* Rfc 3501 6.4.8 UID Command
				
				Arguments:  command name
							command arguments

				Responses:  untagged responses: FETCH, SEARCH

				Result:     OK - UID command completed
							NO - UID command error
							BAD - command unknown or arguments invalid
							
				The UID command has two forms.  In the first form, it takes as its
				arguments a COPY, FETCH, or STORE command with arguments
				appropriate for the associated command.  However, the numbers in
				the message set argument are unique identifiers instead of message
				sequence numbers.

				In the second form, the UID command takes a SEARCH command with
				SEARCH command arguments.  The interpretation of the arguments is
				the same as with SEARCH; however, the numbers returned in a SEARCH
				response for a UID SEARCH command are unique identifiers instead
				of message sequence numbers.  For example, the command UID SEARCH
				1:100 UID 443:557 returns the unique identifiers corresponding to
				the intersection of the message sequence number set 1:100 and the
				UID set 443:557.

				Message set ranges are permitted; however, there is no guarantee
				that unique identifiers be contiguous.  A non-existent unique
				identifier within a message set range is ignored without any error
				message generated.

				The number after the "*" in an untagged FETCH response is always a
				message sequence number, not a unique identifier, even for a UID
				command response.  However, server implementations MUST implicitly
				include the UID message data item as part of any FETCH response
				caused by a UID command, regardless of whether a UID was specified
				as a message data item to the FETCH.
				
				Example:    C: A999 UID FETCH 4827313:4828442 FLAGS
							S: * 23 FETCH (FLAGS (\Seen) UID 4827313)
							S: * 24 FETCH (FLAGS (\Seen) UID 4827943)
							S: * 25 FETCH (FLAGS (\Seen) UID 4828442)
							S: A999 UID FETCH completed
			*/
			if(!m_Authenticated){
				m_pSocket.SendLine(cmdTag + " NO Authenticate first !");
				return;
			}
			if(m_SelectedMailbox.Length == 0){
				m_pSocket.SendLine(cmdTag + " NO Select mailbox first !");
				return;
			}

			string[] args = ParseParams(argsText);
			if(args.Length < 2){ // We must have at least command and message-set or cmd args
				m_pSocket.SendLine(cmdTag + " BAD Invalid arguments");
				return;
			}

			// Get commands args text, we just remove COMMAND
			string cmdArgs = Core.GetArgsText(argsText,args[0]);

			// See if valid command specified with UID command
			switch(args[0].ToUpper())
			{
				case "COPY":
					Copy(cmdTag,cmdArgs,true);
					break;

				case "FETCH":
					Fetch(cmdTag,cmdArgs,true);
					break;

				case "STORE":
					Store(cmdTag,cmdArgs,true);
					break;

				case "SEARCH":
					Search(cmdTag,cmdArgs,true);
					break;

				default:
					m_pSocket.SendLine(cmdTag + " BAD Invalid arguments");
					return;
			}
			/*

			// See if valid command specified with UID command
			switch(args[0].ToUpper())
			{
				case "COPY":
					break;
				case "FETCH":
					break;
				case "STORE":
					break;
				case "SEARCH":
					break;

				default:
					SendData(cmdTag + " BAD Invalid arguments\r\n");
					return;
			}

			// Get commands args text, we just remove COMMAND
			string cmdArgs = Core.GetArgsText(argsText,args[0]);

			// Do command
			switch(args[0].ToUpper())
			{
				case "COPY":
					Copy(cmdTag,cmdArgs,true);
					break;

				case "FETCH":
					Fetch(cmdTag,cmdArgs,true);
					break;

				case "STORE":
					Store(cmdTag,cmdArgs,true);
					break;

				case "SEARCH":
					Search(cmdTag,cmdArgs,true);
					break;
			}*/
		}

		#endregion

		//--- End of Selected State


		//--- Any State ------

		#region function Capability

		private void Capability(string cmdTag)
		{
			/* RFC 3501 6.1.1
			
				Arguments:  none

				Responses:  REQUIRED untagged response: CAPABILITY

				Result:     OK - capability completed
							BAD - command unknown or arguments invalid
			   
				The CAPABILITY command requests a listing of capabilities that the
				server supports.  The server MUST send a single untagged
				CAPABILITY response with "IMAP4rev1" as one of the listed
				capabilities before the (tagged) OK response.

				A capability name which begins with "AUTH=" indicates that the
				server supports that particular authentication mechanism.
				
				Example:    C: abcd CAPABILITY
							S: * CAPABILITY IMAP4rev1 STARTTLS AUTH=GSSAPI
							LOGINDISABLED
							S: abcd OK CAPABILITY completed
							C: efgh STARTTLS
							S: efgh OK STARTLS completed
							<TLS negotiation, further commands are under [TLS] layer>
							C: ijkl CAPABILITY
							S: * CAPABILITY IMAP4rev1 AUTH=GSSAPI AUTH=PLAIN
							S: ijkl OK CAPABILITY completed
			*/

			string reply  = "* CAPABILITY IMAP4rev1 AUTH=DIGEST-MD5 AUTH=CRAM-MD5 NAMESPACE ACL\r\n";
			       reply += cmdTag + " OK CAPABILITY completed\r\n";

			m_pSocket.SendData(reply);
		}

		#endregion

		#region function Noop

		private void Noop(string cmdTag)
		{
			/* RFC 3501 6.1.2 NOOP Command
			
				Arguments:  none

				Responses:  no specific responses for this command (but see below)

				Result:     OK - noop completed
							BAD - command unknown or arguments invalid
			   
				The NOOP command always succeeds.  It does nothing.
				Since any command can return a status update as untagged data, the
				NOOP command can be used as a periodic poll for new messages or
				message status updates during a period of inactivity.  The NOOP
				command can also be used to reset any inactivity autologout timer
				on the server.
				
				Example: C: a002 NOOP
						 S: a002 OK NOOP completed
			*/

			// If there is selected mailobx, see if messages status has changed
			if(m_SelectedMailbox.Length > 0){
				// Get status
				IMAP_Messages messages = m_pServer.OnGetMessagesInfo(this,m_SelectedMailbox);

				// messages status has changed
				if(messages.Count != m_Messages.Count || messages.RecentCount != m_Messages.RecentCount){
					m_Messages = messages;

					string reply = "";
					       reply += "* " + m_Messages.Count + " EXISTS\r\n";
					       reply += "* " + m_Messages.RecentCount + " RECENT\r\n";

					m_pSocket.SendData(reply);
				}
			}

			m_pSocket.SendLine(cmdTag + " OK NOOP completed");
		}

		#endregion

		#region function LogOut

		private void LogOut(string cmdTag)
		{
			/* RFC 3501 6.1.3
			
				Arguments:  none

				Responses:  REQUIRED untagged response: BYE

				Result:     OK - logout completed
							BAD - command unknown or arguments invalid
			   
				The LOGOUT command informs the server that the client is done with
				the connection.  The server MUST send a BYE untagged response
				before the (tagged) OK response, and then close the network
				connection.
				
				Example: C: A023 LOGOUT
						S: * BYE IMAP4rev1 Server logging out
						S: A023 OK LOGOUT completed
						(Server and client then close the connection)
			*/

			string reply  = "* BYE IMAP4rev1 Server logging out\r\n";
			       reply += cmdTag + " OK LOGOUT completed\r\n";

			m_pSocket.SendData(reply);
		}

		#endregion

		//--- End of Any State


		#region function SendData
			
	/*	/// <summary>
		/// Sends data to socket.
		/// </summary>
		/// <param name="data">String data which to send.</param>
		private void SendData(string data)
		{	
			byte[] byte_data = System.Text.Encoding.ASCII.GetBytes(data);
			
			int nCount = m_pSocket.SendData(byte_data);	

			if(m_pServer.LogCommands){
				data = data.Replace("\r\n","<CRLF>");
				m_pLogWriter.AddEntry(data,this.SessionID,this.RemoteEndPoint.Address.ToString(),"S");
			}
		}
*/
/*		/// <summary>
		/// Send stream data to socket.
		/// </summary>
		/// <param name="strm"></param>
		private void SendData(MemoryStream strm)
		{
			//---- split message to blocks -------------------------------//
			long totalSent = 0;
			while(strm.Position < strm.Length){
				int blockSize = 4024;
				byte[] dataBuf = new byte[blockSize];
				int nCount = strm.Read(dataBuf,0,blockSize);
				int countSended = m_pSocket.Socket.Send(dataBuf,nCount,SocketFlags.None);

				totalSent += countSended;

				if(countSended != nCount){
					strm.Position = totalSent;
				}
			}
			//-------------------------------------------------------------//

			if(m_pServer.LogCommands){
				m_pLogWriter.AddEntry("binary " + strm.Length.ToString() + " bytes",this.SessionID,this.RemoteEndPoint.Address.ToString(),"S");
			}
		}
*/
		#endregion
	

		#region method ParseParams

		private string[] ParseParams(string argsText)
		{
			ArrayList p = new ArrayList();

			try{
				while(argsText.Length > 0){
					// Parameter is between ""
					if(argsText.StartsWith("\"")){
						p.Add(argsText.Substring(1,argsText.IndexOf("\"",1) - 1));
						// Remove parsed param
						argsText = argsText.Substring(argsText.IndexOf("\"",1) + 1).Trim();			
					}
					else{
						// Parameter is between ()
						if(argsText.StartsWith("(")){
							p.Add(argsText.Substring(1,argsText.LastIndexOf(")") - 1));
							// Remove parsed param
							argsText = argsText.Substring(argsText.LastIndexOf(")") + 1).Trim();
						}
						else{						
							// Read parameter till " ", probably there is more params
							// Note: If there is ({ before SP, cosider that it's last parameter.
							//       For example body[header.fields (from to)]
							if(argsText.IndexOf(" ") > -1 && argsText.IndexOfAny(new char[]{'(','['},0,argsText.IndexOf(" ")) == -1){
								p.Add(argsText.Substring(0,argsText.IndexOf(" ")));
								// Remove parsed param
								argsText = argsText.Substring(argsText.IndexOf(" ") + 1).Trim();
							}
							// This is last param
							else{
								p.Add(argsText);
								argsText = "";
							}
						}
					}
				}
			}
			catch{
			}

			string[] retVal = new string[p.Count];
			p.CopyTo(retVal);

			return retVal;
		}

		#endregion

		#region method ParseMsgNumbersFromSequenceSet

		private ArrayList ParseMsgNumbersFromSequenceSet(string sequenceSet,bool uid)
		{
			/* Rfc 3501 9.
				seq-number      = nz-number / "*"
                    ; message sequence number (COPY, FETCH, STORE
                    ; commands) or unique identifier (UID COPY,
                    ; UID FETCH, UID STORE commands).
                    ; * represents the largest number in use.  In
                    ; the case of message sequence numbers, it is
                    ; the number of messages in a non-empty mailbox.
                    ; In the case of unique identifiers, it is the
                    ; unique identifier of the last message in the
                    ; mailbox or, if the mailbox is empty, the
                    ; mailbox's current UIDNEXT value.
                    ; The server should respond with a tagged BAD
                    ; response to a command that uses a message
                    ; sequence number greater than the number of
                    ; messages in the selected mailbox.  This
                    ; includes "*" if the selected mailbox is empty.

				seq-range       = seq-number ":" seq-number
                    ; two seq-number values and all values between
                    ; these two regardless of order.
                    ; Example: 2:4 and 4:2 are equivalent and indicate
                    ; values 2, 3, and 4.
                    ; Example: a unique identifier sequence range of
                    ; 3291:* includes the UID of the last message in
                    ; the mailbox, even if that value is less than 3291.

				sequence-set    = (seq-number / seq-range) *("," sequence-set)
                    ; set of seq-number values, regardless of order.
                    ; Servers MAY coalesce overlaps and/or execute the
                    ; sequence in any order.
                    ; Example: a message sequence number set of
                    ; 2,4:7,9,12:* for a mailbox with 15 messages is
                    ; equivalent to 2,4,5,6,7,9,12,13,14,15
                    ; Example: a message sequence number set of *:4,5:7
                    ; for a mailbox with 10 messages is equivalent to
                    ; 10,9,8,7,6,5,4,5,6,7 and MAY be reordered and
                    ; overlap coalesced to be 4,5,6,7,8,9,10.
					
				Valid values: (numbers are message numbers or UID value)
					*) 1
					*) 1,2
					*) 1:2
					*) 1:*
					*) 1,2:*
					*) 1,2:*, ....
			*/

			ArrayList msgList = new ArrayList();
			string[] parts = sequenceSet.Split(',');
			foreach(string p in parts){
				string part = p;

				if(part.Length > 0){
					//--- * handling
					if(uid){
						// unique identifier of the last message
						if(m_Messages.Count > 0){
							part = part.Replace("*",m_Messages[m_Messages.Count - 1].MessageUID.ToString());
						}
						// if the mailbox is empty, the mailbox's current UIDNEXT value
						else{
							part = part.Replace("*",m_Messages.UID_Next.ToString());
						}
					}
					else{
						// the number of messages
						if(m_Messages.Count > 0){
							part = part.Replace("*",m_Messages.Count.ToString());
						}
						else{
							part = part.Replace("*","1");
						}
					}

					//--- sequence range
					if(part.IndexOf(":") > -1){
						string[] seq_set = part.Split(':');

						int startNo = 0;
						int endNo   = 0;

						//2:4 and 4:2 are equivalent 
						if(Convert.ToInt32(seq_set[1]) > Convert.ToInt32(seq_set[0])){
							startNo = Convert.ToInt32(seq_set[0]);
							endNo   = Convert.ToInt32(seq_set[1]);
						}
						else{
							startNo = Convert.ToInt32(seq_set[1]);
							endNo   = Convert.ToInt32(seq_set[0]);
						}

						// ToDo: what to do if uid isn't valid ???

						// need to replace uid with sequence number
						if(uid){
							startNo = m_Messages.IndexFromUID(startNo);
							endNo   = m_Messages.IndexFromUID(endNo);
						}

						if(startNo != -1 && endNo != -1){
							// Add range as single items. Eg. 2 to 5 = 2,3,4,5
							for(int i=startNo;i<=endNo;i++){
								if(!msgList.Contains(i)){
									msgList.Add(i);
								}
							}
						}
					}
					//--- sequence number
					else{
						int msgNo = Convert.ToInt32(part);

						// need to replace uid with sequence number
						if(uid){
							msgNo = m_Messages.IndexFromUID(msgNo);
						}

						if(!msgList.Contains(msgNo)){
							msgList.Add(msgNo);
						}
					}
				}
			}

			return msgList;
		}

		#endregion

		#region method ParseMessageFalgs

		/// <summary>
		/// Parses message flags from string.
		/// </summary>
		/// <param name="falgsString"></param>
		/// <returns></returns>
		private IMAP_MessageFlags ParseMessageFalgs(string falgsString)
		{
			IMAP_MessageFlags mFlags = 0;

			falgsString = falgsString.ToUpper();
			
			if(falgsString.IndexOf("\\ANSWERED") > -1){
				mFlags |= IMAP_MessageFlags.Answered;
			}
			if(falgsString.IndexOf("\\FLAGGED") > -1){
				mFlags |= IMAP_MessageFlags.Flagged;
			}
			if(falgsString.IndexOf("\\DELETED") > -1){
				mFlags |= IMAP_MessageFlags.Deleted;
			}
			if(falgsString.IndexOf("\\SEEN") > -1){
				mFlags |= IMAP_MessageFlags.Seen;
			}
			if(falgsString.IndexOf("\\DRAFT") > -1){
				mFlags |= IMAP_MessageFlags.Draft;
			}

			return mFlags;
		}

		#endregion


		#region Properties Implementation
		
		/// <summary>
		/// Gets session ID.
		/// </summary>
		public string SessionID
		{
			get{ return m_SessionID; }
		}

		/// <summary>
		/// Gets if session authenticated.
		/// </summary>
		public bool Authenticated
		{
			get{ return m_Authenticated; }
		}

		/// <summary>
		/// Gets loggded in user name (session owner).
		/// </summary>
		public string UserName
		{
			get{ return m_UserName; }
		}

		/// <summary>
		/// Gets selected mailbox.
		/// </summary>
		public string SelectedMailbox
		{
			get{ return m_SelectedMailbox; }
		}

		/// <summary>
		/// Gets connected Host(client) EndPoint.
		/// </summary>
		public IPEndPoint RemoteEndPoint
		{
			get{ return (IPEndPoint)m_pSocket.RemoteEndPoint; }
		}
		
		/// <summary>
		/// Gets local EndPoint which accepted client(connected host).
		/// </summary>
		public IPEndPoint LocalEndPoint
		{
			get{ return (IPEndPoint)m_pSocket.LocalEndPoint; }
		}

		/// <summary>
		/// Gets session start time.
		/// </summary>
		public DateTime SessionStartTime
		{
			get{ return m_SessionStartTime; }
		}

		/// <summary>
		/// Gets last data activity time.
		/// </summary>
		public DateTime SessionLastDataTime
		{
			get{ return m_LastDataTime; }
		}

		/// <summary>
		/// Gets or sets custom user data.
		/// </summary>
		public object Tag
		{
			get{ return m_Tag; }

			set{ m_Tag = value; }
		}

		#endregion

	}
}
