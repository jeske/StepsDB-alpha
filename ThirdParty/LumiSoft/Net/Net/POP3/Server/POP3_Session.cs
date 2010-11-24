using System;
using System.IO;
using System.Collections;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using LumiSoft.Net;
using LumiSoft.Net.AUTH;

namespace LumiSoft.Net.POP3.Server
{
	/// <summary>
	/// POP3 Session.
	/// </summary>
	public class POP3_Session : ISocketServerSession
	{		
		private BufferedSocket m_pSocket        = null;  // Referance to client Socket.
		private POP3_Server    m_pServer        = null;  // Referance to POP3 server.
		private string         m_SessionID      = "";    // Holds session ID.
		private string         m_UserName       = "";    // Holds loggedIn UserName.
		private string         m_Password       = "";    // Holds loggedIn Password.
		private bool           m_Authenticated  = false; // Holds authentication flag.
		private string         m_MD5_prefix     = "";    // Session MD5 prefix for APOP command
		private int            m_BadCmdCount    = 0;     // Holds number of bad commands.
		private POP3_Messages  m_POP3_Messages  = null;		
		private DateTime       m_SessionStartTime;
		private DateTime       m_LastDataTime;
	//	private _LogWriter     m_pLogWriter     = null;
		private object         m_Tag            = null;
		
		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="clientSocket">Referance to socket.</param>
		/// <param name="server">Referance to POP3 server.</param>
		/// <param name="logWriter">Log writer.</param>
		public POP3_Session(Socket clientSocket,POP3_Server server,SocketLogger logWriter)
		{
			m_pSocket = new BufferedSocket(clientSocket);
			m_pServer = server;

			m_SessionID        = Guid.NewGuid().ToString();
			m_POP3_Messages    = new POP3_Messages();			
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

			if(m_pServer.LogCommands){
		//		m_pSocket.Logger.AddTextEntry();
		//		m_pLogWriter.AddEntry("//----- Sys: 'Session:'" + this.SessionID + " added " + DateTime.Now);
			}
	
			try{
				// Check if ip is allowed to connect this computer
				if(m_pServer.OnValidate_IpAddress(this.LocalEndPoint,this.RemoteEndPoint)){
					// Notify that server is ready
					m_MD5_prefix = "<" + Guid.NewGuid().ToString().ToLower() + ">";
					m_pSocket.SendLine("+OK " + m_pServer.HostName + " POP3 Server ready " + m_MD5_prefix);

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
				m_pSocket.SendLine("-ERR Session timeout, closing transmission channel");
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
					//		m_pLogWriter.AddEntry("Client aborted/disconnected",this.SessionID,this.RemoteEndPoint.Address.ToString(),"C");

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
		//	AsyncSocketHelper.BeginRecieve(m_pSocket,strm,1024,"\r\n","\r\n",strm,new SocketCallBack(this.EndRecieveCmd),new SocketActivityCallback(this.OnSocketActivity));
			m_pSocket.BeginReadLine(strm,1024,strm,new SocketCallBack(this.EndRecieveCmd));
		}

		#endregion

		#region method EndRecieveCmd

		/// <summary>
		/// Is called if command is recieved.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="exception"></param>
		/// <param name="count"></param>
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
						//	m_pLogWriter.AddEntry(cmdLine + "<CRLF>",this.SessionID,this.RemoteEndPoint.Address.ToString(),"C");
					//	}

						// Exceute command
						if(SwitchCommand(cmdLine)){
							// Session end, close session
							EndSession();
						}
						break;

					case SocketCallBackResult.LengthExceeded:
						m_pSocket.SendLine("-ERR Line too long.");

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
		/// Parses and executes POP3 commmand.
		/// </summary>
		/// <param name="POP3_commandTxt">POP3 command text.</param>
		/// <returns>Returns true,if session must be terminated.</returns>
		private bool SwitchCommand(string POP3_commandTxt)
		{
			//---- Parse command --------------------------------------------------//
			string[] cmdParts = POP3_commandTxt.TrimStart().Split(new char[]{' '});
			string POP3_command = cmdParts[0].ToUpper().Trim();
			string argsText = Core.GetArgsText(POP3_commandTxt,POP3_command);
			//---------------------------------------------------------------------//

			bool getNextCmd = true;

			switch(POP3_command)
			{
				case "USER":
					USER(argsText);
					getNextCmd = false;
					break;

				case "PASS":
					PASS(argsText);
					getNextCmd = false;
					break;
					
				case "STAT":
					STAT();
					getNextCmd = false;
					break;

				case "LIST":
					LIST(argsText);
					getNextCmd = false;
					break;

				case "RETR":					
					RETR(argsText);
					getNextCmd = false;
					break;

				case "DELE":
					DELE(argsText);
					getNextCmd = false;
					break;

				case "NOOP":
					NOOP();
					getNextCmd = false;
					break;

				case "RSET":
					RSET();
					getNextCmd = false;
					break;

				case "QUIT":
					QUIT();
					getNextCmd = false;
					return true;


				//----- Optional commands ----- //
				case "UIDL":
					UIDL(argsText);
					getNextCmd = false;
					break;

				case "APOP":
					APOP(argsText);
					getNextCmd = false;
					break;

				case "TOP":
					TOP(argsText);
					getNextCmd = false;
					break;

				case "AUTH":
					AUTH(argsText);
					getNextCmd = false;
					break;

				case "CAPA":
					CAPA(argsText);
					getNextCmd = false;
					break;
										
				default:					
					m_pSocket.SendLine("-ERR Invalid command");

					//---- Check that maximum bad commands count isn't exceeded ---------------//
					if(m_BadCmdCount > m_pServer.MaxBadCommands-1){
						m_pSocket.SendLine("-ERR Too many bad commands, closing transmission channel");
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


		#region function USER

		private void USER(string argsText)
		{
			/* RFC 1939 7. USER
			Arguments:
				a string identifying a mailbox (required), which is of
				significance ONLY to the server
				
			NOTE:
				If the POP3 server responds with a positive
				status indicator ("+OK"), then the client may issue
				either the PASS command to complete the authentication,
				or the QUIT command to terminate the POP3 session.
			 
			*/

			if(m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You are already authenticated",new SocketCallBack(this.EndSend));
				return;
			}
			if(m_UserName.Length > 0){
				m_pSocket.BeginSendLine("-ERR username is already specified, please specify password",new SocketCallBack(this.EndSend));
				return;
			}

			string[] param = argsText.Split(new char[]{' '});

			// There must be only one parameter - userName
			if(argsText.Length > 0 && param.Length == 1){
				string userName = param[0];
							
				// Check if user isn't logged in already
				if(!m_pServer.IsUserLoggedIn(userName)){
					m_UserName = userName;

					// Send this line last, because it issues a new command and any assignments done
					// after this method may not become wisible to next command.
					m_pSocket.BeginSendLine("+OK User:'" + userName + "' ok",new SocketCallBack(this.EndSend));					
				}
				else{
					m_pSocket.BeginSendLine("-ERR User:'" + userName + "' already logged in",new SocketCallBack(this.EndSend));
				}
			}
			else{
				m_pSocket.BeginSendLine("-ERR Syntax error. Syntax:{USER username}",new SocketCallBack(this.EndSend));
			}
		}

		#endregion

		#region function PASS

		private void PASS(string argsText)
		{	
			/* RFC 7. PASS
			Arguments:
				a server/mailbox-specific password (required)
				
			Restrictions:
				may only be given in the AUTHORIZATION state immediately
				after a successful USER command
				
			NOTE:
				When the client issues the PASS command, the POP3 server
				uses the argument pair from the USER and PASS commands to
				determine if the client should be given access to the
				appropriate maildrop.
				
			Possible Responses:
				+OK maildrop locked and ready
				-ERR invalid password
				-ERR unable to lock maildrop
						
			*/

			if(m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You are already authenticated",new SocketCallBack(this.EndSend));
				return;
			}
			if(m_UserName.Length == 0){
				m_pSocket.BeginSendLine("-ERR please specify username first",new SocketCallBack(this.EndSend));
				return;
			}

			string[] param = argsText.Split(new char[]{' '});

			// There may be only one parameter - password
			if(param.Length == 1){
				string password = param[0];
									
				// Authenticate user
				AuthUser_EventArgs aArgs = m_pServer.OnAuthUser(this,m_UserName,password,"",AuthType.Plain);
				if(aArgs.Validated){					
					m_Password = password;
					m_Authenticated = true;

					// Get user messages info.
					m_pServer.OnGetMessagesInfo(this,m_POP3_Messages);

					m_pSocket.BeginSendLine("+OK Password ok",new SocketCallBack(this.EndSend));
				}
				else{						
					m_pSocket.BeginSendLine("-ERR UserName or Password is incorrect",new SocketCallBack(this.EndSend));					
					m_UserName = ""; // Reset userName !!!
				}
			}
			else{
				m_pSocket.BeginSendLine("-ERR Syntax error. Syntax:{PASS userName}",new SocketCallBack(this.EndSend));
			}
		}

		#endregion

		#region function STAT

		private void STAT()
		{	
			/* RFC 1939 5. STAT
			NOTE:
				The positive response consists of "+OK" followed by a single
				space, the number of messages in the maildrop, a single
				space, and the size of the maildrop in octets.
				
				Note that messages marked as deleted are not counted in
				either total.
			 
			Example:
				C: STAT
				S: +OK 2 320
			*/

			if(!m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You must authenticate first",new SocketCallBack(this.EndSend));
				return;
			}
		
			m_pSocket.BeginSendLine("+OK " + m_POP3_Messages.Count.ToString() + " " + m_POP3_Messages.GetTotalMessagesSize(),new SocketCallBack(this.EndSend));			
		}

		#endregion

		#region function LIST

		private void LIST(string argsText)
		{	
			/* RFC 1939 5. LIST
			Arguments:
				a message-number (optional), which, if present, may NOT
				refer to a message marked as deleted
			 
			NOTE:
				If an argument was given and the POP3 server issues a
				positive response with a line containing information for
				that message.

				If no argument was given and the POP3 server issues a
				positive response, then the response given is multi-line.
				
				Note that messages marked as deleted are not listed.
			
			Examples:
				C: LIST
				S: +OK 2 messages (320 octets)
				S: 1 120				
				S: 2 200
				S: .
				...
				C: LIST 2
				S: +OK 2 200
				...
				C: LIST 3
				S: -ERR no such message, only 2 messages in maildrop
			 
			*/

			if(!m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You must authenticate first",new SocketCallBack(this.EndSend));
				return;
			}

			string[] param = argsText.Split(new char[]{' '});

			// Argument isn't specified, multiline response.
			if(argsText.Length == 0){
				string reply = "+OK " + m_POP3_Messages.Count.ToString() + " messages\r\n";

				// Send message number and size for each message
				foreach(POP3_Message msg in m_POP3_Messages.ActiveMessages){
					reply += msg.MessageNr.ToString() + " " + msg.MessageSize + "\r\n";
				}

				// ".<CRLF>" - means end of list
				reply += ".\r\n";

				m_pSocket.BeginSendData(reply,null,new SocketCallBack(this.EndSend));
			}
			else{
				// If parameters specified,there may be only one parameter - messageNr
				if(param.Length == 1){
					// Check if messageNr is valid
					if(Core.IsNumber(param[0])){
						int messageNr = Convert.ToInt32(param[0]);
						if(m_POP3_Messages.MessageExists(messageNr)){
							POP3_Message msg = m_POP3_Messages[messageNr];

							m_pSocket.BeginSendLine("+OK " + messageNr.ToString() + " " + msg.MessageSize,new SocketCallBack(this.EndSend));
						}
						else{
							m_pSocket.BeginSendLine("-ERR no such message, or marked for deletion",new SocketCallBack(this.EndSend));
						}
					}
					else{
						m_pSocket.BeginSendLine("-ERR message-number is invalid",new SocketCallBack(this.EndSend));
					}
				}
				else{
					m_pSocket.BeginSendLine("-ERR Syntax error. Syntax:{LIST [messageNr]}",new SocketCallBack(this.EndSend));
				}
			}
		}

		#endregion

		#region function RETR
		
		private void RETR(string argsText)
		{
			/* RFC 1939 5. RETR
			Arguments:
				a message-number (required) which may NOT refer to a
				message marked as deleted
			 
			NOTE:
				If the POP3 server issues a positive response, then the
				response given is multi-line.  After the initial +OK, the
				POP3 server sends the message corresponding to the given
				message-number, being careful to byte-stuff the termination
				character (as with all multi-line responses).
				
			Example:
				C: RETR 1
				S: +OK 120 octets
				S: <the POP3 server sends the entire message here>
				S: .
			
			*/

			if(!m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You must authenticate first",new SocketCallBack(this.EndSend));
			}
	
			string[] param = argsText.Split(new char[]{' '});

			// There must be only one parameter - messageNr
			if(argsText.Length > 0 && param.Length == 1){
				// Check if messageNr is valid
				if(Core.IsNumber(param[0])){
					int messageNr = Convert.ToInt32(param[0]);					
					if(m_POP3_Messages.MessageExists(messageNr)){
						POP3_Message msg = m_POP3_Messages[messageNr];
														
						// Raise Event, request message
						byte[] message = m_pServer.OnGetMail(this,msg,m_pSocket.Socket);
						if(message != null){
							//------- Do period handling and send message -----------------------//
							// If line starts with '.', add additional '.'.(Read rfc for more info)
							MemoryStream msgStrm = Core.DoPeriodHandling(message,true);

							byte[] ok     = System.Text.Encoding.Default.GetBytes("+OK\r\n");
							byte[] msgEnd = System.Text.Encoding.Default.GetBytes(".\r\n");

							MemoryStream strm = new MemoryStream();
							strm.Write(ok,0,ok.Length);         // +OK<CRLF>
							msgStrm.WriteTo(strm);              // message data
							strm.Write(msgEnd,0,msgEnd.Length); // .<CRLF>
							strm.Position = 0;

							// Send message asynchronously to client
							m_pSocket.BeginSendData(strm,null,new SocketCallBack(this.EndSend));
						}
						else{									
							m_pSocket.BeginSendLine("-ERR no such message",new SocketCallBack(this.EndSend));
						}
					}
					else{
						m_pSocket.BeginSendLine("-ERR no such message",new SocketCallBack(this.EndSend));
					}
				}
				else{
					m_pSocket.BeginSendLine("-ERR message-number is invalid",new SocketCallBack(this.EndSend));
				}
			}
			else{
				m_pSocket.BeginSendLine("-ERR Syntax error. Syntax:{RETR messageNr}",new SocketCallBack(this.EndSend));
			}
		}

		#endregion

		#region function DELE

		private void DELE(string argsText)
		{	
			/* RFC 1939 5. DELE
			Arguments:
				a message-number (required) which may NOT refer to a
				message marked as deleted
			 
			NOTE:
				The POP3 server marks the message as deleted.  Any future
				reference to the message-number associated with the message
				in a POP3 command generates an error.  The POP3 server does
				not actually delete the message until the POP3 session
				enters the UPDATE state.
			*/

			if(!m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You must authenticate first",new SocketCallBack(this.EndSend));
				return;
			}

			string[] param = argsText.Split(new char[]{' '});

			// There must be only one parameter - messageNr
			if(argsText.Length > 0 && param.Length == 1){
				// Check if messageNr is valid
				if(Core.IsNumber(param[0])){
					int nr = Convert.ToInt32(param[0]);					
					if(m_POP3_Messages.MessageExists(nr)){
						POP3_Message msg = m_POP3_Messages[nr];
						msg.MarkedForDelete = true;

						m_pSocket.BeginSendLine("+OK marked for delete",new SocketCallBack(this.EndSend));
					}
					else{
						m_pSocket.BeginSendLine("-ERR no such message",new SocketCallBack(this.EndSend));
					}
				}
				else{
					m_pSocket.BeginSendLine("-ERR message-number is invalid",new SocketCallBack(this.EndSend));
				}
			}
			else{
				m_pSocket.BeginSendLine("-ERR Syntax error. Syntax:{DELE messageNr}",new SocketCallBack(this.EndSend));
			}
		}

		#endregion

		#region function NOOP

		private void NOOP()
		{
			/* RFC 1939 5. NOOP
			NOTE:
				The POP3 server does nothing, it merely replies with a
				positive response.
			*/

			if(!m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You must authenticate first",new SocketCallBack(this.EndSend));
				return;
			}

			m_pSocket.BeginSendLine("+OK",new SocketCallBack(this.EndSend));
		}

		#endregion

		#region function RSET

		private void RSET()
		{
			/* RFC 1939 5. RSET
			Discussion:
				If any messages have been marked as deleted by the POP3
				server, they are unmarked.  The POP3 server then replies
				with a positive response.
			*/

			if(!m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You must authenticate first",new SocketCallBack(this.EndSend));
				return;
			}

			Reset();

			// Raise SessionResetted event
			m_pServer.OnSessionResetted(this);

			m_pSocket.BeginSendLine("+OK",new SocketCallBack(this.EndSend));
		}

		#endregion

		#region function QUIT

		private void QUIT()
		{
			/* RFC 1939 6. QUIT
			NOTE:
				The POP3 server removes all messages marked as deleted
				from the maildrop and replies as to the status of this
				operation.  If there is an error, such as a resource
				shortage, encountered while removing messages, the
				maildrop may result in having some or none of the messages
				marked as deleted be removed.  In no case may the server
				remove any messages not marked as deleted.

				Whether the removal was successful or not, the server
				then releases any exclusive-access lock on the maildrop
				and closes the TCP connection.
			*/					
			Update();

			m_pSocket.SendLine("+OK POP3 server signing off");
		//	m_pSocket.BeginSendLine("+OK POP3 server signing off",null);			
		}

		#endregion


		//--- Optional commands

		#region function TOP

		private void TOP(string argsText)
		{		
			/* RFC 1939 7. TOP
			Arguments:
				a message-number (required) which may NOT refer to to a
				message marked as deleted, and a non-negative number
				of lines (required)
		
			NOTE:
				If the POP3 server issues a positive response, then the
				response given is multi-line.  After the initial +OK, the
				POP3 server sends the headers of the message, the blank
				line separating the headers from the body, and then the
				number of lines of the indicated message's body, being
				careful to byte-stuff the termination character (as with
				all multi-line responses).
			
			Examples:
				C: TOP 1 10
				S: +OK
				S: <the POP3 server sends the headers of the
					message, a blank line, and the first 10 lines
					of the body of the message>
				S: .
                ...
				C: TOP 100 3
				S: -ERR no such message
			 
			*/

			if(!m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You must authenticate first",new SocketCallBack(this.EndSend));
			}

			string[] param = argsText.Split(new char[]{' '});
			
			// There must be at two parameters - messageNr and nrLines
			if(param.Length == 2){
				// Check if messageNr and nrLines is valid
				if(Core.IsNumber(param[0]) && Core.IsNumber(param[1])){
					int messageNr = Convert.ToInt32(param[0]);
					if(m_POP3_Messages.MessageExists(messageNr)){
						POP3_Message msg = m_POP3_Messages[messageNr];

						byte[] lines = m_pServer.OnGetTopLines(this,msg,Convert.ToInt32(param[1]));
						if(lines != null){
							//------- Do period handling and send message -----------------------//
							// If line starts with '.', add additional '.'.(Read rfc for more info)
							MemoryStream msgStrm = Core.DoPeriodHandling(lines,true);

							byte[] ok     = System.Text.Encoding.Default.GetBytes("+OK\r\n");
							byte[] msgEnd = System.Text.Encoding.Default.GetBytes(".\r\n");

							MemoryStream strm = new MemoryStream();
							strm.Write(ok,0,ok.Length);         // +OK<CRLF>
							msgStrm.WriteTo(strm);              // message data
							strm.Write(msgEnd,0,msgEnd.Length); // .<CRLF>
							strm.Position = 0;

							// Send message asynchronously to client
							m_pSocket.BeginSendData(strm,null,new SocketCallBack(this.EndSend));
						}
						else{
							m_pSocket.BeginSendLine("-ERR no such message",new SocketCallBack(this.EndSend));
						}
					}
					else{
						m_pSocket.BeginSendLine("-ERR no such message",new SocketCallBack(this.EndSend));
					}
				}
				else{
					m_pSocket.BeginSendLine("-ERR message-number or number of lines is invalid",new SocketCallBack(this.EndSend));
				}
			}
			else{
				m_pSocket.BeginSendLine("-ERR Syntax error. Syntax:{TOP messageNr nrLines}",new SocketCallBack(this.EndSend));
			}
		}

		#endregion

		#region function UIDL

		private void UIDL(string argsText)
		{
			/* RFC 1939 UIDL [msg]
			Arguments:
			    a message-number (optional), which, if present, may NOT
				refer to a message marked as deleted
				
			NOTE:
				If an argument was given and the POP3 server issues a positive
				response with a line containing information for that message.

				If no argument was given and the POP3 server issues a positive
				response, then the response given is multi-line.  After the
				initial +OK, for each message in the maildrop, the POP3 server
				responds with a line containing information for that message.	
				
			Examples:
				C: UIDL
				S: +OK
				S: 1 whqtswO00WBw418f9t5JxYwZ
				S: 2 QhdPYR:00WBw1Ph7x7
				S: .
				...
				C: UIDL 2
				S: +OK 2 QhdPYR:00WBw1Ph7x7
				...
				C: UIDL 3
				S: -ERR no such message
			*/

			if(!m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You must authenticate first",new SocketCallBack(this.EndSend));
				return;
			}

			string[] param = argsText.Split(new char[]{' '});

			// Argument isn't specified, multiline response.
			if(argsText.Length == 0){
				string reply = "+OK\r\n";

				// Send message number and size for each message
				foreach(POP3_Message msg in m_POP3_Messages.ActiveMessages){
					reply += msg.MessageNr.ToString() + " " + msg.MessageUID + "\r\n";
				}

				// ".<CRLF>" - means end of list
				reply += ".\r\n";

				m_pSocket.BeginSendData(reply,null,new SocketCallBack(this.EndSend));
			}
			else{
				// If parameters specified,there may be only one parameter - messageID
				if(param.Length == 1){
					// Check if messageNr is valid
					if(Core.IsNumber(param[0])){
						int messageNr = Convert.ToInt32(param[0]);
						if(m_POP3_Messages.MessageExists(messageNr)){
							POP3_Message msg = m_POP3_Messages[messageNr];

							m_pSocket.BeginSendLine("+OK " + messageNr.ToString() + " " + msg.MessageUID,new SocketCallBack(this.EndSend));
						}
						else{
							m_pSocket.BeginSendLine("-ERR no such message",new SocketCallBack(this.EndSend));
						}
					}
					else{
						m_pSocket.BeginSendLine("-ERR message-number is invalid",new SocketCallBack(this.EndSend));
					}
				}
				else{
					m_pSocket.BeginSendLine("-ERR Syntax error. Syntax:{UIDL [messageNr]}",new SocketCallBack(this.EndSend));
				}
			}	
		}

		#endregion

		#region function APOP

		private void APOP(string argsText)
		{
			/* RFC 1939 7. APOP
			Arguments:
				a string identifying a mailbox and a MD5 digest string
				(both required)
				
			NOTE:
				A POP3 server which implements the APOP command will
				include a timestamp in its banner greeting.  The syntax of
				the timestamp corresponds to the `msg-id' in [RFC822], and
				MUST be different each time the POP3 server issues a banner
				greeting.
				
			Examples:
				S: +OK POP3 server ready <1896.697170952@dbc.mtview.ca.us>
				C: APOP mrose c4c9334bac560ecc979e58001b3e22fb
				S: +OK maildrop has 1 message (369 octets)

				In this example, the shared  secret  is  the  string  `tan-
				staaf'.  Hence, the MD5 algorithm is applied to the string

				<1896.697170952@dbc.mtview.ca.us>tanstaaf
				 
				which produces a digest value of
		            c4c9334bac560ecc979e58001b3e22fb
			 
			*/

			if(m_Authenticated){
				m_pSocket.BeginSendLine("-ERR You are already authenticated",new SocketCallBack(this.EndSend));
				return;
			}

			string[] param = argsText.Split(new char[]{' '});

			// There must be two params
			if(param.Length == 2){
				string userName   = param[0];
				string md5HexHash = param[1];

				// Check if user isn't logged in already
				if(m_pServer.IsUserLoggedIn(userName)){
					m_pSocket.BeginSendLine("-ERR User:'" + userName + "' already logged in",new SocketCallBack(this.EndSend));
					return;
				}

				// Authenticate user
				AuthUser_EventArgs aArgs = m_pServer.OnAuthUser(this,userName,md5HexHash,m_MD5_prefix,AuthType.APOP);
				if(aArgs.Validated){
					m_UserName = userName;
					m_Authenticated = true;

					// Get user messages info.
					m_pServer.OnGetMessagesInfo(this,m_POP3_Messages);

					m_pSocket.BeginSendLine("+OK authentication was successful",new SocketCallBack(this.EndSend));
				}
				else{
					m_pSocket.BeginSendLine("-ERR authentication failed",new SocketCallBack(this.EndSend));
				}
			}
			else{
				m_pSocket.BeginSendLine("-ERR syntax error. Syntax:{APOP userName md5HexHash}",new SocketCallBack(this.EndSend));
			}
		}

		#endregion

		#region function AUTH

		private void AUTH(string argsText)
		{
			/* Rfc 1734
				
				AUTH mechanism

					Arguments:
						a string identifying an IMAP4 authentication mechanism,
						such as defined by [IMAP4-AUTH].  Any use of the string
						"imap" used in a server authentication identity in the
						definition of an authentication mechanism is replaced with
						the string "pop".
						
					Possible Responses:
						+OK maildrop locked and ready
						-ERR authentication exchange failed

					Restrictions:
						may only be given in the AUTHORIZATION state

					Discussion:
						The AUTH command indicates an authentication mechanism to
						the server.  If the server supports the requested
						authentication mechanism, it performs an authentication
						protocol exchange to authenticate and identify the user.
						Optionally, it also negotiates a protection mechanism for
						subsequent protocol interactions.  If the requested
						authentication mechanism is not supported, the server						
						should reject the AUTH command by sending a negative
						response.

						The authentication protocol exchange consists of a series
						of server challenges and client answers that are specific
						to the authentication mechanism.  A server challenge,
						otherwise known as a ready response, is a line consisting
						of a "+" character followed by a single space and a BASE64
						encoded string.  The client answer consists of a line
						containing a BASE64 encoded string.  If the client wishes
						to cancel an authentication exchange, it should issue a
						line with a single "*".  If the server receives such an
						answer, it must reject the AUTH command by sending a
						negative response.

						A protection mechanism provides integrity and privacy
						protection to the protocol session.  If a protection
						mechanism is negotiated, it is applied to all subsequent
						data sent over the connection.  The protection mechanism
						takes effect immediately following the CRLF that concludes
						the authentication exchange for the client, and the CRLF of
						the positive response for the server.  Once the protection
						mechanism is in effect, the stream of command and response
						octets is processed into buffers of ciphertext.  Each
						buffer is transferred over the connection as a stream of
						octets prepended with a four octet field in network byte
						order that represents the length of the following data.
						The maximum ciphertext buffer length is defined by the
						protection mechanism.

						The server is not required to support any particular
						authentication mechanism, nor are authentication mechanisms
						required to support any protection mechanisms.  If an AUTH
						command fails with a negative response, the session remains
						in the AUTHORIZATION state and client may try another
						authentication mechanism by issuing another AUTH command,
						or may attempt to authenticate by using the USER/PASS or
						APOP commands.  In other words, the client may request
						authentication types in decreasing order of preference,
						with the USER/PASS or APOP command as a last resort.

						Should the client successfully complete the authentication
						exchange, the POP3 server issues a positive response and
						the POP3 session enters the TRANSACTION state.
						
				Examples:
							S: +OK POP3 server ready
							C: AUTH KERBEROS_V4
							S: + AmFYig==
							C: BAcAQU5EUkVXLkNNVS5FRFUAOCAsho84kLN3/IJmrMG+25a4DT
								+nZImJjnTNHJUtxAA+o0KPKfHEcAFs9a3CL5Oebe/ydHJUwYFd
								WwuQ1MWiy6IesKvjL5rL9WjXUb9MwT9bpObYLGOKi1Qh
							S: + or//EoAADZI=
							C: DiAF5A4gA+oOIALuBkAAmw==
							S: +OK Kerberos V4 authentication successful
								...
							C: AUTH FOOBAR
							S: -ERR Unrecognized authentication type
			 
			*/
			if(m_Authenticated){
				m_pSocket.BeginSendLine("-ERR already authenticated",new SocketCallBack(this.EndSend));
				return;
			}
			
				
			//------ Parse parameters -------------------------------------//
			string userName = "";
			string password = "";
			AuthUser_EventArgs aArgs = null;

			string[] param = argsText.Split(new char[]{' '});
			switch(param[0].ToUpper())
			{
				case "PLAIN":
					m_pSocket.BeginSendLine("-ERR Unrecognized authentication type.",new SocketCallBack(this.EndSend));
					break;

				case "LOGIN":

					#region LOGIN authentication

				    //---- AUTH = LOGIN ------------------------------
					/* Login
					C: AUTH LOGIN-MD5
					S: + VXNlcm5hbWU6
					C: username_in_base64
					S: + UGFzc3dvcmQ6
					C: password_in_base64
					
					   VXNlcm5hbWU6 base64_decoded= USERNAME
					   UGFzc3dvcmQ6 base64_decoded= PASSWORD
					*/
					// Note: all strings are base64 strings eg. VXNlcm5hbWU6 = UserName.
			
					
					// Query UserName
					m_pSocket.SendLine("+ VXNlcm5hbWU6");

					string userNameLine = m_pSocket.ReadLine();
					// Encode username from base64
					if(userNameLine.Length > 0){
						userName = System.Text.Encoding.Default.GetString(Convert.FromBase64String(userNameLine));
					}
						
					// Query Password
					m_pSocket.SendLine("+ UGFzc3dvcmQ6");

					string passwordLine = m_pSocket.ReadLine();
					// Encode password from base64
					if(passwordLine.Length > 0){
						password = System.Text.Encoding.Default.GetString(Convert.FromBase64String(passwordLine));
					}
						
					aArgs = m_pServer.OnAuthUser(this,userName,password,"",AuthType.Plain);
					if(aArgs.Validated){
						m_pSocket.BeginSendLine("+OK Authentication successful.",new SocketCallBack(this.EndSend));
						
						m_UserName = userName;
						m_Authenticated = true;

						// Get user messages info.
						m_pServer.OnGetMessagesInfo(this,m_POP3_Messages);
					}
					else{
						m_pSocket.BeginSendLine("-ERR Authentication failed",new SocketCallBack(this.EndSend));
					}

					#endregion

					break;

				case "CRAM-MD5":
					
					#region CRAM-MD5 authentication

					/* Cram-M5
					C: AUTH CRAM-MD5
					S: + <md5_calculation_hash_in_base64>
					C: base64(decoded:username password_hash)
					*/
					
					string md5Hash = "<" + Guid.NewGuid().ToString().ToLower() + ">";
					m_pSocket.SendLine("+ " + Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(md5Hash)));

					string reply = m_pSocket.ReadLine();

					reply = System.Text.Encoding.Default.GetString(Convert.FromBase64String(reply));
					string[] replyArgs = reply.Split(' ');
					userName = replyArgs[0];
					
					aArgs = m_pServer.OnAuthUser(this,userName,replyArgs[1],md5Hash,AuthType.CRAM_MD5);
					if(aArgs.Validated){
						m_pSocket.BeginSendLine("+OK Authentication successful.",new SocketCallBack(this.EndSend));
						
						m_UserName = userName;
						m_Authenticated = true;

						// Get user messages info.
						m_pServer.OnGetMessagesInfo(this,m_POP3_Messages);
					}
					else{
						m_pSocket.BeginSendLine("-ERR Authentication failed",new SocketCallBack(this.EndSend));
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
					 * S: +OK Authentication successful.
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
								m_pSocket.BeginSendLine("+OK Authentication successful.",new SocketCallBack(this.EndSend));
							}
							else{
								m_pSocket.BeginSendLine("-ERR Authentication failed",new SocketCallBack(this.EndSend));
							}
						}
						else{
							m_pSocket.BeginSendLine("-ERR Authentication failed",new SocketCallBack(this.EndSend));
						}
					}
					else{
						m_pSocket.BeginSendLine("-ERR Authentication failed",new SocketCallBack(this.EndSend));
					}

					#endregion

					break;

				default:
					m_pSocket.BeginSendLine("-ERR Unrecognized authentication type.",new SocketCallBack(this.EndSend));
					break;
			}
			//-----------------------------------------------------------------//
		}

		#endregion

		#region function CAPA

		private void CAPA(string argsText)
		{
			/* rfc 2449 5.  The CAPA Command
			
				The POP3 CAPA command returns a list of capabilities supported by the
				POP3 server.  It is available in both the AUTHORIZATION and
				TRANSACTION states.

				A capability description MUST document in which states the capability
				is announced, and in which states the commands are valid.

				Capabilities available in the AUTHORIZATION state MUST be announced
				in both states.

				If a capability is announced in both states, but the argument might
				differ after authentication, this possibility MUST be stated in the
				capability description.

				(These requirements allow a client to issue only one CAPA command if
				it does not use any TRANSACTION-only capabilities, or any
				capabilities whose values may differ after authentication.)

				If the authentication step negotiates an integrity protection layer,
				the client SHOULD reissue the CAPA command after authenticating, to
				check for active down-negotiation attacks.

				Each capability may enable additional protocol commands, additional
				parameters and responses for existing commands, or describe an aspect
				of server behavior.  These details are specified in the description
				of the capability.
				
				Section 3 describes the CAPA response using [ABNF].  When a
				capability response describes an optional command, the <capa-tag>
				SHOULD be identical to the command keyword.  CAPA response tags are
				case-insensitive.

				CAPA

				Arguments:
					none

				Restrictions:
					none

				Discussion:
					An -ERR response indicates the capability command is not
					implemented and the client will have to probe for
					capabilities as before.

					An +OK response is followed by a list of capabilities, one
					per line.  Each capability name MAY be followed by a single
					space and a space-separated list of parameters.  Each
					capability line is limited to 512 octets (including the
					CRLF).  The capability list is terminated by a line
					containing a termination octet (".") and a CRLF pair.

				Possible Responses:
					+OK -ERR

					Examples:
						C: CAPA
						S: +OK Capability list follows
						S: TOP
						S: USER
						S: SASL CRAM-MD5 KERBEROS_V4
						S: RESP-CODES
						S: LOGIN-DELAY 900
						S: PIPELINING
						S: EXPIRE 60
						S: UIDL
						S: IMPLEMENTATION Shlemazle-Plotz-v302
						S: .
			*/

			string capaResponse = "";
			capaResponse += "+OK Capability list follows\r\n";
			capaResponse += "PIPELINING\r\n";
			capaResponse += "UIDL\r\n";
			capaResponse += "TOP\r\n";
			capaResponse += "USER\r\n";
			capaResponse += "SASL LOGIN CRAM-MD5 DIGEST-MD5\r\n";
			capaResponse += ".\r\n";
			

			m_pSocket.BeginSendLine(capaResponse,new SocketCallBack(this.EndSend));
		}

		#endregion
				

		#region function Reset

		private void Reset()
		{		
			/* RFC 1939 5. RSET
			Discussion:
				If any messages have been marked as deleted by the POP3
				server, they are unmarked.
			*/
			m_POP3_Messages.ResetDeleteFlags();
		}

		#endregion

		#region function Update

		private void Update()
		{
			/* RFC 1939 6.
			NOTE:
				When the client issues the QUIT command from the TRANSACTION state,
				the POP3 session enters the UPDATE state.  (Note that if the client
				issues the QUIT command from the AUTHORIZATION state, the POP3
				session terminates but does NOT enter the UPDATE state.)

				If a session terminates for some reason other than a client-issued
				QUIT command, the POP3 session does NOT enter the UPDATE state and
				MUST not remove any messages from the maildrop.
			*/

			if(m_Authenticated){

				// Delete all message which are marked for deletion ---//
				foreach(POP3_Message msg in m_POP3_Messages.Messages){
					if(msg.MarkedForDelete){
						m_pServer.OnDeleteMessage(this,msg);
					}
				}
				//-----------------------------------------------------//
			}
		}

		#endregion


		#region function EndSend
		
		/// <summary>
		/// Is called when asynchronous send completes.
		/// </summary>
		/// <param name="result">If true, then send was successfull.</param>
		/// <param name="count">Count sended.</param>
		/// <param name="exception">Exception happend on send. NOTE: available only is result=false.</param>
		/// <param name="tag">User data.</param>
		private void EndSend(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			try{
				switch(result)
				{
					case SocketCallBackResult.Ok:
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


		#region Properties Implementation

		/// <summary>
		/// Gets session ID.
		/// </summary>
		public string SessionID
		{
			get{ return m_SessionID; }
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
		/// Gets loggded in user name (session owner).
		/// </summary>
		public string UserName
		{
			get{ return m_UserName; }
		}

		/// <summary>
		/// Gets EndPoint which accepted conection.
		/// </summary>
		public IPEndPoint LocalEndPoint
		{
			get{ return (IPEndPoint)m_pSocket.LocalEndPoint; }
		}

		/// <summary>
		/// Gets connected Host(client) EndPoint.
		/// </summary>
		public IPEndPoint RemoteEndPoint
		{
			get{ return (IPEndPoint)m_pSocket.RemoteEndPoint; }
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
