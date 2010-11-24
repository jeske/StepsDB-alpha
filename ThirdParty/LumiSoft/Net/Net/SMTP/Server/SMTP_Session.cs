using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Collections;
using System.Text;
using LumiSoft.Net;
using LumiSoft.Net.SMTP;
using LumiSoft.Net.AUTH;

namespace LumiSoft.Net.SMTP.Server
{
	/// <summary>
	/// SMTP Session.
	/// </summary>
	public class SMTP_Session : ISocketServerSession
	{		
		private SMTP_Cmd_Validator m_CmdValidator = null;
				
		private BufferedSocket m_pSocket       = null;
		private SMTP_Server    m_pServer       = null;
		private MemoryStream   m_pMsgStream    = null;
		private string         m_SessionID     = "";      // Holds session ID.
		private string         m_EhloName      = "";      // Holds session ID.
		private string         m_UserName      = "";      // Holds loggedIn UserName.
		private bool           m_Authenticated = false;   // Holds authentication flag.
		private string         m_Reverse_path  = "";      // Holds sender's reverse path.
		private Hashtable      m_Forward_path  = null;    // Holds Mail to.	
		private int            m_BadCmdCount   = 0;       // Holds number of bad commands.
		private BodyType       m_BodyType;
		private bool           m_BDat          = false;
		private DateTime       m_SessionStart;
		private DateTime       m_LastDataTime;
		private object         m_Tag           = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="clientSocket">Referance to socket.</param>
		/// <param name="server">Referance to SMTP server.</param>
		/// <param name="logWriter">Log writer.</param>
		internal SMTP_Session(Socket clientSocket,SMTP_Server server,SocketLogger logWriter)
		{						
			m_pSocket    = new BufferedSocket(clientSocket);
			m_pServer    = server;
            
			m_pMsgStream   = new MemoryStream();
			m_SessionID    = Guid.NewGuid().ToString();
			m_BodyType     = BodyType.x7_bit;
			m_Forward_path = new Hashtable();
			m_CmdValidator = new SMTP_Cmd_Validator();
			m_SessionStart = DateTime.Now;
			m_LastDataTime = DateTime.Now;

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
				ValidateIP_EventArgs oArg = m_pServer.OnValidate_IpAddress(this);
				if(oArg.Validated){
					if(m_pServer.GreetingText.Length > 0){
						m_pSocket.SendLine("220 " + m_pServer.GreetingText);
					}
					else{
						m_pSocket.SendLine("220 " + m_pServer.HostName + " SMTP Server ready");
					}

					BeginRecieveCmd();
				}
				else{
					// There is user specified error text, send it to connected socket
					if(oArg.ErrorText.Length > 0){
						m_pSocket.SendLine(oArg.ErrorText);
					}

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
				m_pSocket.SendLine("421 Session timeout, closing transmission channel");
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

						// Exceute command
						if(SwitchCommand(cmdLine)){
							// Session end, close session
							EndSession();
						}
						break;

					case SocketCallBackResult.LengthExceeded:
						m_pSocket.SendLine("500 Line too long.");

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
		/// Executes SMTP command.
		/// </summary>
		/// <param name="SMTP_commandTxt">Original command text.</param>
		/// <returns>Returns true if must end session(command loop).</returns>
		private bool SwitchCommand(string SMTP_commandTxt)
		{
			//---- Parse command --------------------------------------------------//
			string[] cmdParts = SMTP_commandTxt.TrimStart().Split(new char[]{' '});
			string SMTP_command = cmdParts[0].ToUpper().Trim();
			string argsText = Core.GetArgsText(SMTP_commandTxt,SMTP_command);
			//---------------------------------------------------------------------//

			bool getNextCmd = true;

			switch(SMTP_command)
			{
				case "HELO":
					HELO(argsText);
					getNextCmd = false;
					break;

				case "EHLO":
					EHLO(argsText);
					getNextCmd = false;
					break;

				case "AUTH":
					AUTH(argsText);
					break;

				case "MAIL":
					MAIL(argsText);
					getNextCmd = false;
					break;
					
				case "RCPT":
					RCPT(argsText);
					getNextCmd = false;
					break;

				case "DATA":
					BeginDataCmd(argsText);
					getNextCmd = false;
					break;

				case "BDAT":
					BeginBDATCmd(argsText);
					getNextCmd =  false;
					break;

				case "RSET":
					RSET(argsText);
					getNextCmd = false;
					break;

			//	case "VRFY":
			//		VRFY();
			//		break;

			//	case "EXPN":
			//		EXPN();
			//		break;

				case "HELP":
					HELP();
					break;

				case "NOOP":
					NOOP();
					getNextCmd = false;
				break;
				
				case "QUIT":
					QUIT(argsText);
					getNextCmd = false;
					return true;
										
				default:					
					m_pSocket.SendLine("500 command unrecognized");

					//---- Check that maximum bad commands count isn't exceeded ---------------//
					if(m_BadCmdCount > m_pServer.MaxBadCommands-1){
						m_pSocket.SendLine("421 Too many bad commands, closing transmission channel");
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


		#region function HELO

		private void HELO(string argsText)
		{
			/* Rfc 2821 4.1.1.1
			These commands, and a "250 OK" reply to one of them, confirm that
			both the SMTP client and the SMTP server are in the initial state,
			that is, there is no transaction in progress and all state tables and
			buffers are cleared.
			
			Syntax:
				 "HELO" SP Domain CRLF
			*/

			m_EhloName = argsText;

			ResetState();

			m_pSocket.BeginSendLine("250 " + m_pServer.HostName + " Hello [" + this.RemoteEndPoint.Address.ToString() + "]",new SocketCallBack(this.EndSend));
			m_CmdValidator.Helo_ok = true;
		}

		#endregion

		#region function EHLO

		private void EHLO(string argsText)
		{		
			/* Rfc 2821 4.1.1.1
			These commands, and a "250 OK" reply to one of them, confirm that
			both the SMTP client and the SMTP server are in the initial state,
			that is, there is no transaction in progress and all state tables and
			buffers are cleared.
			*/

			m_EhloName = argsText;

			ResetState();

			//--- Construct supported AUTH types value ----------------------------//
			string authTypesString = "";
			if((m_pServer.SupportedAuthentications & SaslAuthTypes.Login) != 0){
				authTypesString += "LOGIN ";
			}
			if((m_pServer.SupportedAuthentications & SaslAuthTypes.Cram_md5) != 0){
				authTypesString += "CRAM-MD5 ";
			}
			if((m_pServer.SupportedAuthentications & SaslAuthTypes.Digest_md5) != 0){
				authTypesString += "DIGEST-MD5 ";
			}
			authTypesString = authTypesString.Trim();
			//-----------------------------------------------------------------------//

			string reply = "";
				reply += "250-" + m_pServer.HostName + " Hello [" + this.RemoteEndPoint.Address.ToString() + "]\r\n";
				reply += "250-PIPELINING\r\n";
				reply += "250-SIZE " + m_pServer.MaxMessageSize + "\r\n";
		//		reply += "250-DSN\r\n";
		//		reply += "250-HELP\r\n";
				reply += "250-8BITMIME\r\n";
				reply += "250-BINARYMIME\r\n";
				reply += "250-CHUNKING\r\n";
				if(authTypesString.Length > 0){	
					reply += "250-AUTH " + authTypesString +  "\r\n";
				}
			    reply += "250 Ok\r\n";
			
			m_pSocket.BeginSendData(reply,null,new SocketCallBack(this.EndSend));
				
			m_CmdValidator.Helo_ok = true;
		}

		#endregion

		#region function AUTH

		private void AUTH(string argsText)
		{
			/* Rfc 2554 AUTH --------------------------------------------------//
			Restrictions:
		         After an AUTH command has successfully completed, no more AUTH
				 commands may be issued in the same session.  After a successful
				 AUTH command completes, a server MUST reject any further AUTH
				 commands with a 503 reply.
				 
			Remarks: 
				If an AUTH command fails, the server MUST behave the same as if
				the client had not issued the AUTH command.
			*/
			if(m_Authenticated){
				m_pSocket.SendLine("503 already authenticated");
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
					m_pSocket.SendLine("504 Unrecognized authentication type.");
					break;

				case "LOGIN":

					#region LOGIN authentication

				    //---- AUTH = LOGIN ------------------------------
					/* Login
					C: AUTH LOGIN-MD5
					S: 334 VXNlcm5hbWU6
					C: username_in_base64
					S: 334 UGFzc3dvcmQ6
					C: password_in_base64
					
					   VXNlcm5hbWU6 base64_decoded= USERNAME
					   UGFzc3dvcmQ6 base64_decoded= PASSWORD
					*/
					// Note: all strings are base64 strings eg. VXNlcm5hbWU6 = UserName.
			
					
					// Query UserName
					m_pSocket.SendLine("334 VXNlcm5hbWU6");

					string userNameLine = m_pSocket.ReadLine();
					// Encode username from base64
					if(userNameLine.Length > 0){
						userName = System.Text.Encoding.Default.GetString(Convert.FromBase64String(userNameLine));
					}
						
					// Query Password
					m_pSocket.SendLine("334 UGFzc3dvcmQ6");

					string passwordLine = m_pSocket.ReadLine();
					// Encode password from base64
					if(passwordLine.Length > 0){
						password = System.Text.Encoding.Default.GetString(Convert.FromBase64String(passwordLine));
					}
					
					aArgs = m_pServer.OnAuthUser(this,userName,password,"",AuthType.Plain);
					if(aArgs.Validated){
						m_pSocket.SendLine("235 Authentication successful.");
						m_Authenticated = true;
						m_UserName = userName;
					}
					else{
						m_pSocket.SendLine("535 Authentication failed");
					}

					#endregion

					break;

				case "CRAM-MD5":
					
					#region CRAM-MD5 authentication

					/* Cram-M5
					C: AUTH CRAM-MD5
					S: 334 <md5_calculation_hash_in_base64>
					C: base64(username password_hash)
					*/
					
					string md5Hash = "<" + Guid.NewGuid().ToString().ToLower() + ">";
					m_pSocket.SendLine("334 " + Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(md5Hash)));

					string reply = m_pSocket.ReadLine();

					reply = System.Text.Encoding.Default.GetString(Convert.FromBase64String(reply));
					string[] replyArgs = reply.Split(' ');
					userName = replyArgs[0];
					
					aArgs = m_pServer.OnAuthUser(this,userName,replyArgs[1],md5Hash,AuthType.CRAM_MD5);
					if(aArgs.Validated){
						m_pSocket.SendLine("235 Authentication successful.");
						m_Authenticated = true;
						m_UserName = userName;
					}
					else{
						m_pSocket.SendLine("535 Authentication failed");
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
					 * S: 334 base64(realm="elwood.innosoft.com",nonce="OA6MG9tEQGm2hh",qop="auth",algorithm=md5-sess)
					 * C: base64(username="chris",realm="elwood.innosoft.com",nonce="OA6MG9tEQGm2hh",
					 *    nc=00000001,cnonce="OA6MHXh6VqTrRk",digest-uri="imap/elwood.innosoft.com",
                     *    response=d388dad90d4bbd760a152321f2143af7,qop=auth)
					 * S: 334 base64(rspauth=ea40f60335c427b5527b84dbabcdfffd)
					 * C:
					 * S: 235 Authentication successful.
					*/

					string realm = m_pServer.HostName;
					string nonce = AuthHelper.GenerateNonce();

					m_pSocket.SendLine("334 " + AuthHelper.Base64en(AuthHelper.Create_Digest_Md5_ServerResponse(realm,nonce)));

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
							m_pSocket.SendLine("334 " + AuthHelper.Base64en("rspauth=" + aArgs.ReturnData));
					
							// We must got empty line here
							clientResponse = m_pSocket.ReadLine();
							if(clientResponse == ""){
								m_pSocket.SendLine("235 Authentication successful.");
								m_Authenticated = true;
								m_UserName = userName;
							}
							else{
								m_pSocket.SendLine("535 Authentication failed");
							}
						}
						else{
							m_pSocket.SendLine("535 Authentication failed");
						}
					}
					else{
						m_pSocket.SendLine("535 Authentication failed");
					}
				
					#endregion

					break;

				default:
					m_pSocket.SendLine("504 Unrecognized authentication type.");
					break;
			}
			//-----------------------------------------------------------------//
		}

		#endregion

		#region function MAIL

		private void MAIL(string argsText)
		{
			/* RFC 2821 3.3
			NOTE:
				This command tells the SMTP-receiver that a new mail transaction is
				starting and to reset all its state tables and buffers, including any
				recipients or mail data.  The <reverse-path> portion of the first or
				only argument contains the source mailbox (between "<" and ">"
				brackets), which can be used to report errors (see section 4.2 for a
				discussion of error reporting).  If accepted, the SMTP server returns
				 a 250 OK reply.
				 
				MAIL FROM:<reverse-path> [SP <mail-parameters> ] <CRLF>
				reverse-path = "<" [ A-d-l ":" ] Mailbox ">"
				Mailbox = Local-part "@" Domain
				
				body-value ::= "7BIT" / "8BITMIME" / "BINARYMIME"
				
				Examples:
					C: MAIL FROM:<ned@thor.innosoft.com>
					C: MAIL FROM:<ned@thor.innosoft.com> SIZE=500000 BODY=8BITMIME AUTH=xxxx
			*/

			if(!m_CmdValidator.MayHandle_MAIL){
				if(m_CmdValidator.MailFrom_ok){
					m_pSocket.BeginSendLine("503 Sender already specified",new SocketCallBack(this.EndSend));
				}
				else{
					m_pSocket.BeginSendLine("503 Bad sequence of commands",new SocketCallBack(this.EndSend));
				}
				return;
			}

			//------ Parse parameters -------------------------------------------------------------------//
			string   senderEmail  = "";
			long     messageSize  = 0;
			BodyType bodyType     = BodyType.x7_bit;
			bool     isFromParam  = false;

			// Parse while all params parsed or while is breaked
			while(argsText.Length > 0){
				if(argsText.ToLower().StartsWith("from:")){
					// Remove from:
                    argsText = argsText.Substring(5).Trim();

					// If there is more parameters
					if(argsText.IndexOf(" ") > -1){
						senderEmail = argsText.Substring(0,argsText.IndexOf(" "));
						argsText = argsText.Substring(argsText.IndexOf(" ")).Trim();
					}
					else{
						senderEmail = argsText;
						argsText = "";
					}

					// If address between <>, remove <>
					if(senderEmail.StartsWith("<") && senderEmail.EndsWith(">")){
						senderEmail = senderEmail.Substring(1,senderEmail.Length - 2);
					}

					isFromParam = true;
				}
				else if(argsText.ToLower().StartsWith("size=")){
					// Remove size=
					argsText = argsText.Substring(5).Trim();

					string sizeS = "";
					// If there is more parameters
					if(argsText.IndexOf(" ") > -1){
						sizeS = argsText.Substring(0,argsText.IndexOf(" "));
						argsText  = argsText.Substring(argsText.IndexOf(" ")).Trim();
					}
					else{
						sizeS = argsText;
						argsText  = "";
					}

					// See if value ok
					if(Core.IsNumber(sizeS)){
						messageSize = Convert.ToInt64(sizeS);
					}
					else{
						m_pSocket.BeginSendLine("501 SIZE parameter value is invalid. Syntax:{MAIL FROM:<address> [SIZE=msgSize] [BODY=8BITMIME]}",new SocketCallBack(this.EndSend));
						return;
					}
				}
				else if(argsText.ToLower().StartsWith("body=")){
					// Remove body=
					argsText = argsText.Substring(5).Trim();

					string bodyTypeS = "";
					// If there is more parameters
					if(argsText.IndexOf(" ") > -1){
						bodyTypeS = argsText.Substring(0,argsText.IndexOf(" "));
						argsText = argsText.Substring(argsText.IndexOf(" ")).Trim();
					}
					else{
						bodyTypeS = argsText;
						argsText = "";
					}

					// See if value ok
					switch(bodyTypeS.ToUpper())
					{						
						case "7BIT":
							bodyType = BodyType.x7_bit;
							break;
						case "8BITMIME":
							bodyType = BodyType.x8_bit;
							break;
						case "BINARYMIME":
							bodyType = BodyType.binary;									
							break;
						default:
							m_pSocket.BeginSendLine("501 BODY parameter value is invalid. Syntax:{MAIL FROM:<address> [BODY=(7BIT/8BITMIME)]}",new SocketCallBack(this.EndSend));
							return;					
					}
				}
				else if(argsText.ToLower().StartsWith("auth=")){
					// Currently just eat AUTH keyword

					// Remove auth=
					argsText = argsText.Substring(5).Trim();

					string authS = "";
					// If there is more parameters
					if(argsText.IndexOf(" ") > -1){
						authS = argsText.Substring(0,argsText.IndexOf(" "));
						argsText  = argsText.Substring(argsText.IndexOf(" ")).Trim();
					}
					else{
						authS = argsText;
						argsText  = "";
					}
				}
				else{
					m_pSocket.BeginSendLine("501 Error in parameters. Syntax:{MAIL FROM:<address> [SIZE=msgSize] [BODY=8BITMIME]}",new SocketCallBack(this.EndSend));
					return;
				}
			}
			
			// If required parameter 'FROM:' is missing
			if(!isFromParam){
				m_pSocket.BeginSendLine("501 Required param FROM: is missing. Syntax:{MAIL FROM:<address> [SIZE=msgSize] [BODY=8BITMIME]}",new SocketCallBack(this.EndSend));
				return;
			}

			// Parse sender's email address
		//	senderEmail = reverse_path;
			//---------------------------------------------------------------------------------------------//
			
			//--- Check message size
			if(m_pServer.MaxMessageSize > messageSize){
				// Check if sender is ok
				ValidateSender_EventArgs eArgs = m_pServer.OnValidate_MailFrom(this,senderEmail,senderEmail);
				if(eArgs.Validated){															
					// See note above
					ResetState();

					// Store reverse path
					m_Reverse_path = senderEmail;
					m_CmdValidator.MailFrom_ok = true;

					//-- Store params
					m_BodyType = bodyType;

					m_pSocket.BeginSendLine("250 OK <" + senderEmail + "> Sender ok",new SocketCallBack(this.EndSend));
				}			
				else{
					if(eArgs.ErrorText != null && eArgs.ErrorText.Length > 0){
						m_pSocket.BeginSendLine("550 " + eArgs.ErrorText,new SocketCallBack(this.EndSend));
					}
					else{
						m_pSocket.BeginSendLine("550 You are refused to send mail here",new SocketCallBack(this.EndSend));
					}
				}
			}
			else{
				m_pSocket.BeginSendLine("552 Message exceeds allowed size",new SocketCallBack(this.EndSend));
			}			
		}

		#endregion

		#region function RCPT

		private void RCPT(string argsText)
		{
			/* RFC 2821 4.1.1.3 RCPT
			NOTE:
				This command is used to identify an individual recipient of the mail
				data; multiple recipients are specified by multiple use of this
				command.  The argument field contains a forward-path and may contain
				optional parameters.
				
				Relay hosts SHOULD strip or ignore source routes, and
				names MUST NOT be copied into the reverse-path.  
				
				Example:
					RCPT TO:<@hosta.int,@jkl.org:userc@d.bar.org>

					will normally be sent directly on to host d.bar.org with envelope
					commands

					RCPT TO:<userc@d.bar.org>
					RCPT TO:<userc@d.bar.org> SIZE=40000
						
				RCPT TO:<forward-path> [ SP <rcpt-parameters> ] <CRLF>			
			*/

			/* RFC 2821 3.3
				If a RCPT command appears without a previous MAIL command, 
				the server MUST return a 503 "Bad sequence of commands" response.
			*/
			if(!m_CmdValidator.MayHandle_RCPT || m_BDat){
				m_pSocket.BeginSendLine("503 Bad sequence of commands",new SocketCallBack(this.EndSend));
				return;
			}

			// Check that recipient count isn't exceeded
			if(m_Forward_path.Count > m_pServer.MaxRecipients){
				m_pSocket.BeginSendLine("452 Too many recipients",new SocketCallBack(this.EndSend));
				return;
			}

			//------ Parse parameters -------------------------------------------------------------------//
			string recipientEmail = "";
			long   messageSize    = 0;
			bool   isToParam      = false;

			// Parse while all params parsed or while is breaked
			while(argsText.Length > 0){
				if(argsText.ToLower().StartsWith("to:")){
					// Remove to:
                    argsText = argsText.Substring(3).Trim();

					// If there is more parameters
					if(argsText.IndexOf(" ") > -1){
						recipientEmail = argsText.Substring(0,argsText.IndexOf(" "));
						argsText = argsText.Substring(argsText.IndexOf(" ")).Trim();
					}
					else{
						recipientEmail = argsText;
						argsText = "";
					}

					// If address between <>, remove <>
					if(recipientEmail.StartsWith("<") && recipientEmail.EndsWith(">")){
						recipientEmail = recipientEmail.Substring(1,recipientEmail.Length - 2);
					}

					// See if value ok
					if(recipientEmail.Length == 0){
						m_pSocket.BeginSendLine("501 Recipient address isn't specified. Syntax:{RCPT TO:<address> [SIZE=msgSize]}",new SocketCallBack(this.EndSend));
						return;
					}

					isToParam = true;
				}
				else if(argsText.ToLower().StartsWith("size=")){
					// Remove size=
					argsText = argsText.Substring(5).Trim();

					string sizeS = "";
					// If there is more parameters
					if(argsText.IndexOf(" ") > -1){
						sizeS = argsText.Substring(0,argsText.IndexOf(" "));
						argsText  = argsText.Substring(argsText.IndexOf(" ")).Trim();
					}
					else{
						sizeS = argsText;
						argsText  = "";
					}

					// See if value ok
					if(Core.IsNumber(sizeS)){
						messageSize = Convert.ToInt64(sizeS);
					}
					else{
						m_pSocket.BeginSendLine("501 SIZE parameter value is invalid. Syntax:{RCPT TO:<address> [SIZE=msgSize]}",new SocketCallBack(this.EndSend));
						return;
					}
				}
				else{
					m_pSocket.BeginSendLine("501 Error in parameters. Syntax:{RCPT TO:<address> [SIZE=msgSize]}",new SocketCallBack(this.EndSend));
					return;
				}
			}

			//--- regex param parse strings
		/*	string[] exps = new string[2];
			exps[0] = @"(?<param>TO)[\s]{0,}:\s{0,}<?\s{0,}(?<value>[\w\@\.\-\*\+\=\#\/]*)\s{0,}>?(\s|$)";
			exps[1] = @"(?<param>SIZE)[\s]{0,}=\s{0,}(?<value>[\w]*)(\s|$)"; 

			_Parameter[] param = _ParamParser.Paramparser_NameValue(argsText,exps);
			foreach(_Parameter parameter in param){
				// Possible params:
				// TO:
				// SIZE=				
				switch(parameter.ParamName.ToUpper()) // paramInf[0] because of param syntax: pramName =/: value
				{
					//------ Required paramters -----//
					case "TO":
						if(parameter.ParamValue.Length == 0){
							m_pSocket.BeginSendLine("501 Recipient address isn't specified. Syntax:{RCPT TO:<address> [SIZE=msgSize]}",new SocketCallBack(this.EndSend));
							return;
						}
						else{
							forward_path = parameter.ParamValue;
							isToParam = true;
						}
						break;

					//------ Optional parameters ---------------------//
					case "SIZE":
						if(parameter.ParamValue.Length == 0){
							m_pSocket.BeginSendLine("501 Size parameter isn't specified. Syntax:{RCPT TO:<address> [SIZE=msgSize]}",new SocketCallBack(this.EndSend));
							return;
						}
						else{
							if(Core.IsNumber(parameter.ParamValue)){
								messageSize = Convert.ToInt64(parameter.ParamValue);
							}
							else{
								m_pSocket.BeginSendLine("501 SIZE parameter value is invalid. Syntax:{RCPT TO:<address> [SIZE=msgSize]}",new SocketCallBack(this.EndSend));
								return;
							}
						}
						break;

					default:
						m_pSocket.BeginSendLine("501 Error in parameters. Syntax:{RCPT TO:<address> [SIZE=msgSize]}",new SocketCallBack(this.EndSend));
						return;
				}
			}*/
			
			// If required parameter 'TO:' is missing
			if(!isToParam){
				m_pSocket.BeginSendLine("501 Required param TO: is missing. Syntax:<RCPT TO:{address> [SIZE=msgSize]}",new SocketCallBack(this.EndSend));
				return;
			}

			// Parse recipient's email address
		//	recipientEmail = forward_path;
			//---------------------------------------------------------------------------------------------//

			// Check message size
			if(m_pServer.MaxMessageSize > messageSize){
				// Check if email address is ok
				ValidateRecipient_EventArgs rcpt_args = m_pServer.OnValidate_MailTo(this,recipientEmail,recipientEmail,m_Authenticated);
				if(rcpt_args.Validated){
					// Check if mailbox size isn't exceeded
					if(m_pServer.Validate_MailBoxSize(this,recipientEmail,messageSize)){
						// Store reciptient
						if(!m_Forward_path.Contains(recipientEmail)){
							m_Forward_path.Add(recipientEmail,recipientEmail);
						}				
						m_CmdValidator.RcptTo_ok = true;

						m_pSocket.BeginSendLine("250 OK <" + recipientEmail + "> Recipient ok",new SocketCallBack(this.EndSend));						
					}
					else{					
						m_pSocket.BeginSendLine("552 Mailbox size limit exceeded",new SocketCallBack(this.EndSend));
					}
				}
				// Recipient rejected
				else{
					if(rcpt_args.LocalRecipient){
						m_pSocket.BeginSendLine("550 <" + recipientEmail + "> No such user here",new SocketCallBack(this.EndSend));
					}
					else{
						m_pSocket.BeginSendLine("550 <" + recipientEmail + "> Relay not allowed",new SocketCallBack(this.EndSend));
					}
				}
			}
			else{
				m_pSocket.BeginSendLine("552 Message exceeds allowed size",new SocketCallBack(this.EndSend));
			}
		}

		#endregion

		#region function DATA

		#region method BeginDataCmd

		private void BeginDataCmd(string argsText)
		{	
			/* RFC 2821 4.1.1
			NOTE:
				Several commands (RSET, DATA, QUIT) are specified as not permitting
				parameters.  In the absence of specific extensions offered by the
				server and accepted by the client, clients MUST NOT send such
				parameters and servers SHOULD reject commands containing them as
				having invalid syntax.
			*/

			if(argsText.Length > 0){
				m_pSocket.BeginSendLine("500 Syntax error. Syntax:{DATA}",new SocketCallBack(this.EndSend));
				return;
			}


			/* RFC 2821 4.1.1.4 DATA
			NOTE:
				If accepted, the SMTP server returns a 354 Intermediate reply and
				considers all succeeding lines up to but not including the end of
				mail data indicator to be the message text.  When the end of text is
				successfully received and stored the SMTP-receiver sends a 250 OK
				reply.
				
				The mail data is terminated by a line containing only a period, that
				is, the character sequence "<CRLF>.<CRLF>" (see section 4.5.2).  This
				is the end of mail data indication.
					
				
				When the SMTP server accepts a message either for relaying or for
				final delivery, it inserts a trace record (also referred to
				interchangeably as a "time stamp line" or "Received" line) at the top
				of the mail data.  This trace record indicates the identity of the
				host that sent the message, the identity of the host that received
				the message (and is inserting this time stamp), and the date and time
				the message was received.  Relayed messages will have multiple time
				stamp lines.  Details for formation of these lines, including their
				syntax, is specified in section 4.4.
   
			*/


			/* RFC 2821 DATA
			NOTE:
				If there was no MAIL, or no RCPT, command, or all such commands
				were rejected, the server MAY return a "command out of sequence"
				(503) or "no valid recipients" (554) reply in response to the DATA
				command.
			*/
			if(!m_CmdValidator.MayHandle_DATA || m_BDat){
				m_pSocket.BeginSendLine("503 Bad sequence of commands",new SocketCallBack(this.EndSend));
				return;
			}

			if(m_Forward_path.Count == 0){
				m_pSocket.BeginSendLine("554 no valid recipients given",new SocketCallBack(this.EndSend));
				return;
			}

			// reply: 354 Start mail input
			m_pSocket.SendLine("354 Start mail input; end with <CRLF>.<CRLF>");

			//---- Construct server headers for message----------------------------------------------------------------//
			string header  = "Received: from " + Core.GetHostName(this.RemoteEndPoint.Address) + " (" + this.RemoteEndPoint.Address.ToString() + ")\r\n"; 
			header += "\tby " + m_pServer.HostName + " with SMTP; " + DateTime.Now.ToUniversalTime().ToString("r",System.Globalization.DateTimeFormatInfo.InvariantInfo) + "\r\n";
					    
			byte[] headers = System.Text.Encoding.ASCII.GetBytes(header);
			m_pMsgStream.Write(headers,0,headers.Length);
			//---------------------------------------------------------------------------------------------------------//

            // Begin recieving data
			m_pSocket.BeginReadData(m_pMsgStream,m_pServer.MaxMessageSize,"\r\n.\r\n",".\r\n",null,new SocketCallBack(this.EndDataCmd));		
		}

		#endregion

		#region method EndDataCmd

		/// <summary>
		/// Is called when DATA command is finnished.
		/// </summary>
		/// <param name="result"></param>
		/// <param name="count"></param>
		/// <param name="exception"></param>
		/// <param name="tag"></param>
		private void EndDataCmd(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			try{
			//	if(m_pServer.LogCommands){
			//		m_pLogWriter.AddEntry("big binary " + count.ToString() + " bytes",this.SessionID,this.RemoteEndPoint.Address.ToString(),"S");
			//	}

				switch(result)
				{
					case SocketCallBackResult.Ok:
						using(MemoryStream msgStream = Core.DoPeriodHandling(m_pMsgStream,false)){
					//		if(Core.ScanInvalid_CR_or_LF(msgStream)){
					//			m_pSocket.BeginSendLine("500 Message contains invalid CR or LF combination.",new SocketCallBack(this.EndSend));
					//		}
					//		else{
								m_pMsgStream.SetLength(0);
								
								// Store message
								m_pMsgStream.Position = 0;
								NewMail_EventArgs oArg = m_pServer.OnStoreMessage(this,msgStream);

								// There is custom reply text, send it
								if(oArg.ReplyText.Length > 0){
									m_pSocket.BeginSendLine("250 " + oArg.ReplyText,new SocketCallBack(this.EndSend));
								}
								else{
									m_pSocket.BeginSendLine("250 OK",new SocketCallBack(this.EndSend));
								}
					//		}
						}						
						break;

					case SocketCallBackResult.LengthExceeded:
						m_pSocket.BeginSendLine("552 Requested mail action aborted: exceeded storage allocation",new SocketCallBack(this.EndSend));
						break;

					case SocketCallBackResult.SocketClosed:
						EndSession();
						return;

					case SocketCallBackResult.Exception:
						OnError(exception);
						return;
				}

				/* RFC 2821 4.1.1.4 DATA
					NOTE:
						Receipt of the end of mail data indication requires the server to
						process the stored mail transaction information.  This processing
						consumes the information in the reverse-path buffer, the forward-path
						buffer, and the mail data buffer, and on the completion of this
						command these buffers are cleared.
				*/
				ResetState();

				// Command completed ok, get next command
			//	BeginRecieveCmd();
			}
			catch(Exception x){
				OnError(x);
			}
		}

		#endregion
		
		#endregion

		#region function BDAT

		#region method BeginBDATCmd

		private void BeginBDATCmd(string argsText)
		{
			/*RFC 3030 2
				The BDAT verb takes two arguments.  The
				first argument indicates the length, in octets, of the binary data
				chunk.  The second optional argument indicates that the data chunk
				is the last.
				
				The message data is sent immediately after the trailing <CR>
				<LF> of the BDAT command line.  Once the receiver-SMTP receives the
				specified number of octets, it will return a 250 reply code.

				The optional LAST parameter on the BDAT command indicates that this
				is the last chunk of message data to be sent.  The last BDAT command
				MAY have a byte-count of zero indicating there is no additional data
				to be sent.  Any BDAT command sent after the BDAT LAST is illegal and
				MUST be replied to with a 503 "Bad sequence of commands" reply code.
				The state resulting from this error is indeterminate.  A RSET command
				MUST be sent to clear the transaction before continuing.
				
				A 250 response MUST be sent to each successful BDAT data block within
				a mail transaction.

				bdat-cmd   ::= "BDAT" SP chunk-size [ SP end-marker ] CR LF
				chunk-size ::= 1*DIGIT
				end-marker ::= "LAST"
			*/

			if(!m_CmdValidator.MayHandle_BDAT){
				m_pSocket.BeginSendLine("503 Bad sequence of commands",new SocketCallBack(this.EndSend));
				return;
			}

			string[] param = argsText.Split(new char[]{' '});
			if(param.Length > 0 && param.Length < 3){				
				if(Core.IsNumber(param[0])){
					// LAST specified
					bool lastChunk = false;
					if(param.Length == 2){
						lastChunk = true;
					}
					
					// Add header to first bdat block only
					if(!m_BDat){
						//---- Construct server headers for message----------------------------------------------------------------//
						string header  = "Received: from " + Core.GetHostName(this.RemoteEndPoint.Address) + " (" + this.RemoteEndPoint.Address.ToString() + ")\r\n"; 
						header += "\tby " + m_pServer.HostName + " with SMTP; " + DateTime.Now.ToUniversalTime().ToString("r",System.Globalization.DateTimeFormatInfo.InvariantInfo) + "\r\n";
					    
						byte[] headers = System.Text.Encoding.ASCII.GetBytes(header);
						m_pMsgStream.Write(headers,0,headers.Length);
						//---------------------------------------------------------------------------------------------------------//
					}

					// Begin recieving data
					m_pSocket.BeginReadData(m_pMsgStream,Convert.ToInt64(param[0]),m_pServer.MaxMessageSize - m_pMsgStream.Length,lastChunk,new SocketCallBack(this.EndBDatCmd));

					m_BDat = true;
				}
				else{
					m_pSocket.BeginSendLine("500 Syntax error. Syntax:{BDAT chunk-size [LAST]}",new SocketCallBack(this.EndSend));
				}
			}
			else{
				m_pSocket.BeginSendLine("500 Syntax error. Syntax:{BDAT chunk-size [LAST]}",new SocketCallBack(this.EndSend));
			}		
		}

		#endregion

		#region method EndBDatCmd

		private void EndBDatCmd(SocketCallBackResult result,long count,Exception exception,object tag)
		{
			try{
			//	if(m_pServer.LogCommands){
			//		m_pLogWriter.AddEntry("big binary " + count.ToString() + " bytes",this.SessionID,this.RemoteEndPoint.Address.ToString(),"S");
			//	}

				switch(result)
				{
					case SocketCallBackResult.Ok:
						// BDAT command completed, got all data junks
						if((bool)tag){
					//		if(Core.ScanInvalid_CR_or_LF(m_pMsgStream)){
					//			m_pSocket.BeginSendLine("500 Message contains invalid CR or LF combination.",new SocketCallBack(this.EndSend));
					//		}
					//		else{								
								// Store message
								m_pMsgStream.Position = 0;
								NewMail_EventArgs oArg = m_pServer.OnStoreMessage(this,m_pMsgStream);

								// There is custom reply text, send it
								if(oArg.ReplyText.Length > 0){
									m_pSocket.BeginSendLine("250 " + oArg.ReplyText,new SocketCallBack(this.EndSend));
								}
								else{
									m_pSocket.BeginSendLine("250 Message stored OK.",new SocketCallBack(this.EndSend));
								}
					//		}

							/* RFC 2821 4.1.1.4 DATA
							NOTE:
								Receipt of the end of mail data indication requires the server to
								process the stored mail transaction information.  This processing
								consumes the information in the reverse-path buffer, the forward-path
								buffer, and the mail data buffer, and on the completion of this
								command these buffers are cleared.
							*/
							ResetState();
						
							m_BDat = false;
						}
						else{
							m_pSocket.BeginSendLine("250 Data block recieved OK.",new SocketCallBack(this.EndSend));
						}
						break;

					case SocketCallBackResult.LengthExceeded:
						m_pSocket.BeginSendLine("552 Requested mail action aborted: exceeded storage allocation",new SocketCallBack(this.EndSend));
						break;

					case SocketCallBackResult.SocketClosed:
						EndSession();
						return;

					case SocketCallBackResult.Exception:
						OnError(exception);
						return;
				}				

				// Command completed ok, get next command
			//	BeginRecieveCmd();
			}
			catch(Exception x){
				OnError(x);
			}
		}

		#endregion

		#endregion

		#region function RSET

		private void RSET(string argsText)
		{
			/* RFC 2821 4.1.1
			NOTE:
				Several commands (RSET, DATA, QUIT) are specified as not permitting
				parameters.  In the absence of specific extensions offered by the
				server and accepted by the client, clients MUST NOT send such
				parameters and servers SHOULD reject commands containing them as
				having invalid syntax.
			*/

			if(argsText.Length > 0){
				m_pSocket.BeginSendLine("500 Syntax error. Syntax:{RSET}",new SocketCallBack(this.EndSend));
				return;
			}

			/* RFC 2821 4.1.1.5 RESET (RSET)
			NOTE:
				This command specifies that the current mail transaction will be
				aborted.  Any stored sender, recipients, and mail data MUST be
				discarded, and all buffers and state tables cleared.  The receiver
				MUST send a "250 OK" reply to a RSET command with no arguments.
			*/
			
			ResetState();

			m_pSocket.BeginSendLine("250 OK",new SocketCallBack(this.EndSend));
		}

		#endregion

		#region function VRFY

		private void VRFY()
		{
			/* RFC 821 VRFY 
			Example:
				S: VRFY Lumi
				R: 250 Ivar Lumi <ivx@lumisoft.ee>
				
				S: VRFY lum
				R: 550 String does not match anything.			 
			*/

			// ToDo: Parse user, add new event for cheking user

		//	SendData("250 OK\r\n");

			m_pSocket.BeginSendLine("502 Command not implemented",new SocketCallBack(this.EndSend));
		}

		#endregion

		#region function NOOP

		private void NOOP()
		{
			/* RFC 2821 4.1.1.9 NOOP (NOOP)
			NOTE:
				This command does not affect any parameters or previously entered
				commands.  It specifies no action other than that the receiver send
				an OK reply.
			*/

			m_pSocket.BeginSendLine("250 OK",new SocketCallBack(this.EndSend));
		}

		#endregion

		#region function QUIT

		private void QUIT(string argsText)
		{
			/* RFC 2821 4.1.1
			NOTE:
				Several commands (RSET, DATA, QUIT) are specified as not permitting
				parameters.  In the absence of specific extensions offered by the
				server and accepted by the client, clients MUST NOT send such
				parameters and servers SHOULD reject commands containing them as
				having invalid syntax.
			*/

			if(argsText.Length > 0){
				m_pSocket.BeginSendLine("500 Syntax error. Syntax:<QUIT>",new SocketCallBack(this.EndSend));
				return;
			}

			/* RFC 2821 4.1.1.10 QUIT (QUIT)
			NOTE:
				This command specifies that the receiver MUST send an OK reply, and
				then close the transmission channel.
			*/

			// reply: 221 - Close transmission cannel
			m_pSocket.SendLine("221 Service closing transmission channel");
		//	m_pSocket.BeginSendLine("221 Service closing transmission channel",null);	
		}

		#endregion


		//---- Optional commands
		
		#region function EXPN

		private void EXPN()
		{
			/* RFC 821 EXPN 
			NOTE:
				This command asks the receiver to confirm that the argument
				identifies a mailing list, and if so, to return the
				membership of that list.  The full name of the users (if
				known) and the fully specified mailboxes are returned in a
				multiline reply.
			
			Example:
				S: EXPN lsAll
				R: 250-ivar lumi <ivx@lumisoft.ee>
				R: 250-<willy@lumisoft.ee>
				R: 250 <kaido@lumisoft.ee>
			*/

	//		SendData("250 OK\r\n");

			m_pSocket.SendLine("502 Command not implemented");
		}

		#endregion

		#region function HELP

		private void HELP()
		{
			/* RFC 821 HELP
			NOTE:
				This command causes the receiver to send helpful information
				to the sender of the HELP command.  The command may take an
				argument (e.g., any command name) and return more specific
				information as a response.
			*/

	//		SendData("250 OK\r\n");

			m_pSocket.SendLine("502 Command not implemented");
		}

		#endregion


		#region function ResetState

		private void ResetState()
		{
			//--- Reset variables
			m_BodyType = BodyType.x7_bit;
			m_Forward_path.Clear();
			m_Reverse_path  = "";
	//		m_Authenticated = false; // ??? must clear or not, no info.
			m_CmdValidator.Reset();
			m_CmdValidator.Helo_ok = true;

			m_pMsgStream.SetLength(0);
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
		/// Gets client reported EHLO/HELO name.
		/// </summary>
		public string EhloName
		{
			get{ return m_EhloName; }
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
		/// Gets body type.
		/// </summary>
		public BodyType BodyType
		{
			get{ return m_BodyType; }
		}

		/// <summary>
		/// Gets local EndPoint which accepted client(connected host).
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
		/// Gets sender.
		/// </summary>
		public string MailFrom
		{
			get{ return m_Reverse_path; }
		}

		/// <summary>
		/// Gets recipients.
		/// </summary>
		public string[] MailTo
		{
			get{
				string[] to = new string[m_Forward_path.Count];
				m_Forward_path.Values.CopyTo(to,0);

				return to; 
			}
		}

		/// <summary>
		/// Gets session start time.
		/// </summary>
		public DateTime SessionStartTime
		{
			get{ return m_SessionStart; }
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
