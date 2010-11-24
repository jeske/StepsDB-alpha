using System;
using System.IO;
using System.ComponentModel;
using System.Collections;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
//using System.Windows.Forms;
using LumiSoft.Net;
using LumiSoft.Net.AUTH;

namespace LumiSoft.Net.SMTP.Server
{	
	#region Event delegates

	/// <summary>
	/// Represents the method that will handle the AuthUser event for SMTP_Server.
	/// </summary>
	/// <param name="sender">The source of the event. </param>
	/// <param name="e">A AuthUser_EventArgs that contains the event data.</param>
	public delegate void AuthUserEventHandler(object sender,AuthUser_EventArgs e);

	/// <summary>
	/// Represents the method that will handle the ValidateMailFrom event for POP3_Server.
	/// </summary>
	/// <param name="sender">The source of the event. </param>
	/// <param name="e">A ValidateSender_EventArgs that contains the event data.</param>
	public delegate void ValidateMailFromHandler(object sender,ValidateSender_EventArgs e);

	/// <summary>
	/// Represents the method that will handle the ValidateMailTo event for POP3_Server.
	/// </summary>
	/// <param name="sender">The source of the event. </param>
	/// <param name="e">A ValidateRecipient_EventArgs that contains the event data.</param>
	public delegate void ValidateMailToHandler(object sender,ValidateRecipient_EventArgs e);

	/// <summary>
	/// Represents the method that will handle the ValidateMailboxSize event for POP3_Server.
	/// </summary>
	/// <param name="sender">The source of the event. </param>
	/// <param name="e">A ValidateMailboxSize_EventArgs that contains the event data.</param>
	public delegate void ValidateMailboxSize(object sender,ValidateMailboxSize_EventArgs e);

	/// <summary>
	/// Represents the method that will handle the StoreMessage event for POP3_Server.
	/// </summary>
	/// <param name="sender">The source of the event. </param>
	/// <param name="e">A NewMail_EventArgs that contains the event data.</param>
	public delegate void NewMailEventHandler(object sender,NewMail_EventArgs e);

	#endregion
	
	/// <summary>
	/// SMTP server component.
	/// </summary>
	public class SMTP_Server : SocketServer
	{				
		private int           m_MaxMessageSize = 1000000; 
		private int           m_MaxRecipients  = 100; 
		private SaslAuthTypes m_SupportedAuth  = SaslAuthTypes.All;
		private string        m_GreetingText   = "";
      
		#region Event declarations

		/// <summary>
		/// Occurs when new computer connected to POP3 server.
		/// </summary>
		public event ValidateIPHandler ValidateIPAddress = null;

		/// <summary>
		/// Occurs when connected user tryes to authenticate.
		/// </summary>
		public event AuthUserEventHandler AuthUser = null;

		/// <summary>
		/// Occurs when server needs to validate sender.
		/// </summary>
		public event ValidateMailFromHandler ValidateMailFrom = null;

		/// <summary>
		/// Occurs when server needs to validate recipient.
		/// </summary>
		public event ValidateMailToHandler ValidateMailTo = null;

		/// <summary>
		/// Occurs when server needs to validate recipient mailbox size.
		/// </summary>
		public event ValidateMailboxSize ValidateMailboxSize = null;

		/// <summary>
		/// Occurs when server has accepted message to store.
		/// </summary>
		public event NewMailEventHandler StoreMessage = null;

		/// <summary>
		/// Occurs when SMTP session has finished and session log is available.
		/// </summary>
		public event LogEventHandler SessionLog = null;

		#endregion


		/// <summary>
		/// Defalut constructor.
		/// </summary>
		public SMTP_Server() : base()
		{
			IPEndPoint = new IPEndPoint(IPAddress.Any,25);
		}


		#region override InitNewSession

		/// <summary>
		/// 
		/// </summary>
		/// <param name="socket"></param>
		protected override void InitNewSession(Socket socket)
		{
			SocketLogger logger = new SocketLogger(socket,this.SessionLog);
			SMTP_Session session = new SMTP_Session(socket,this,logger);
		}

		#endregion


		#region Properties Implementaion
		
		/// <summary>
		/// Gets or sets server greeting text.
		/// </summary>
		public string GreetingText
		{
			get{ return m_GreetingText; }

			set{ m_GreetingText = value; }
		}

		/// <summary>
		/// Maximum message size (KB).
		/// </summary>
		public int MaxMessageSize 
		{
			get{ return m_MaxMessageSize; }

			set{ m_MaxMessageSize = value; }
		}

		/// <summary>
		/// Maximum recipients per message.
		/// </summary>
		public int MaxRecipients
		{
			get{ return m_MaxRecipients; }

			set{ m_MaxRecipients = value; }
		}

		/// <summary>
		/// Gets or sets server supported authentication types.
		/// </summary>
		public SaslAuthTypes SupportedAuthentications
		{
			get{ return m_SupportedAuth; }

			set{ m_SupportedAuth = value; }
		}
		
		#endregion

		#region Events Implementation

		#region method OnValidate_IpAddress
		
		/// <summary>
		/// Raises event ValidateIP event.
		/// </summary>
		/// <param name="session">Reference to current smtp session.</param>
		internal ValidateIP_EventArgs OnValidate_IpAddress(SMTP_Session session) 
		{	
			ValidateIP_EventArgs oArg = new ValidateIP_EventArgs(session.LocalEndPoint,session.RemoteEndPoint);
			if(this.ValidateIPAddress != null){
				this.ValidateIPAddress(this, oArg);
			}

			session.Tag = oArg.SessionTag;

			return oArg;						
		}

		#endregion

		#region method OnAuthUser

		/// <summary>
		/// Raises event AuthUser.
		/// </summary>
		/// <param name="session">Reference to current smtp session.</param>
		/// <param name="userName">User name.</param>
		/// <param name="passwordData">Password compare data,it depends of authentication type.</param>
		/// <param name="data">For md5 eg. md5 calculation hash.It depends of authentication type.</param>
		/// <param name="authType">Authentication type.</param>
		/// <returns></returns>
		internal AuthUser_EventArgs OnAuthUser(SMTP_Session session,string userName,string passwordData,string data,AuthType authType)
		{
			AuthUser_EventArgs oArgs = new AuthUser_EventArgs(session,userName,passwordData,data,authType);
			if(this.AuthUser != null){
				this.AuthUser(this,oArgs);
			}

			return oArgs;
		}

		#endregion

		#region method OnValidate_MailFrom

		/// <summary>
		/// Raises event ValidateMailFrom.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="reverse_path"></param>
		/// <param name="email"></param>
		/// <returns></returns>
		internal ValidateSender_EventArgs OnValidate_MailFrom(SMTP_Session session,string reverse_path,string email) 
		{	
			ValidateSender_EventArgs oArg = new ValidateSender_EventArgs(session,email);
			if(this.ValidateMailFrom != null){
				this.ValidateMailFrom(this, oArg);
			}

			return oArg;						
		}

		#endregion

		#region method OnValidate_MailTo

		/// <summary>
		/// Raises event ValidateMailTo.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="forward_path"></param>
		/// <param name="email"></param>
		/// <param name="authenticated"></param>
		/// <returns></returns>
		internal ValidateRecipient_EventArgs OnValidate_MailTo(SMTP_Session session,string forward_path,string email,bool authenticated) 
		{	
			ValidateRecipient_EventArgs oArg = new ValidateRecipient_EventArgs(session,email,authenticated);
			if(this.ValidateMailTo != null){
				this.ValidateMailTo(this, oArg);
			}

			return oArg;						
		}

		#endregion

		#region method Validate_MailBoxSize

		/// <summary>
		/// Raises event ValidateMailboxSize.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="eAddress"></param>
		/// <param name="messageSize"></param>
		/// <returns></returns>
		internal bool Validate_MailBoxSize(SMTP_Session session,string eAddress,long messageSize)
		{
			ValidateMailboxSize_EventArgs oArgs = new ValidateMailboxSize_EventArgs(session,eAddress,messageSize);
			if(this.ValidateMailboxSize != null){
				this.ValidateMailboxSize(this,oArgs);
			}

			return oArgs.IsValid;
		}

		#endregion


		#region method OnStoreMessage

		/// <summary>
		/// Raises event StoreMessage.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="msgStream"></param>
		internal NewMail_EventArgs OnStoreMessage(SMTP_Session session,MemoryStream msgStream) 
		{			
			NewMail_EventArgs oArg = new NewMail_EventArgs(session,msgStream);
			if(this.StoreMessage != null){				
				this.StoreMessage(this,oArg);
			}
	
			return oArg;					
		}

		#endregion

		#endregion
		
	}
}
