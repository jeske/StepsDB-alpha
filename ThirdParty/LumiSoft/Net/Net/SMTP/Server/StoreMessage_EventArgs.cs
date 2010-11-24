using System;
using System.IO;

namespace LumiSoft.Net.SMTP.Server
{
	/// <summary>
	/// Provides data for the NewMailEvent event.
	/// </summary>
	public class NewMail_EventArgs
	{
		private SMTP_Session m_pSession  = null;
		private MemoryStream m_MsgStream = null;
		private string       m_ReplyText = "";

		/// <summary>
		/// Default constructor.
		/// </summary>		
		/// <param name="session">Reference to smtp session.</param>
		/// <param name="msgStream">Message stream.</param>
		public NewMail_EventArgs(SMTP_Session session,MemoryStream msgStream)
		{	
			m_pSession      = session;
			m_MsgStream     = msgStream;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets reference to smtp session.
		/// </summary>
		public SMTP_Session Session
		{
			get{ return m_pSession; }
		}

		/// <summary>
		/// Message stream - stream where message has stored.
		/// </summary>
		public MemoryStream MessageStream
		{
			get{ return m_MsgStream; }
		}

		/// <summary>
		/// Message size.
		/// </summary>
		public long MessageSize
		{
			get{
				long retVal = 0;
				if(m_MsgStream != null){
					retVal = m_MsgStream.Length;
				}
				
				return retVal;
			}
		}

		/// <summary>
		/// Sender's email address.
		/// </summary>
		public string MailFrom
		{
			get{ return m_pSession.MailFrom; }
		}

		/// <summary>
		/// Receptient's email address.
		/// </summary>
		public string[] MailTo
		{
			get{ return m_pSession.MailTo; }
		}

		/// <summary>
		/// Gets or sets custom reply text what is shown to connected client after message has stored and 250 reply.
		/// For example ReplyText="Message stored ok" will be visible to connected client: "250 Message stored ok".
		/// </summary>
		public string ReplyText
		{
			get{ return m_ReplyText; }

			set{ m_ReplyText = value.Replace("\r","").Replace("\n",""); }
		}

		#endregion

	}
}
