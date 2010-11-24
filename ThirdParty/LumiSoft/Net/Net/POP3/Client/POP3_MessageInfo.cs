using System;

namespace LumiSoft.Net.POP3.Client
{
	/// <summary>
	/// Holds POP3 message info.
	/// </summary>
	public class POP3_MessageInfo
	{
		private string m_MessageID   = "";
		private int    m_MessageNo   = 0;
		private long   m_MessageSize = 0;
		
		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="messageID"></param>
		/// <param name="messageNo"></param>
		/// <param name="messageSize"></param>
		public POP3_MessageInfo(string messageID,int messageNo,long messageSize)
		{	
			m_MessageID   = messageID;
			m_MessageNo   = messageNo;
			m_MessageSize = messageSize;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets message unique ID returned by pop3 server.
		/// </summary>
		public string MessegeID
		{
			get{ return m_MessageID; }
		}

		/// <summary>
		/// Gets message number in POP3 server.
		/// </summary>
		[Obsolete("Use MessageNumber instead !")]
		public int MessageNr
		{
			get{ return m_MessageNo; }
		}

		/// <summary>
		/// Gets message number in POP3 server.
		/// </summary>
		public int MessageNumber
		{
			get{ return m_MessageNo; }
		}

		/// <summary>
		/// Gets message size.
		/// </summary>
		public long MessageSize
		{
			get{ return m_MessageSize; }
		}

		#endregion

	}
}
