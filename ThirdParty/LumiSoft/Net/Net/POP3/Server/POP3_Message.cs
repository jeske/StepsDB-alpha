using System;

namespace LumiSoft.Net.POP3.Server
{
	/// <summary>
	/// Holds POP3_Message info (ID,Size,...).
	/// </summary>
	public class POP3_Message
	{
		private POP3_Messages m_pMessages       = null;
		private string        m_MessageID       = "";    // Holds message ID.
		private string        m_MessageUID      = "";
		private int           m_MessageSize     = 0;     // Holds message size.
		private bool          m_MarkedForDelete = false; // Holds marked for delete flag.
		private object        m_Tag             = null;
		
		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="messages"></param>
		public POP3_Message(POP3_Messages messages)
		{	
			m_pMessages = messages;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets or sets message ID.
		/// </summary>
		public string MessageID
		{
			get{ return m_MessageID; }

			set{ m_MessageID = value; }
		}

		/// <summary>
		/// Gets or sets message UID. This UID is reported in UIDL command.
		/// </summary>
		public string MessageUID
		{
			get{ return m_MessageUID; }

			set{ m_MessageUID = value; }
		}

		/// <summary>
		/// Gets or sets message size.
		/// </summary>
		public int MessageSize
		{
			get{ return m_MessageSize; }

			set{ m_MessageSize = value; }
		}

		/// <summary>
		/// Gets or sets message state flag.
		/// </summary>
		public bool MarkedForDelete
		{
			get{ return m_MarkedForDelete; }

			set{ m_MarkedForDelete = value; }
		}

		/// <summary>
		/// Gets message number. NOTE message number is 1 based (not zero based).
		/// </summary>
		public int MessageNr
		{
			get{ return m_pMessages.Messages.IndexOf(this)+1; }
		}

		/// <summary>
		/// Gets or sets user data for message.
		/// </summary>
		public object Tag
		{
			get{ return m_Tag; }

			set{ m_Tag = value; }
		}

		#endregion

	}
}
