using System;

namespace LumiSoft.Net.IMAP.Server
{
	/// <summary>
	/// IMAP message info.
	/// </summary>
	public class IMAP_Message
	{
		private IMAP_Messages     m_Messages  = null;
		private string            m_MessageID = "";
		private int               m_UID       = 1;
		private IMAP_MessageFlags m_Flags;
		private long              m_Size      = 0;
		private DateTime          m_Date;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="messages"></param>
		/// <param name="messageID">Internal messageID.</param>
		/// <param name="UID">Message UID. NOTE: message uid must increase all the time, for new messages.</param>
		/// <param name="flags">Message flags.</param>
		/// <param name="size">Message size.</param>
		/// <param name="date">Message receive date.</param>
		internal IMAP_Message(IMAP_Messages messages,string messageID,int UID,IMAP_MessageFlags flags,long size,DateTime date)
		{
			m_Messages  = messages;
			m_MessageID = messageID;
			m_UID       = UID;
			m_Flags     = flags;
			m_Size      = size;
			m_Date      = date;
		}


		#region function FlagsToString

		/// <summary>
		/// Converts message flags to string. Eg. \SEEN \DELETED .
		/// </summary>
		/// <returns></returns>
		public string FlagsToString()
		{
			return IMAP_Utils.MessageFlagsToString(this.Flags);
		/*	string retVal = "";
			if(((int)IMAP_MessageFlags.Answered & (int)this.Flags) != 0){
				retVal += " \\ANSWERED";
			}
			if(((int)IMAP_MessageFlags.Flagged & (int)this.Flags) != 0){
				retVal += " \\FLAGGED";
			}
			if(((int)IMAP_MessageFlags.Deleted & (int)this.Flags) != 0){
				retVal += " \\DELETED";
			}
			if(((int)IMAP_MessageFlags.Seen & (int)this.Flags) != 0){
				retVal += " \\SEEN";
			}
			if(((int)IMAP_MessageFlags.Draft & (int)this.Flags) != 0){
				retVal += " \\DRAFT";
			}

			return retVal.Trim();*/
		}

		#endregion

		#region function SetFlags

		internal void SetFlags(IMAP_MessageFlags flags)
		{
			m_Flags = flags;
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets message number.
		/// </summary>
		public int MessageNo
		{
			get{
				if(m_Messages != null){
					return m_Messages.IndexOf(this);
				}
				else{
					return -1;
				}
			}
		}

		/// <summary>
		/// Gets internal messageID.
		/// </summary>
		public string MessageID
		{
			get{ return m_MessageID; }

			set{ m_MessageID = value; }
		}

		/// <summary>
		/// Gets message UID.
		/// </summary>
		public int MessageUID
		{
			get{ return m_UID; }
		}

		/// <summary>
		/// Gets message flags.
		/// </summary>
		public IMAP_MessageFlags Flags
		{
			get{ return m_Flags; }
		}

		/// <summary>
		/// Gets message size.
		/// </summary>
		public long Size
		{
			get{ return m_Size; }
		}

		/// <summary>
		/// Gets message size.
		/// </summary>
		public DateTime Date
		{
			get{ return m_Date; }
		}

		#endregion

	}
}
