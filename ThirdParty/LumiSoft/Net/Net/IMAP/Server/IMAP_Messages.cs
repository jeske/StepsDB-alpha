using System;
using System.Collections;

namespace LumiSoft.Net.IMAP.Server
{		
	/// <summary>
	/// IMAP messages info collection.
	/// </summary>
	public class IMAP_Messages
	{
		private SortedList m_Messages  = null;
		private string     m_Folder    = "";
		private string     m_Error     = null;
		private bool       m_ReadOnly  = false;
		private int        m_FolderUID = 124221;
		
		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="folder"></param>
		public IMAP_Messages(string folder)
		{
			m_Folder   = folder;
			m_Messages = new SortedList();
		}
		
		/// <summary>
		/// Adds new message to list.
		/// </summary>
		/// <param name="messageID">Internal messageID.</param>
		/// <param name="UID">Message UID. NOTE: message uid must increase all the time, for new messages.</param>
		/// <param name="flags">Message flags.</param>
		/// <param name="size">Message size.</param>
		/// <param name="date">Message receive date.</param>
		public void AddMessage(string messageID,int UID,IMAP_MessageFlags flags,long size,DateTime date)
		{
			m_Messages.Add(UID,new IMAP_Message(this,messageID,UID,flags,size,date));
		}

		/// <summary>
		/// Removes message from list.
		/// </summary>
		/// <param name="msg">Message which to remove.</param>
		public void RemoveMessage(IMAP_Message msg)
		{
			m_Messages.RemoveAt(msg.MessageNo - 1);
		}

		/// <summary>
		/// Gets message 1-based index.
		/// </summary>
		/// <param name="message"></param>
		/// <returns></returns>
		public int IndexOf(IMAP_Message message)
		{
			return m_Messages.IndexOfValue(message) + 1;
		}

		/// <summary>
		/// Gets message 1-based message index from message UID.
		/// </summary>
		/// <param name="uid"></param>
		/// <returns></returns>
		public int IndexFromUID(int uid)
		{
			int retVal = 0;
			foreach(IMAP_Message msg in m_Messages.GetValueList()){
				retVal++;
				if(msg.MessageUID == uid){
					return retVal;
				}
			}

			//--- No exact UID
			if(uid == 1){
				return 1;
			}
			else if(m_Messages.Count > 0 && uid > ((IMAP_Message)(m_Messages.GetValueList()[m_Messages.Count - 1])).MessageUID){
				return m_Messages.Count + 1;
			}
			// Find nearest
			else if(m_Messages.Count > 0){				
				foreach(IMAP_Message msg in m_Messages.GetValueList()){
					if(msg.MessageUID > uid){
						return msg.MessageNo;
					}
				}
			}
			
			return -1;
		}

		/// <summary>
		/// Gets messages marked for delete.
		/// </summary>
		/// <returns></returns>
		public IMAP_Message[] GetDeleteMessages()
		{
			ArrayList retVal = new ArrayList();
			foreach(IMAP_Message msg in m_Messages.GetValueList()){
				if(((int)IMAP_MessageFlags.Deleted & (int)msg.Flags) != 0){
					retVal.Add(msg);
				}
			}

			IMAP_Message[] messages = new IMAP_Message[retVal.Count];
			retVal.CopyTo(messages);

			return messages;
		}


		#region Properties Implementation

		/// <summary>
		/// Gets specified message.
		/// </summary>
		public IMAP_Message this[int msgNo]
		{
			get{ return (IMAP_Message)m_Messages.GetByIndex(msgNo); }
		}

		/// <summary>
		/// Gets first unseen message number.
		/// </summary>
		/// <returns></returns>
		public int FirstUnseen
		{
			get{ 
				for(int i=0;i<m_Messages.Count;i++){
					IMAP_Message msg = (IMAP_Message)m_Messages.GetByIndex(i);
					if(((int)IMAP_MessageFlags.Recent & (int)msg.Flags) != 0){
						return i+1;
					}
				}
			
				return 0; 
			}
		}
		
		/// <summary>
		/// Gets unseen messages count.
		/// </summary>
		/// <returns></returns>
		public int UnSeenCount
		{
			get{ 
				int nCount = 0;
				foreach(IMAP_Message msg in m_Messages.GetValueList()){
					if(((int)IMAP_MessageFlags.Seen & (int)msg.Flags) == 0){
						nCount++;
					}
				}
				return nCount; 
			}
		}

		/// <summary>
		/// Gets new messages count.
		/// </summary>
		/// <returns></returns>
		public int RecentCount
		{
			get{ 
				int nCount = 0;
				foreach(IMAP_Message msg in m_Messages.GetValueList()){
					if(((int)IMAP_MessageFlags.Recent & (int)msg.Flags) != 0){
						nCount++;
					}
				}
				return nCount; 
			}
		}

		/// <summary>
		/// Gets messages marked for delete count.
		/// </summary>
		/// <returns></returns>
		public int DeleteCount
		{
			get{ 
				int nCount = 0;
				foreach(IMAP_Message msg in m_Messages.GetValueList()){
					if(((int)IMAP_MessageFlags.Deleted & (int)msg.Flags) != 0){
						nCount++;
					}
				}
				return nCount; 
			}
		}

		/// <summary>
		/// Gets messages total count.
		/// </summary>
		public int Count
		{
			get{ return m_Messages.Count; }
		}

		/// <summary>
		/// Gets mailbox which messages contains.
		/// </summary>
		public string Mailbox
		{
			get{ return m_Folder; }
		}

		/// <summary>
		/// Gets or sets mailbox UID value.
		/// </summary>
		public int MailboxUID
		{
			get{ return m_FolderUID; }

			set{ m_FolderUID = value; }
		}

		/// <summary>
		/// Gets predictable next UID value.(Max(messageUID) + 1).
		/// </summary>
		public int UID_Next
		{
			get{
				if(m_Messages.Count > 0){
					return ((IMAP_Message)m_Messages.GetByIndex(m_Messages.Count-1)).MessageUID + 1; 
				}
				else{
					return 1;
				}
			}
		}

		/// <summary>
		/// Gets or sets if messages folder is read only.
		/// </summary>
		public bool ReadOnly
		{
			get{ return m_ReadOnly; }

			set{ m_ReadOnly = value; }
		}

		/// <summary>
		/// Gets or sets custom error text, which is returned to client.
		/// </summary>
		public string ErrorText
		{
			get{ return m_Error; }

			set{ m_Error = value; }
		}

		#endregion

	}
}
