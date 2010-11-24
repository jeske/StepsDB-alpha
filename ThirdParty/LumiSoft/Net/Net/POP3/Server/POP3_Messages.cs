using System;
using System.Collections;

namespace LumiSoft.Net.POP3.Server
{
	/// <summary>
	/// POP3 messages collection.
	/// </summary>
	public class POP3_Messages
	{
		private ArrayList m_POP3_Messages = null;
		
		/// <summary>
		/// Default constructor.
		/// </summary>
		public POP3_Messages()
		{	
			m_POP3_Messages = new ArrayList();
		}


		#region function AddMessage

		/// <summary>
		/// Adds new message to message list.
		/// </summary>
		/// <param name="messageID">Message ID.</param>
		/// <param name="uid">Message UID. This UID is reported in UIDL command.</param>
		/// <param name="messageSize">Message size in bytes.</param>
		public void AddMessage(string messageID,string uid,int messageSize)
		{
			AddMessage(messageID,uid,messageSize,null);
		}

		/// <summary>
		/// Adds new message to message list.
		/// </summary>
		/// <param name="messageID">Message ID.</param>
		/// <param name="uid">Message UID. This UID is reported in UIDL command.</param>
		/// <param name="messageSize">Message size in bytes.</param>
		/// <param name="tag">User data for message.</param>
		public void AddMessage(string messageID,string uid,int messageSize,object tag)
		{
			POP3_Message msg = new POP3_Message(this);
			msg.MessageUID   = uid;
			msg.MessageID    = messageID;
			msg.MessageSize  = messageSize;
			msg.Tag          = tag;

			m_POP3_Messages.Add(msg);
		}

		#endregion

		#region function GetMessage

		/// <summary>
		/// Gets specified message from message list.
		/// </summary>
		/// <param name="messageNr">Number of message which to get.</param>
		/// <returns></returns>
		public POP3_Message GetMessage(int messageNr)
		{
			return (POP3_Message)m_POP3_Messages[messageNr];
		}

		#endregion

		#region function MessageExists

		/// <summary>
		/// Checks if message exists. NOTE marked for delete messages returns false.
		/// </summary>
		/// <param name="nr">Number of message which to check.</param>
		/// <returns></returns>
		public bool MessageExists(int nr)
		{
			try
			{
				if(nr > 0 && nr <= m_POP3_Messages.Count){
					POP3_Message msg = (POP3_Message)m_POP3_Messages[nr-1];
					if(!msg.MarkedForDelete){
						return true;
					}
				}
			}
			catch{
			}
			
			return false;			
		}

		#endregion

		#region function GetTotalMessagesSize

		/// <summary>
		/// Gets messages total sizes. NOTE messages marked for deletion is excluded.
		/// </summary>
		/// <returns></returns>
		public int GetTotalMessagesSize()
		{
			int totalSize = 0;
			foreach(POP3_Message msg in m_POP3_Messages){
				if(!msg.MarkedForDelete){
					totalSize += msg.MessageSize;
				}
			}

			return totalSize;
		}

		#endregion


		#region function ResetDeleteFlags

		/// <summary>
		/// Unmarks all messages, which are marked for deletion.
		/// </summary>
		public void ResetDeleteFlags()
		{
			foreach(POP3_Message msg in m_POP3_Messages){
				msg.MarkedForDelete = false;
			}
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets count of messages. NOTE messages marked for deletion are excluded.
		/// </summary>
		public int Count
		{
			get{
				int messageCount = 0;
				foreach(POP3_Message msg in m_POP3_Messages){
					if(!msg.MarkedForDelete){
						messageCount++;
					}
				}
				return messageCount; 
			}
		}

		/// <summary>
		/// Gets messages, which aren't marked for deletion.
		/// </summary>
		public POP3_Message[] ActiveMessages
		{			
			get{
				//--- Make array of unmarked messages --------//
				ArrayList activeMessages = new ArrayList();
				foreach(POP3_Message msg in m_POP3_Messages){
					if(!msg.MarkedForDelete){
						activeMessages.Add(msg);
					}
				}
				//--------------------------------------------//
				
				POP3_Message[] retVal = new POP3_Message[activeMessages.Count];
				activeMessages.CopyTo(retVal);
				return retVal; 
			}
		}


		/// <summary>
		/// Referance to Messages ArrayList.
		/// </summary>
		internal ArrayList Messages
		{
			get{ return m_POP3_Messages; }
		}

		/// <summary>
		/// Gets specified message.
		/// </summary>
		internal POP3_Message this[int messageNr]
		{
			get{ return (POP3_Message)m_POP3_Messages[messageNr-1]; }
		}

		#endregion

	}
}
