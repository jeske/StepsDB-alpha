using System;
using System.Collections;
using System.Text.RegularExpressions;

namespace LumiSoft.Net.IMAP.Server
{	
	/// <summary>
	/// IMAP folders collection.
	/// </summary>
	public class IMAP_Folders
	{
		private IMAP_Session m_pSession  = null;
		private ArrayList    m_Mailboxes = null;
		private string       m_RefName   = "";
		private string       m_Mailbox   = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="session">Owner IMAP session.</param>
		/// <param name="referenceName">Folder Path. Eg. Inbox\.</param>
		/// <param name="folder">Folder name.</param>
		public IMAP_Folders(IMAP_Session session,string referenceName,string folder)
		{
			m_pSession  = session;
			m_Mailboxes = new ArrayList();
			m_RefName   = referenceName;
			m_Mailbox   = folder;
		}

		/// <summary>
		/// Adds folder to folders list.
		/// </summary>
		/// <param name="folder">Full path to folder, path separator = '/'. Eg. Inbox/myFolder .</param>
		/// <param name="selectable">Gets or sets if folder is selectable(SELECT command can select this folder).</param>
		public void Add(string folder,bool selectable)
		{
			if(m_RefName.Length > 0){
				// Check if starts with reference name
				if(!folder.ToLower().StartsWith(m_RefName.ToLower())){
					return;
				}
			}

			// !!! This row probably can be removed, regex below handles it.
			// REMOVE ME:
			// Eg. "INBOX", exact mailbox wanted.
			if(m_Mailbox.IndexOf("*") == -1 && m_Mailbox.IndexOf("%") == -1 && m_Mailbox.ToLower() != folder.ToLower()){				
				return;
			}

			// Mailbox wildchar handling.
			// * - ALL   
			// % - won't take sub folders, only current
			// * *mailbox* *mailbox% *mailbox mailbox* mailbox%

			// convert IMAP pattern into regex pattern
			// escape everything fishy, then convert * -> .* and % to [^/]* (ie anything other than a separator)
			string rePattern = "^" + m_RefName + Regex.Replace(m_Mailbox, "([^a-zA-Z0-9*% ])","\\$1").Replace("*", ".*").Replace("%","[^/]*") + "$";
 
			if(Regex.IsMatch(folder,rePattern,RegexOptions.IgnoreCase)){
				m_Mailboxes.Add(new IMAP_Folder(folder,selectable));
				return;
			}	
		}


		/// <summary>
		/// Gets current IMAP session.
		/// </summary>
		public IMAP_Session Session
		{
			get{ return m_pSession; }
		}

		/// <summary>
		/// Gest list of IMAP folders.
		/// </summary>
		public IMAP_Folder[] Folders
		{
			get{ 
				IMAP_Folder[] retVal = new IMAP_Folder[m_Mailboxes.Count];
				m_Mailboxes.CopyTo(retVal);
				return retVal; 
			}
		}
	}
}
