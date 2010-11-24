using System;
using System.IO;
using System.Collections;
using System.Net;
using System.Net.Sockets;
using LumiSoft.Net;

namespace LumiSoft.Net
{
	// ToDo: Partial logging, allow to specify log part size.
	//       If log part gets full, write it file.

	/// <summary>
	/// Summary description for SocketLogger.
	/// </summary>
	public class SocketLogger
	{
		private Socket          m_pSocket     = null;
		private string          m_SessionID   = "";
	//	private string          m_UserName    = "";
		private LogEventHandler m_pLogHandler = null;
		private ArrayList       m_pEntries    = null;
		private string s = ""; //****
		
		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="socket"></param>
		/// <param name="logHandler"></param>
		public SocketLogger(Socket socket,LogEventHandler logHandler)
		{	
			m_pSocket     = socket;
			m_pLogHandler = logHandler;
		
			m_pEntries = new ArrayList();
		}

		/// <summary>
		/// Adds data read(from remoteEndpoint) entry.
		/// </summary>
		/// <param name="text"></param>
		/// <param name="size"></param>
		public void AddReadEntry(string text,long size)
		{
			m_pEntries.Add(new SocketLogEntry(text,size,SocketLogEntryType.ReadFromRemoteEP));

			s += CreateEntry(text,">>>");
		}

		/// <summary>
		/// Adds data send(to remoteEndpoint) entry.
		/// </summary>
		/// <param name="text"></param>
		/// <param name="size"></param>
		public void AddSendEntry(string text,long size)
		{
			m_pEntries.Add(new SocketLogEntry(text,size,SocketLogEntryType.SendToRemoteEP));

			s += CreateEntry(text,"<<<");
		}

		/// <summary>
		/// Adds free text entry.
		/// </summary>
		/// <param name="text"></param>
		public void AddTextEntry(string text)
		{
			m_pEntries.Add(new SocketLogEntry(text,0,SocketLogEntryType.FreeText));

			s += CreateEntry(text,"---");
		}

		/// <summary>
		/// 
		/// </summary>
		public void Flush()
		{
			if(m_pLogHandler != null){
				s += "//----- Sys: 'Session:'" + this.SessionID + " removed " + DateTime.Now + "\r\n";

				m_pLogHandler(this,new Log_EventArgs(s,this));				
		//		m_Flushed = true;
				s = "";
			}
		}

		
		private string CreateEntry(string text,string prefix)
		{
			string retVal = "";

			if(text.EndsWith("\r\n")){
				text = text.Substring(0,text.Length - 2);
			}

			string remIP = "xxx.xxx.xxx.xxx";
			try{
				if(m_pSocket.RemoteEndPoint != null){
					remIP = ((IPEndPoint)m_pSocket.RemoteEndPoint).Address.ToString();
				}
			}
			catch{
			}

			string[] lines = text.Replace("\r\n","\n").Split('\n');
			foreach(string line in lines){
				retVal += "SessionID: " + m_SessionID + "  RemIP: " + remIP + "  " + prefix + "  '" + line + "'\r\n";
			}

			return retVal;
		}


		/// <summary>
		/// Gets or sets session ID.
		/// </summary>
		public string SessionID
		{
			get{ return m_SessionID; }

			set{ 
				m_SessionID = value; 
				s = "//----- Sys: 'Session:'" + this.SessionID + " added " + DateTime.Now + "\r\n";
			}
		}

/*		/// <summary>
		/// Gets or sets logged in user name.
		/// </summary>
		public string UserName
		{
			get{ return m_UserName; }

			set{ m_UserName = value; }
		}
*/
		/// <summary>
		/// Gets log entries.
		/// </summary>
		public SocketLogEntry[] LogEntries
		{
			get{
				SocketLogEntry[] retVal = new SocketLogEntry[m_pEntries.Count];
				m_pEntries.CopyTo(retVal);
				return retVal; 
			}
		}

		/// <summary>
		/// Gets local endpoint.
		/// </summary>
		public IPEndPoint LocalEndPoint
		{
			get{ return (IPEndPoint)m_pSocket.LocalEndPoint; }
		}

		/// <summary>
		/// Gets remote endpoint.
		/// </summary>
		public IPEndPoint RemoteEndPoint
		{
			get{ return (IPEndPoint)m_pSocket.RemoteEndPoint; }
		}

	}
}
