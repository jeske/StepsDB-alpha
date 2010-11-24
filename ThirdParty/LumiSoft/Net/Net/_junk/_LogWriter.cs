using System;

namespace LumiSoft.Net
{
	// ToDo: Partial logging, allow to specify log part size.
	//       If log part gets full, write it file.

	/// <summary>
	/// Cached log writer.
	/// </summary>
	public class _LogWriter
	{
		private string          m_Log        = "";
		private LogEventHandler m_LogHandler = null;
		private bool            m_Flushed    = false;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="logHandler"></param>
		public _LogWriter(LogEventHandler logHandler)
		{
			m_LogHandler = logHandler;
		}


		#region function AddEntry

		/// <summary>
		/// Writes log entry to log cache.
		/// </summary>
		/// <param name="logText"></param>
		/// <param name="sessionID"></param>
		/// <param name="IP"></param>
		/// <param name="prefix"></param>
		public void AddEntry(string logText,string sessionID,string IP,string prefix)
		{
			if(4 - sessionID.Length > 0){
				for(int i=0;i<4-sessionID.Length;i++){
					sessionID = " " + sessionID;
				}
			}

			string logEntry = "";

			if(prefix == "C"){
				logEntry = "SessionID: " + sessionID + "  IP: " + IP + "  >>>  " + prefix + ": '" + logText + "'";				
				m_Log += logEntry + "\r\n";	
				return;
			}

			if(prefix == "S"){
				logEntry = "SessionID: " + sessionID + "  IP: " + IP + "  <<<  " + prefix + ": '" + logText + "'";					
				m_Log += logEntry + "\r\n";	
				return;
			}

			logEntry = "SessionID: " + sessionID + "  IP: " + IP + "  >>>  " + prefix + ": '" + logText + "'";
			m_Log += logEntry + "\r\n";		
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="logText"></param>
		public void AddEntry(string logText)
		{
			m_Log += logText + "\r\n";
		}

		#endregion

		#region function Flush

		/// <summary>
		/// Writes all log entries to log file.
		/// </summary>
		public void Flush()
		{
			if(!m_Flushed && m_LogHandler != null){
				m_LogHandler(this,new Log_EventArgs(m_Log));
				m_Flushed = true;
			}
		}

		#endregion
	}
}
