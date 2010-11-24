using System;

namespace LumiSoft.Net
{
	/// <summary>
	/// Provides data for the SessionLog event for POP3_Server and SMTP_Server.
	/// </summary>
	public class Log_EventArgs
	{
		private string       m_LogText  = "";
		private SocketLogger m_pLoggger = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="logText"></param>
		/// <param name="logger"></param>
		public Log_EventArgs(string logText,SocketLogger logger)
		{	
			m_LogText = logText;
			m_pLoggger = logger;
		}


		#region Properties Implementation

		/// <summary>
		/// Gets log text.
		/// </summary>
		public string LogText
		{
			get{ return m_LogText; }
		}

		/// <summary>
		/// Gets logger.
		/// </summary>
		public SocketLogger Logger
		{
			get{ return m_pLoggger; }
		}

		#endregion

	}
}
