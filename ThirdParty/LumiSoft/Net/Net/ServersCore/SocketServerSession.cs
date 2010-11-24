using System;

namespace LumiSoft.Net.ServersCore
{
	/// <summary>
	/// This is base class for SocketServer sessions.
	/// </summary>
	public class SocketServerSession
	{
		private BufferedSocket m_pSocket           = null;    // Referance to client Socket.
		private string         m_SessionID         = "";      // Holds session ID.
		private string         m_UserName          = "";      // Holds loggedIn UserName.
		private bool           m_Authenticated     = false;   // Holds authentication flag.
		private int            m_BadCmdCount       = 0;       // Holds number of bad commands.
		private DateTime       m_SessionStartTime;
		private DateTime       m_LastDataTime;
	//	private _LogWriter     m_pLogWriter        = null;
		private object         m_Tag               = null;

		public SocketServerSession(BufferedSocket socket)
		{			
		}

		/// <summary>
		/// Is called by server when session has timed out.
		/// </summary>
		public void OnSessionTimeout()
		{
		}


		#region Properties Implementation
		
		/// <summary>
		/// Gets session ID.
		/// </summary>
		public string SessionID
		{
			get{ return m_SessionID; }
		}

		/// <summary>
		/// Gets if session authenticated.
		/// </summary>
		public bool Authenticated
		{
			get{ return m_Authenticated; }
		}

		/// <summary>
		/// Gets loggded in user name (session owner).
		/// </summary>
		public string UserName
		{
			get{ return m_UserName; }
		}

		/// <summary>
		/// Gets connected Host(client) EndPoint.
		/// </summary>
		public IPEndPoint RemoteEndPoint
		{
			get{ return (IPEndPoint)m_pSocket.RemoteEndPoint; }
		}
		
		/// <summary>
		/// Gets local EndPoint which accepted client(connected host).
		/// </summary>
		public IPEndPoint LocalEndPoint
		{
			get{ return (IPEndPoint)m_pSocket.LocalEndPoint; }
		}

		/// <summary>
		/// Gets session start time.
		/// </summary>
		public DateTime SessionStartTime
		{
			get{ return m_SessionStartTime; }
		}

		/// <summary>
		/// Gets last data activity time.
		/// </summary>
		public DateTime SessionLastDataTime
		{
			get{ return m_LastDataTime; }
		}

		/// <summary>
		/// Gets or sets custom user data.
		/// </summary>
		public object Tag
		{
			get{ return m_Tag; }

			set{ m_Tag = value; }
		}

		#endregion
	}
}
