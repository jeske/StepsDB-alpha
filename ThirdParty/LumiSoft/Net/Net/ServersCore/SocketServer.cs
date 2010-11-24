using System;
using System.IO;
using System.Collections;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace LumiSoft.Net
{
	/// <summary>
	/// This is base class for Socket and Session based servers.
	/// </summary>
	public class SocketServer : System.ComponentModel.Component
	{
		private Socket              m_pListner           = null;
		protected Hashtable         m_pSessions          = null;
		private ArrayList           m_pQueuedConnections = null;
		private bool                m_Running            = false;
		private System.Timers.Timer m_pTimer             = null;
		private IPEndPoint          m_pIPEndPoint        = null;
		private string              m_HostName           = "";
		private int                 m_SessionIdleTimeOut = 30000;
		private int                 m_MaxConnections     = 1000;
		private int                 m_MaxBadCommands     = 8;       
		private bool                m_LogCmds            = false;

		#region Events declarations

		/// <summary>
		/// Occurs when server or session has system error(unhandled error).
		/// </summary>
		public event ErrorEventHandler SysError = null;

		#endregion

		/// <summary>
		/// Default constructor.
		/// </summary>
		public SocketServer()
		{
			m_pSessions          = new Hashtable();
			m_pQueuedConnections = new ArrayList();
			m_pTimer             = new System.Timers.Timer(15000);
			m_pIPEndPoint        = new IPEndPoint(IPAddress.Any,25);
			m_HostName           = System.Net.Dns.GetHostName();

			m_pTimer.AutoReset = true;
			m_pTimer.Elapsed += new System.Timers.ElapsedEventHandler(this.m_pTimer_Elapsed);
		}

		#region method Dispose

		/// <summary>
		/// Clean up any resources being used and stops server.
		/// </summary>
		public new void Dispose()
		{
			base.Dispose();

			StopServer();				
		}

		#endregion


		#region method StartServer

		/// <summary>
		/// Starts server.
		/// </summary>
		public void StartServer()
		{
			if(!m_Running){
				m_Running = true;

				// Start accepting ang queueing connections
				Thread tr = new Thread(new ThreadStart(this.StartProcCons));
				tr.Start();

				// Start proccessing queued connections
				Thread trSessionCreator = new Thread(new ThreadStart(this.StartProcQueuedCons));
				trSessionCreator.Start();

				m_pTimer.Enabled = true;
			}
		}

		#endregion

		#region method StopServer

		/// <summary>
		/// Stops server. NOTE: Active sessions aren't cancled.
		/// </summary>
		public void StopServer()
		{
			if(m_Running && m_pListner != null){
				// Stop accepting new connections
				m_pListner.Close();
				m_pListner = null;

				// Wait queued connections to be proccessed, there won't be new queued connections, 
				// because Listner socket is closed.
				Thread.Sleep(100);

				m_Running = false;
			}
		}

		#endregion


		#region method StartProcCons

		/// <summary>
		/// Starts proccessiong incoming connections (Accepts and queues connections).
		/// </summary>
		private void StartProcCons()
		{	
			try{
				m_pListner = new Socket(AddressFamily.InterNetwork,SocketType.Stream,ProtocolType.Tcp);
				m_pListner.Bind(m_pIPEndPoint);
				m_pListner.Listen(500);
				
				// Accept connections and queue them			
				while(m_Running){
					// We have reached maximum connection limit
					if(m_pSessions.Count > m_MaxConnections){
						// Wait while some active connectins are closed
						while(m_pSessions.Count > m_MaxConnections){
							Thread.Sleep(100);
						}
					}

					// Wait incoming connection
					Socket s = m_pListner.Accept();

					// Add session to queue
					lock(m_pQueuedConnections){
						m_pQueuedConnections.Add(s);
					}
				}
			}
			catch(SocketException x){
				// Socket listening stopped, happens when StopServer is called.
				// We need just skip this error.
				if(x.ErrorCode == 10004){			
				}
				else{
					OnSysError("WE MUST NEVER REACH HERE !!! StartProcCons:",x);
				}
			}
			catch(Exception x){
				OnSysError("WE MUST NEVER REACH HERE !!! StartProcCons:",x);
			}
		}

		#endregion
		
		#region method StartProcQueuedCons

		/// <summary>
		/// Starts queueed connections proccessing (Creates and starts session foreach connection).
		/// </summary>
		private void StartProcQueuedCons()
		{
			try{
				while(m_Running){
					// Get copy of queued connections, because we must release lock as soon as possible,
					// because no new connections can be added when m_pQueueSessions is locked.
					Socket[] connections = null;
					lock(m_pQueuedConnections){
						connections = new Socket[m_pQueuedConnections.Count];
						m_pQueuedConnections.CopyTo(connections);

						// Clear queue we have copy of entries
						m_pQueuedConnections.Clear();
					}

					// Start sessions
					for(int i=0;i<connections.Length;i++){
						// If session throws exception, handle it here or all queue is lost
						try{
							InitNewSession(connections[i]);
						}
						catch(Exception x){
							OnSysError("StartProcQueuedCons InitNewSession():",x);
						}
					}

					// If there few new connections to proccess, delay proccessing. We need to it 
					// because if there is few or none connections proccess, while loop takes too much CPU.
					if(m_pQueuedConnections.Count < 10){
						Thread.Sleep(50);
					}
				}
			}
			catch(Exception x){
				OnSysError("WE MUST NEVER REACH HERE !!! StartProcQueuedCons:",x);
			}
		}

		#endregion


		#region method AddSession

		/// <summary>
		/// 
		/// </summary>
		/// <param name="session"></param>
		internal protected void AddSession(object session)
		{
			lock(m_pSessions){
				m_pSessions.Add(session.GetHashCode(),session);
			}			
		}

		#endregion

		#region method RemoveSession

		/// <summary>
		/// 
		/// </summary>
		/// <param name="session"></param>
		internal protected void RemoveSession(object session)
		{
			lock(m_pSessions){
				m_pSessions.Remove(session.GetHashCode());
			}			
		}

		#endregion


		#region method OnSysError

		/// <summary>
		/// 
		/// </summary>
		/// <param name="text"></param>
		/// <param name="x"></param>
		internal protected void OnSysError(string text,Exception x)
		{
			if(this.SysError != null){
				this.SysError(this,new Error_EventArgs(x,new StackTrace()));
			}
		}

		#endregion

		#region method OnSessionTimeoutTimer

		/// <summary>
		/// This method must get timedout sessions and end them.
		/// </summary>
		private void OnSessionTimeoutTimer()
		{
			try{
				// Close/Remove timed out sessions
				lock(m_pSessions){
					ISocketServerSession[] sessions = new ISocketServerSession[m_pSessions.Count];
					m_pSessions.Values.CopyTo(sessions,0);

					// Loop sessions and and call OnSessionTimeout() for timed out sessions.
					for(int i=0;i<sessions.Length;i++){	
						// If session throws exception, handle it here or next sessions timouts are not handled.
						try{
							// Session timed out
							if(DateTime.Now > sessions[i].SessionLastDataTime.AddMilliseconds(this.SessionIdleTimeOut)){
								sessions[i].OnSessionTimeout();
							}
						}
						catch(Exception x){
							OnSysError("OnTimer:",x);
						}
					}
				}
			}
			catch(Exception x){
				OnSysError("WE MUST NEVER REACH HERE !!! OnTimer:",x);
			}
		}

		#endregion

		#region method m_pTimer_Elapsed

		private void m_pTimer_Elapsed(object sender,System.Timers.ElapsedEventArgs e)
		{	
			OnSessionTimeoutTimer();

		/*	try{
				// Close/Remove timed out sessions
				lock(m_pSessions){
					SMTP_Session[] sessions = new SMTP_Session[m_pSessions.Count];
					m_pSessions.Values.CopyTo(sessions,0);

					// Loop sessions and and call OnSessionTimeout() for timed out sessions.
					for(int i=0;i<sessions.Length;i++){	
						// If session throws exception, handle it here or next sessions timouts are not handled.
						try{
							// Session timed out
							if(DateTime.Now > sessions[i].SessionLastDataTime.AddMilliseconds(m_SessionIdleTimeOut)){
								sessions[i].OnSessionTimeout();
							}
						}
						catch(Exception x){
							OnSysError("OnTimer:",x);
						}
					}
				}
			}
			catch(Exception x){
				OnSysError("WE MUST NEVER REACH HERE !!! OnTimer:",x);
			}*/
		}

		#endregion


		#region virtual method InitNewSession

		/// <summary>
		/// Initialize and start new session here. Session isn't added to session list automatically, 
		/// session must add itself to server session list by calling AddSession().
		/// </summary>
		/// <param name="socket">Connected client socket.</param>
		protected virtual void InitNewSession(Socket socket)
		{
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets or sets which IP address to listen.
		/// </summary>
		[Obsolete("Use IPEndPoint instead !")]
		public string IpAddress 
		{
			get{ return IPEndPoint.Address.ToString(); }

			set{
				if(value.ToLower().IndexOf("all") > -1){
					IPEndPoint.Address = IPAddress.Any;
				}
				else{
					IPEndPoint.Address = IPAddress.Parse(value); 
				}
			}
		}


		/// <summary>
		/// Gets or sets which port to listen.
		/// </summary>
		[Obsolete("Use IPEndPoint instead !")]
		public int Port 
		{
			get{ return IPEndPoint.Port;	}

			set{ IPEndPoint.Port = value; }
		}

		/// <summary>
		/// Gets or sets maximum session threads.
		/// </summary>
		[Obsolete("Use MaxConnections instead !")]
		public int Threads 
		{
			get{ return MaxConnections; }

			set{ MaxConnections = value; }
		}

		/// <summary>
		/// Command idle timeout in milliseconds.
		/// </summary>
		[Obsolete("Not used any more, now there is only one time out (SessionIdleTimeOut).")]		
		public int CommandIdleTimeOut 
		{
			get{ return 60000; }

			set{ }
		}

//-------------------------------------------------------------


		/// <summary>
		/// Gets or sets IPEndPoint server to listen. NOTE: If server running and changeing IPEndPoint, server will be restarted automatically.
		/// </summary>
		public IPEndPoint IPEndPoint 
		{
			get{ return m_pIPEndPoint;	}

			set{ 
				if(value != null && !m_pIPEndPoint.Equals(value)){					
					m_pIPEndPoint = value;

					// We need to restart server to take effect IP or Port change
					if(m_Running){
						StopServer();
						StartServer();
					}
				}
			}
		}

		/// <summary>
		/// Gets or sets maximum allowed connections.
		/// </summary>
		public int MaxConnections 
		{
			get{ return m_MaxConnections; }

			set{ m_MaxConnections = value; }
		}


		/// <summary>
		/// Runs and stops server.
		/// </summary>
		public bool Enabled 
		{
			get{ return m_Running; }

			set{				
				if(value != m_Running & !this.DesignMode){
					if(value){
						StartServer();
					}
					else{
						StopServer();
					}
				}
			}
		}
	
		/// <summary>
		/// Gets or sets if to log commands.
		/// </summary>
		public bool LogCommands
		{
			get{ return m_LogCmds; }

			set{ m_LogCmds = value; }
		}

		/// <summary>
		/// Session idle timeout in milliseconds.
		/// </summary>
		public int SessionIdleTimeOut 
		{
			get{ return m_SessionIdleTimeOut; }

			set{ m_SessionIdleTimeOut = value; }
		}
				
		/// <summary>
		/// Gets or sets maximum bad commands allowed to session.
		/// </summary>
		public int MaxBadCommands
		{
			get{ return m_MaxBadCommands; }

			set{ m_MaxBadCommands = value; }
		}

		/// <summary>
		/// Gets or set host name that is reported to clients.
		/// </summary>
		public string HostName
		{
			get{ return m_HostName; }

			set{
				if(value.Length > 0){
					m_HostName = value;
				}
			}
		}

		/// <summary>
		/// Gets active sessions.
		/// </summary>
		public Hashtable Sessions
		{
			get{ return m_pSessions; }
		}

		#endregion

	}
}
