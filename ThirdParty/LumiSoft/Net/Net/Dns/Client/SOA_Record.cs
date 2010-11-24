using System;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// SOA record class.
	/// </summary>
	[Serializable]
	public class SOA_Record : DnsRecordBase
	{
		private string m_NameServer = "";
		private string m_AdminEmail = "";
		private long   m_Serial     = 0;
		private long   m_Refresh    = 0;
		private long   m_Retry      = 0;
		private long   m_Expire     = 0;
		private long   m_Minimum    = 0;
		
		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="nameServer">Name server.</param>
		/// <param name="adminEmail">Zone administrator email.</param>
		/// <param name="serial">Version number of the original copy of the zone.</param>
		/// <param name="refresh">Time interval(in seconds) before the zone should be refreshed.</param>
		/// <param name="retry">Time interval(in seconds) that should elapse before a failed refresh should be retried.</param>
		/// <param name="expire">Time value(in seconds) that specifies the upper limit on the time interval that can elapse before the zone is no longer authoritative.</param>
		/// <param name="minimum">Minimum TTL(in seconds) field that should be exported with any RR from this zone.</param>
		/// <param name="ttl">TTL value.</param>
		public SOA_Record(string nameServer,string adminEmail,long serial,long refresh,long retry,long expire,long minimum,int ttl) : base(QTYPE.SOA,ttl)
		{
			m_NameServer = nameServer;
			m_AdminEmail = adminEmail;
			m_Serial     = serial;
			m_Refresh    = refresh;
			m_Retry      = retry;
			m_Expire     = expire;
			m_Minimum    = minimum;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets name server.
		/// </summary>
		public string NameServer
		{
			get{ return m_NameServer; }
		}

		/// <summary>
		/// Gets zone administrator email.
		/// </summary>
		public string AdminEmail
		{
			get{ return m_AdminEmail; }
		}

		/// <summary>
		/// Gets version number of the original copy of the zone.
		/// </summary>
		public long Serial
		{
			get{ return m_Serial; }
		}

		/// <summary>
		/// Gets time interval(in seconds) before the zone should be refreshed.
		/// </summary>
		public long Refresh
		{
			get{ return m_Refresh; }
		}

		/// <summary>
		/// Gets time interval(in seconds) that should elapse before a failed refresh should be retried.
		/// </summary>
		public long Retry
		{
			get{ return m_Retry; }
		}

		/// <summary>
		/// Gets time value(in seconds) that specifies the upper limit on the time interval that can elapse before the zone is no longer authoritative.
		/// </summary>
		public long Expire
		{
			get{ return m_Expire; }
		}

		/// <summary>
		/// Gets minimum TTL(in seconds) field that should be exported with any RR from this zone. 
		/// </summary>
		public long Minimum
		{
			get{ return m_Minimum; }
		}

		#endregion

	}
}
