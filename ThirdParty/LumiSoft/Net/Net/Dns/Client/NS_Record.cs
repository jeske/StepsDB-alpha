using System;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// NS record class.
	/// </summary>
	[Serializable]
	public class NS_Record : DnsRecordBase
	{
		private string m_NameServer = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="nameServer">Name server name.</param>
		/// <param name="ttl">TTL value.</param>
		public NS_Record(string nameServer,int ttl) : base(QTYPE.NS,ttl)
		{
			m_NameServer = nameServer;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets name server name.
		/// </summary>
		public string NameServer
		{
			get{ return m_NameServer; }
		}

		#endregion

	}
}
