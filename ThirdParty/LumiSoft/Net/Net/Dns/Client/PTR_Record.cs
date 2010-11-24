using System;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// PTR record class.
	/// </summary>
	[Serializable]
	public class PTR_Record : DnsRecordBase
	{
		private string m_DomainName = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="domainName">DomainName.</param>
		/// <param name="ttl">TTL value.</param>
		public PTR_Record(string domainName,int ttl) : base(QTYPE.PTR,ttl)
		{
			m_DomainName = domainName;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets domain name.
		/// </summary>
		public string DomainName
		{
			get{ return m_DomainName; }
		}

		#endregion

	}
}
