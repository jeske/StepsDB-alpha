using System;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// CNAME record class.
	/// </summary>
	[Serializable]
	public class CNAME_Record : DnsRecordBase
	{
		private string m_Alias = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="alias">Alias.</param>
		/// <param name="ttl">TTL value.</param>
		public CNAME_Record(string alias,int ttl) : base(QTYPE.CNAME,ttl)
		{
			m_Alias = alias;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets alias.
		/// </summary>
		public string Alias
		{
			get{ return m_Alias; }
		}

		#endregion

	}
}
