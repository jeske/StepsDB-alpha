using System;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// MX record class.
	/// </summary>
	[Serializable]
	public class MX_Record : DnsRecordBase
	{
		private int    m_Preference = 0;
		private string m_Host       = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="preference">MX record preference.</param>
		/// <param name="host">Mail host dns name.</param>
		/// <param name="ttl">TTL value.</param>
		public MX_Record(int preference,string host,int ttl) : base(QTYPE.MX,ttl)
		{
			m_Preference = preference;
			m_Host       = host;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets MX record preference. The lower number is the higher priority server.
		/// </summary>
		public int Preference
		{
			get{ return m_Preference; }
		}

		/// <summary>
		/// Gets mail host dns name.
		/// </summary>
		public string Host
		{
			get{ return m_Host; }
		}

		#endregion

	}
}
