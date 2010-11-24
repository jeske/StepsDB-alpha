using System;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// A record class.
	/// </summary>
	[Serializable]
	public class A_Record : DnsRecordBase
	{
		private string m_IP  = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="IP">IP address.</param>
		/// <param name="ttl">TTL value.</param>
		public A_Record(string IP,int ttl) : base(QTYPE.A,ttl)
		{
			m_IP = IP;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets mail host dns name.
		/// </summary>
		public string IP
		{
			get{ return m_IP; }
		}

		#endregion

	}
}
