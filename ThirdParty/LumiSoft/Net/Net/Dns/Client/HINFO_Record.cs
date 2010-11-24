using System;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// HINFO record.
	/// </summary>
	public class HINFO_Record : DnsRecordBase
	{
		private string m_CPU = "";
		private string m_OS  = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="cpu">Host CPU.</param>
		/// <param name="os">Host OS.</param>
		/// <param name="ttl">TTL value.</param>
		public HINFO_Record(string cpu,string os,int ttl) : base(QTYPE.HINFO,ttl)
		{
			m_CPU = cpu;
			m_OS  = os;
		}


		#region Properties Implementation

		/// <summary>
		/// Gets host's CPU.
		/// </summary>
		public string CPU
		{
			get{ return m_CPU; }
		}

		/// <summary>
		/// Gets host's OS.
		/// </summary>
		public string OS
		{
			get{ return m_OS; }
		}
        
		#endregion
	}
}
