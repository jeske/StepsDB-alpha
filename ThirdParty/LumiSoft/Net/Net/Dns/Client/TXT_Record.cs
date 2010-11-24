using System;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// TXT record class.
	/// </summary>
	[Serializable]
	public class TXT_Record : DnsRecordBase
	{
		private string m_Text = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="text">Text.</param>
		/// <param name="ttl">TTL value.</param>
		public TXT_Record(string text,int ttl) : base(QTYPE.TXT,ttl)
		{
			m_Text = text;
		}

		#region Properties Implementation

		/// <summary>
		/// Gets text.
		/// </summary>
		public string Text
		{
			get{ return m_Text; }
		}

		#endregion
	}
}
