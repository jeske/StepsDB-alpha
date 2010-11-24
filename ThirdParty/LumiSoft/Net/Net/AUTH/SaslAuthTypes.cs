using System;

namespace LumiSoft.Net.AUTH
{	
	/// <summary>
	/// SASL authentications
	/// </summary>
	public enum SaslAuthTypes
	{
		/// <summary>
		/// Non authentication
		/// </summary>
		None = 0,

		/// <summary>
		/// LOGIN.
		/// </summary>
		Login = 1,

		/// <summary>
		/// CRAM-MD5
		/// </summary>
		Cram_md5 = 2,

		/// <summary>
		/// DIGEST-MD5.
		/// </summary>
		Digest_md5 = 4,

		/// <summary>
		/// All authentications.
		/// </summary>
		All = 0xF,
	}
}
