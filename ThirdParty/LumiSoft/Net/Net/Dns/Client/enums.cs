using System;

namespace LumiSoft.Net.Dns.Client
{
	#region enum OPCODE

	/// <summary>
	/// 
	/// </summary>
	internal enum OPCODE
	{
		/// <summary>
		///  a standard query.
		/// </summary>
		QUERY    = 0,       

		/// <summary>
		/// an inverse query.
		/// </summary>
		IQUERY   = 1,

		/// <summary>
		/// a server status request.
		/// </summary>
		STATUS   = 2, 		
	}

	#endregion
}
