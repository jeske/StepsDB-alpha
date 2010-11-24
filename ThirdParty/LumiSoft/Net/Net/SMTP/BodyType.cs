using System;

namespace LumiSoft.Net.SMTP
{
	/// <summary>
	/// Holds body(mime) type.
	/// </summary>
	public enum BodyType
	{
		/// <summary>
		/// ASCII body.
		/// </summary>
		x7_bit = 1,

		/// <summary>
		/// ANSI body.
		/// </summary>
		x8_bit = 2,

		/// <summary>
		/// Binary body.
		/// </summary>
		binary = 4,
	}
}
