using System;

namespace LumiSoft.Net
{
	/// <summary>
	/// Summary description for ISocketServerSession.
	/// </summary>
	public interface ISocketServerSession
	{
		/// <summary>
		/// 
		/// </summary>
		void OnSessionTimeout();

		/// <summary>
		/// 
		/// </summary>
		DateTime SessionLastDataTime
		{
			get;
		}
	}
}
