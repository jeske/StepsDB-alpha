using System;

namespace LumiSoft.Net.SMTP.Client
{
	/// <summary>
	/// Summary description for PartOfMessage_EventArgs.
	/// </summary>
	public class PartOfMessage_EventArgs
	{
		private string m_JobID         = "";
		private long   m_SentBlockSize = 0;
		private long   m_TotalSent     = 0;
		private long   m_MessageSize   = 0;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="jobID"></param>
		/// <param name="sentBlockSize">Size of sent block.</param>
		/// <param name="totalSent"></param>
		/// <param name="messageSize"></param>
		public PartOfMessage_EventArgs(string jobID,long sentBlockSize,long totalSent,long messageSize)
		{
			m_JobID         = jobID;
			m_SentBlockSize = sentBlockSize;
			m_TotalSent     = totalSent;
			m_MessageSize   = messageSize;
		}


		#region Properties Implementation

		/// <summary>
		/// Gets job ID which these properties are.
		/// </summary>
		public string JobID
		{
			get{ return m_JobID; }
		}

		/// <summary>
		/// Gets bytes what has sent  on this sendjob.
		/// </summary>
		public long SentBlockSize
		{
			get{ return m_SentBlockSize; }
		}

		/// <summary>
		/// Gets total bytes what has been sent on this sendjob.
		/// </summary>
		public long TotalSent
		{
			get{ return m_TotalSent; }
		}

		/// <summary>
		/// Gets message size.
		/// </summary>
		public long MessageSize
		{
			get{ return m_MessageSize; }
		}

		#endregion

	}
}
