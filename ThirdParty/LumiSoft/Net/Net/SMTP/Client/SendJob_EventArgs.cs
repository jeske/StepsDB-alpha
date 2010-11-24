using System;

namespace LumiSoft.Net.SMTP.Client
{
	/// <summary>
	/// Summary description for SendJob_Eventargs.
	/// </summary>
	public class SendJob_EventArgs
	{
		private string   m_JobID     = "";
		private string[] m_To        = null;
		private string[] m_DefEmails = null;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="jobID"></param>
		/// <param name="to"></param>
		public SendJob_EventArgs(string jobID,string[] to) : this(jobID,to,null)
		{				
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="jobID"></param>
		/// <param name="to"></param>
		/// <param name="defectiveEmails"></param>
		public SendJob_EventArgs(string jobID,string[] to,string[] defectiveEmails)
		{	
			m_JobID     = jobID;
			m_To        = to;
			m_DefEmails = defectiveEmails;
		}


		#region Properties Implementation

		/// <summary>
		/// Gets send job ID.
		/// </summary>
		public string JobID
		{
			get{ return m_JobID; }
		}

		/// <summary>
		/// Gets this send job's email addresses.
		/// </summary>
		public string[] To
		{
			get{ return m_To; }
		}

		/// <summary>
		/// Gets email addresses to which message couldn't be sent.
		/// </summary>
		public string[] DeffectiveEmails
		{
			get{ return m_DefEmails; }
		}

		#endregion

	}
}
