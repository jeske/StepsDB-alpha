using System;

namespace LumiSoft.Net.SMTP.Server
{
	/// <summary>
	/// SMTP command order validator.
	/// </summary>
	internal class SMTP_Cmd_Validator
	{
		private bool m_Helo_ok       = false;
		private bool m_Authenticated = false;
		private bool m_MailFrom_ok   = false;
		private bool m_RcptTo_ok     = false;
		private bool m_BdatLast_ok   = false;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public SMTP_Cmd_Validator()
		{			
		}


		#region function Reset

		/// <summary>
		/// Resets state.
		/// </summary>
		public void Reset()
		{
			m_Helo_ok       = false;
			m_Authenticated = false;
			m_MailFrom_ok   = false;
			m_RcptTo_ok     = false;
			m_BdatLast_ok   = false;
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets if may handle MAIL command.
		/// </summary>
		public bool MayHandle_MAIL
		{
			get{ return m_Helo_ok && !MailFrom_ok; }
		}

		/// <summary>
		/// Gets if may handle RCPT command.
		/// </summary>
		public bool MayHandle_RCPT
		{
			get{ return MailFrom_ok; }
		}

		/// <summary>
		/// Gets if may handle DATA command.
		/// </summary>
		public bool MayHandle_DATA
		{
			get{ return RcptTo_ok; }
		}

		/// <summary>
		/// Gets if may handle BDAT command.
		/// </summary>
		public bool MayHandle_BDAT
		{
			get{ return RcptTo_ok && !m_BdatLast_ok; }
		}

		/// <summary>
		/// Gets if may handle AUTH command.
		/// </summary>
		public bool MayHandle_AUTH
		{
			get{ return !m_Authenticated; }
		}

		/// <summary>
		/// Gest or sets if HELO command handled.
		/// </summary>
		public bool Helo_ok
		{
			get{ return m_Helo_ok; }

			set{ m_Helo_ok = value; }
		}

		/// <summary>
		/// Gest or sets if AUTH command handled.
		/// </summary>
		public bool Authenticated
		{
			get{ return m_Authenticated; }

			set{ m_Authenticated = value; }
		}

		/// <summary>
		/// Gest or sets if MAIL command handled.
		/// </summary>
		public bool MailFrom_ok
		{
			get{ return m_MailFrom_ok; }

			set{ m_MailFrom_ok = value; }
		}

		/// <summary>
		/// Gest or sets if RCPT command handled.
		/// </summary>
		public bool RcptTo_ok
		{
			get{ return m_RcptTo_ok; }

			set{ m_RcptTo_ok = value; }
		}

		/// <summary>
		/// Gest or sets if BinaryMime.
		/// </summary>
		public bool BDAT_Last_ok
		{
			get{ return m_BdatLast_ok; }

			set{ m_BdatLast_ok = value; }
		}

		#endregion

	}
}
