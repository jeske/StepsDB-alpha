using System;
using System.IO;
using System.Text;
using System.Collections;

namespace LumiSoft.Net.Mime
{
	/// <summary>
	/// Simple Mime constructor. 
	/// Use this class if you need to construct simple email messages, use Mime class insted if you need to create complex messages.
	/// </summary>
	/// <example>
	/// <code>
	///	MimeConstructor m = new MimeConstructor();
	///	m.From = new Mailbox("display name","sender@domain.com");
	///	m.To = new Mailbox[]{new Mailbox("display name","to@domain.com")};
	///	m.Subject = "test subject";
	///	m.Text = "Test text";
	///	
	///	// Get mime data
	///	// byte[] mimeData = m.ToByteData();
	///	// or m.ToStream(storeStream)
	///	// or m.ToFile(fileName)
	/// </code>
	/// </example>
	[Obsolete("This class is will be removed, use Mime class instead.")]
	public class MimeConstructor
	{
		private HeaderFieldCollection m_pHeader      = null;
		private string                m_Body         = "";
		private string                m_BodyHtml     = "";
		private string                m_CharSet      = "";
		private Attachments           m_pAttachments = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public MimeConstructor()
		{
			m_pHeader = new HeaderFieldCollection();
			m_pHeader.Add("From:","");
			m_pHeader.Add("To:","");			
			m_pHeader.Add("Subject:","");
			m_pHeader.Add("Date:",DateTime.Now.ToUniversalTime().ToString("r",System.Globalization.DateTimeFormatInfo.InvariantInfo));
			m_pHeader.Add("Message-ID:","<" + Guid.NewGuid().ToString().Replace("-","") + "@" + System.Net.Dns.GetHostName() + ">");			
			m_pHeader.Add("MIME-Version:","1.0");

			m_pAttachments = new Attachments();

			m_CharSet = "utf-8";
		}


		// REMOVE ME: obsolete
		#region method ConstructBinaryMime

		/// <summary>
		/// Constructs mime message. [obsolete]
		/// </summary>
		/// <returns></returns>
		[Obsolete("Use ToStream(storeStream) instead ! Warning: stream position in ToStream isn't set to 0 any more",true)]
		public MemoryStream ConstructBinaryMime()
		{
			MemoryStream ms = new MemoryStream();
			ToStream(ms);
			ms.Position = 0;
			return ms;
		}

		#endregion

		// REMOVE ME: obsolete
		#region method ConstructMime

		/// <summary>
		/// Constructs mime message. [obsolete]
		/// </summary>
		/// <returns></returns>
		[Obsolete("Use ToStringData() instead",true)]
		public string ConstructMime()
		{
			return System.Text.Encoding.Default.GetString(ConstructBinaryMime().ToArray());
		}

		#endregion


		#region function ConstructBody

		private string ConstructBody(string boundaryID)
		{
			// TODO: encode text as quoted-printable

			StringBuilder str = new StringBuilder();

			str.Append("--" + boundaryID + "\r\n");
			str.Append("Content-Type: text/plain;\r\n");
				str.Append("\tcharset=\"" + m_CharSet + "\"\r\n");
			str.Append("Content-Transfer-Encoding: base64\r\n\r\n");

			str.Append(SplitString(Convert.ToBase64String(System.Text.Encoding.GetEncoding(m_CharSet).GetBytes(this.Body)) + "\r\n\r\n"));
			
			// We have html body, construct it.
			if(this.BodyHtml.Length > 0){
				str.Append("--" + boundaryID + "\r\n");
				str.Append("Content-Type: text/html;\r\n");
				str.Append("\tcharset=\"" + m_CharSet + "\"\r\n");
				str.Append("Content-Transfer-Encoding: base64\r\n\r\n");

				str.Append(SplitString(Convert.ToBase64String(System.Text.Encoding.GetEncoding(m_CharSet).GetBytes(this.BodyHtml))));
			}

			return str.ToString();
		}

		#endregion

//** Make some global method
		#region method AddressesToString

		/// <summary>
		/// Converts address array to RFC 2822 address field fomat.
		/// </summary>
		/// <param name="addresses"></param>
		/// <returns></returns>
		private string AddressesToString(MailboxAddress[] addresses)
		{
			string retVal = "";
			for(int i=0;i<addresses.Length;i++){
				// For last address don't add , and <TAB>
				if(i == (addresses.Length - 1)){
					retVal += addresses[i].MailboxString;
				}
				else{
					retVal += addresses[i].MailboxString + ",\t";
				}
			}

			return retVal;
		}

		#endregion
//** Make some global method
		#region method AddressesToArray

		/// <summary>
		///  Converts RFC 2822 address filed value to address array.
		/// </summary>
		/// <param name="addressFieldValue"></param>
		/// <returns></returns>
		private MailboxAddress[] AddressesToArray(string addressFieldValue)
		{			
			// We need to parse right !!! 
			// Can't use standard String.Split() because commas in quoted strings must be skiped.
			// Example: "ivar, test" <ivar@lumisoft.ee>,"xxx" <ivar2@lumisoft.ee>

			string[] retVal = TextUtils.SplitQuotedString(addressFieldValue,',');

			MailboxAddress[] xxx = new MailboxAddress[retVal.Length];			
			for(int i=0;i<retVal.Length;i++){				
				xxx[i] = MailboxAddress.Parse(retVal[i].Trim()); // Trim <TAB>s
			}
			
            return xxx;
		}

		#endregion


		#region method ToStringData

		/// <summary>
		/// Stores mime message to string.
		/// </summary>
		/// <returns></returns>
		public string ToStringData()
		{
			return System.Text.Encoding.Default.GetString(ToByteData());
		}

		#endregion

		#region method ToByteData

		/// <summary>
		/// Stores mime message to byte[].
		/// </summary>
		/// <returns></returns>
		public byte[] ToByteData()
		{
			MemoryStream ms = new MemoryStream();
			ToStream(ms);
			return ms.ToArray();
		}

		#endregion

		#region method ToFile

		/// <summary>
		/// Stores mime message to specified file.
		/// </summary>
		/// <param name="fileName">File name.</param>
		public void ToFile(string fileName)
		{
			using(FileStream fs = File.Create(fileName)){
				ToStream(fs);
			}
		}

		#endregion

		#region method ToStream

		/// <summary>
		/// Stores mime message to specified stream. Stream position stays where mime writing ends.
		/// </summary>
		/// <param name="storeStream">Stream where to store mime message.</param>
		public void ToStream(Stream storeStream)
		{
			byte[] buf = null; 

			string mainBoundaryID = "----=_NextPart_" + Guid.NewGuid().ToString().Replace("-","_");

			// Write headers
			buf = System.Text.Encoding.Default.GetBytes(m_pHeader.ToHeaderString("utf-8"));
			storeStream.Write(buf,0,buf.Length);


			// Content-Type:
			buf = System.Text.Encoding.Default.GetBytes("Content-Type: " + "multipart/mixed;\r\n\tboundary=\"" + mainBoundaryID + "\"\r\n");
			storeStream.Write(buf,0,buf.Length);

			// 
			buf = System.Text.Encoding.Default.GetBytes("\r\nThis is a multi-part message in MIME format.\r\n\r\n");
			storeStream.Write(buf,0,buf.Length);

			
			string bodyBoundaryID = "----=_NextPart_" + Guid.NewGuid().ToString().Replace("-","_");

			buf = System.Text.Encoding.Default.GetBytes("--" + mainBoundaryID + "\r\n");
			storeStream.Write(buf,0,buf.Length);
			buf = System.Text.Encoding.Default.GetBytes("Content-Type: multipart/alternative;\r\n\tboundary=\"" + bodyBoundaryID + "\"\r\n\r\n");
			storeStream.Write(buf,0,buf.Length);

			buf = System.Text.Encoding.Default.GetBytes(ConstructBody(bodyBoundaryID));
			storeStream.Write(buf,0,buf.Length);

			buf = System.Text.Encoding.Default.GetBytes("--" + bodyBoundaryID + "--\r\n");
			storeStream.Write(buf,0,buf.Length);

			//-- Construct attachments
			foreach(Attachment att in m_pAttachments){
				buf = System.Text.Encoding.Default.GetBytes("\r\n--" + mainBoundaryID + "\r\n");
				storeStream.Write(buf,0,buf.Length);

				buf = System.Text.Encoding.Default.GetBytes("Content-Type: application/octet;\r\n\tname=\"" + Core.CanonicalEncode(att.FileName,m_CharSet) + "\"\r\n");
				storeStream.Write(buf,0,buf.Length);

				buf = System.Text.Encoding.Default.GetBytes("Content-Transfer-Encoding: base64\r\n");
				storeStream.Write(buf,0,buf.Length);

				buf = System.Text.Encoding.Default.GetBytes("Content-Disposition: attachment;\r\n\tfilename=\"" + Core.CanonicalEncode(att.FileName,m_CharSet) + "\"\r\n\r\n");
				storeStream.Write(buf,0,buf.Length);
				
				buf = System.Text.Encoding.Default.GetBytes(SplitString(Convert.ToBase64String(att.FileData)));
				storeStream.Write(buf,0,buf.Length);
			}

			buf = System.Text.Encoding.Default.GetBytes("\r\n");
			storeStream.Write(buf,0,buf.Length);
			
			buf = System.Text.Encoding.Default.GetBytes("--" + mainBoundaryID + "--\r\n");
			storeStream.Write(buf,0,buf.Length);
		}

		#endregion
        

		#region method SplitString

		private string SplitString(string sIn)
		{
			StringBuilder str = new StringBuilder();

			int len = sIn.Length;
			int pos = 0;
			while(pos < len){
				if((len - pos) > 76){
					str.Append(sIn.Substring(pos,76) + "\r\n");
				}
				else{
					str.Append(sIn.Substring(pos,sIn.Length - pos) + "\r\n");
				}
				pos += 76;
			}

			return str.ToString();
		}

		#endregion

		
		#region Properties Implementation

		/// <summary>
		/// Gets message header. You can do message header customization here.
		/// </summary>
		public HeaderFieldCollection Header
		{
			get{ return m_pHeader; }
		}

		/// <summary>
		/// Gets header as RFC 2822 message headers.
		/// </summary>
		public string HeaderString
		{
			get{ return m_pHeader.ToHeaderString("utf-8"); }
		}

		/// <summary>
		/// Gets or sets header field "<b>Message-ID:</b>" value. Returns null if value isn't set.
		/// </summary>
		public string MessageID
		{
			get{
				if(m_pHeader.Contains("Message-ID:")){
					return m_pHeader.GetFirst("Message-ID:").Value;
				}
				else{
					return null;
				}
			}

			set{ 
				if(m_pHeader.Contains("Message-ID:")){
					m_pHeader.GetFirst("Message-ID:").Value = value;
				}
				else{
					m_pHeader.Add("Message-ID:",value);
				}
			}
		}

		/// <summary>
		/// Gets or sets header field "<b>To:</b>" value. Returns null if value isn't set.
		/// </summary>
		public MailboxAddress[] To
		{
			get{ 
				if(m_pHeader.Contains("To:")){
					return AddressesToArray(m_pHeader.GetFirst("To:").Value);
				}
				else{
					return null;
				}
			}

			set{
				if(m_pHeader.Contains("To:")){
					m_pHeader.GetFirst("To:").Value = AddressesToString(value);
				}
				else{
					m_pHeader.Add("To:",AddressesToString(value));
				}
			}
		}

		/// <summary>
		/// Gets or sets header field "<b>Cc:</b>" value. Returns null if value isn't set.
		/// </summary>
		public MailboxAddress[] Cc
		{
			get{ 
				if(m_pHeader.Contains("Cc:")){
					return AddressesToArray(m_pHeader.GetFirst("Cc:").Value);
				}
				else{
					return null;
				}
			}

			set{ 
				if(m_pHeader.Contains("Cc:")){
					m_pHeader.GetFirst("Cc:").Value = AddressesToString(value);
				}
				else{
					m_pHeader.Add("Cc:",AddressesToString(value));
				}
			}
		}

		/// <summary>
		/// Gets or sets header field "<b>Bcc:</b>" value. Returns null if value isn't set.
		/// </summary>
		public MailboxAddress[] Bcc
		{
			get{ 
				if(m_pHeader.Contains("Bcc:")){
					return AddressesToArray(m_pHeader.GetFirst("Bcc:").Value);
				}
				else{
					return null;
				}
			}

			set{ 
				if(m_pHeader.Contains("Bcc:")){
					m_pHeader.GetFirst("Bcc:").Value = AddressesToString(value);
				}
				else{
					m_pHeader.Add("Bcc:",AddressesToString(value));
				}
			}
		}

		/// <summary>
		/// Gets or sets header field "<b>From:</b>" value. Returns null if value isn't set.
		/// </summary>
		public MailboxAddress From
		{
			get{  
				if(m_pHeader.Contains("From:")){
					return MailboxAddress.Parse(m_pHeader.GetFirst("From:").Value);
				}
				else{
					return null;
				}
			}

			set{ 
				if(m_pHeader.Contains("From:")){
					m_pHeader.GetFirst("From:").Value = value.MailboxString;
				}
				else{
					m_pHeader.Add("From:",value.MailboxString);
				}
			}
		}

		/// <summary>
		/// Gets or sets header field "<b>Disposition-Notification-To:</b>" value. Returns null if value isn't set.
		/// </summary>
		public string DSN
		{
			get{ 
				if(m_pHeader.Contains("Disposition-Notification-To:")){
					return m_pHeader.GetFirst("Disposition-Notification-To:").Value;
				}
				else{
					return null;
				}
			}

			set{ 
				if(m_pHeader.Contains("Disposition-Notification-To:")){
					m_pHeader.GetFirst("Disposition-Notification-To:").Value = value;
				}
				else{
					m_pHeader.Add("Disposition-Notification-To:",value);
				}
			}
		}

		/// <summary>
		/// Gets or sets header field "<b>Subject:</b>" value. Returns null if value isn't set.
		/// </summary>
		public string Subject
		{
			get{ 
				if(m_pHeader.Contains("Subject:")){
					return m_pHeader.GetFirst("Subject:").Value;
				}
				else{
					return null;
				}
			}

			set{ 
				if(m_pHeader.Contains("Subject:")){
					m_pHeader.GetFirst("Subject:").Value = value;
				}
				else{
					m_pHeader.Add("Subject:",value);
				}
			}
		}

		/// <summary>
		/// Gets or sets header field "<b>Date:</b>" value. Returns null if value isn't set.
		/// </summary>
		public DateTime Date
		{
			get{ 
				if(m_pHeader.Contains("Date:")){
					return DateTime.ParseExact(m_pHeader.GetFirst("Date:").Value,"r",System.Globalization.DateTimeFormatInfo.InvariantInfo);
				}
				else{
					return DateTime.MinValue;
				}
			}

			set{				
				if(m_pHeader.Contains("Date:")){
					m_pHeader.GetFirst("Date:").Value = value.ToUniversalTime().ToString("r",System.Globalization.DateTimeFormatInfo.InvariantInfo);
				}
				else{
					m_pHeader.Add("Date:",value.ToUniversalTime().ToString("r",System.Globalization.DateTimeFormatInfo.InvariantInfo));
				}
			}
		}

		/// <summary>
		/// Gets or sets body text.
		/// </summary>
		public string Body
		{
			get{ return m_Body; }

			set{
				if(value == null){
					m_Body = "";
				}
				else{
					m_Body = value; 
				}
			}
		}

		/// <summary>
		/// Gets or sets html body.
		/// </summary>
		public string BodyHtml
		{
			get{ return m_BodyHtml; }

			set{ m_BodyHtml = value; }
		}

		/// <summary>
		/// Gets or sets message charset. Default is 'utf-8'.
		/// </summary>
		public string CharSet
		{
			get{ return m_CharSet; }

			set{ m_CharSet = value; }
		}

		/// <summary>
		/// Gets referance to attachments collection.
		/// </summary>
		public Attachments Attachments
		{
			get{ return m_pAttachments; }
		}

		#endregion

	}
}
