using System;
using System.IO;
using System.Collections;
using System.Text;

namespace LumiSoft.Net.Mime
{
	#region enum ContentDisposition

	/// <summary>
	/// Content disposition.
	/// </summary>
	[Obsolete("Use Mime class instead, this class will be removed !")]
	public enum Disposition
	{
		/// <summary>
		/// Content is attachment.
		/// </summary>
		Attachment = 0,

		/// <summary>
		/// Content is embbed resource.
		/// </summary>
		Inline = 1,

		/// <summary>
		/// Content is unknown.
		/// </summary>
		Unknown = 40
	}

	#endregion

	/// <summary>
	/// Mime parser.
	/// </summary>
	/// <example>
	/// <code>
	/// // NOTE: load you message to byte[] here (from file,POP3_Client or IMAP_Client, ...).
	/// byte[] data = null;
	/// 
	/// MimeParser p = new MimeParser(data);
	/// 
	/// // Do your stuff here
	/// string from = p.Form;
	/// 
	/// </code>
	/// </example>
	[Obsolete("Use Mime class instead, this class will be removed !")]
	public class MimeParser
	{		
		private string       m_Headers    = "";
		private string       m_BoundaryID = "";
		private MemoryStream m_MsgStream  = null;
		private ArrayList    m_Entries    = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="msg">Mime message which to parse.</param>
		public MimeParser(byte[] msg)
		{
			m_MsgStream = new MemoryStream(msg);

			m_Headers    = ParseHeaders(m_MsgStream);
			m_BoundaryID = ParseHeaderFiledSubField("Content-Type:","boundary",m_Headers);
		}


		#region function ParseAddress

		private string[] ParseAddress(string headers,string fieldName)
		{
			// ToDo: Return as eAddress 

			string addressFieldValue = ParseHeaderField(fieldName,headers);
			
		//	string[] addressList = addressFieldValue.Split(new char[]{','});
		//	for(int i=0;i<addressList.Length;i++){
		//		addressList[i] = Core.CanonicalDecode(addressList[i].Trim());
		//	}
			
			ArrayList a = new ArrayList();
			string buff = "";
			bool quotedString = false;
			for(int i=0;i<addressFieldValue.Length;i++){
				// Start or close quoted string ("")
				if(addressFieldValue[i] == '\"'){
					if(quotedString){
						quotedString = false;
					}
					else{
						quotedString = true;
					}
				}

				// If ',' isn't between "" or last one(no ending ','), split string
				if(i == addressFieldValue.Length - 1 || (!quotedString && addressFieldValue[i] == ',')){
					// last one(no ending ','), don't loose last char
					if(addressFieldValue[i] != ','){
						buff += addressFieldValue[i];
					}
					a.Add(Core.CanonicalDecode(buff));					
					buff = "";
				}
				else{
					buff += addressFieldValue[i];
				}
			}

			// ToDo: don't return "return new string[]{""};", get rid of it

			if(a.Count > 0){
				return (string[])a.ToArray(typeof(string));
			}
			else{
				return new string[]{""};
			}
		}

		#endregion

		#region function ParseContentType

		/// <summary>
		/// Parse content type.
		/// </summary>
		/// <param name="headers"></param>
		/// <returns></returns>
		internal string ParseContentType(string headers)
		{
			string contentType = ParseHeaderField("CONTENT-TYPE:",headers);
			if(contentType.Length > 0){
				return contentType.Split(';')[0];
			}
			else{
				return "text/plain";
			}
		}

		#endregion

		#region function ParseEntries

		/// <summary>
		/// Parses mime entries.
		/// </summary>
		/// <param name="msgStrm"></param>
		/// <param name="pos"></param>
		/// <param name="boundaryID"></param>
		internal ArrayList ParseEntries(MemoryStream msgStrm,int pos,string boundaryID)
		{
			ArrayList entries = null;
		
			// Entries are already parsed
			if(m_Entries != null){
				return m_Entries;
			}

			entries = new ArrayList();

			// If message doesn't have entries and have 1 entry (simple text message or contains only attachment).
			if(this.ContentType.ToLower().IndexOf("multipart/") == -1){
				entries.Add(new MimeEntry(msgStrm.ToArray(),this));
				m_Entries = entries;

				return m_Entries;
			}

			msgStrm.Position = pos;

			if(boundaryID.Length > 0){
				MemoryStream strmEntry = new MemoryStream();
				StreamLineReader reader = new StreamLineReader(msgStrm);
				byte[] lineData = reader.ReadLine();

				// Search first entry
				while(lineData != null){
					string line = System.Text.Encoding.Default.GetString(lineData);
					if(line.StartsWith("--" + boundaryID)){
						break;
					}
					
					lineData = reader.ReadLine();
				}

				// Start reading entries
				while(lineData != null){
					// Read entry data
					string line = System.Text.Encoding.Default.GetString(lineData);
					// Next boundary
					if(line.StartsWith("--" + boundaryID) && strmEntry.Length > 0){
						// Add Entry
						entries.Add(new MimeEntry(strmEntry.ToArray(),this));						
											
						strmEntry.SetLength(0);
					}
					else{
						strmEntry.Write(lineData,0,lineData.Length);
						strmEntry.Write(new byte[]{(byte)'\r',(byte)'\n'},0,2);
					}
						
					lineData = reader.ReadLine();
				}
			}

			return entries;
		}

		#endregion


		#region function GetEntries

		/// <summary>
		/// Gets mime entries, including nested entries. 
		/// </summary>
		/// <param name="entries"></param>
		/// <param name="allEntries"></param>
		private void GetEntries(ArrayList entries,ArrayList allEntries)
		{				
			if(entries != null){
				allEntries.AddRange(entries);
			}

			if(entries != null){
				foreach(MimeEntry ent in entries){
					GetEntries(ent.MimeEntries,allEntries);
				}
			}
		}

		#endregion


		#region method ParseHeaderField

		/// <summary>
		/// Parse header specified header field value.
		/// </summary>
		/// <param name="fieldName">Header field which to parse. Eg. Subject: .</param>
		/// <returns>Restuns specified header filed value.</returns>
		public string ParseHeaderField(string fieldName)
		{
			return ParseHeaderField(fieldName,m_Headers);
		}

		#endregion


		#region static function ParseDate

		/// <summary>
		/// Parses rfc2822 datetime.
		/// </summary>
		/// <param name="date">Date string</param>
		/// <returns></returns>
		public static DateTime ParseDateS(string date)
		{
			/*
			GMT  -0000
			EDT  -0400
			EST  -0500
			CDT  -0500
			CST  -0600
			MDT  -0600
			MST  -0700
			PDT  -0700
			PST  -0800
			
			BST  +0100 British Summer Time
			*/

			date = date.Replace("GMT","-0000");
			date = date.Replace("EDT","-0400");
			date = date.Replace("EST","-0500");
			date = date.Replace("CDT","-0500");
			date = date.Replace("CST","-0600");
			date = date.Replace("MDT","-0600");
			date = date.Replace("MST","-0700");
			date = date.Replace("PDT","-0700");
			date = date.Replace("PST","-0800");
			date = date.Replace("BST","+0100");

			// Remove () from datest similar "Mon, 13 Oct 2003 20:50:57 +0300 (EEST)"
			if(date.IndexOf(" (") > -1){
				date = date.Substring(0,date.IndexOf(" ("));
			}

			date = date.Trim();
			//Remove multiple continues spaces
			while(date.IndexOf("  ") > -1){
				date = date.Replace("  "," ");
			}

			string[] formats = new string[]{
				"r",
				"ddd, d MMM yyyy HH':'mm':'ss zzz",
			    "ddd, d MMM yyyy H':'mm':'ss zzz",
			    "ddd, d MMM yy HH':'mm':'ss zzz",
		        "ddd, d MMM yy H':'mm':'ss zzz",
				"ddd, dd MMM yyyy HH':'mm':'ss zzz",
			    "ddd, dd MMM yyyy H':'mm':'ss zzz",
			    "ddd, dd MMM yy HH':'mm':'ss zzz",
			    "ddd, dd MMM yy H':'mm':'ss zzz",
				"dd'-'MMM'-'yyyy HH':'mm':'ss zzz",
			    "dd'-'MMM'-'yyyy H':'mm':'ss zzz",
				"d'-'MMM'-'yyyy HH':'mm':'ss zzz",
				"d'-'MMM'-'yyyy H':'mm':'ss zzz",
				"d MMM yyyy HH':'mm':'ss zzz",
				"d MMM yyyy H':'mm':'ss zzz",
				"dd MMM yyyy HH':'mm':'ss zzz",
				"dd MMM yyyy H':'mm':'ss zzz",
			};

			return DateTime.ParseExact(date.Trim(),formats,System.Globalization.DateTimeFormatInfo.InvariantInfo,System.Globalization.DateTimeStyles.None); 
		}

		#endregion

		#region static function ParseHeaders

		/// <summary>
		/// Parses headers from message or mime entry.
		/// </summary>
		/// <param name="entryStrm">Stream from where to read headers.</param>
		/// <returns>Returns header lines.</returns>
		public static string ParseHeaders(Stream entryStrm)
		{
			/*3.1.  GENERAL DESCRIPTION
			A message consists of header fields and, optionally, a body.
			The  body  is simply a sequence of lines containing ASCII charac-
			ters.  It is separated from the headers by a null line  (i.e.,  a
			line with nothing preceding the CRLF).
			*/

			byte[] crlf = new byte[]{(byte)'\r',(byte)'\n'};
			MemoryStream msHeaders = new MemoryStream();
			StreamLineReader r = new StreamLineReader(entryStrm);
			byte[] lineData = r.ReadLine();
			while(lineData != null){
				if(lineData.Length == 0){
					break;
				}

				msHeaders.Write(lineData,0,lineData.Length);
				msHeaders.Write(crlf,0,crlf.Length);
				lineData = r.ReadLine();
			}

			return System.Text.Encoding.Default.GetString(msHeaders.ToArray());
		}

		#endregion

		#region static function ParseHeaderField

		/// <summary>
		/// Parse header specified header field value.
		/// 
		/// Use this method only if you need to get only one header field, otherwise use
		/// MimeParser.ParseHeaderField(string fieldName,string headers).
		/// This avoid parsing headers multiple times.
		/// </summary>
		/// <param name="fieldName">Header field which to parse. Eg. Subject: .</param>
		/// <param name="entryStrm">Stream from where to read headers.</param>
		/// <returns></returns>
		public static string ParseHeaderField(string fieldName,Stream entryStrm)
		{
			return ParseHeaderField(fieldName,ParseHeaders(entryStrm));
		}

		/// <summary>
		/// Parse header specified header field value.
		/// </summary>
		/// <param name="fieldName">Header field which to parse. Eg. Subject: .</param>
		/// <param name="headers">Full headers string. Use MimeParser.ParseHeaders() to get this value.</param>
		public static string ParseHeaderField(string fieldName,string headers)
		{
			/* Rfc 2822 2.2 Header Fields
				Header fields are lines composed of a field name, followed by a colon
				(":"), followed by a field body, and terminated by CRLF.  A field
				name MUST be composed of printable US-ASCII characters (i.e.,
				characters that have values between 33 and 126, inclusive), except
				colon.  A field body may be composed of any US-ASCII characters,
				except for CR and LF.  However, a field body may contain CRLF when
				used in header "folding" and  "unfolding" as described in section
				2.2.3.  All field bodies MUST conform to the syntax described in
				sections 3 and 4 of this standard. 
				
			   Rfc 2822 2.3 (Multiline header fields)
				The process of moving from this folded multiple-line representation
				of a header field to its single line representation is called
				"unfolding". Unfolding is accomplished by simply removing any CRLF
				that is immediately followed by WSP.  Each header field should be
				treated in its unfolded form for further syntactic and semantic
				evaluation.
				
				Example:
					Subject: aaaaa<CRLF>
					<TAB or SP>aaaaa<CRLF>
			*/

			using(TextReader r = new StreamReader(new MemoryStream(System.Text.Encoding.Default.GetBytes(headers)))){
				string line = r.ReadLine();
				while(line != null){
					// Find line where field begins
					if(line.ToUpper().StartsWith(fieldName.ToUpper())){
						// Remove field name and start reading value
						string fieldValue = line.Substring(fieldName.Length).Trim();

						// see if multi line value. See commnt above.
						line = r.ReadLine();
						while(line != null && (line.StartsWith("\t") || line.StartsWith(" "))){
							fieldValue += line;
							line = r.ReadLine();
						}

						return fieldValue;
					}

					line = r.ReadLine();
				}
			}

			return "";
		}

		#endregion

		#region static function ParseHeaderFiledSubField

		/// <summary>
		/// Parses header field sub field value. 
		/// For example: CONTENT-TYPE: application\octet-stream; name="yourFileName.xxx",
		/// fieldName="CONTENT-TYPE:" and subFieldName="name".
		/// </summary>
		/// <param name="fieldName">Main header field name.</param>
		/// <param name="subFieldName">Header field's sub filed name.</param>
		/// <param name="headers">Full headrs string.</param>
		/// <returns></returns>
		public static string ParseHeaderFiledSubField(string fieldName,string subFieldName,string headers)
		{
			string mainFiled = ParseHeaderField(fieldName,headers);
			// Parse sub field value
			if(mainFiled.Length > 0){
				int index = mainFiled.ToUpper().IndexOf(subFieldName.ToUpper());
				if(index > -1){	
					mainFiled = mainFiled.Substring(index + subFieldName.Length + 1); // Remove "subFieldName="

					// subFieldName value may be in "" and without
					if(mainFiled.StartsWith("\"")){						
						return mainFiled.Substring(1,mainFiled.IndexOf("\"",1) - 1);
					}
					// value without ""
					else{
						int endIndex = mainFiled.Length;
						if(mainFiled.IndexOf(" ") > -1){
							endIndex = mainFiled.IndexOf(" ");
						}

						return mainFiled.Substring(0,endIndex);
					}						
				}
			}
			
			return "";			
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets message headers.
		/// </summary>
		public string Headers
		{
			get{ return m_Headers; }
		}

		/// <summary>
		/// Gets sender.
		/// </summary>
		public string From
		{
			get{ return (ParseAddress(m_Headers,"FROM:"))[0]; }
		}

		/// <summary>
		/// Gets recipients.
		/// </summary>
		public string[] To
		{
			get{ return ParseAddress(m_Headers,"TO:"); }
		}

		/// <summary>
		/// Gets cc.
		/// </summary>
		public string[] Cc
		{
			get{ return ParseAddress(m_Headers,"CC:"); }
		}

		/// <summary>
		/// Gets bcc.
		/// </summary>
		public string[] Bcc
		{
			get{ return ParseAddress(m_Headers,"BCC:"); }
		}

		/// <summary>
		/// Gets subject.
		/// </summary>
		public string Subject
		{
			get{ return Core.CanonicalDecode(ParseHeaderField("SUBJECT:",m_Headers)); }
		}

		/// <summary>
		/// Gets message body text.
		/// </summary>
		public string BodyText
		{
			get{
				m_Entries = ParseEntries(m_MsgStream,m_Headers.Length,m_BoundaryID);

				// Find first text entry
				ArrayList entries = new ArrayList();				
				GetEntries(this.MimeEntries,entries);
				
				foreach(MimeEntry ent in entries){
					if(ent.ContentType.ToUpper().IndexOf("TEXT/PLAIN") > -1 && ent.ContentDisposition != Disposition.Attachment){
						return ent.DataS;
					}
				}

				return "";
			}
		}

		/// <summary>
		/// Gets message body HTML.
		/// </summary>
		public string BodyHtml
		{
			get{
				m_Entries = ParseEntries(m_MsgStream,m_Headers.Length,m_BoundaryID);

				// Find first text entry
				ArrayList entries = new ArrayList();				
				GetEntries(this.MimeEntries,entries);
				
				foreach(MimeEntry ent in entries){
					if(ent.ContentType.ToUpper().IndexOf("TEXT/HTML") > -1){
						return ent.DataS;
					}
				}

				return "";
			}
		}

		/// <summary>
		/// Gets messageID.
		/// </summary>
		public string MessageID
		{
			get{ return ParseHeaderField("MESSAGE-ID:",m_Headers); }
		}

		/// <summary>
		/// Gets message content type.
		/// </summary>
		public string ContentType
		{
			get{ return ParseContentType(m_Headers); }
		}

		/// <summary>
		/// Gets message date.
		/// </summary>
		public DateTime MessageDate
		{
			get{ 
				try{
					return ParseDateS(ParseHeaderField("DATE:",m_Headers));
				}
				catch{
					return DateTime.Today;
				}
			}
		}

		/// <summary>
		/// Gets message mime entries.
		/// </summary>
		public ArrayList MimeEntries
		{
			get{ 
				m_Entries = ParseEntries(m_MsgStream,m_Headers.Length,m_BoundaryID);

				return m_Entries; 
			}
		}

		/// <summary>
		/// Gets mime entries which Content-Disposition: Attachment or Content-Disposition: Inline.
		/// </summary>
		public ArrayList Attachments
		{			
			get{
				ArrayList retVal  = new ArrayList();
				ArrayList entries = new ArrayList();
				
				GetEntries(this.MimeEntries,entries);

				// Loop all entries and find attachments
				foreach(MimeEntry entry in entries){
					if(entry.ContentDisposition == Disposition.Attachment || entry.ContentDisposition == Disposition.Inline){
						retVal.Add(entry);
					}
				}

				return retVal; 
			}
		}

		#endregion

	}
}
