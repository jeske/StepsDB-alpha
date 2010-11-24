using System;
using System.IO;
using System.Collections;
using System.Text;

namespace LumiSoft.Net.Mime
{
	/// <summary>
	/// Mime entry.
	/// </summary>
	[Obsolete("Use Mime classes instead, this class will be removed !")]
	public class MimeEntry
	{
		private string    m_Headers         = "";
		private string    m_ContentType     = "";
		private string    m_CharSet         = "";
		private string    m_ContentEncoding = "";
		private byte[]    m_Data            = null;
		private ArrayList m_Entries         = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="mimeEntry"></param>
		/// <param name="mime"></param>		
		public MimeEntry(byte[] mimeEntry,MimeParser mime)
		{
			MemoryStream entryStrm = new MemoryStream(mimeEntry);

			m_Headers     = MimeParser.ParseHeaders(entryStrm);
			m_ContentType = mime.ParseContentType(m_Headers);

			m_Entries = new ArrayList();

			// != multipart content
			if(m_ContentType.ToLower().IndexOf("multipart") == -1){
				m_CharSet         = ParseCharSet(m_Headers);
				m_ContentEncoding = ParseEncoding(m_Headers);

				m_Data = new byte[entryStrm.Length - entryStrm.Position];
				entryStrm.Read(m_Data,0,m_Data.Length);
			}
			// multipart content, get nested entries
			else{
				long s = (int)entryStrm.Position;
				string boundaryID = MimeParser.ParseHeaderFiledSubField("Content-Type:","boundary",m_Headers);
				m_Entries = mime.ParseEntries(entryStrm,(int)entryStrm.Position,boundaryID);

				entryStrm.Position = s;
				m_Data = new byte[entryStrm.Length - s];
				entryStrm.Read(m_Data,0,m_Data.Length);
			}
		}


		#region function ParseCharSet

		/// <summary>
		/// Parse charset.
		/// </summary>
		/// <param name="headers"></param>
		/// <returns></returns>
		private string ParseCharSet(string headers)
		{			
			string charset = MimeParser.ParseHeaderFiledSubField("Content-Type:","charset",headers);
			// charset ends with ; remove it. 
			if(charset.EndsWith(";")){
				charset = charset.Substring(0,charset.Length - 1);
			}
			if(charset.Length > 0){
				try{
					Encoding.GetEncoding(charset);

					return charset;
				}
				catch{
					return "ascii";
				}
			}
			// If no charset, consider it as ascii
			else{				
				return "ascii";				
			}
		}

		#endregion

		#region function ParseEncoding

		/// <summary>
		/// Parse encoding. quoted-printable,7bit,8bit,base64 is supported.
		/// </summary>
		/// <param name="headers"></param>
		/// <returns></returns>
		private string ParseEncoding(string headers)
		{
			string encoding = MimeParser.ParseHeaderField("CONTENT-TRANSFER-ENCODING:",headers);			
			if(encoding.Length > 0){
				return encoding;
			}
			// If no encoding, consider it as ascii
			else{
				return "7bit";
			}
		}

		#endregion

		#region function ParseContentDisposition

		private Disposition ParseContentDisposition(string headers)
		{
			string disposition = MimeParser.ParseHeaderField("CONTENT-DISPOSITION:",headers);
			if(disposition.ToUpper().IndexOf("ATTACHMENT") > -1){
				return Disposition.Attachment;
			}

			if(disposition.ToUpper().IndexOf("INLINE") > -1){
				return Disposition.Inline;
			}

			return Disposition.Unknown;
		}

		#endregion


		#region function DecodeData

		/// <summary>
		/// Decode entry data.
		/// </summary>
		/// <param name="mimeDataEntry"></param>
		/// <returns></returns>
		private byte[] DecodeData(byte[] mimeDataEntry)
		{
			switch(m_ContentEncoding.ToLower())
			{
				case "quoted-printable":
					if(m_ContentType.ToLower().IndexOf("text") > -1){
						return Core.QuotedPrintableDecodeB(mimeDataEntry,true);
					}
					else{
						return Core.QuotedPrintableDecodeB(mimeDataEntry,false);
					}					

		//		case "7bit":
		//			return mimeDataEntry;

		//		case "8bit":
		//			return mimeDataEntry;

				case "base64":
					string dataStr = Encoding.Default.GetString(mimeDataEntry);
					if(dataStr.Trim().Length > 0){
						return Convert.FromBase64String(dataStr);
					}
					else{
						return new byte[]{};
					}

				default:
					return mimeDataEntry;
			}
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets content encoding.
		/// </summary>
		public string ContentEncoding
		{
			get{ return m_ContentEncoding; }
		}

		/// <summary>
		/// Gets content type.
		/// </summary>
		public string ContentType
		{
			get{ return m_ContentType; }
		}

		/// <summary>
		/// Gets content disposition type.
		/// </summary>
		public Disposition ContentDisposition
		{ 
			get{ return ParseContentDisposition(m_Headers); }
		}

		/// <summary>
		/// Gets headers.
		/// </summary>
		public string Headers
		{
			get{ return m_Headers; }
		}

		/// <summary>
		/// Gets file name. NOTE: available only if ContentDisposition.Attachment.
		/// </summary>
		public string FileName
		{
			get{ return Core.CanonicalDecode(MimeParser.ParseHeaderFiledSubField("Content-Disposition:","filename",m_Headers)); }
		}

		/// <summary>
		/// Gets mime entry decoded data.
		/// </summary>
		public byte[] Data
		{
			get{ return DecodeData(m_Data); }
		}

		/// <summary>
		/// Gets mime entry non decoded data.
		/// </summary>
		public byte[] DataNonDecoded
		{
			get{ return m_Data; }
		}

		/// <summary>
		/// Gets string data. NOTE: available only if content-type=text/xxx.
		/// </summary>
		public string DataS
		{
			get{
				try{
					return Encoding.GetEncoding(m_CharSet).GetString(this.Data);
				}
				catch{
					return Encoding.Default.GetString(this.Data);
				}
			}
		}

		/// <summary>
		/// Gets nested mime entries.
		/// </summary>
		public ArrayList MimeEntries
		{
			get{ return m_Entries; }
		}

		#endregion

	}
}
