using System;
using System.IO;
using System.Collections;
using LumiSoft.Net.Mime;

namespace LumiSoft.Net.IMAP.Server
{
	/// <summary>
	/// FETCH command helper methods.
	/// </summary>
	internal class FetchHelper
	{				
		#region function ParseHeaderFields

		/// <summary>
		/// Returns requested header fields lines.
		/// </summary>
		/// <param name="fieldsStr">Header fields to get.</param>
		/// <param name="data">Message data.</param>
		/// <returns></returns>
		public static string ParseHeaderFields(string fieldsStr,byte[] data)
		{
			string retVal = "";

			string[] fields = fieldsStr.Split(' ');
            using(MemoryStream mStrm = new MemoryStream(data)){
				TextReader r = new StreamReader(mStrm);
				string line = r.ReadLine();
				
				bool fieldFound = false;
				// Loop all header lines
				while(line != null){ 
					// End of header
					if(line.Length == 0){
						break;
					}

					// Field continues
					if(fieldFound && line.StartsWith("\t")){
						retVal += line + "\r\n";
					}
					else{
						fieldFound = false;

						// Check if wanted field
						foreach(string field in fields){
							if(line.Trim().ToLower().StartsWith(field.Trim().ToLower())){
								retVal += line + "\r\n";
								fieldFound = true;
							}
						}
					}

					line = r.ReadLine();
				}
			}

			// Add header terminating blank line
			retVal += "\r\n"; 

			return retVal;
		}

		#endregion

		#region function ParseHeaderFieldsNot

		/// <summary>
		/// Returns header fields lines except requested.
		/// </summary>
		/// <param name="fieldsStr">Header fields to skip.</param>
		/// <param name="data">Message data.</param>
		/// <returns></returns>
		public static string ParseHeaderFieldsNot(string fieldsStr,byte[] data)
		{
			string retVal = "";

			string[] fields = fieldsStr.Split(' ');
            using(MemoryStream mStrm = new MemoryStream(data)){
				TextReader r = new StreamReader(mStrm);
				string line = r.ReadLine();
				
				bool fieldFound = false;
				// Loop all header lines
				while(line != null){ 
					// End of header
					if(line.Length == 0){
						break;
					}

					// Filed continues
					if(fieldFound && line.StartsWith("\t")){
						retVal += line + "\r\n";
					}
					else{
						fieldFound = false;

						// Check if wanted field
						foreach(string field in fields){
							if(line.Trim().ToLower().StartsWith(field.Trim().ToLower())){								
								fieldFound = true;
							}
						}

						if(!fieldFound){
							retVal += line + "\r\n";
						}
					}

					line = r.ReadLine();
				}
			}

			return retVal;
		}

		#endregion

		#region function ParseMimeEntry

		/// <summary>
		/// Returns requested mime entry data.
		/// </summary>
		/// <param name="parser"></param>
		/// <param name="mimeEntryNo"></param>
		/// <returns>Returns requested mime entry data or NULL if requested entry doesn't exist.</returns>
		public static byte[] ParseMimeEntry(LumiSoft.Net.Mime.Mime parser,string mimeEntryNo)
		{
			MimeEntity mEntry = null;
			string[] parts = mimeEntryNo.Split('.');
			foreach(string part in parts){
				int mEntryNo = Convert.ToInt32(part);				
				if(mEntry == null){					
					if(mEntryNo > 0 && mEntryNo <= parser.MimeEntities.Length){
						mEntry = parser.MimeEntities[mEntryNo - 1];						
					}
					else{
						return null;
					}
				}
				else{				
					if(mEntryNo > 0 && mEntryNo <= mEntry.ChildEntities.Count){
						mEntry = mEntry.ChildEntities[mEntryNo - 1];
					}
					else{
						return null;
					}
				}
			}

			if(mEntry != null){
				return mEntry.DataEncoded;
			}
			else{
				return null;
			}
		}

		#endregion


		#region construct ConstructEnvelope

		/// <summary>
		/// Construct FETCH ENVELOPE response.
		/// </summary>
		/// <param name="parser"></param>
		/// <returns></returns>
		public static string ConstructEnvelope(LumiSoft.Net.Mime.Mime parser)
		{
			/* Rfc 3501 7.4.2
				ENVELOPE
				A parenthesized list that describes the envelope structure of a
				message.  This is computed by the server by parsing the
				[RFC-2822] header into the component parts, defaulting various
				fields as necessary.

				The fields of the envelope structure are in the following
				order: date, subject, from, sender, reply-to, to, cc, bcc,
				in-reply-to, and message-id.  The date, subject, in-reply-to,
				and message-id fields are strings.  The from, sender, reply-to,
				to, cc, and bcc fields are parenthesized lists of address
				structures.

				An address structure is a parenthesized list that describes an
				electronic mail address.  The fields of an address structure
				are in the following order: personal name, [SMTP]
				at-domain-list (source route), mailbox name, and host name.

				[RFC-2822] group syntax is indicated by a special form of
				address structure in which the host name field is NIL.  If the
				mailbox name field is also NIL, this is an end of group marker
				(semi-colon in RFC 822 syntax).  If the mailbox name field is
				non-NIL, this is a start of group marker, and the mailbox name
				field holds the group name phrase.

				If the Date, Subject, In-Reply-To, and Message-ID header lines
				are absent in the [RFC-2822] header, the corresponding member
				of the envelope is NIL; if these header lines are present but
				empty the corresponding member of the envelope is the empty
				string.
			*/
			// ((sender))
			// ENVELOPE ("date" "subject" from sender reply-to to cc bcc in-reply-to "messageID")
			
			string envelope = "ENVELOPE (";
			
			// date
			envelope += "\"" + parser.MainEntity.Date.ToString("r",System.Globalization.DateTimeFormatInfo.InvariantInfo) + "\" ";
			
			// subject
			if(parser.MainEntity.Subject != null){
				envelope += "\"" + Escape(parser.MainEntity.Subject) + "\" ";
			}
			else{
				envelope += "NIL ";
			}

			// from
			if(parser.MainEntity.From != null){
				envelope += "(";
				foreach(MailboxAddress adr in parser.MainEntity.From.Mailboxes){
					envelope += "(\"" + Escape(adr.DisplayName) + "\" NIL \"" + Escape(adr.LocalPart) + "\" \"" + Escape(adr.Domain) + "\") ";
				}
				envelope = envelope.TrimEnd();
				envelope += ") ";
			}
			else{
				envelope += "NIL ";
			}	

			// sender
			if(parser.MainEntity.Sender != null){
				MailboxAddress adr = parser.MainEntity.Sender;
				envelope += "((\"" + Escape(adr.DisplayName) + "\" NIL \"" + Escape(adr.LocalPart) + "\" \"" + Escape(adr.Domain) + "\")) ";
			}
			else{
				envelope += "NIL ";
			}			

			// reply-to
			if(parser.MainEntity.ReplyTo != null){
				envelope += "(";
				foreach(MailboxAddress adr in parser.MainEntity.ReplyTo.Mailboxes){
					envelope += "(\"" + Escape(adr.DisplayName) + "\" NIL \"" + Escape(adr.LocalPart) + "\" \"" + Escape(adr.Domain) + "\") ";
				}
				envelope = envelope.TrimEnd();
				envelope += ") ";
			}
			else{
				envelope += "NIL ";
			}

			// to
			if(parser.MainEntity.To != null){
				envelope += "(";
				foreach(MailboxAddress adr in parser.MainEntity.To.Mailboxes){
					envelope += "(\"" + Escape(adr.DisplayName) + "\" NIL \"" + Escape(adr.LocalPart) + "\" \"" + Escape(adr.Domain) + "\") ";
				}
				envelope = envelope.TrimEnd();
				envelope += ") ";
			}
			else{
				envelope += "NIL ";
			}

			// cc
			if(parser.MainEntity.Cc != null){
				envelope += "(";
				foreach(MailboxAddress adr in parser.MainEntity.Cc.Mailboxes){
					envelope += "(\"" + Escape(adr.DisplayName) + "\" NIL \"" + Escape(adr.LocalPart) + "\" \"" + Escape(adr.Domain) + "\") ";
				}
				envelope = envelope.TrimEnd();
				envelope += ") ";
			}
			else{
				envelope += "NIL ";
			}

			// bcc
			if(parser.MainEntity.Bcc != null){
				envelope += "(";
				foreach(MailboxAddress adr in parser.MainEntity.Bcc.Mailboxes){
					envelope += "(\"" + Escape(adr.DisplayName) + "\" NIL \"" + Escape(adr.LocalPart) + "\" \"" + Escape(adr.Domain) + "\") ";
				}
				envelope = envelope.TrimEnd();
				envelope += ") ";
			}
			else{
				envelope += "NIL ";
			}

			// in-reply-to
			if(parser.MainEntity.Header.Contains("in-reply-to:")){
				envelope += "\"" + Escape(parser.MainEntity.Header.GetFirst("in-reply-to:").Value) + "\" ";
			}
			else{
				envelope += "NIL ";
			}

			// message-id
			if(parser.MainEntity.MessageID != null){
				envelope += "\"" + Escape(parser.MainEntity.MessageID) + "\"";
			}
			else{
				envelope += "NIL";
			}

			envelope += ")";

			return envelope;
		}

		#endregion

		#region construct BODYSTRUCTURE

		/// <summary>
		/// Constructs FETCH BODY and BODYSTRUCTURE response.
		/// </summary>
		/// <param name="parser"></param>
		/// <param name="bodystructure"></param>
		/// <returns></returns>
		public static string ConstructBodyStructure(LumiSoft.Net.Mime.Mime parser,bool bodystructure)
		{
			/* Rfc 3501 7.4.2 BODYSTRUCTURE

				For example, a simple text message of 48 lines and 2279 octets
				can have a body structure of: ("TEXT" "PLAIN" ("CHARSET"
				"US-ASCII") NIL NIL "7BIT" 2279 48)
				
				For example, a two part message consisting of a text and a
				BASE64-encoded text attachment can have a body structure of:
				(("TEXT" "PLAIN" ("CHARSET" "US-ASCII") NIL NIL "7BIT" 1152
				23)("TEXT" "PLAIN" ("CHARSET" "US-ASCII" "NAME" "cc.diff")
				"<960723163407.20117h@cac.washington.edu>" "Compiler diff"
				"BASE64" 4554 73) "MIXED")


				// Basic fields for multipart
				(nestedMimeEntries) conentSubType
			
				// Extention data for multipart
				(conentTypeSubFields) contentDisposition contentLanguage [contentLocation]
				
				contentDisposition  - ("disposition" {(subFileds) or NIL}) or NIL
							
				contentType         - 'TEXT'
				conentSubType       - 'PLAIN'
				conentTypeSubFields - '("CHARSET" "iso-8859-1" ...)'
				contentID           - Content-ID field
				contentDescription  - Content-Description field
				contentEncoding     - 'quoted-printable'
				contentSize         - mimeEntry NOT ENCODED data size
				[envelope]          - NOTE: included only if contentType = "message" !!!
				[contentLines]      - number of content lines. NOTE: included only if contentType = "text" !!!
				
				// Basic fields for non-multipart
				contentType conentSubType (conentTypeSubFields) contentID contentDescription contentEncoding contentSize contentLines

				// Extention data for non-multipart
				contentDataMd5 contentDisposition contentLanguage [conentLocation]
			
			
				body language
					A string or parenthesized list giving the body language
					value as defined in [LANGUAGE-TAGS].

				body location
					A string list giving the body content URI as defined in	[LOCATION].
				
			*/
						
			string str = "";

			if(bodystructure){
				str += "BODYSTRUCTURE ";
			}
			else{
				str += "BODY ";
			}


			// Multipart message
			if((parser.MainEntity.ContentType & MediaType_enum.Multipart) != 0){
				str += ConstructMultiPart(parser.MainEntity,bodystructure);
			}
			// Single part message
			else{
				str += ConstructSinglePart(parser.MainEntity,bodystructure);
			}
			//***
            
		/*	if((parser.MainEntity.ContentType & MediaType_enum.Multipart) != 0){
				str += "(";
			}

			str += ConstructPart(parser.MainEntity.ChildEntities,bodystructure);
			
			if((parser.MainEntity.ContentType & MediaType_enum.Multipart) != 0){
				//******
				// conentSubType
				if(parser.MainEntity.ContentTypeString.Split('/').Length == 2){
					str += " \"" + parser.MainEntity.ContentTypeString.Split('/')[1].Replace(";","") + "\"";					
				}
				else{
					str += " NIL";
				}

				// Need to add extended fields
				if(bodystructure){
					str += " ";

					// conentTypeSubFields
					string longContentType = parser.MainEntity.ContentTypeString;
					if(longContentType.IndexOf(";") > -1){
						str += "(";
						string[] fields = longContentType.Split(';');
						for(int i=1;i<fields.Length;i++){
							string[] nameValue = fields[i].Replace("\"","").Trim().Split(new char[]{'='},2);

							str += "\"" + nameValue[0] + "\" \"" + nameValue[1] + "\"";

							if(i < fields.Length - 1){
								str += " ";
							}
						}
						str += ") ";
					}

					// contentDisposition
					str += "NIL ";

					// contentLanguage
					str += "NIL";
				}
				
				str += ")";
			}	*/

			return str;
		}

		private static string ConstructParts(MimeEntityCollection entries,bool bodystructure)
		{
			string str = "";

			foreach(MimeEntity ent in entries){
				// multipart
				if(ent.ChildEntities.Count > 0){
					str += ConstructMultiPart(ent,bodystructure);
				}
				// non-multipart
				else{
					str +=  ConstructSinglePart(ent,bodystructure);
				}
			}

			return str;
		}

		#region method ConstructMultiPart

		private static string ConstructMultiPart(MimeEntity ent,bool bodystructure)
		{
			string str = "(";

			str += ConstructParts(ent.ChildEntities,bodystructure);

			str += " ";
			
			// conentSubType			
			string contentType = ent.ContentTypeString.Split(';')[0];
			if(contentType.Split('/').Length == 2){
				str += "\"" + contentType.Split('/')[1].Replace(";","") + "\""; 
			}
			else{
				str += "NIL";
			}

			// Need to add extended fields
			if(bodystructure){
				str += " ";

				// conentTypeSubFields
				string longContentType = ent.ContentTypeString;
				if(longContentType.IndexOf(";") > -1){
					str += "(";
					string[] fields = longContentType.Split(';');
					for(int i=1;i<fields.Length;i++){
						string[] nameValue = fields[i].Replace("\"","").Trim().Split(new char[]{'='},2);

						str += "\"" + nameValue[0] + "\" \"" + nameValue[1] + "\"";

						if(i < fields.Length - 1){
							str += " ";
						}
					}
					str += ") ";
				}

				// contentDisposition
				str += "NIL ";

				// contentLanguage
				str += "NIL";
			}

			str += ")";

			return str;			
		}

		#endregion

		#region method ConstructSinglePart

		private static string ConstructSinglePart(MimeEntity ent,bool bodystructure)
		{			
			string str =  "(";

			// contentType
			str += "\"" + ent.ContentTypeString.Split('/')[0] + "\" ";

			// conentSubType
			string contentType = ent.ContentTypeString.Split(';')[0];
			if(contentType.Split('/').Length == 2){
				str += "\"" + contentType.Split('/')[1].Replace(";","") + "\" "; 
			}
			else{
				str += "NIL ";
			}

			// conentTypeSubFields
			string longContentType = ent.ContentTypeString;
			if(longContentType.IndexOf(";") > -1){
				str += "(";
				string[] fields = longContentType.Split(';');
				for(int i=1;i<fields.Length;i++){
					string[] nameValue = fields[i].Replace("\"","").Trim().Split(new char[]{'='},2);

					str += "\"" + nameValue[0] + "\" \"" + nameValue[1] + "\"";

					if(i < fields.Length - 1){
						str += " ";
					}
				}
				str += ") ";
			}
			else{
				// if content is attachment and content type name field is missing, use filename for it
				string fileName = ent.ContentDisposition_FileName;
				if(fileName != null){
					str += "(\"name\" \"" + fileName + "\") ";
				}
				else{
					str += "NIL ";
				}
			}

			// contentID
			string contentID = ent.ContentID;
			if(contentID != null){
				str += "\"" + contentID + "\" ";
			}
			else{
				str += "NIL ";
			}

			// contentDescription
			string contentDescription = ent.ContentDescription;
			if(contentDescription != null){
				str += "\"" + contentDescription + "\" ";
			}
			else{
				str += "NIL ";
			}

			// contentEncoding
			if(ent.Header.GetFirst("Content-Transfer-Encoding:") != null){
				str += "\"" + ent.Header.GetFirst("Content-Transfer-Encoding:").Value + "\" ";
			}
			else{
				str += "NIL ";
			}			

			// contentSize
			str += ent.DataEncoded.Length + " ";

			// envelope NOTE: included only if contentType = "message" !!!

			// contentLines NOTE: included only if contentType = "text" !!!
			if((ent.ContentType & MediaType_enum.Text) != 0){
				StreamLineReader r = new StreamLineReader(new MemoryStream(ent.DataEncoded));
				int nLines = 0;
				byte[] line = new byte[0];
				while(line != null){
					line = r.ReadLine();
					nLines++;
				}
				str += nLines;
			}
		
			// Need to add extended fields
			if(bodystructure){
				str += " ";

				// md5
				str += "NIL ";

				// contentDisposition
				if(ent.Header.GetFirst("Content-Disposition:") != null){
					string contDispos = ent.Header.GetFirst("Content-Disposition:").Value;

					str += "(";

					string[] fields = contDispos.Split(';');

					str += "\"" + fields[0] + "\" ";

					if(fields.Length > 1){
						str += "(";
						for(int i=1;i<fields.Length;i++){
							string[] nameValue = fields[i].Replace("\"","").Trim().Split(new char[]{'='},2);

							str += "\"" + nameValue[0] + "\" \"" + nameValue[1] + "\"";

							if(i < fields.Length - 1){
								str += " ";
							}
						}
						str += ")";
					}
					else{
						str += "NIL";
					}

					str += ") ";
				}
				else{
					str += "NIL ";
				}

				// contentLanguage
				str += "NIL";
			}

			str += ")";

			return str;
		}

		#endregion

		#endregion


		#region static method Escape

		private static string Escape(string text)
		{
			text = text.Replace("\\","\\\\");
			text = text.Replace("\"","\\\"");

			return text;
		}

		#endregion

	}
}
