using System;
using System.IO;

namespace LumiSoft.Net.Mime
{
	/// <summary>
	/// Provides mime related utility methods.
	/// </summary>
	public class MimeUtils
	{
		#region static function ParseDate

		/// <summary>
		/// Parses rfc 2822 datetime.
		/// </summary>
		/// <param name="date">Date string.</param>
		/// <returns></returns>
		public static DateTime ParseDate(string date)
		{
			/* Rfc 2822 3.3. Date and Time Specification.			 
				date-time       = [ day-of-week "," ] date FWS time [CFWS]
				date            = day month year
				time            = hour ":" minute [ ":" second ] FWS zone
			*/

			/* IMAP date format. 
			    date-time       = date FWS time [CFWS]
				date            = day-month-year
				time            = hour ":" minute [ ":" second ] FWS zone
			*/

			//--- Replace timezone constants -------//
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


			date = date.ToLower();
			date = date.Replace("gmt","-0000");
			date = date.Replace("edt","-0400");
			date = date.Replace("est","-0500");
			date = date.Replace("cdt","-0500");
			date = date.Replace("cst","-0600");
			date = date.Replace("mdt","-0600");
			date = date.Replace("mst","-0700");
			date = date.Replace("pdt","-0700");
			date = date.Replace("pst","-0800");
			date = date.Replace("bst","+0100");
			//----------------------------------------//

			//--- Replace month constants ---//
			date = date.Replace("jan","01");
			date = date.Replace("feb","02");
			date = date.Replace("mar","03");
			date = date.Replace("apr","04");
			date = date.Replace("may","05");
			date = date.Replace("jun","06");
			date = date.Replace("jul","07");
			date = date.Replace("aug","08");
			date = date.Replace("sep","09");
			date = date.Replace("oct","10");
			date = date.Replace("nov","11");
			date = date.Replace("dec","12");
			//-------------------------------//

			//  If date contains optional "day-of-week,", remove it
			if(date.IndexOf(',') > -1){
				date = date.Substring(date.IndexOf(',') + 1);
			}

			// Remove () from date. "Mon, 13 Oct 2003 20:50:57 +0300 (EEST)"
			if(date.IndexOf(" (") > -1){
				date = date.Substring(0,date.IndexOf(" ("));
			}

			date = date.Trim();
			//Remove multiple continues spaces
			while(date.IndexOf("  ") > -1){
				date = date.Replace("  "," ");
			}

			// Split date into parts
			string timeString = "";
            string[] d_m_y_h_m_s_z = new string[7];			
			string[] dateparts = date.Split(' ');
			// Rfc 2822 date (day month year time timeZone)
			if(dateparts.Length == 5){
				d_m_y_h_m_s_z[0] = dateparts[0];
				d_m_y_h_m_s_z[1] = dateparts[1];
				d_m_y_h_m_s_z[2] = dateparts[2];

				timeString = dateparts[3];

				d_m_y_h_m_s_z[6] = dateparts[4];
			}
			// IMAP date (day-month-year time timeZone)
			else if(dateparts.Length == 3){
				string[] d_m_y = dateparts[0].Split('-');
				if(d_m_y.Length == 3){
					d_m_y_h_m_s_z[0] = d_m_y[0];
					d_m_y_h_m_s_z[1] = d_m_y[1];
					d_m_y_h_m_s_z[2] = d_m_y[2];

					timeString = dateparts[1];

					d_m_y_h_m_s_z[6] = dateparts[2];
				}
                else{
					throw new Exception("Invalid date time value !");
				}
			}
			else{
				throw new Exception("Invalid date time value !");
			}

			// Parse time part (hour ":" minute [ ":" second ])
			string[] timeParts = timeString.Split(':');
			if(timeParts.Length == 3){
				d_m_y_h_m_s_z[3] = timeParts[0];
				d_m_y_h_m_s_z[4] = timeParts[1];
				d_m_y_h_m_s_z[5] = timeParts[2];
			}
			else if(timeParts.Length == 2){
				d_m_y_h_m_s_z[3] = timeParts[0];
				d_m_y_h_m_s_z[4] = timeParts[1];
				d_m_y_h_m_s_z[5] = "00";
			}
			else{
				throw new Exception("Invalid date time value !");
			}

			//--- Construct date string--------------------------//
			string normalizedDate = "";
			// Day
			if(d_m_y_h_m_s_z[0].Length == 1){
				normalizedDate += "0" + d_m_y_h_m_s_z[0] + " ";
			}
			else{
				normalizedDate += d_m_y_h_m_s_z[0] + " ";
			}
			// Month
			if(d_m_y_h_m_s_z[1].Length == 1){
				normalizedDate += "0" + d_m_y_h_m_s_z[1] + " ";
			}
			else{
				normalizedDate += d_m_y_h_m_s_z[1] + " ";
			}
			// Year
			if(d_m_y_h_m_s_z[2].Length == 2){
				normalizedDate += "20" + d_m_y_h_m_s_z[2] + " ";
			}
			else{
				normalizedDate += d_m_y_h_m_s_z[2] + " ";
			}
			// Hour
			if(d_m_y_h_m_s_z[3].Length == 1){
				normalizedDate += "0" + d_m_y_h_m_s_z[3] + ":";
			}
			else{
				normalizedDate += d_m_y_h_m_s_z[3] + ":";
			}
			// Minute
			if(d_m_y_h_m_s_z[4].Length == 1){
				normalizedDate += "0" + d_m_y_h_m_s_z[4] + ":";
			}
			else{
				normalizedDate += d_m_y_h_m_s_z[4] + ":";
			}
			// Second
			if(d_m_y_h_m_s_z[5].Length == 1){
				normalizedDate += "0" + d_m_y_h_m_s_z[5] + " ";
			}
			else{
				normalizedDate += d_m_y_h_m_s_z[5] + " ";
			}
			// TimeZone
			normalizedDate += d_m_y_h_m_s_z[6];
			//------------------------------------------------------//
			
			string dateFormat = "dd MM yyyy HH':'mm':'ss zzz";
			return DateTime.ParseExact(normalizedDate,dateFormat,System.Globalization.DateTimeFormatInfo.InvariantInfo,System.Globalization.DateTimeStyles.None);
		}

		#endregion

		#region static method DateTimeToRfc2822

		/// <summary>
		/// Converts date to rfc 2822 date time string.
		/// </summary>
		/// <param name="dateTime">Date time value.</param>
		/// <returns></returns>
		public static string DateTimeToRfc2822(DateTime dateTime)
		{
			return dateTime.ToUniversalTime().ToString("r",System.Globalization.DateTimeFormatInfo.InvariantInfo);
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
			/* Rfc 2822 3.1.  GENERAL DESCRIPTION
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
				
			   Rfc 2822 2.2.3 (Multiline header fields)
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

		#region static function ParseHeaderFiledParameter

		/// <summary>
		/// Parses header field parameter value. 
		/// For example: CONTENT-TYPE: application\octet-stream; name="yourFileName.xxx",
		/// fieldName="CONTENT-TYPE:" and subFieldName="name".
		/// </summary>
		/// <param name="fieldName">Main header field name.</param>
		/// <param name="parameterName">Header field's parameter name.</param>
		/// <param name="headers">Full headrs string.</param>
		/// <returns></returns>
		public static string ParseHeaderFiledParameter(string fieldName,string parameterName,string headers)
		{
			string mainFiled = ParseHeaderField(fieldName,headers);
			// Parse sub field value
			if(mainFiled.Length > 0){
				int index = mainFiled.ToUpper().IndexOf(parameterName.ToUpper());
				if(index > -1){	
					mainFiled = mainFiled.Substring(index + parameterName.Length + 1); // Remove "subFieldName="

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


		#region static method CreateMessageID

		/// <summary>
		/// Creates Rfc 2822 3.6.4 message-id. Syntax: '&lt;' id-left '@' id-right '&gt;'.
		/// </summary>
		/// <returns></returns>
		public static string CreateMessageID()
		{
			return "<" + Guid.NewGuid().ToString().Replace("-","") + "@" + Guid.NewGuid().ToString().Replace("-","") + ">";
		}

		#endregion
	}
}
