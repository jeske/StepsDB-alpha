using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Collections;
using System.Text;
using System.Text.RegularExpressions;

namespace LumiSoft.Net
{
	#region enum AuthType

	/// <summary>
	/// Authentication type.
	/// </summary>
	public enum AuthType
	{
		/// <summary>
		/// Plain username/password authentication.
		/// </summary>
		Plain = 0,

		/// <summary>
		/// APOP
		/// </summary>
		APOP  = 1,

		/// <summary>
		/// Not implemented.
		/// </summary>
		LOGIN = 2,	
	
		/// <summary>
		/// Cram-md5 authentication.
		/// </summary>
		CRAM_MD5 = 3,	

		/// <summary>
		/// DIGEST-md5 authentication.
		/// </summary>
		DIGEST_MD5 = 4,	
	}

	#endregion

	/// <summary>
	/// Provides net core utility methods.
	/// </summary>
	public class Core
	{

		#region method DoPeriodHandling

		/// <summary>
		/// Does period handling.
		/// </summary>
		/// <param name="data"></param>
		/// <param name="add_Remove">If true add periods, else removes periods.</param>
		/// <returns></returns>
		public static MemoryStream DoPeriodHandling(byte[] data,bool add_Remove)
		{
			using(MemoryStream strm = new MemoryStream(data)){
				return DoPeriodHandling(strm,add_Remove);
			}
		}

		/// <summary>
		/// Does period handling.
		/// </summary>
		/// <param name="strm">Input stream.</param>
		/// <param name="add_Remove">If true add periods, else removes periods.</param>
		/// <returns></returns>
		public static MemoryStream DoPeriodHandling(Stream strm,bool add_Remove)
		{
			return DoPeriodHandling(strm,add_Remove,true);
		}

		/// <summary>
		/// Does period handling.
		/// </summary>
		/// <param name="strm">Input stream.</param>
		/// <param name="add_Remove">If true add periods, else removes periods.</param>
		/// <param name="setStrmPosTo0">If true sets stream position to 0.</param>
		/// <returns></returns>
		public static MemoryStream DoPeriodHandling(Stream strm,bool add_Remove,bool setStrmPosTo0)
		{			
			MemoryStream replyData = new MemoryStream();

			byte[] crlf = new byte[]{(byte)'\r',(byte)'\n'};

			if(setStrmPosTo0){
				strm.Position = 0;
			}

			StreamLineReader r = new StreamLineReader(strm);
			byte[] line = r.ReadLine();

			// Loop through all lines
			while(line != null){
				if(line.Length > 0){
					if(line[0] == (byte)'.'){
						/* Add period Rfc 2821 4.5.2
						   -  Before sending a line of mail text, the SMTP client checks the
						   first character of the line.  If it is a period, one additional
						   period is inserted at the beginning of the line.
						*/
						if(add_Remove){
							replyData.WriteByte((byte)'.');
							replyData.Write(line,0,line.Length);
						}
						/* Remove period Rfc 2821 4.5.2
						 If the first character is a period , the first characteris deleted.							
						*/
						else{
							replyData.Write(line,1,line.Length-1);
						}
					}
					else{
						replyData.Write(line,0,line.Length);
					}
				}					

				replyData.Write(crlf,0,crlf.Length);

				// Read next line
				line = r.ReadLine();
			}

			replyData.Position = 0;

			return replyData;
		}

		#endregion

		#region method ScanInvalid_CR_or_LF

		/// <summary>
		/// Scans invalid CR or LF combination in stream. Returns true if contains invalid CR or LF combination.
		/// </summary>
		/// <param name="strm">Stream which to check.</param>
		/// <returns>Returns true if contains invalid CR or LF combination.</returns>
		public static bool ScanInvalid_CR_or_LF(Stream strm)
		{
			StreamLineReader lineReader = new StreamLineReader(strm);
			byte[] line = lineReader.ReadLine();
			while(line != null){
				foreach(byte b in line){
					// Contains CR or LF. It cannot conatian such sumbols, because CR must be paired with LF
					// and we currently reading lines with CRLF combination.
					if(b == 10 || b == 13){
						return true;
					}
				}

				line = lineReader.ReadLine();
			}

			return false;
		}

		#endregion

		
		#region method GetHostName

		/// <summary>
		/// Gets host name. If fails returns 'UnkownHost'.
		/// </summary>
		/// <param name="IP"></param>
		/// <returns></returns>
		public static string GetHostName(IPAddress IP)
		{
			// ToDo: use LS dns client instead, ms is slow

			try{
				return System.Net.Dns.GetHostByAddress(IP).HostName;
			}
			catch{
				return "UnknownHost";
			}
		}

		#endregion


		#region method GetArgsText

		/// <summary>
		/// Gets argument part of command text.
		/// </summary>
		/// <param name="input">Input srting from where to remove value.</param>
		/// <param name="cmdTxtToRemove">Command text which to remove.</param>
		/// <returns></returns>
		public static string GetArgsText(string input,string cmdTxtToRemove)
		{
			string buff = input.Trim();
			if(buff.Length >= cmdTxtToRemove.Length){
				buff = buff.Substring(cmdTxtToRemove.Length);
			}
			buff = buff.Trim();

			return buff;
		}

		#endregion

		
		#region method IsNumber

		/// <summary>
		/// Checks if specified string is number(long).
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static bool IsNumber(string str)
		{
			try{
				Convert.ToInt64(str);
				return true;
			}
			catch{
				return false;
			}
		}

		#endregion


		#region method Base64Encode

		/// <summary>
		/// Encodes data with base64 encoding.
		/// </summary>
		/// <param name="data">Data to encode.</param>
		/// <returns></returns>
		public static byte[] Base64Encode(byte[] data)
		{			
			MemoryStream base64Data = new MemoryStream(System.Text.Encoding.Default.GetBytes(Convert.ToBase64String(data)));

			// System encode won't split base64 data to lines, we need to do it manually
			MemoryStream retVal = new MemoryStream();
			while(true){
				byte[] dataLine = new byte[76];
				int readedCount = base64Data.Read(dataLine,0,dataLine.Length);
				// End of stream reached, end reading
				if(readedCount == 0){
					break;
				}

				retVal.Write(dataLine,0,readedCount);
				retVal.Write(new byte[]{(byte)'\r',(byte)'\n'},0,2);
			}
			
			return retVal.ToArray();
		}

		#endregion

		#region method Base64Decode

		/// <summary>
		/// Decodes base64 data.
		/// </summary>
		/// <param name="base64Data">Base64 decoded data.</param>
		/// <returns></returns>
		public static byte[] Base64Decode(byte[] base64Data)
		{
			string dataStr = System.Text.Encoding.Default.GetString(base64Data);
			if(dataStr.Trim().Length > 0){
				return Convert.FromBase64String(dataStr);
			}
			else{
				return new byte[]{};
			}
		}

		#endregion

		#region method QuotedPrintableEncode

		/// <summary>
		/// Encodes data with quoted-printable encoding.
		/// </summary>
		/// <param name="data">Data to encode.</param>
		/// <returns></returns>
		public static byte[] QuotedPrintableEncode(byte[] data)
		{
			/* Rfc 2045 6.7. Quoted-Printable Content-Transfer-Encoding
			 
			(2) (Literal representation) Octets with decimal values of 33 through 60 inclusive, 
				and 62 through 126, inclusive, MAY be represented as the US-ASCII characters which
				correspond to those octets (EXCLAMATION POINT through LESS THAN, and GREATER THAN 
				through TILDE, respectively).
			
			(3) (White Space) Octets with values of 9 and 32 MAY be represented as US-ASCII TAB (HT) and 
			    SPACE characters, respectively, but MUST NOT be so represented at the end of an encoded line. 
				You must encode it =XX.
			
			(5) Encoded lines must not be longer than 76 characters, not counting the trailing CRLF. 
				If longer lines are to be encoded with the Quoted-Printable encoding, "soft" line breaks
				must be used.  An equal sign as the last character on a encoded line indicates such 
				a non-significant ("soft") line break in the encoded text.
				
			*)  If binary data is encoded in quoted-printable, care must be taken to encode 
			    CR and LF characters as "=0D" and "=0A", respectively.	 

			*/

			int lineLength = 0;
			// Encode bytes <= 33 , >= 126 and 61 (=)
			MemoryStream retVal = new MemoryStream();
			foreach(byte b in data){
				// Suggested line length is exceeded, add soft line break
				if(lineLength > 75){
					retVal.Write(new byte[]{(byte)'=',(byte)'\r',(byte)'\n'},0,3);
					lineLength = 0;
				}

				// We need to encode that byte
				if(b <= 33 || b >= 126 || b == 61){					
					retVal.Write(new byte[]{(byte)'='},0,1);
					retVal.Write(Core.ToHex(b),0,2);
					lineLength += 3;
				}
				// We don't need to encode that byte, just write it to stream
				else{
					retVal.WriteByte(b);
					lineLength++;
				}
			}

			return retVal.ToArray();
		}

		#endregion

		#region method QuotedPrintableDecode

		/// <summary>
		/// quoted-printable decoder.
		/// </summary>
		/// <param name="encoding">Input string encoding.</param>
		/// <param name="data">Data which to encode.</param>
		/// <param name="includeCRLF">Specified if line breaks are included or skipped.</param>
		/// <returns>Returns decoded data with specified encoding.</returns>
		public static string QuotedPrintableDecode(System.Text.Encoding encoding,byte[] data,bool includeCRLF)
		{
			return encoding.GetString(QuotedPrintableDecodeB(data,includeCRLF));
		}

		/// <summary>
		/// quoted-printable decoder.
		/// </summary>
		/// <param name="data">Data which to encode.</param>
		/// <param name="includeCRLF">Specified if line breaks are included or skipped. For text data CRLF is usually included and for binary data excluded.</param>
		/// <returns>Returns decoded data.</returns>
		public static byte[] QuotedPrintableDecodeB(byte[] data,bool includeCRLF)
		{
			MemoryStream strm = new MemoryStream(data);			
			MemoryStream dStrm = new MemoryStream();

			int b = strm.ReadByte();
			while(b > -1){
				// Hex eg. =E4
				if(b == '='){
					byte[] buf = new byte[2];
					strm.Read(buf,0,2);

					// <CRLF> followed by =, it's splitted line
					if(!(buf[0] == '\r' && buf[1] == '\n')){
						try{
							byte[] convertedByte = FromHex(buf);
							dStrm.Write(convertedByte,0,convertedByte.Length);
						}
						catch{ // If worng hex value, just skip this chars							
						}
					}
				}
				else{
					// For text line breaks are included, for binary data they are excluded

					if(includeCRLF){
						dStrm.WriteByte((byte)b);
					}
					else{
						// Skip \r\n they must be escaped
						if(b != '\r' && b != '\n'){
							dStrm.WriteByte((byte)b);
						}
					}					
				}

				b = strm.ReadByte();
			}

			return dStrm.ToArray();
		}

		#endregion

		#region method QDecode

		/// <summary>
		/// "Q" decoder. This is same as quoted-printable, except '_' is converted to ' '.
		/// </summary>
		/// <param name="encoding">Input string encoding.</param>
		/// <param name="data">String which to encode.</param>
		/// <returns>Returns decoded string.</returns>		
		public static string QDecode(System.Text.Encoding encoding,string data)
		{
			return QuotedPrintableDecode(encoding,System.Text.Encoding.ASCII.GetBytes(data.Replace("_"," ")),true);

		//  REMOVEME:
		//  15.09.2004 - replace must be done before encoding
		//	return QuotedPrintableDecode(encoding,System.Text.Encoding.ASCII.GetBytes(data)).Replace("_"," ");
		}

		#endregion

		#region method CanonicalDecode

		/// <summary>
		/// Canonical decoding. Decodes all canonical encoding occurences in specified text.
		/// Usually mime message header unicode/8bit values are encoded as Canonical.
		/// Format: =?charSet?type[Q or B]?encoded_string?= .
		/// Defined in RFC 2047.
		/// </summary>
		/// <param name="text">Text to decode.</param>
		/// <returns></returns>
		public static string CanonicalDecode(string text)
		{
			/* RFC 2047			 
				Generally, an "encoded-word" is a sequence of printable ASCII
				characters that begins with "=?", ends with "?=", and has two "?"s in
				between.
				
				Syntax: =?charSet?type[Q or B]?encoded_string?=
				
				Examples:
					=?utf-8?q?Buy a Rolex?=
					=?iso-8859-1?B?bORs5D8=?=
			*/

			StringBuilder retVal = new StringBuilder();
			int offset = 0;
			while(offset < text.Length){
				// Search start and end of canonical entry
				int iStart = text.IndexOf("=?",offset);
				int iEnd = -1;
				if(iStart > -1){
					// End index must be over start index position
					iEnd = text.IndexOf("?=",iStart + 2);
				}
				
				if(iStart > -1 && iEnd > -1){
					// Add left side non encoded text of encoded text, if there is any
					if((iStart - offset) > 0){
						retVal.Append(text.Substring(offset,iStart - offset));
					}

					while(true){
						// Check if it is encoded entry
						string[] charset_type_text = text.Substring(iStart + 2,iEnd - iStart - 2).Split('?');
						if(charset_type_text.Length == 3){
							// Try to parse encoded text
							try{
								Encoding enc = Encoding.GetEncoding(charset_type_text[0]);
								// QEecoded text
								if(charset_type_text[1].ToLower() == "q"){
									retVal.Append(Core.QDecode(enc,charset_type_text[2]));
								}
								// Base64 encoded text
								else{
									retVal.Append(enc.GetString(Core.Base64Decode(Encoding.Default.GetBytes(charset_type_text[2]))));
								}
							}
							catch{
								// Parsing failed, just leave text as is.
								retVal.Append(text.Substring(iStart,iEnd - iStart + 2));
							}

							// Move current offset in string
							offset = iEnd + 2;
							break;
						}
						// This isn't right end tag, try next
						else if(charset_type_text.Length < 3){
							// Try next end tag
							iEnd = text.IndexOf("?=",iEnd + 2);
						
							// No suitable end tag for active start tag, move offset over start tag.
							if(iEnd == -1){								
								retVal.Append("=?");
								offset = iStart + 2;
								break;
							}
						}
						// Illegal start tag or start tag is just in side some text, move offset over start tag.
						else{						
							retVal.Append("=?");
							offset = iStart + 2;
							break;
						}
					}
				}
				// There are no more entries
				else{
					// Add remaining non encoded text, if there is any.
					if(text.Length > offset){
						retVal.Append(text.Substring(offset));
						offset = text.Length;
					}
				}				
			}

			return retVal.ToString();
		}

/*		/// <summary>
		/// Canonical decoding. Decodes all canonical encoding occurences in specified text.
		/// Usually mime message header unicode/8bit values are encoded as Canonical.
		/// Format: =?charSet?type[Q or B]?encoded string?= .
		/// Defined in RFC 2047.
		/// </summary>
		/// <param name="text">Text to decode.</param>
		/// <returns>Returns decoded text.</returns>
		public static string CanonicalDecode(string text)
		{
			// =?charSet?type[Q or B]?encoded string?=
			// 
			// Examples:
			//   =?utf-8?q?Buy a Rolex?=
			//   =?ISO-8859-1?Q?Asb=F8rn_Miken?=

			Regex regex = new Regex(@"\=\?(?<charSet>[\w\-]*)\?(?<type>[qQbB])\?(?<text>[\w\s_\-=*+;:,./]*)\?\=");

			MatchCollection m = regex.Matches(text);
			foreach(Match match in m){
				try{
					System.Text.Encoding enc = System.Text.Encoding.GetEncoding(match.Groups["charSet"].Value);
					// QDecode
					if(match.Groups["type"].Value.ToLower() == "q"){
						text = text.Replace(match.Value,Core.QDecode(enc,match.Groups["text"].Value));
					}
					// Base64
					else{
						text = text.Replace(match.Value,enc.GetString(Convert.FromBase64String(match.Groups["text"].Value)));
					}
				}
				catch{
					// If parsing fails, just leave this string as is
				}
			}

			return text;
		}
*/
		#endregion

		#region method CanonicalEncode

		/// <summary>
		/// Canonical encoding.
		/// </summary>
		/// <param name="str">String to encode.</param>
		/// <param name="charSet">With what charset to encode string. If you aren't sure about it, utf-8 is suggested.</param>
		/// <returns>Returns encoded text.</returns>
		public static string CanonicalEncode(string str,string charSet)
		{
			// Contains non ascii chars, need to encode
			if(!IsAscii(str)){
				string retVal = "=?" + charSet + "?" + "B?";
				retVal += Convert.ToBase64String(System.Text.Encoding.GetEncoding(charSet).GetBytes(str));
				retVal += "?=";

				return retVal;
			}

			return str;
		}

		#endregion

		#region method IsAscii

		/// <summary>
		/// Checks if specified string data is acii data.
		/// </summary>
		/// <param name="data"></param>
		/// <returns></returns>
		public static bool IsAscii(string data)
		{			
			foreach(char c in data){
				if((int)c > 127){ 
					return false;
				}
			}

			return true;
		}

		#endregion


		#region method ToHex

		/// <summary>
		/// Convert byte to hex data.
		/// </summary>
		/// <param name="byteValue">Byte to convert.</param>
		/// <returns></returns>
		public static byte[] ToHex(byte byteValue)
		{
			return ToHex(new byte[]{byteValue});
		}

		/// <summary>
		/// Converts data to hex data.
		/// </summary>
		/// <param name="data">Data to convert.</param>
		/// <returns></returns>
		public static byte[] ToHex(byte[] data)
		{
			char[] hexChars = new char[]{'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

			MemoryStream retVal = new MemoryStream(data.Length * 2);
			foreach(byte b in data){
				byte[] hexByte = new byte[2];
                
				// left 4 bit of byte
				hexByte[0] = (byte)hexChars[(b & 0xF0) >> 4];

				// right 4 bit of byte
				hexByte[1] = (byte)hexChars[b & 0x0F];

				retVal.Write(hexByte,0,2);
			}

			return retVal.ToArray();
		}

		#endregion

		#region method FromHex

		/// <summary>
		/// Converts hex byte data to normal byte data. Hex data must be in two bytes pairs, for example: 0F,FF,A3,... .
		/// </summary>
		/// <param name="hexData">Hex data.</param>
		/// <returns></returns>
		public static byte[] FromHex(byte[] hexData)
		{
			if(hexData.Length < 2 || (hexData.Length / (double)2 != Math.Floor(hexData.Length / (double)2))){
				throw new Exception("Illegal hex data, hex data must be in two bytes pairs, for example: 0F,FF,A3,... .");
			}

			MemoryStream retVal = new MemoryStream(hexData.Length / 2);
			// Loop hex value pairs
			for(int i=0;i<hexData.Length;i+=2){
				byte[] hexPairInDecimal = new byte[2];
				// We need to convert hex char to decimal number, for example F = 15
				for(int h=0;h<2;h++){
					if(((char)hexData[i + h]) == '0'){
						hexPairInDecimal[h] = 0;
					}
					else if(((char)hexData[i + h]) == '1'){
						hexPairInDecimal[h] = 1;
					}
					else if(((char)hexData[i + h]) == '2'){
						hexPairInDecimal[h] = 2;
					}
					else if(((char)hexData[i + h]) == '3'){
						hexPairInDecimal[h] = 3;
					}
					else if(((char)hexData[i + h]) == '4'){
						hexPairInDecimal[h] = 4;
					}
					else if(((char)hexData[i + h]) == '5'){
						hexPairInDecimal[h] = 5;
					}
					else if(((char)hexData[i + h]) == '6'){
						hexPairInDecimal[h] = 6;
					}
					else if(((char)hexData[i + h]) == '7'){
						hexPairInDecimal[h] = 7;
					}
					else if(((char)hexData[i + h]) == '8'){
						hexPairInDecimal[h] = 8;
					}
					else if(((char)hexData[i + h]) == '9'){
						hexPairInDecimal[h] = 9;
					}
					else if(((char)hexData[i + h]) == 'A' || ((char)hexData[i + h]) == 'a'){
						hexPairInDecimal[h] = 10;
					}
					else if(((char)hexData[i + h]) == 'B' || ((char)hexData[i + h]) == 'b'){
						hexPairInDecimal[h] = 11;
					}
					else if(((char)hexData[i + h]) == 'C' || ((char)hexData[i + h]) == 'c'){
						hexPairInDecimal[h] = 12;
					}
					else if(((char)hexData[i + h]) == 'D' || ((char)hexData[i + h]) == 'd'){
						hexPairInDecimal[h] = 13;
					}
					else if(((char)hexData[i + h]) == 'E' || ((char)hexData[i + h]) == 'e'){
						hexPairInDecimal[h] = 14;
					}
					else if(((char)hexData[i + h]) == 'F' || ((char)hexData[i + h]) == 'f'){
						hexPairInDecimal[h] = 15;
					}
				}

				// Join hex 4 bit(left hex cahr) + 4bit(right hex char) in bytes 8 it
				retVal.WriteByte((byte)((hexPairInDecimal[0] << 4) | hexPairInDecimal[1]));
			}

			return retVal.ToArray();
		}

		#endregion

	}
}
