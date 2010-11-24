using System;
using LumiSoft.Net.Mime;

namespace LumiSoft.Net.IMAP.Server
{
	/// <summary>
	/// Summary description for _SearchHelper.
	/// </summary>
	public class _SearchHelper
	{
		#region method MatchSearchKey

		/// <summary>
		/// Checks if message matches for specified search key.
		/// </summary>
		/// <param name="searchKey"></param>
		/// <param name="searchKeyValue"></param>
		/// <param name="messageInfo"></param>
		/// <param name="msg"></param>
		/// <returns></returns>
		public static bool MatchSearchKey(string searchKey,object searchKeyValue,IMAP_Message messageInfo,LumiSoft.Net.Mime.Mime msg)
		{						
			// BEFORE <date>
			//	Messages whose internal date (disregarding time and timezone)
			//	is earlier than the specified date.
			if(searchKey == "BEFORE"){
				if(messageInfo.Date.Date < (DateTime)searchKeyValue){
					return true;
				}
			}
			// BODY <string>
			//	Messages that contain the specified string in the body of the message.
			else if(searchKey == "BODY"){
				if(msg.BodyText.IndexOf((string)searchKeyValue) > -1){
					return true;
				}
			}
			// HEADER <field-name> <string>
			//	Messages that have a header with the specified field-name (as
			//	defined in [RFC-2822]) and that contains the specified string
			//	in the text of the header (what comes after the colon).  If the
			//	string to search is zero-length, this matches all messages that
			//	have a header line with the specified field-name regardless of
			//	the contents.
			else if(searchKey == "HEADER"){				
				string[] headerField_value = (string[])searchKeyValue;

				if(msg.MainEntity.Header.Contains(headerField_value[0])){
					if(headerField_value[1].Length == 0){
						return true;
					}
					else if(msg.MainEntity.Header.GetFirst(headerField_value[0]).Value.IndexOf(headerField_value[1]) > -1){
						return true;
					}
				}
			}
			// KEYWORD <flag>
			//	Messages with the specified keyword flag set.
			else if(searchKey == "KEYWORD"){
				if((messageInfo.Flags & IMAP_Utils.ParseMessageFalgs((string)searchKeyValue)) != 0){
					return true;
				}
			}					
			// LARGER <n>
			//	Messages with an [RFC-2822] size larger than the specified number of octets.
			else if(searchKey == "LARGER"){
				if(messageInfo.Size > Convert.ToInt64(searchKeyValue)){
					return true;
				}
			}
			// ON <date>
			//	Messages whose internal date (disregarding time and timezone)
			//	is within the specified date.
			else if(searchKey == "ON"){
				if(messageInfo.Date.Date == (DateTime)searchKeyValue){
					return true;
				}
			}
			// SENTBEFORE <date>
			//	Messages whose [RFC-2822] Date: header (disregarding time and
			//	timezone) is earlier than the specified date.
			else if(searchKey == "SENTBEFORE"){				
				if(msg.MainEntity.Date.Date < (DateTime)searchKeyValue){
					return true;
				}
			}
			// SENTON <date>
			//	Messages whose [RFC-2822] Date: header (disregarding time and
			//	timezone) is within the specified date.
			else if(searchKey == "SENTON"){				
				if(msg.MainEntity.Date.Date == (DateTime)searchKeyValue){
					return true;
				}
			}
			// SENTSINCE <date>
			//	Messages whose [RFC-2822] Date: header (disregarding time and
			//	timezone) is within or later than the specified date.
			else if(searchKey == "SENTSINCE"){
				if(msg.MainEntity.Date.Date >= (DateTime)searchKeyValue){
					return true;
				}
			}
			// SINCE <date>
			//	Messages whose internal date (disregarding time and timezone)
			//	is within or later than the specified date.	
			else if(searchKey == "SINCE"){
				if(msg.MainEntity.Date.Date >= (DateTime)searchKeyValue){
					return true;
				}
			}
			// SMALLER <n>
			//	Messages with an [RFC-2822] size smaller than the specified	number of octets.
			else if(searchKey == "SMALLER"){
				if(messageInfo.Size < Convert.ToInt64(searchKeyValue)){
					return true;
				}
			}
			// TEXT <string>
			//	Messages that contain the specified string in the header or	body of the message.				
			else if(searchKey == "TEXT"){
				// TODO:
			}
			// UID <sequence set>
			//	Messages with unique identifiers corresponding to the specified
			//	unique identifier set.  Sequence set ranges are permitted.
			else if(searchKey == "UID"){				
				if(((string)searchKeyValue).IndexOf(":") > -1){
					string[] start_end = ((string)searchKeyValue).Split(':');
					if(messageInfo.MessageUID >= Convert.ToInt64(start_end[0]) && messageInfo.MessageUID <= Convert.ToInt64(start_end[1])){
						return true;
					}
				}
				else{
					if(messageInfo.MessageUID == Convert.ToInt64(searchKeyValue)){
						return true;
					}
				}
			}
			// UNKEYWORD <flag>
			//	Messages that do not have the specified keyword flag set.
			else if(searchKey == "UNKEYWORD"){
				if((messageInfo.Flags & IMAP_Utils.ParseMessageFalgs((string)searchKeyValue)) == 0){
					return true;
				}
			}

			return false;
		}

		#endregion


		#region method ParseDate

		/// <summary>
		/// Parses SEARCH command date.
		/// </summary>
		/// <param name="date">Date string.</param>
		/// <returns>Returns date.</returns>
		public static DateTime ParseDate(string date)
		{
			return DateTime.ParseExact(date.Trim(),new string[]{"d-MMM-yyyy"},System.Globalization.DateTimeFormatInfo.InvariantInfo,System.Globalization.DateTimeStyles.None); 
		}

		#endregion

	}
}
