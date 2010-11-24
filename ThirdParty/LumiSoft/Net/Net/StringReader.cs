using System;
using System.Text;

namespace LumiSoft.Net
{
	/// <summary>
	/// String reader.
	/// </summary>
	public class StringReader
	{
		private string m_SourceString = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="source">Source string.</param>
		public StringReader(string source)
		{	
			m_SourceString = source;
		}


		#region method QuotedReadToDelimiter

		/// <summary>
		/// Reads string to specified delimiter or to end of underlying string.Delimiter in quoted string is skipped.
		/// For example: delimiter = ',', text = '"aaaa,eee",qqqq' - then result is '"aaaa,eee"'.
		/// </summary>
		/// <param name="delimiter">Data delimiter.</param>
		/// <returns></returns>
		public string QuotedReadToDelimiter(char delimiter)
		{
			StringBuilder currentSplitBuffer = new StringBuilder(); // Holds active
			bool          inQuotedString     = false;               // Holds flag if position is quoted string or not

			for(int i=0;i<m_SourceString.Length;i++){
				char c = m_SourceString[i];

				if(c == '\"'){
					// Start/end quoted string area
					inQuotedString = !inQuotedString;
				}
			
				// Current char is split char and it isn't in quoted string, do split
				if(!inQuotedString && c == delimiter){					
					string retVal = currentSplitBuffer.ToString();

					// Remove readed string + delimiter from source string
					m_SourceString = m_SourceString.Substring(retVal.Length + 1);

					return retVal;
				}
				else{
					currentSplitBuffer.Append(c);
				}
			}

			// If we reached so far then we are end of string, return it
			m_SourceString = "";
			return currentSplitBuffer.ToString();
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets currently remaining string.
		/// </summary>
		public string SourceString
		{
			get{ return m_SourceString; }
		}

		#endregion
	}
}
