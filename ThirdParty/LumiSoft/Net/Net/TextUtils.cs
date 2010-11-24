using System;
using System.Collections;
using System.Text;

namespace LumiSoft.Net
{
	/// <summary>
	/// This class provides useful text methods.
	/// </summary>
	public class TextUtils
	{
		#region static method RemoveQuotes

		/// <summary>
		/// Removes start and end quote from string, if its quouted string. For example: "text" will be text.
		/// </summary>
		/// <param name="text">Text from where to remove quotes.</param>
		/// <returns></returns>
		public static string RemoveQuotes(string text)
		{
			text = text.Trim();

			if(text.StartsWith("\"")){
				text = text.Substring(1);
			}
			if(text.EndsWith("\"")){
				text = text.Substring(0,text.Length - 1);
			}

			return text;
		}

		#endregion

		#region method QuoteString

		/// <summary>
		/// Quotes specified string. Note: Quotes inside text will be escaped to \".
		/// </summary>
		/// <param name="text">String to quote.</param>
		/// <returns></returns>
		public static string QuoteString(string text)
		{
			return "\"" + text.Replace("\"","\\\"") + "\"";
		}

		#endregion

	//	public void UnQuoteString(string text)

	//	public void EscapeString(string text,char[] charsToEscape)
	//	public void UnEscapeString(string text)

		#region static method SplitQuotedString

		/// <summary>
		/// Splits string into string arrays. This split method won't split qouted strings, but only text outside of qouted string.
		/// For example: '"text1, text2",text3' will be 2 parts: "text1, text2" and text3.
		/// </summary>
		/// <param name="text">Text to split.</param>
		/// <param name="splitChar">Char that splits text.</param>
		/// <returns></returns>
		public static string[] SplitQuotedString(string text,char splitChar)
		{
			ArrayList     splitParts         = new ArrayList();     // Holds splitted parts
			StringBuilder currentSplitBuffer = new StringBuilder(); // Holds active
			bool          inQuotedString     = false;               // Holds flag if position is quoted string or not

			foreach(char c in text){
                if(c == '\"'){
					// Start/end quoted string area
					inQuotedString = !inQuotedString;
				}
			
				// Current char is split char and it isn't in quoted string, do split
				if(!inQuotedString && c == splitChar){
					// Add current currentSplitBuffer value to splitted parts list
					splitParts.Add(currentSplitBuffer.ToString());

					// Begin next splitted part (clear old buffer)
					currentSplitBuffer = new StringBuilder();
				}
				else{
					currentSplitBuffer.Append(c);
				}
			}
			// Add last split part to splitted parts list
			splitParts.Add(currentSplitBuffer.ToString());

			string[] retVal = new string[splitParts.Count];
			for(int i=0;i<splitParts.Count;i++){
				retVal[i] = (string)splitParts[i];
			}

			return retVal;
		}

		#endregion


		#region method QuotedIndexOf

		/// <summary>
		/// Gets first index of specified char. The specified char in quoted string is skipped.
		/// Returns -1 if specified char doesn't exist.
		/// </summary>
		/// <param name="text">Text in what to check.</param>
		/// <param name="indexChar">Char what index to get.</param>
		/// <returns></returns>
		public static int QuotedIndexOf(string text,char indexChar)
		{
			int  retVal         = -1;
			bool inQuotedString = false; // Holds flag if position is quoted string or not			
			for(int i=0;i<text.Length;i++){
				char c = text[i];

				if(c == '\"'){
					// Start/end quoted string area
					inQuotedString = !inQuotedString;
				}

				// Current char is what index we want and it isn't in quoted string, return it's index
				if(!inQuotedString && c == indexChar){
					return i;
				}
			}

			return retVal;
		}

		#endregion


		#region static method SplitString

		/// <summary>
		/// Splits string into string arrays.
		/// </summary>
		/// <param name="text">Text to split.</param>
		/// <param name="splitChar">Char Char that splits text.</param>
		/// <returns></returns>
		public static string[] SplitString(string text,char splitChar)
		{
			ArrayList splitParts = new ArrayList();  // Holds splitted parts

			int lastSplitPoint = 0;
			int textLength     = text.Length;
			for(int i=0;i<textLength;i++){
				if(text[i] == splitChar){
					// Add current currentSplitBuffer value to splitted parts list
					splitParts.Add(text.Substring(lastSplitPoint,i - lastSplitPoint));

					lastSplitPoint = i + 1;
				}
			}
			// Add last split part to splitted parts list
			if(lastSplitPoint <= textLength){
				splitParts.Add(text.Substring(lastSplitPoint));
			}

			string[] retVal = new string[splitParts.Count];
			splitParts.CopyTo(retVal,0);

			return retVal;
		}

		#endregion
	}
}
