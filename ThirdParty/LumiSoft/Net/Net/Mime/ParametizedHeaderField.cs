using System;
using System.Collections;

namespace LumiSoft.Net.Mime
{
	/// <summary>
	/// Parametized header field. 
	/// <p/>
	/// Syntax: value;parameterName=parameterValue;parameterName=parameterValue;... .
	/// Example: (Content-Type:) text/html; charset="ascii".
	/// </summary>
	public class ParametizedHeaderField
	{
		private HeaderField                    m_pHeaderField = null;
		private HeaderFieldParameterCollection m_pParameters  = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="headerField">Source header field.</param>
		public ParametizedHeaderField(HeaderField headerField)
		{
			m_pHeaderField = headerField;

			m_pParameters = new HeaderFieldParameterCollection(this);
		}

		
		#region method ParseParameters

		/// <summary>
		/// Parses parameters from header field.
		/// </summary>
		/// <returns></returns>
		internal Hashtable ParseParameters()
		{
			// Syntax: value;parameterName=parameterValue;parameterName=parameterValue;... 
			string[] paramNameValues = TextUtils.SplitQuotedString(m_pHeaderField.Value,';');

			Hashtable retVal = new Hashtable();
			// Skip value, other entries are parameters
			for(int i=1;i<paramNameValues.Length;i++){
				string[] paramNameValue = paramNameValues[i].Trim().Split(new char[]{'='},2);					
				if(!retVal.ContainsKey(paramNameValue[0].ToLower())){
					if(paramNameValue.Length == 2){						
						// Remove quotes("")
						paramNameValue[1] = TextUtils.RemoveQuotes(paramNameValue[1]);

						retVal.Add(paramNameValue[0].ToLower(),paramNameValue[1]);
					}
					else{
						retVal.Add(paramNameValue[0].ToLower(),"");
					}
				}
			}

			return retVal;
		}

		#endregion

		#region method StoreParameters

		/// <summary>
		/// Stores parameters to header field Value property.
		/// </summary>
		/// <param name="value"></param>
		/// <param name="parameters"></param>
		internal void StoreParameters(string value,Hashtable parameters)
		{
			string retVal = value;
			foreach(DictionaryEntry entry in parameters){
				retVal += ";\t" + entry.Key + "=\"" + entry.Value + "\"";
			}

			// Syntax: value;parameterName=parameterValue;parameterName=parameterValue;... ;
			m_pHeaderField.Value = retVal;
		}

		#endregion

        
		#region Properties Implementation

		/// <summary>
		/// Gets header field name.
		/// </summary>
		public string Name
		{
			get{ return m_pHeaderField.Name; }
		}

		/// <summary>
		/// Gets or sets header field value.
		/// </summary>
		public string Value
		{
			get{ 
				// Syntax: value;parameterName=parameterValue;parameterName=parameterValue;... ;
				// First item is value
				return TextUtils.SplitQuotedString(m_pHeaderField.Value,';')[0];
			}

			set{
				StoreParameters(value,ParseParameters());
			}
		}

		/// <summary>
		/// Gets header field parameters.
		/// </summary>
		public HeaderFieldParameterCollection Parameters
		{
			get{ return m_pParameters; }
		}

		#endregion

	}
}
