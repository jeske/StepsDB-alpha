using System;
using System.Collections;
using System.Text.RegularExpressions;

namespace LumiSoft.Net
{
	#region class _Parameter

	internal class _Parameter
	{
		private string m_ParamName  = "";
		private string m_ParamValue = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="paramName"></param>
		/// <param name="paramValue"></param>
		public _Parameter(string paramName,string paramValue)
		{
			m_ParamName  = paramName;
			m_ParamValue = paramValue;
		}

		#region Properties implementation

		/// <summary>
		/// 
		/// </summary>
		public string ParamName
		{
			get{ return m_ParamName; }
		}

		/// <summary>
		/// 
		/// </summary>
		public string ParamValue
		{
			get{ return m_ParamValue; }
		}

		#endregion
	}

	#endregion

	/// <summary>
	/// Summary description for _ParamParser.
	/// </summary>
	internal class _ParamParser
	{
	//	public _ParamParser()
	//	{			
	//	}
	

		#region function Paramparser_NameValue

		/// <summary>
		/// Parses name-value params.
		/// </summary>
		/// <param name="source">Parse source.</param>
		/// <param name="expressions">Expressions importance order. NOTE: must contain param and value groups.</param>
		public static _Parameter[] Paramparser_NameValue(string source,string[] expressions)
		{
			string tmp = source.Trim();
			ArrayList param = new ArrayList();
			foreach(string exp in expressions){
				Regex r = new Regex(exp,RegexOptions.IgnoreCase);
				Match m = r.Match(tmp);				
				if(m.Success){
					param.Add(new _Parameter(m.Result("${param}").Trim(),m.Result("${value}")));

					// remove matched string part form tmp
					tmp = tmp.Replace(m.ToString(),"").Trim();
				}				
			}

			// There are some unparsed params, add them as UnParsed
			if(tmp.Trim().Length > 0){
				param.Add(new _Parameter("UNPARSED",tmp));
			}

			_Parameter[] retVal = new _Parameter[param.Count];
			param.CopyTo(retVal);

			return retVal;
		}

		#endregion

	}
}
