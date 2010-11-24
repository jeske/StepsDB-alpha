using System;
using System.IO;
using System.Text;
using System.Collections;

namespace LumiSoft.Net
{
	/// <summary>
	/// Byte[] line parser.
	/// </summary>
	public class StreamLineReader
	{
		private Stream m_StrmSource = null;
		private string m_Encoding   = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="strmSource"></param>
		public StreamLineReader(Stream strmSource)
		{
			m_StrmSource = strmSource;
		}


		#region method ReadLine

		/// <summary>
		/// Reads byte[] line from stream. NOTE: Returns null if end of stream reached.
		/// </summary>
		/// <returns>Return null if end of stream reached.</returns>
		public byte[] ReadLine()
		{
			MemoryStream strmLineBuf = new MemoryStream();
			byte      prevByte = 0;

			int currByteInt = m_StrmSource.ReadByte();
			while(currByteInt > -1){
				strmLineBuf.WriteByte((byte)currByteInt);

				// Line found
				if((prevByte == (byte)'\r' && (byte)currByteInt == (byte)'\n')){
					strmLineBuf.SetLength(strmLineBuf.Length - 2); // Remove <CRLF>

					return strmLineBuf.ToArray();
				}
				
				// Store byte
				prevByte = (byte)currByteInt;

				// Read next byte
				currByteInt = m_StrmSource.ReadByte();				
			}

			// Line isn't terminated with <CRLF> and has some bytes left, return them.
			if(strmLineBuf.Length > 0){
				return strmLineBuf.ToArray();
			}

			return null;
		}

		#endregion

		#region method ReadLineString

		/// <summary>
		/// Reads string line from stream. String is converted with specified Encoding property from byte[] line. NOTE: Returns null if end of stream reached.
		/// </summary>
		/// <returns></returns>
		public string ReadLineString()
		{
			byte[] line = ReadLine();
			if(line != null){
				if(m_Encoding == null || m_Encoding == ""){
					return System.Text.Encoding.Default.GetString(line);					
				}
				else{
					return System.Text.Encoding.GetEncoding(m_Encoding).GetString(line);
				}
			}
			else{
				return null;
			}
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets or sets charset encoding to use for string based methods. Default("") encoding is system default encoding.
		/// </summary>
		public string Encoding
		{
			get{ return m_Encoding; }

			set{
				// Check if encoding is valid
				System.Text.Encoding.GetEncoding(value);

				m_Encoding = value;
			}
		}

		#endregion

	}
}
