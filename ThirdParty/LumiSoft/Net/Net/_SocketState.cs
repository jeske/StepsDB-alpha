using System;
using System.IO;
using LumiSoft.Net;

namespace LumiSoft.Net
{
	/// <summary>
	/// Summary description for _SocketState.
	/// </summary>
	internal class _SocketState
	{
		private ReadType       m_RecvType;
		private Stream         m_pStream     = null;
		private string         m_RemFromEnd  = "";
		private object         m_Tag         = null;
		private SocketCallBack m_pCallback   = null;
		private int            m_NextRead    = 1;
		private long           m_ReadCount   = 0;
		private long           m_MaxLength   = 1000;
		private long           m_LenthToRead = 0;
		private byte[]         m_RecvBuffer  = null;
		private _FixedStack    m_pStack      = null;

		public _SocketState(Stream strm,long maxLength,string terminator,string removeFromEnd,object tag,SocketCallBack callBack)
		{			
			m_pStream    = strm;
			m_MaxLength  = maxLength;
			m_RemFromEnd = removeFromEnd;
			m_Tag        = tag;
			m_pCallback  = callBack;

			m_pStack = new _FixedStack(terminator);
			m_RecvType = ReadType.Terminator;
		}

		public _SocketState(Stream strm,long lengthToRead,long maxLength,object tag,SocketCallBack callBack)
		{			
			m_pStream     = strm;
			m_LenthToRead = lengthToRead;
			m_MaxLength   = maxLength;
			m_Tag         = tag;
			m_pCallback   = callBack;

			m_RecvType = ReadType.Length;
		}

		/// <summary>
		/// Gets 
		/// </summary>
		public _FixedStack Stack
		{
			get{ return m_pStack; }
		}

		/// <summary>
		/// Gets 
		/// </summary>
		public ReadType ReadType
		{
			get{ return m_RecvType; }
		}

		/// <summary>
		/// Gets 
		/// </summary>
		public SocketCallBack Callback
		{
			get{ return m_pCallback; }
		}

		/// <summary>
		/// Gets 
		/// </summary>
		public Stream Stream
		{
			get{ return m_pStream; }
		}

		/// <summary>
		/// Gets 
		/// </summary>
		public string RemFromEnd
		{
			get{ return m_RemFromEnd; }
		}

		public long MaxLength
		{
			get{ return m_MaxLength; }
		}

		public long LenthToRead
		{
			get{ return m_LenthToRead; }
		}

		public byte[] RecvBuffer
		{
			get{ return m_RecvBuffer; }

			set{ m_RecvBuffer = value; }
		}

		public int NextRead
		{
			get{ return m_NextRead; }

			set{ m_NextRead = value; }
		}

		public long ReadCount
		{
			get{ return m_ReadCount; }

			set{ m_ReadCount = value; }
		}

		public object Tag
		{
			get{ return m_Tag; }

			set{ m_Tag = value; }
		}
	}

	internal enum ReadType
	{
		Terminator,
		Length,
		ShutDown
	}
}
