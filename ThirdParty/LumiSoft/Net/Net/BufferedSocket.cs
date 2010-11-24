using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections;

namespace LumiSoft.Net
{
	/// <summary>
	/// 
	/// </summary>
	public delegate void SocketCallBack(SocketCallBackResult result,long count,Exception x,object tag);

//	/// <summary>
//	/// 
//	/// </summary>
//	public delegate void SocketActivityCallback(object tag);

	/// <summary>
	/// Asynchronous command execute result.
	/// </summary>
	public enum SocketCallBackResult
	{
		/// <summary>
		/// Operation was successfull.
		/// </summary>
		Ok,

		/// <summary>
		/// Exceeded maximum allowed size.
		/// </summary>
		LengthExceeded,

		/// <summary>
		/// Connected client closed connection.
		/// </summary>
		SocketClosed,

		/// <summary>
		/// Exception happened.
		/// </summary>
		Exception,
	}

	/// <summary>
	/// Sokcet + buffer. Socket data reads are buffered. At first Recieve returns data from
	/// internal buffer and if no data available, gets more from socket. Socket buffer is also
	/// user settable, you can add data to socket buffer directly with AppendBuffer().
	/// </summary>
	public class BufferedSocket
	{
		private Socket       m_pSocket   = null;
		private byte[]       m_Buffer    = null;
		private long         m_BufPos    = 0;
		private bool         m_Closed    = false;
		private Encoding     m_pEncoding = null;
		private SocketLogger m_pLogger   = null;

		/// <summary>
		/// Is called when there is some activity on socket (Read or Send).
		/// </summary>
		public event EventHandler Activity = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="socket">Source socket which to buffer.</param>
		public BufferedSocket(Socket socket)
		{
			m_pSocket   = socket;
			m_Buffer    = new byte[0];
			m_pEncoding = Encoding.Default;
		}

		#region method Connect

		/// <summary>
		/// 
		/// </summary>
		/// <param name="remoteEP"></param>
		public void Connect(EndPoint remoteEP)
		{
			m_pSocket.Connect(remoteEP);
		}

		#endregion

		#region method BeginConnect

		/// <summary>
		/// 
		/// </summary>
		/// <param name="remoteEP"></param>
		/// <param name="callback"></param>
		/// <param name="state"></param>
		/// <returns></returns>
		public IAsyncResult BeginConnect(EndPoint remoteEP,AsyncCallback callback,object state)
		{
			return m_pSocket.BeginConnect(remoteEP,callback,state);
		}

		#endregion

		#region method EndConnect

		/// <summary>
		/// 
		/// </summary>
		/// <param name="asyncResult"></param>
		/// <returns></returns>
		public void EndConnect(IAsyncResult asyncResult)
		{
			m_pSocket.EndConnect(asyncResult);
		}

		#endregion


		#region method Receive

		/// <summary>
		/// Receives data from buffer. If there isn't data in buffer, then receives more data from socket.
		/// </summary>
		/// <param name="buffer"></param>
		/// <returns></returns>
		public int Receive(byte[] buffer)
		{
			// There isn't data in buffer, get more
			if(this.AvailableInBuffer == 0){
				byte[] buf = new byte[10000];
				int countReaded = m_pSocket.Receive(buf);
				
				if(countReaded != buf.Length){
					m_Buffer = new byte[countReaded];
					Array.Copy(buf,0,m_Buffer,0,countReaded);
				}
				else{
					m_Buffer = buf;
				}

				m_BufPos = 0;
			}

			return ReceiveFromFuffer(buffer);
		}

		#endregion

		#region method BeginReceive

		/// <summary>
		/// 
		/// </summary>
		/// <param name="buffer"></param>
		/// <param name="offset"></param>
		/// <param name="size"></param>
		/// <param name="socketFlags"></param>
		/// <param name="callback"></param>
		/// <param name="state"></param>
		public void BeginReceive(byte[] buffer,int offset,int size,SocketFlags socketFlags,AsyncCallback callback,object state)
		{
			m_pSocket.BeginReceive(buffer,offset,size,socketFlags,callback,state);
		}

		#endregion

		#region method EndReceive

		/// <summary>
		/// 
		/// </summary>
		/// <param name="asyncResult"></param>
		/// <returns></returns>
		public int EndReceive(IAsyncResult asyncResult)
		{
			return m_pSocket.EndReceive(asyncResult);
		}

		#endregion

		#region method Send

		/// <summary>
		/// 
		/// </summary>
		/// <param name="buffer"></param>
		/// <returns></returns>
		public int Send(byte[] buffer)
		{
			return m_pSocket.Send(buffer);
		}

		#endregion

		#region method Send

		/// <summary>
		/// 
		/// </summary>
		/// <param name="buffer"></param>
		/// <param name="offset"></param>
		/// <param name="size"></param>
		/// <param name="socketFlags"></param>
		public int Send(byte[] buffer,int offset,int size,SocketFlags socketFlags)
		{
			return m_pSocket.Send(buffer,offset,size,socketFlags);
		}

		#endregion


		#region method SetSocketOption

		/// <summary>
		/// 
		/// </summary>
		/// <param name="otpionLevel"></param>
		/// <param name="optionName"></param>
		/// <param name="optionValue"></param>
		public void SetSocketOption(SocketOptionLevel otpionLevel,SocketOptionName optionName,int optionValue)
		{
			m_pSocket.SetSocketOption(otpionLevel,optionName,optionValue);
		}

		#endregion


		#region method Bind

		/// <summary>
		/// Binds socket to local endpoint.
		/// </summary>
		/// <param name="localEP"></param>
		public void Bind(EndPoint localEP)
		{
			m_pSocket.Bind(localEP);
		}

		#endregion


		#region method Shutdown

		/// <summary>
		/// 
		/// </summary>
		/// <param name="how"></param>
		public void Shutdown(SocketShutdown how)
		{
			m_Closed = true;
			m_pSocket.Shutdown(how);
		}

		#endregion

		#region method Close

		/// <summary>
		/// 
		/// </summary>
		public void Close()
		{
			m_Closed = true;
			m_pSocket.Close();
		//	m_pSocket = null;
			m_Buffer = new byte[0];
		}

		#endregion

		
		#region method ReceiveFromFuffer

		/// <summary>
		/// Receives data from buffer.
		/// </summary>
		/// <param name="buffer"></param>
		/// <returns></returns>
		public int ReceiveFromFuffer(byte[] buffer)
		{
			int countInBuff = this.AvailableInBuffer;
			// There is more data in buffer as requested
			if(countInBuff > buffer.Length){
				Array.Copy(m_Buffer,m_BufPos,buffer,0,buffer.Length);

				m_BufPos += buffer.Length;

				return buffer.Length;
			}
			else{
				Array.Copy(m_Buffer,m_BufPos,buffer,0,countInBuff);

				// Reset buffer and pos, because we used all data from buffer
				m_Buffer = new byte[0];
				m_BufPos = 0;

				return countInBuff;
			}
		}

		#endregion
		
		#region method AppendBuffer

		internal void AppendBuffer(byte[] data,int length)
		{
			if(m_Buffer.Length == 0){
				m_Buffer = new byte[length];
				Array.Copy(data,0,m_Buffer,0,length);
			}
			else{
				byte[] newBuff = new byte[m_Buffer.Length + length];
				Array.Copy(m_Buffer,0,newBuff,0,m_Buffer.Length);
				Array.Copy(data,0,newBuff,m_Buffer.Length,length);

				m_Buffer = newBuff;
			}
		}

		#endregion


		
		#region method ReadLine()

		/// <summary>
		/// Reads line from socket.
		/// </summary>
		/// <returns></returns>
		public string ReadLine()
		{
			return ReadLine(1024);
		}

		#endregion

		#region method ReadLine(maxLength)

		/// <summary>
		/// Reads line from socket.
		/// </summary>
		/// <param name="maxLength">Maximum length to read.</param>
		/// <returns></returns>
		public string ReadLine(long maxLength)
		{
			using(MemoryStream storeStream = new MemoryStream()){
				ReadReplyCode code = ReadData(storeStream,maxLength,"\r\n","\r\n");	
				if(code != ReadReplyCode.Ok){
					throw new ReadException(code,code.ToString());
				}

				return m_pEncoding.GetString(storeStream.ToArray()).Trim();
			}
		}

		#endregion

		#region method BeginReadLine

		/// <summary>
		/// Starts reading line from socket asynchronously.
		/// </summary>
		/// <param name="strm">Stream where to store line.</param>
		/// <param name="maxLength">Maximum line length.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if receive completes.</param>
		public void BeginReadLine(Stream strm,long maxLength,object tag,SocketCallBack callBack)
		{
			BeginReadData(strm,maxLength,"\r\n","\r\n",tag,callBack);
		}

		#endregion

		#region method SendLine

		/// <summary>
		/// Sends line to socket.
		/// </summary>
		/// <param name="line"></param>
		public void SendLine(string line)
		{
			if(!line.EndsWith("\r\n")){
				line += "\r\n";
			}

			byte[] data = m_pEncoding.GetBytes(line);
			int countSended = m_pSocket.Send(data);
			if(countSended != data.Length){
				throw new Exception("Send error, didn't send all bytes !");
				// ToDo: if this happens just try to resend unsent bytes
			}

			// Logging stuff
			if(m_pLogger != null){
				m_pLogger.AddSendEntry(line,data.Length);
			}

			OnActivity();
		}

		#endregion

		#region method BeginSendLine

		/// <summary>
		/// Starts sending line to socket asynchronously.
		/// </summary>
		/// <param name="line">Data line.</param>
		/// <param name="callBack">Callback to be called if sending ends.</param>
		public void BeginSendLine(string line,SocketCallBack callBack)
		{
			BeginSendLine(line,null,callBack);	
		}

		/// <summary>
		/// Starts sending line to socket asynchronously.
		/// </summary>
		/// <param name="line">Data line.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Callback to be called if sending ends.</param>
		public void BeginSendLine(string line,object tag,SocketCallBack callBack)
		{
			if(!line.EndsWith("\r\n")){
				line += "\r\n";
			}

			BeginSendData(new MemoryStream(m_pEncoding.GetBytes(line)),tag,callBack);			
		}

		#endregion


		// 3 types of read
		//   1) To some terminator
		//   2) Specified length
		//   3) While socket is closed with ShutDown
	
		#region method ReadData (terminator)

		/// <summary>
		/// Reads data from socket while specified terminator is reached.
		/// If maximum length is exceeded, reading continues but data won't be stored to stream.
		/// </summary>
		/// <param name="storeStream">Stream where to store readed data.</param>
		/// <param name="maxLength">Maximum length to read.</param>
		/// <param name="terminator">Terminator which trminates reading.</param>
		/// <param name="removeFromEnd">Part of trminator what to remove from end. Can be empty or max part is terminator.</param>
		/// <returns></returns>
		public ReadReplyCode ReadData(Stream storeStream,long maxLength,string terminator,string removeFromEnd)
		{
			if(storeStream == null){
				throw new Exception("Parameter storeStream can't be null !");
			}

			ReadReplyCode replyCode = ReadReplyCode.Ok;

			try{
				_FixedStack stack = new _FixedStack(terminator);
				long readedCount      = 0;
				int  nextReadWriteLen = 1;
				while(nextReadWriteLen > 0){
					//Read byte(s)
					byte[] b = new byte[nextReadWriteLen];
					int countRecieved = this.Receive(b);
					if(countRecieved > 0){
						readedCount += countRecieved;

						// Write byte(s) to buffer, if length isn't exceeded.
						if(readedCount <= maxLength){							
							storeStream.Write(b,0,countRecieved);
						}

						// Write to stack(terminator checker)
						nextReadWriteLen = stack.Push(b,countRecieved);
					}
					// Client disconnected
					else{
						return ReadReplyCode.SocketClosed;
					}

					OnActivity();
				}
                
				// Check if length is exceeded
				if(readedCount > maxLength){
					return ReadReplyCode.LengthExceeded;
				}

				// If reply is ok then remove chars if any specified by 'removeFromEnd'.
				if(replyCode == ReadReplyCode.Ok && removeFromEnd.Length > 0){					
					storeStream.SetLength(storeStream.Length - removeFromEnd.Length);				
				}

				// Logging stuff
				if(m_pLogger != null){
					if(storeStream is MemoryStream && storeStream.Length < 200){
						MemoryStream ms = (MemoryStream)storeStream;
						m_pLogger.AddReadEntry(m_pEncoding.GetString(ms.ToArray()),readedCount);
					}
					else{
						m_pLogger.AddReadEntry("Big binary, readed " + readedCount.ToString() + " bytes.",readedCount);
					}
				}
			}
			catch(Exception x){
				replyCode = ReadReplyCode.UnKnownError;	

				if(x is SocketException){
					SocketException xS = (SocketException)x;
					if(xS.ErrorCode == 10060){
						return ReadReplyCode.TimeOut;
					}					
				}
			}

			return replyCode;
		}

		#endregion

		#region method ReadData (length)

		/// <summary>
		/// Reads specified amount of data from socket.
		/// </summary>
		/// <param name="storeStream">Stream where to store readed data.</param>
		/// <param name="countToRead">Amount of data to read.</param>
		/// <param name="storeToStream">Specifes if to store readed data to stream or junk it.</param>
		/// <returns></returns>
		public ReadReplyCode ReadData(Stream storeStream,long countToRead,bool storeToStream)
		{
			ReadReplyCode replyCode = ReadReplyCode.Ok;

			try{
				long readedCount  = 0;
				while(readedCount < countToRead){
					byte[] b = new byte[4000];
					// Ensure that we don't get more data than needed
					if((countToRead - readedCount) < 4000){
						b = new byte[countToRead - readedCount];
					}

					int countRecieved = this.Receive(b);
					if(countRecieved > 0){
						readedCount += countRecieved;

						if(storeToStream){
							storeStream.Write(b,0,countRecieved);
						}
					}
					// Client disconnected
					else{						
						return ReadReplyCode.SocketClosed;
					}

					OnActivity();
				}

				// Logging stuff
				if(m_pLogger != null){
					m_pLogger.AddReadEntry("Big binary, readed " + readedCount.ToString() + " bytes.",readedCount);
				}
			}
			catch(Exception x){
				replyCode = ReadReplyCode.UnKnownError;	

				if(x is SocketException){
					SocketException xS = (SocketException)x;
					if(xS.ErrorCode == 10060){
						return ReadReplyCode.TimeOut;
					}					
				}
			}

			return replyCode;
		}

		#endregion

		#region method ReadData (shutdown)

		/// <summary>
		/// Reads data while socket is closed with shutdown.
		/// If maximum length is exceeded, reading continues but data won't be stored to stream.
		/// </summary>
		/// <param name="storeStream">Stream where to store readed data.</param>
		/// <param name="maxLength">Maximum length to read.</param>
		public ReadReplyCode ReadData(Stream storeStream,long maxLength)
		{	
			try{
				byte[] data          = new byte[4000];
				long   readedCount   = 0;
				int    recievedCount = this.Receive(data);
				while(recievedCount > 0){
					readedCount += recievedCount;

					if(readedCount <= maxLength){
						storeStream.Write(data,0,recievedCount);
					}

					data          = new byte[4000];
					recievedCount = this.Receive(data);

					OnActivity();
				}

				// Check if length is exceeded
				if(readedCount > maxLength){
					return ReadReplyCode.LengthExceeded;
				}

				// Logging stuff
				if(m_pLogger != null){
					m_pLogger.AddReadEntry("Big binary, readed " + readedCount.ToString() + " bytes.",readedCount);
				}
			}
			catch(Exception x){
				if(x is SocketException){
					SocketException xS = (SocketException)x;
					if(xS.ErrorCode == 10060){
						return ReadReplyCode.TimeOut;
					}					
				}

				return ReadReplyCode.UnKnownError;
			}			

			return ReadReplyCode.Ok;
		}

		#endregion

		
		#region method SendData (string)
		
		/// <summary>
		/// Sends data to socket.
		/// </summary>
		/// <param name="data"></param>
		public void SendData(string data)
		{
			SendData(new MemoryStream(m_pEncoding.GetBytes(data)));
		}

		#endregion

		#region method SendData (byte[])
		
		/// <summary>
		/// Sends data to socket.
		/// </summary>
		/// <param name="data"></param>
		public void SendData(byte[] data)
		{
			SendData(new MemoryStream(data));
		}

		#endregion

		#region method SendData (Stream)
		
		/// <summary>
		/// Sends data to socket.
		/// </summary>
		/// <param name="dataStream"></param>
		public void SendData(Stream dataStream)
		{
			byte[] data = new byte[4000];
			long sendedCount = 0;
			int  readedCount = dataStream.Read(data,0,data.Length);
			while(readedCount > 0){
				int count = m_pSocket.Send(data,readedCount,0);
				if(count != readedCount){
					throw new Exception("Send error, didn't send all bytes !");
				}
                
				sendedCount += readedCount;
				readedCount = dataStream.Read(data,0,data.Length);
			
				OnActivity();
			}

			// Logging stuff
			if(m_pLogger != null){
				if(dataStream is MemoryStream && dataStream.Length < 200){
					MemoryStream ms = (MemoryStream)dataStream;
					m_pLogger.AddSendEntry(m_pEncoding.GetString(ms.ToArray()),sendedCount);
				}
				else{
					m_pLogger.AddSendEntry("Big binary, sended " + sendedCount.ToString() + " bytes.",sendedCount);
				}
			}
		}

		#endregion
		


		#region method BeginReadData (terminator)

		/// <summary>
		/// Begins asynchronous data reading.
		/// </summary>
		/// <param name="strm">Stream where to store data.</param>
		/// <param name="maxLength">Maximum length of data which may read.</param>
		/// <param name="terminator">Terminator string which terminates reading. eg '\r\n'.</param>
		/// <param name="removeFromEnd">Removes following string at end of data.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if receive completes.</param>
		public void BeginReadData(Stream strm,long maxLength,string terminator,string removeFromEnd,object tag,SocketCallBack callBack)
		{
			_SocketState state = new _SocketState(strm,maxLength,terminator,removeFromEnd,tag,callBack);

			ProccessData_Term(state);
		}

		#endregion
		
		#region method BeginReadData (length)

		/// <summary>
		/// Begins asynchronous data reading.
		/// </summary>
		/// <param name="strm">Stream where to store data.</param>
		/// <param name="lengthToRead">Length of data to read.</param>
		/// <param name="maxLength">Maximum length of data which may read.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if receive completes.</param>
		public void BeginReadData(Stream strm,long lengthToRead,long maxLength,object tag,SocketCallBack callBack)
		{	
			_SocketState state = new _SocketState(strm,lengthToRead,maxLength,tag,callBack);

			ProccessData_Len(state);
		}

		#endregion


		#region method OnRecievedData

		/// <summary>
		/// Is called from asynchronous socket if data is recieved.
		/// </summary>
		/// <param name="a"></param>
		private void OnRecievedData(IAsyncResult a)
		{
			_SocketState state = (_SocketState)a.AsyncState;

			try{
				// Socket is closed by session, we don't need to get data or call callback method.
				// This mainlty happens when session timesout and session is ended.
				if(!this.IsClosed){
					int countReaded = this.EndReceive(a);			
					if(countReaded > 0){
						this.AppendBuffer(state.RecvBuffer,countReaded);
				
						if(state.ReadType == ReadType.Terminator){
							ProccessData_Term(state);
						}
						else{
							ProccessData_Len(state);
						}
					}
					// Client disconnected 
					else if(state.Callback != null){					
						state.Callback(SocketCallBackResult.SocketClosed,state.ReadCount,null,state.Tag);
					}
				}

				OnActivity();
			}
			catch(Exception x){
				if(state.Callback != null){
					state.Callback(SocketCallBackResult.Exception,state.ReadCount,x,state.Tag);
				}
			}
		}

		#endregion

		#region method ProccessData_Term

		private void ProccessData_Term(_SocketState state)
		{
			while(state.NextRead > 0){
				// We used buffer, request more data
				if(this.AvailableInBuffer < state.NextRead){
					// Store nextReadWriteLen for next call of this command
				//	state.NextRead = state.NextRead;

					// Recieve next bytes
					byte[] buff = new byte[4000];
					state.RecvBuffer = buff; 
					this.BeginReceive(buff,0,buff.Length,0,new AsyncCallback(OnRecievedData),state);

					// End this method, if data arrives, this method is called again
					return;
				}

				//Read byte(s)
				byte[] b = new byte[state.NextRead];
				int countRecieved = this.ReceiveFromFuffer(b);

				// Increase readed count
				state.ReadCount += countRecieved;

				// Write byte(s) to buffer, if length isn't exceeded.
				if(state.ReadCount < state.MaxLength){
					state.Stream.Write(b,0,countRecieved);
				}

				// Write to stack(terminator checker)
				state.NextRead = state.Stack.Push(b,countRecieved);
			}


			// If we reach so far, then we have successfully readed data			

			if(state.ReadCount < state.MaxLength){
				// Remove "removeFromEnd" from end
				if(state.RemFromEnd.Length > 0 && state.Stream.Length > state.RemFromEnd.Length){
					state.Stream.SetLength(state.Stream.Length - state.RemFromEnd.Length);
				}		
				state.Stream.Position = 0;

				// Logging stuff
				if(m_pLogger != null){
					if(state.Stream.Length < 200 && state.Stream is MemoryStream){
						MemoryStream ms = (MemoryStream)state.Stream;
						m_pLogger.AddReadEntry(m_pEncoding.GetString(ms.ToArray()),state.ReadCount);
					}
					else{
						m_pLogger.AddReadEntry("Big binary, readed " + state.ReadCount.ToString() + " bytes.",state.ReadCount);
					}
				}

				// We got all data successfully, call EndRecieve call back
				if(state.Callback != null){
					state.Callback(SocketCallBackResult.Ok,state.ReadCount,null,state.Tag);
				}
			}
			else if(state.Callback != null){
				state.Callback(SocketCallBackResult.LengthExceeded,state.ReadCount,null,state.Tag);
			}
		}

		#endregion

		#region method ProccessData_Len

		private void ProccessData_Len(_SocketState state)
		{
			long dataAvailable = this.AvailableInBuffer;
			if(dataAvailable > 0){				
				byte[] data = new byte[dataAvailable];
				// Ensure that we don't get more data than needed !!!
				if(dataAvailable > (state.LenthToRead - state.ReadCount)){
					data = new byte[state.LenthToRead - state.ReadCount];
				}
				int countRecieved = this.ReceiveFromFuffer(data);
			
				// Increase readed count
				state.ReadCount += data.Length;

				// Message size exceeded, just don't store it
				if(state.ReadCount < state.MaxLength){
					state.Stream.Write(data,0,data.Length);
				}
			}

			// We got all data successfully, call EndRecieve call back
			if(state.ReadCount == state.LenthToRead){
				// Message size exceeded
				if(state.ReadCount > state.MaxLength){
					if(state.Callback != null){
						state.Callback(SocketCallBackResult.LengthExceeded,state.ReadCount,null,state.Tag);
					}
				}
				else{
					// Logging stuff
					if(m_pLogger != null){
						m_pLogger.AddReadEntry("Big binary, readed " + state.ReadCount.ToString() + " bytes.",state.ReadCount);
					}

					if(state.Callback != null){
						state.Callback(SocketCallBackResult.Ok,state.ReadCount,null,state.Tag);
					}
				}
			}
			else{
				// Recieve next bytes
				byte[] buff = new byte[1024];
				state.RecvBuffer = buff; 
				this.BeginReceive(buff,0,buff.Length,0,new AsyncCallback(OnRecievedData),state);
			}
		}

		#endregion



		#region method BeginSendData (string)

		/// <summary>
		/// Begins asynchronous sending.
		/// </summary>
		/// <param name="data">Data to send.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if send completes.</param>
		public void BeginSendData(string data,object tag,SocketCallBack callBack)
		{
			BeginSendData(new MemoryStream(m_pEncoding.GetBytes(data)),tag,callBack);
		}

		#endregion

		#region method BeginSendData (Stream)

		/// <summary>
		/// Begins asynchronous sending.
		/// </summary>
		/// <param name="strm">Data to send.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if send completes.</param>
		public void BeginSendData(Stream strm,object tag,SocketCallBack callBack)
		{
			SendDataBlock(strm,tag,callBack);
		}

		#endregion


		#region method OnSendedData

		/// <summary>
		/// Is called from asynchronous socket if data is sended.
		/// </summary>
		/// <param name="a"></param>
		private void OnSendedData(IAsyncResult a)
		{
			object[]       param    = (object[])a.AsyncState;
			Stream         strm     = (Stream)param[0];
			object         tag      = param[1];
			SocketCallBack callBack = (SocketCallBack)param[2];

			// ToDo: move to actual sendedCount. Currently strm.Position.

            try{
				int countSended = m_pSocket.EndSend(a);
				
				// Send next data block
				if(strm.Position < strm.Length){
					SendDataBlock(strm,tag,callBack);
				}
				// We sended all data
				else{
					// Logging stuff
					if(m_pLogger != null){
						if(strm is MemoryStream && strm.Length < 200){
							MemoryStream ms = (MemoryStream)strm;
							m_pLogger.AddSendEntry(m_pEncoding.GetString(ms.ToArray()),strm.Length);
						}
						else{
							m_pLogger.AddSendEntry("Big binary, readed " + strm.Position.ToString() + " bytes.",strm.Length);
						}
					}

					if(callBack != null){
						callBack(SocketCallBackResult.Ok,strm.Position,null,tag);
					}
				}

				OnActivity();
			}
			catch(Exception x){
				if(callBack != null){
					callBack(SocketCallBackResult.Exception,strm.Position,x,tag);
				}
			}
		}

		#endregion

		#region method SendDataBlock

		/// <summary>
		/// Starts sending block of data.
		/// </summary>
		/// <param name="strm">Data to send.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if send completes.</param>
		private void SendDataBlock(Stream strm,object tag,SocketCallBack callBack)
		{
			byte[] data = new byte[4000];
			int countReaded = strm.Read(data,0,data.Length);

			m_pSocket.BeginSend(data,0,countReaded,0,new AsyncCallback(OnSendedData),new object[]{strm,tag,callBack});						
		}

		#endregion


		private void OnActivity()
		{
			if(this.Activity != null){
				this.Activity(this,new EventArgs());
			}
		}

		/// <summary>
		/// Gets or sets socket encoding for string(ReadLine,SendLine,....) operations.
		/// </summary>
		public Encoding SocketEncoding
		{
			get{ return m_pEncoding; }
			
			set{ m_pEncoding = value; }
		}

		/// <summary>
		/// Gets or sets logging source. If this is setted, reads/writes are logged to it.
		/// </summary>
		public SocketLogger Logger
		{
			get{ return m_pLogger; }

			set{ m_pLogger = value; }
		}

		
		#region Properties Implementation

		internal Socket Socket
		{
			get{ return m_pSocket; }
		}

		internal byte[] Buffer
		{
			get{ return m_Buffer; }
		}

		/// <summary>
		/// 
		/// </summary>
		public int Available
		{
			get{ return (m_Buffer.Length - (int)m_BufPos) + m_pSocket.Available; }
		}

		/// <summary>
		/// 
		/// </summary>
		public bool Connected
		{
			get{ return m_pSocket.Connected; }
		}

		/// <summary>
		/// 
		/// </summary>
		public bool IsClosed
		{
			get{ return m_Closed; }
		}

		/// <summary>
		/// 
		/// </summary>
		public EndPoint LocalEndPoint
		{
			get{ return m_pSocket.LocalEndPoint; }
		}

		/// <summary>
		/// 
		/// </summary>
		public EndPoint RemoteEndPoint
		{
			get{ return m_pSocket.RemoteEndPoint; }
		}


		/// <summary>
		/// Gets the amount of data in buffer.
		/// </summary>
		public int AvailableInBuffer
		{
			get{ return m_Buffer.Length - (int)m_BufPos; }
		}

		#endregion
	}
}
