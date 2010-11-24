using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Collections;

namespace LumiSoft.Net
{
	/// <summary>
	/// 
	/// </summary>
	public delegate void SocketCallBack(SocketCallBackResult result,long count,Exception x,object tag);

	/// <summary>
	/// 
	/// </summary>
	public delegate void SocketActivityCallback(object tag);

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

	// ToDo: Rename to SocketHelper and Collect all Socket related methods here.

	/// <summary>
	/// Helper methods for asynchronous socket.
	/// </summary>
	public class AsyncSocketHelper
	{

		#region static method BeginRecieve

		/// <summary>
		/// Begins asynchronous recieveing.
		/// </summary>
		/// <param name="socket">Socket from where to get data.</param>
		/// <param name="strm">Stream where to store data.</param>
		/// <param name="maxLength">Maximum length of data which may read.</param>
		/// <param name="terminator">Terminator string which terminates reading. eg '\r\n'.</param>
		/// <param name="removeFromEnd">Removes following string at end of data.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if receive completes.</param>
		/// <param name="activityCallback">Method to call, if data block is completed. Data is retrieved as blocks, 
		/// for example data1 data2 ..., activity callback is called foreach data block.</param>
		public static void BeginRecieve(BufferedSocket socket,MemoryStream strm,long maxLength,string terminator,string removeFromEnd,object tag,SocketCallBack callBack,SocketActivityCallback activityCallback)
		{
			Hashtable param = new Hashtable();
			param.Add("recieveType","term");
			param.Add("socket",socket);
			param.Add("strm",strm);
			param.Add("removeFromEnd",removeFromEnd);
			param.Add("tag",tag);
			param.Add("callBack",callBack);
			param.Add("activityCallback",activityCallback);
			param.Add("stack",new _FixedStack(terminator));
			param.Add("nextReadWriteLen",1);
			param.Add("readedCount",(long)0);
			param.Add("maxLength",maxLength);
			param.Add("recieveBuffer",new byte[0]);

			ProccessData_Term(param);
		}

		/// <summary>
		/// Begins asynchronous recieveing.
		/// </summary>
		/// <param name="socket">Socket from where to get data.</param>
		/// <param name="strm">Stream where to store data.</param>
		/// <param name="lengthToRead">Length of data to read.</param>
		/// <param name="maxLength">Maximum length of data which may read.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if receive completes.</param>
		/// <param name="activityCallback">Method to call, if data block is completed. Data is retrieved as blocks, 
		/// for example data1 data2 ..., activity callback is called foreach data block.</param>
		public static void BeginRecieve(BufferedSocket socket,MemoryStream strm,long lengthToRead,long maxLength,object tag,SocketCallBack callBack,SocketActivityCallback activityCallback)
		{
			Hashtable param = new Hashtable();
			param.Add("recieveType","len");
			param.Add("socket",socket);
			param.Add("strm",strm);
			param.Add("lengthToRead",lengthToRead);
			param.Add("maxLength",maxLength);
			param.Add("tag",tag);
			param.Add("callBack",callBack);
			param.Add("activityCallback",activityCallback);
			param.Add("readedCount",(long)0);
			param.Add("recieveBuffer",new byte[0]);

			ProccessData_Len(param);
		}

		#endregion

		#region static method OnRecievedData

		/// <summary>
		/// Is called from asynchronous socket if data is recieved.
		/// </summary>
		/// <param name="a"></param>
		private static void OnRecievedData(IAsyncResult a)
		{
			Hashtable              param            = (Hashtable)a.AsyncState;
			BufferedSocket         socket           = (BufferedSocket)param["socket"];
			object                 tag              = param["tag"];
			SocketCallBack         callBack         = (SocketCallBack)param["callBack"];
			SocketActivityCallback activityCallback = (SocketActivityCallback)param["activityCallback"];
			byte[]                 buffer           = (byte[])param["recieveBuffer"];

			try{
				// Call activity call back, if specified
				if(activityCallback != null){
					activityCallback(tag);
				}

				// Socket is closed by session, we don't need to get data or call callback method.
				// This mainlty happens when session timesout and session is ended.
				if(!socket.IsClosed){
					int countReaded = socket.EndReceive(a);			
					if(countReaded > 0){
						socket.AppendBuffer(buffer,countReaded);
							
						if(param["recieveType"].ToString() == "term"){
							ProccessData_Term(param);
						}
						else{
							ProccessData_Len(param);
						}
					}
					// Client disconnected 
					else{					
						callBack(SocketCallBackResult.SocketClosed,(long)param["readedCount"],null,tag);
					}
				}
			}
			catch(Exception x){
				callBack(SocketCallBackResult.Exception,(long)param["readedCount"],x,tag);
			}
		}

		#endregion

		#region static method ProccessData_Term

		private static void ProccessData_Term(Hashtable param)
		{
			BufferedSocket socket           = (BufferedSocket)param["socket"];
			MemoryStream   strm             = (MemoryStream)param["strm"];
			string         removeFromEnd    = (string)param["removeFromEnd"];
			_FixedStack    stack            = (_FixedStack)param["stack"];
			int            nextReadWriteLen = (int)param["nextReadWriteLen"];
			object         tag              = param["tag"];
			SocketCallBack callBack         = (SocketCallBack)param["callBack"];

			while(nextReadWriteLen > 0){
				// We used buffer, request more data
				if(socket.AvailableInBuffer < nextReadWriteLen){
					// Store nextReadWriteLen for next call of this command
					param["nextReadWriteLen"] = nextReadWriteLen;

					// Recieve next bytes
					byte[] buff = new byte[1024];
					param["recieveBuffer"] = buff; 
					socket.BeginReceive(buff,0,buff.Length,0,new AsyncCallback(OnRecievedData),param);

					// End this method, if data arrives, this method is called again
					return;
				}

				//Read byte(s)
				byte[] b = new byte[nextReadWriteLen];
				int countRecieved = socket.ReceiveFromFuffer(b);

				// Increase readed count
				param["readedCount"] = ((long)param["readedCount"] + countRecieved);

				// Write byte(s) to buffer, if length isn't exceeded.
				if((long)param["readedCount"] < (long)param["maxLength"]){
					strm.Write(b,0,countRecieved);
				}
				// Message size exceeded, we must junk stream data. 
				else if(strm.Length > 0){
					strm.SetLength(0);
				}

				// Write to stack(terminator checker)
				nextReadWriteLen = stack.Push(b,countRecieved);
			}


			// If we reach so far, then we have successfully readed data			

			if((long)param["readedCount"] < (long)param["maxLength"]){
				// Remove "removeFromEnd" from end
				if(removeFromEnd.Length > 0 && strm.Length > removeFromEnd.Length){
					strm.SetLength(strm.Length - removeFromEnd.Length);
				}		
				strm.Position = 0;

				// We got all data successfully, call EndRecieve call back
				callBack(SocketCallBackResult.Ok,(long)param["readedCount"],null,tag);
			}
			else{
				callBack(SocketCallBackResult.LengthExceeded,(long)param["readedCount"],null,tag);
			}
		}

		#endregion

		#region static method ProccessData_Len

		private static void ProccessData_Len(Hashtable param)
		{
			BufferedSocket socket   = (BufferedSocket)param["socket"];
			MemoryStream   strm     = (MemoryStream)param["strm"];
			object         tag      = param["tag"];
			SocketCallBack callBack = (SocketCallBack)param["callBack"];

			long dataAvailable = socket.AvailableInBuffer;
			if(dataAvailable > 0){				
				byte[] data = new byte[dataAvailable];
				// Ensure that we don't get more data than needed !!!
				if(dataAvailable > ((long)param["lengthToRead"] - (long)param["readedCount"])){
					data = new byte[(long)param["lengthToRead"] - (long)param["readedCount"]];
				}
				int countRecieved = socket.ReceiveFromFuffer(data);
			
				// Increase readed count
				param["readedCount"] = ((long)param["readedCount"] + data.Length);

				if((long)param["readedCount"] < (long)param["maxLength"]){
					strm.Write(data,0,data.Length);
				}
				// Message size exceeded, we must junk stream data. 
				else if(strm.Length > 0){
					strm.SetLength(0);
				}
			}

			// We got all data successfully, call EndRecieve call back
			if((long)param["readedCount"] == (long)param["lengthToRead"]){
				callBack(SocketCallBackResult.Ok,(long)param["readedCount"],null,tag);
			}
			else{
				// Recieve next bytes
				byte[] buff = new byte[1024];
				param["recieveBuffer"] = buff; 
				socket.BeginReceive(buff,0,buff.Length,0,new AsyncCallback(OnRecievedData),param);
			}
		}

		#endregion

	
		#region static method BeginSend
	
		/// <summary>
		/// Begins asynchronous sending.
		/// </summary>
		/// <param name="socket">Socket where to send data.</param>
		/// <param name="strm">Data to send.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if send completes.</param>
		public static void BeginSend(BufferedSocket socket,Stream strm,object tag,SocketCallBack callBack)
		{
			SendDataBlock(socket,strm,tag,callBack);
		}

		#endregion

		#region static method OnSendedData

		/// <summary>
		/// Is called from asynchronous socket if data is sended.
		/// </summary>
		/// <param name="a"></param>
		private static void OnSendedData(IAsyncResult a)
		{
			object[]       param    = (object[])a.AsyncState;
			BufferedSocket socket   = (BufferedSocket)param[0];
			MemoryStream   strm     = (MemoryStream)param[1];
			object         tag      = param[2];
			SocketCallBack callBack = (SocketCallBack)param[3];

            try{
				int countSended = socket.Socket.EndSend(a);
				
				// Send next data block
				if(strm.Position < strm.Length){
					SendDataBlock(socket,strm,tag,callBack);
				}
				// We sended all data
				else{
					callBack(SocketCallBackResult.Ok,strm.Position,null,tag);
				}
			}
			catch(Exception x){
				callBack(SocketCallBackResult.Exception,strm.Position,x,tag);
			}
		}

		#endregion

		#region static method SendDataBlock

		/// <summary>
		/// Starts sending block of data.
		/// </summary>
		/// <param name="socket">Socket where to send data.</param>
		/// <param name="strm">Data to send.</param>
		/// <param name="tag">User data.</param>
		/// <param name="callBack">Method to call, if send completes.</param>
		private static void SendDataBlock(BufferedSocket socket,Stream strm,object tag,SocketCallBack callBack)
		{
			byte[] data = new byte[1024];
			int countReaded = strm.Read(data,0,data.Length);

			socket.Socket.BeginSend(data,0,countReaded,0,new AsyncCallback(OnSendedData),new object[]{socket,strm,tag,callBack});						
		}

		#endregion


	}
}
