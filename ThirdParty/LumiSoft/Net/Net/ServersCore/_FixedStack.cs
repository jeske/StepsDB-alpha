using System;
using System.Collections;

namespace LumiSoft.Net
{
	/// <summary>
	/// Fixed Stack, last-in-first-out. Fix me: this isn't Stack, this is Queue.
	/// </summary>
	internal class _FixedStack
	{
		private byte[] m_SackList   = null;
		private byte[] m_TerminaTor = null;

		/// <summary>
		/// Terminator holder and checker stack.
		/// </summary>
		/// <param name="terminator"></param>
		public _FixedStack(string terminator)
		{						
			m_TerminaTor = System.Text.Encoding.ASCII.GetBytes(terminator);
			m_SackList = new byte[m_TerminaTor.Length];

			// Init empty array
			for(int i=0;i<m_TerminaTor.Length;i++){
				m_SackList[i] = (byte)0;
			}
		}

		#region function Push

		/// <summary>
		/// Pushes new bytes to stack.(Last in, first out). 
		/// </summary>
		/// <param name="bytes"></param>
		/// <param name="count">Count to push from bytes parameter</param>
		/// <returns>Returns number of bytes may be pushed next push.
		/// NOTE: returns 0 if stack contains terminator.
		/// </returns>
		public int Push(byte[] bytes,int count)
		{
			int termLen = m_TerminaTor.Length;

			if(bytes.Length > termLen){
				throw new Exception("bytes.Length is too big, can't be more than terminator.length !");
			}
			if(count > termLen){
				throw new Exception("count is too big, can't be more than terminator.length !");
			}
			
			// Move stack bytes which will stay and append new ones
		//	Array.Copy(m_SackList,count,m_SackList,0,m_SackList.Length - count);
		//	Array.Copy(bytes,0,m_SackList,m_SackList.Length - count,count);
		// Code above is slower than code below, when message size > 5 mb
						
			// Append new bytes to end and remove first bytes
			if(count != termLen){
				byte[] newStack = new byte[termLen];
				for(int i=0;i<termLen;i++){
					// Write old bytes
					if(termLen - count > i){
						newStack[i] = m_SackList[count + i];
					}
					// Write new bytes
					else{
						newStack[i] = bytes[i - (termLen - count)];				
					}
				}
				m_SackList = newStack;
			}
			// Push count is equal to stack, just set is as new stack
			else{
				m_SackList = bytes;
			}

		//	int index = Array.IndexOf(m_SackList,m_TerminaTor[0]);
		// Code above is slower than code below, when message size > 5 mb

			int index = -1;
			for(int i=0;i<termLen;i++){				
				if(m_SackList[i] == m_TerminaTor[0]){
					index = i;
					break;
				}
			}

			if(index > -1){
				if(index == 0){
					// Check if contains full terminator
					for(int i=0;i<m_SackList.Length;i++){
						if(m_SackList[i] != m_TerminaTor[i]){
							return 1;
						}
					}
					return 0; // If reaches so far, contains terminator
				}
				
				return 1;				
			}
			else{
				return termLen;
			}
		}

		#endregion

		#region function ContainsTerminator

		/// <summary>
		/// Check if stack contains terminator.
		/// </summary>
		/// <returns></returns>
		public bool ContainsTerminator()
		{	
			for(int i=0;i<m_SackList.Length;i++){
				if((byte)m_SackList[i] != m_TerminaTor[i]){
					return false;
				}
			}

			return true;
		}

		#endregion

		
		#region Properties Implementation

	/*	/// <summary>
		/// 
		/// </summary>
		public int Count
		{
			get{ return m_SackList.Count; }
		}*/

		#endregion

	}
}
