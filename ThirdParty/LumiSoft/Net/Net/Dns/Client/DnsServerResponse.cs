using System;
using System.Collections;

namespace LumiSoft.Net.Dns.Client
{
	/// <summary>
	/// This class holds dns server response.
	/// </summary>
	[Serializable]
	public class DnsServerResponse
	{
		private bool      m_Success             = true;
		private RCODE     m_RCODE               = RCODE.NO_ERROR;
		private ArrayList m_pAnswers            = null;
		private ArrayList m_pAuthoritiveAnswers = null;
		private ArrayList m_pAdditionalAnswers  = null;
		
		internal DnsServerResponse(bool connectionOk,RCODE rcode,ArrayList answers,ArrayList authoritiveAnswers,ArrayList additionalAnswers)
		{
			m_Success             = connectionOk;
			m_RCODE               = rcode;	
			m_pAnswers            = answers;
			m_pAuthoritiveAnswers = authoritiveAnswers;
			m_pAdditionalAnswers  = additionalAnswers;
		}


		#region method GetARecords

		/// <summary>
		/// Gets IPv4 host addess records.
		/// </summary>
		/// <returns></returns>
		public A_Record[] GetARecords()
		{
			return (A_Record[])FilterRecords(m_pAnswers,typeof(A_Record)).ToArray(typeof(A_Record));
		}

		#endregion

		#region method GetNSRecords

		/// <summary>
		/// Gets name server records.
		/// </summary>
		/// <returns></returns>
		public NS_Record[] GetNSRecords()
		{
			return (NS_Record[])FilterRecords(m_pAnswers,typeof(NS_Record)).ToArray(typeof(NS_Record));
		}

		#endregion

		#region method GetCNAMERecords

		/// <summary>
		/// Gets CNAME records.
		/// </summary>
		/// <returns></returns>
		public CNAME_Record[] GetCNAMERecords()
		{
			return (CNAME_Record[])FilterRecords(m_pAnswers,typeof(CNAME_Record)).ToArray(typeof(CNAME_Record));
		}

		#endregion

		#region method GetSOARecords

		/// <summary>
		/// Gets SOA records.
		/// </summary>
		/// <returns></returns>
		public SOA_Record[] GetSOARecords()
		{
			return (SOA_Record[])FilterRecords(m_pAnswers,typeof(SOA_Record)).ToArray(typeof(SOA_Record));
		}

		#endregion

		#region method GetPTRRecords

		/// <summary>
		/// Gets PTR records.
		/// </summary>
		/// <returns></returns>
		public PTR_Record[] GetPTRRecords()
		{	
			return (PTR_Record[])FilterRecords(m_pAnswers,typeof(PTR_Record)).ToArray(typeof(PTR_Record));
		}

		#endregion

		#region method GetHINFORecords

		/// <summary>
		/// Gets HINFO records.
		/// </summary>
		/// <returns></returns>
		public HINFO_Record[] GetHINFORecords()
		{	
			return (HINFO_Record[])FilterRecords(m_pAnswers,typeof(HINFO_Record)).ToArray(typeof(HINFO_Record));
		}

		#endregion

		#region method GetMXRecords

		/// <summary>
		/// Gets MX records.(MX records are sorted by preference, lower array element is prefered)
		/// </summary>
		/// <returns></returns>
		public MX_Record[] GetMXRecords()
		{
			MX_Record[] mxRecords = (MX_Record[])FilterRecords(m_pAnswers,typeof(MX_Record)).ToArray(typeof(MX_Record));
			SortedList mx            = new SortedList();
			ArrayList  duplicateList = new ArrayList();
			foreach(MX_Record mxRecord in mxRecords){
				if(!mx.Contains(mxRecord.Preference)){
					mx.Add(mxRecord.Preference,mxRecord);
				}
				else{
					duplicateList.Add(mxRecord);
				}
			}

			mxRecords = new MX_Record[mx.Count + duplicateList.Count];
			mx.Values.CopyTo(mxRecords,0);
			duplicateList.CopyTo(mxRecords,mx.Count);

			return mxRecords;
		}

		#endregion

		#region method GetTXTRecords

		/// <summary>
		/// Gets text records.
		/// </summary>
		/// <returns></returns>
		public TXT_Record[] GetTXTRecords()
		{
			return (TXT_Record[])FilterRecords(m_pAnswers,typeof(TXT_Record)).ToArray(typeof(TXT_Record));
		}

		#endregion

		#region method GetAAAARecords

		/// <summary>
		/// Gets IPv6 host addess records.
		/// </summary>
		/// <returns></returns>
		public A_Record[] GetAAAARecords()
		{
			return (A_Record[])FilterRecords(m_pAnswers,typeof(A_Record)).ToArray(typeof(A_Record));
		}

		#endregion


		#region method FilterRecords

		/// <summary>
		/// Filters out specified type of records from answer.
		/// </summary>
		/// <param name="answers"></param>
		/// <param name="type"></param>
		/// <returns></returns>
		private ArrayList FilterRecords(ArrayList answers,Type type)
		{
			ArrayList aRecords = new ArrayList();
			foreach(object answer in answers){
				if(answer.GetType() == type){
					aRecords.Add(answer);
				}
			}

			return aRecords;
		}

		#endregion


		#region Properties Implementation

		/// <summary>
		/// Gets if connection to dns server was successful.
		/// </summary>
		public bool ConnectionOk
		{
			get{ return m_Success; }
		}

		/// <summary>
		/// Gets dns server response code.
		/// </summary>
		public RCODE ResponseCode
		{
			get{ return m_RCODE; }
		}

		
		/// <summary>
		/// Gets all resource records returned by server (answer records section + authority records section + additional records section). 
		/// NOTE: Before using this property ensure that ConnectionOk=true and ResponseCode=RCODE.NO_ERROR.
		/// </summary>
		public DnsRecordBase[] AllAnswers
		{
			get{
				DnsRecordBase[] retVal = new DnsRecordBase[m_pAnswers.Count + m_pAuthoritiveAnswers.Count + m_pAdditionalAnswers.Count];
				m_pAnswers.CopyTo(retVal,0);
				m_pAuthoritiveAnswers.CopyTo(retVal,m_pAnswers.Count);
				m_pAdditionalAnswers.CopyTo(retVal,m_pAnswers.Count + m_pAuthoritiveAnswers.Count);

				return retVal; 
			}
		}

		/// <summary>
		/// Gets dns server returned answers. NOTE: Before using this property ensure that ConnectionOk=true and ResponseCode=RCODE.NO_ERROR.
		/// </summary>
		/// <code>
		/// // NOTE: DNS server may return diffrent record types even if you query MX.
		/// //       For example you query lumisoft.ee MX and server may response:	
		///	//		 1) MX - mail.lumisoft.ee
		///	//		 2) A  - lumisoft.ee
		///	// 
		///	//       Before casting to right record type, see what type record is !
		///				
		/// 
		/// foreach(DnsRecordBase record in Answers){
		///		// MX record, cast it to MX_Record
		///		if(record.RecordType == QTYPE.MX){
		///			MX_Record mx = (MX_Record)record;
		///		}
		/// }
		/// </code>
		public DnsRecordBase[] Answers
		{
			get{ 
				DnsRecordBase[] retVal = new DnsRecordBase[m_pAnswers.Count];
				m_pAnswers.CopyTo(retVal);

				return retVal; 
			}
		}

		/// <summary>
		/// Gets name server resource records in the authority records section. NOTE: Before using this property ensure that ConnectionOk=true and ResponseCode=RCODE.NO_ERROR.
		/// </summary>
		public DnsRecordBase[] AuthoritiveAnswers
		{
			get{ 
				DnsRecordBase[] retVal = new DnsRecordBase[m_pAuthoritiveAnswers.Count];
				m_pAuthoritiveAnswers.CopyTo(retVal);

				return retVal; 
			}
		}

		/// <summary>
		/// Gets resource records in the additional records section. NOTE: Before using this property ensure that ConnectionOk=true and ResponseCode=RCODE.NO_ERROR.
		/// </summary>
		public DnsRecordBase[] AdditionalAnswers
		{
			get{ 
				DnsRecordBase[] retVal = new DnsRecordBase[m_pAdditionalAnswers.Count];
				m_pAdditionalAnswers.CopyTo(retVal);

				return retVal; 
			}
		}

		#endregion
	}
}
