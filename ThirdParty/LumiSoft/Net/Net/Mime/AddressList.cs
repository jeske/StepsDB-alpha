using System;
using System.Collections;

namespace LumiSoft.Net.Mime
{
	/// <summary>
	/// Rfc 2822 3.4 address-list. Rfc defines two types of addresses mailbox and group.
	/// <p/>
	/// <p style="margin-top: 0; margin-bottom: 0"/><b>address-list</b> syntax: address *("," address).
	/// <p style="margin-top: 0; margin-bottom: 0"/><b>address</b> syntax: mailbox / group.
	/// <p style="margin-top: 0; margin-bottom: 0"/><b>mailbox</b> syntax: ['"'dispaly-name'"' ]&lt;localpart@domain&gt;.
	/// <p style="margin-top: 0; margin-bottom: 0"/><b>group</b> syntax: '"'dispaly-name'":' [mailbox *(',' mailbox)]';'.
	/// </summary>
	public class AddressList : IEnumerable
	{
		private HeaderField m_HeaderField = null;
		private ArrayList   m_pAddresses  = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public AddressList()
		{
			m_pAddresses = new ArrayList();
		}

		
		#region method Add

		/// <summary>
		/// Adds a new address to the end of the collection.
		/// </summary>
		/// <param name="address">Address to add.</param>
		public void Add(Address address)
		{
			address.Owner = this;
			m_pAddresses.Add(address);
		
			OnCollectionChanged();
		}

		#endregion

		#region method Insert

		/// <summary>
		/// Inserts a new address into the collection at the specified location.
		/// </summary>
		/// <param name="index">The location in the collection where you want to add the address.</param>
		/// <param name="address">Address to add.</param>
		public void Insert(int index,Address address)
		{
			address.Owner = this;
			m_pAddresses.Insert(index,address);
	
			OnCollectionChanged();
		}

		#endregion


		#region method Remove
		
		/// <summary>
		/// Removes address at the specified index from the collection.
		/// </summary>
		/// <param name="index">Index of the address which to remove.</param>
		public void Remove(int index)
		{
			Remove((Address)m_pAddresses[index]);
		}

		/// <summary>
		/// Removes specified address from the collection.
		/// </summary>
		/// <param name="address">Address to remove.</param>
		public void Remove(Address address)
		{
			address.Owner = null;
			m_pAddresses.Remove(address);
		
			OnCollectionChanged();
		}

		#endregion

		#region method Clear

		/// <summary>
		/// Clears the collection of all addresses.
		/// </summary>
		public void Clear()
		{
			foreach(Address address in m_pAddresses){
				address.Owner = null;
			}
			m_pAddresses.Clear();

			OnCollectionChanged();
		}

		#endregion


		#region method Parse

		/// <summary>
		/// Parses address-list from string.
		/// </summary>
		/// <param name="addressList">Address list string.</param>
		/// <returns></returns>
		public void Parse(string addressList)
		{
			addressList = addressList.Trim();
				
			StringReader reader = new StringReader(addressList);
			while(reader.SourceString.Length > 0){
				// See if mailbox or group. If ',' is before ':', then mailbox
				// Example: xxx@domain.com,	group:xxxgroup@domain.com;
				int commaIndex = TextUtils.QuotedIndexOf(reader.SourceString,','); 
				int colonIndex = TextUtils.QuotedIndexOf(reader.SourceString,':');

				// Mailbox
				if(colonIndex == -1 || (commaIndex < colonIndex && commaIndex != -1)){
					// Read to ',' or to end if last element
					m_pAddresses.Add(MailboxAddress.Parse(reader.QuotedReadToDelimiter(',')));					
				}
					// Group
				else{
					// Read to ';', this is end of group
					m_pAddresses.Add(GroupAddress.Parse(reader.QuotedReadToDelimiter(';')));

					// If there are next items, remove first comma because it's part of group address
					if(reader.SourceString.Length > 0){
						reader.QuotedReadToDelimiter(',');
					}
				}
			}

			OnCollectionChanged();
		}

		#endregion


		#region method ToAddressListString
        
		/// <summary>
		/// Convert addresses to Rfc 2822 address-list string.
		/// </summary>
		/// <returns></returns>
		public string ToAddressListString()
		{
			string retVal = "";
			for(int i=0;i<m_pAddresses.Count;i++){
				if(m_pAddresses[i] is MailboxAddress){
					// For last address don't add , and <TAB>
					if(i == (m_pAddresses.Count - 1)){
						retVal += ((MailboxAddress)m_pAddresses[i]).MailboxString;
					}
					else{
						retVal += ((MailboxAddress)m_pAddresses[i]).MailboxString + ",\t";
					}
				}
				else if(m_pAddresses[i] is GroupAddress){
					// For last address don't add , and <TAB>
					if(i == (m_pAddresses.Count - 1)){
						retVal += ((GroupAddress)m_pAddresses[i]).GroupString;
					}
					else{
						retVal += ((GroupAddress)m_pAddresses[i]).GroupString + ",\t";
					}
				}
			}
            
			return retVal;
		}

		#endregion


		#region internal method OnCollectionChanged

		/// <summary>
		/// This called when collection has changed. Item is added,deleted,changed or collection cleared.
		/// </summary>
		internal void OnCollectionChanged()
		{
			// AddressList is bounded to HeaderField, update header field value
			if(m_HeaderField != null){
				m_HeaderField.Value = this.ToAddressListString();
			}
		}

		#endregion


		#region interface IEnumerator

		/// <summary>
		/// Gets enumerator.
		/// </summary>
		/// <returns></returns>
		public IEnumerator GetEnumerator()
		{
			return m_pAddresses.GetEnumerator();
		}

		#endregion

		#region Properties Implementation

		/// <summary>
		/// Gets all mailbox addresses. Note: group address mailbox addresses are also included.
		/// </summary>
		public MailboxAddress[] Mailboxes
		{
			get{ 
				ArrayList adressesAll = new ArrayList();
				foreach(Address adress in this){
					if(adress.IsGroupAddress){
						adressesAll.Add((MailboxAddress)adress);
					}
					else{
						foreach(Address groupChildAddress in ((GroupAddress)adress).GroupMembers){
							adressesAll.Add((MailboxAddress)groupChildAddress);
						}
					}
				}

				MailboxAddress[] retVal = new MailboxAddress[adressesAll.Count];
				adressesAll.CopyTo(retVal);

				return retVal;
			}
		}

		/// <summary>
		/// Gets address from specified index.
		/// </summary>
		public Address this[int index]
		{
			get{ return (Address)m_pAddresses[index]; }
		}

		/// <summary>
		/// Gets address count in the collection.
		/// </summary>
		public int Count
		{
			get{ return m_pAddresses.Count; }
		}


		/// <summary>
		/// Bound address-list to specified header field.
		/// </summary>
		internal HeaderField BoundedHeaderField
		{
			get{ return m_HeaderField; }

			set{m_HeaderField = value; }
		}

		#endregion

	}
}
