using System;

namespace LumiSoft.Net.Mime.Parser
{
	/// <summary>
	/// Electronic address.
	/// </summary>
	[Obsolete("Use Mailbox class instead, this class will be removed.")]
	public class eAddress
	{
		private string m_eAddress = "";

		/// <summary>
		/// Default constructor.
		/// </summary>
		/// <param name="address">Electronic address. Eg. "Ivar Lumi" ivar@lumisoft.ee.</param>
		public eAddress(string address)
		{
			m_eAddress = address.Trim();
		}


		#region Properties Implementation

		/// <summary>
		/// Gets email address. Eg. ivar@lumisoft.ee .
		/// </summary>
		public string Email
		{
			get{ 
				// If email address between <>.  "Ivar Lumi" <ivar@lumisoft.ee>
				if(m_eAddress.EndsWith(">") && m_eAddress.IndexOf("<") > -1){
					return m_eAddress.Substring(m_eAddress.LastIndexOf("<") + 1,m_eAddress.Length - m_eAddress.LastIndexOf("<") - 2);
				}
				// If email address without <>. "Ivar Lumi" <ivar@lumisoft.ee>;ivar@lumisoft.ee
				else if(m_eAddress.LastIndexOf(" ") > -1){
					return m_eAddress.Substring(m_eAddress.LastIndexOf(" ") + 1).Replace("<","").Replace(">","").Trim();
				}
				return m_eAddress;
			}
		}

		/// <summary>
		/// Gets mailbox. Eg. mailbox=ivar from ivar@lumisoft.ee .
		/// </summary>
		public string Mailbox
		{
			get{ 
				if(this.Email.IndexOf("@") > -1){
					return this.Email.Substring(0,this.Email.IndexOf("@"));
				}
				return this.Email;
			}
		}

		/// <summary>
		/// Gets domain. Eg. domain=lumisoft.ee from ivar@lumisoft.ee .
		/// </summary>
		public string Domain
		{
			get{ 
				if(this.Email.IndexOf("@") > -1){
					return this.Email.Substring(this.Email.IndexOf("@") + 1);
				}
				return "";
			}
		}

		/// <summary>
		/// Gets name.  Eg. name='Ivar Lumi' from "Ivar Lumi" ivar@lumisoft.ee .
		/// </summary>
		public string Name
		{
			get{ 
				// If electronic address name between "". "Ivar Lumi" ivar@lumisoft.ee
				int startIndex = m_eAddress.IndexOf("\"");
				if(startIndex > -1 && m_eAddress.LastIndexOf("\"") > startIndex){
					return m_eAddress.Substring(startIndex + 1,m_eAddress.LastIndexOf("\"") - startIndex - 1);
				}
				// If Ivar <ivar@lumisoft.ee> or Ivar<ivar@lumisoft.ee>
				else if(m_eAddress.EndsWith(">") && m_eAddress.IndexOf("<") > -1){
					return m_eAddress.Substring(0,m_eAddress.IndexOf("<"));
				}
				else{
					return "";
				}
			}
		}

		/// <summary>
		/// Gets full electronic address. Eg. "Ivar Lumi" ivar@lumisoft.ee .
		/// </summary>
		public string ElectronicAddress
		{
			get{ return m_eAddress; }
		}

		#endregion

	}
}
