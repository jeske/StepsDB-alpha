using System;
using System.IO;
using System.Collections;

namespace LumiSoft.Net.Mime
{
	/// <summary>
	/// Class for creating,parsing,modifing rfc 2822 mime messages.
	/// </summary>
	/// <remarks>
	/// <code>
	/// 
	/// Message examples:
	/// 
	/// <B>Simple message:</B>
	/// 
	/// //--- Beginning of message
	/// From: sender@domain.com
	/// To: recipient@domain.com
	/// Subject: Message subject.
	/// Content-Type: text/plain
	/// 
	/// Message body text. Bla blaa
	/// blaa,blaa.
	/// //--- End of message
	/// 
	/// 
	/// In simple message MainEntity is whole message.
	/// 
	/// <B>Message with attachments:</B>
	/// 
	/// //--- Beginning of message
	/// From: sender@domain.com
	/// To: recipient@domain.com
	/// Subject: Message subject.
	/// Content-Type: multipart/mixed; boundary="multipart_mixed"
	/// 
	/// --multipart_mixed	/* text entity */
	///	Content-Type: text/plain
	///	
	///	Message body text. Bla blaa
	///	blaa,blaa.	
	///	--multipart_mixed	/* attachment entity */
	///	Content-Type: application/octet-stream
	///	
	///	attachment_data
	///	--multipart_mixed--
	///	//--- End of message
	///	
	///	MainEntity is multipart_mixed entity and text and attachment entities are child entities of MainEntity.
	/// </code>
	/// </remarks>
	/// <example>
	/// <code>
	/// // Parsing example:
	/// Mime m = Mime.Parse("message.eml");
	/// // Do your stuff with mime
	/// </code>
	/// <code>
	/// // Creating a new simple message
	/// Mime m = new Mime();
	/// MimeEntity mainEntity = m.MainEntity;
	/// // Force to create From: header field
	/// mainEntity.From = new AddressList();
	/// mainEntity.From.Add(new MailboxAddress("dispaly name","user@domain.com"));
	/// // Force to create To: header field
	/// mainEntity.To = new AddressList();
	/// mainEntity.To.Add(new MailboxAddress("dispaly name","user@domain.com"));
	/// mainEntity.Subject = "subject";
	/// mainEntity.ContentType = MediaType_enum.Text_plain;
	/// mainEntity.ContentTransferEncoding = ContentTransferEncoding_enum.QuotedPrintable;
	/// mainEntity.DataText = "Message body text.";
	/// 
	/// m.ToFile("message.eml");
	/// </code>
	/// <code>
	/// // Creating message with text and attachments
	/// Mime m = new Mime();
	/// MimeEntity mainEntity = m.MainEntity;
	/// // Force to create From: header field
	/// mainEntity.From = new AddressList();
	/// mainEntity.From.Add(new MailboxAddress("dispaly name","user@domain.com"));
	/// // Force to create To: header field
	/// mainEntity.To = new AddressList();
	/// mainEntity.To.Add(new MailboxAddress("dispaly name","user@domain.com"));
	/// mainEntity.Subject = "subject";
	/// mainEntity.ContentType = MediaType_enum.Multipart_mixed;
	/// 
	/// MimeEntity textEntity = mainEntity.ChildEntities.Add();
	/// textEntity.ContentType = MediaType_enum.Text_plain;
	/// textEntity.ContentTransferEncoding = ContentTransferEncoding_enum.QuotedPrintable;
	/// textEntity.DataText = "Message body text.";
	/// 
	/// MimeEntity attachmentEntity = mainEntity.ChildEntities.Add();
	/// attachmentEntity.ContentType = MediaType_enum.Application_octet_stream;
	/// attachmentEntity.ContentDisposition = ContentDisposition_enum.Attachment;
	/// attachmentEntity.ContentTransferEncoding = ContentTransferEncoding_enum.Base64;
	/// attachmentEntity.ContentDisposition_FileName = "yourfile.xxx";
	/// attachmentEntity.DataFromFile("yourfile.xxx");
	/// // or
	/// attachmentEntity.Data = your_attachment_data;
	/// </code>
	/// </example>
	public class Mime
	{
		private MimeEntity m_pMainEntity = null;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public Mime()
		{
			m_pMainEntity = new MimeEntity();

			// Add default header fields
			m_pMainEntity.MessageID = MimeUtils.CreateMessageID();
			m_pMainEntity.Date = DateTime.Now;
			m_pMainEntity.MimeVersion = "1.0";
		}


		#region static method Parse

		/// <summary>
		/// Parses mime message from byte[] data.
		/// </summary>
		/// <param name="data">Mime message data.</param>
		/// <returns></returns>
		public static Mime Parse(byte[] data)
		{
			using(MemoryStream ms = new MemoryStream(data)){
				return Parse(ms);
			}
		}

		/// <summary>
		/// Parses mime message from file.
		/// </summary>
		/// <param name="fileName">Mime message file.</param>
		/// <returns></returns>
		public static Mime Parse(string fileName)
		{
			using(FileStream fs = File.OpenRead(fileName)){
				return Parse(fs);
			}
		}

		/// <summary>
		/// Parses mime message from stream.
		/// </summary>
		/// <param name="stream">Mime message stream.</param>
		/// <returns></returns>
		public static Mime Parse(Stream stream)
		{
			Mime mime = new Mime();
			mime.MainEntity.Parse(stream,null);

			return mime;
		}

		#endregion


		#region method ToStringData

		/// <summary>
		/// Stores mime message to string.
		/// </summary>
		/// <returns></returns>
		public string ToStringData()
		{
			return System.Text.Encoding.Default.GetString(this.ToByteData());
		}

		#endregion

		#region method ToByteData

		/// <summary>
		/// Stores mime message to byte[].
		/// </summary>
		/// <returns></returns>
		public byte[] ToByteData()
		{
			using(MemoryStream ms = new MemoryStream()){
				ToStream(ms);

				return ms.ToArray();
			}
		}

		#endregion

		#region method ToFile

		/// <summary>
		/// Stores mime message to specified file.
		/// </summary>
		/// <param name="fileName">File name.</param>
		public void ToFile(string fileName)
		{
			using(FileStream fs = File.Create(fileName)){
				ToStream(fs);
			}
		}

		#endregion

		#region method ToStream

		/// <summary>
		/// Stores mime message to specified stream. Stream position stays where mime writing ends.
		/// </summary>
		/// <param name="storeStream">Stream where to store mime message.</param>
		public void ToStream(Stream storeStream)
		{
			m_pMainEntity.ToStream(storeStream);
		}

		#endregion


		#region function GetEntities

		/// <summary>
		/// Gets mime entities, including nested entries. 
		/// </summary>
		/// <param name="entities"></param>
		/// <param name="allEntries"></param>
		private void GetEntities(MimeEntityCollection entities,ArrayList allEntries)
		{				
			if(entities != null){
				foreach(MimeEntity ent in entities){
					allEntries.Add(ent);

					// Add child entities, if any
					if(ent.ChildEntities.Count > 0){
						GetEntities(ent.ChildEntities,allEntries);
					}
				}
			}
		}

		#endregion


		#region Properties Implementaion

		/// <summary>
		/// Message main(top-level) entity.
		/// </summary>
		public MimeEntity MainEntity
		{
			get{ return m_pMainEntity; }
		}

		/// <summary>
		/// Gets all mime entities contained in message, including child entities.
		/// </summary>
		public MimeEntity[] MimeEntities
		{
			get{ 
				ArrayList allEntities = new ArrayList();
				allEntities.Add(m_pMainEntity);
				GetEntities(m_pMainEntity.ChildEntities,allEntities);

				return (MimeEntity[])allEntities.ToArray(typeof(MimeEntity)); 
			}
		}
		
		/// <summary>
		/// Gets mime entities which Content-Disposition: is Attachment or Inline.
		/// </summary>
		public MimeEntity[] Attachments
		{
			get{
				ArrayList attachments = new ArrayList();
				MimeEntity[] entities = this.MimeEntities;
				foreach(MimeEntity entity in entities){
					if(entity.ContentDisposition == ContentDisposition_enum.Attachment || entity.ContentDisposition == ContentDisposition_enum.Inline){
						attachments.Add(entity);
					}
				}

				return (MimeEntity[])attachments.ToArray(typeof(MimeEntity)); 
			}
		}
			
		/// <summary>
		/// Gets message body text. Returns null if no body text specified.
		/// </summary>
		public string BodyText
		{
			get{ 
				MimeEntity[] entities = this.MimeEntities;
				foreach(MimeEntity entity in entities){
					if(entity.ContentType == MediaType_enum.Text_plain){
						return entity.DataText;
					}
				}

				return null;
			}
		}

		/// <summary>
		/// Gets message body html. Returns null if no body html text specified.
		/// </summary>
		public string BodyHtml
		{
			get{
				MimeEntity[] entities = this.MimeEntities;
				foreach(MimeEntity entity in entities){
					if(entity.ContentType == MediaType_enum.Text_html){
						return entity.DataText;
					}
				}

				return null;
			}
		}
		
		#endregion

	}
}
