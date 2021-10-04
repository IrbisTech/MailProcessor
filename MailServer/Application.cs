using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Reflection;
using System.Runtime.Remoting.Messaging;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Serialization;
using cls_TIAExport.ws_ValidateClain;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.collection;
using Microsoft.Office.Interop.Word;
using OpenPop.Mime;
using OpenPop.Mime.Decode;
using OpenPop.Mime.Header;
using OpenPop.Pop3;
using OpenPop.Pop3.Exceptions;
using OpenPop.Common.Logging;
using SelectPdf;
using MailMessage = Microsoft.Office.Interop.Word.MailMessage;
using Message = OpenPop.Mime.Message;
using HiQPdf;
using Document = Microsoft.Office.Interop.Word.Document;
using HtmlToPdf = HiQPdf.HtmlToPdf;
using PdfPageSize = HiQPdf.PdfPageSize;
using PdfMargins = HiQPdf.PdfMargins;
using Rectangle = Microsoft.Office.Interop.Word.Rectangle;
using Renci.SshNet;

namespace MailServer
{

    public static class MsOfficeHelper
    {
        /// <summary>
        /// Detects if a given office document is protected by a password or not.
        /// Supported formats: Word, Excel and PowerPoint (both legacy and OpenXml).
        /// </summary>
        /// <param name="fileName">Path to an office document.</param>
        /// <returns>True if document is protected by a password, false otherwise.</returns>
        public static bool IsPasswordProtected(string fileName)
        {
            using (var stream = File.OpenRead(fileName))
                return IsPasswordProtected(stream);
        }

        /// <summary>
        /// Detects if a given office document is protected by a password or not.
        /// Supported formats: Word, Excel and PowerPoint (both legacy and OpenXml).
        /// </summary>
        /// <param name="stream">Office document stream.</param>
        /// <returns>True if document is protected by a password, false otherwise.</returns>
        public static bool IsPasswordProtected(Stream stream)
        {
            // minimum file size for office file is 4k
            if (stream.Length < 4096)
                return false;

            // read file header
            stream.Seek(0, SeekOrigin.Begin);
            var compObjHeader = new byte[0x20];
            ReadFromStream(stream, compObjHeader);

            // check if we have plain zip file
            if (compObjHeader[0] == 'P' && compObjHeader[1] == 'K')
            {
                // this is a plain OpenXml document (not encrypted)
                return false;
            }

            // check compound object magic bytes
            if (compObjHeader[0] != 0xD0 || compObjHeader[1] != 0xCF)
            {
                // unknown document format
                return false;
            }

            int sectionSizePower = compObjHeader[0x1E];
            if (sectionSizePower < 8 || sectionSizePower > 16)
            {
                // invalid section size
                return false;
            }
            int sectionSize = 2 << (sectionSizePower - 1);

            const int defaultScanLength = 32768;
            long scanLength = Math.Min(defaultScanLength, stream.Length);

            // read header part for scan
            stream.Seek(0, SeekOrigin.Begin);
            var header = new byte[scanLength];
            ReadFromStream(stream, header);

            // check if we detected password protection
            if (ScanForPassword(stream, header, sectionSize))
                return true;

            // if not, try to scan footer as well

            // read footer part for scan
            stream.Seek(-scanLength, SeekOrigin.End);
            var footer = new byte[scanLength];
            ReadFromStream(stream, footer);

            // finally return the result
            return ScanForPassword(stream, footer, sectionSize);
        }

        static void ReadFromStream(Stream stream, byte[] buffer)
        {
            int bytesRead, count = buffer.Length;
            while (count > 0 && (bytesRead = stream.Read(buffer, 0, count)) > 0)
                count -= bytesRead;
            if (count > 0) throw new EndOfStreamException();
        }

        static bool ScanForPassword(Stream stream, byte[] buffer, int sectionSize)
        {
            const string afterNamePadding = "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

            try
            {
                string bufferString = Encoding.ASCII.GetString(buffer, 0, buffer.Length);

                // try to detect password protection used in new OpenXml documents
                // by searching for "EncryptedPackage" or "EncryptedSummary" streams
                const string encryptedPackageName = "E\0n\0c\0r\0y\0p\0t\0e\0d\0P\0a\0c\0k\0a\0g\0e" + afterNamePadding;
                const string encryptedSummaryName = "E\0n\0c\0r\0y\0p\0t\0e\0d\0S\0u\0m\0m\0a\0r\0y" + afterNamePadding;
                if (bufferString.Contains(encryptedPackageName) ||
                    bufferString.Contains(encryptedSummaryName))
                    return true;

                // try to detect password protection for legacy Office documents
                const int coBaseOffset = 0x200;
                const int sectionIdOffset = 0x74;

                // check for Word header
                const string wordDocumentName = "W\0o\0r\0d\0D\0o\0c\0u\0m\0e\0n\0t" + afterNamePadding;
                int headerOffset = bufferString.IndexOf(wordDocumentName, StringComparison.InvariantCulture);
                int sectionId;
                if (headerOffset >= 0)
                {
                    sectionId = BitConverter.ToInt32(buffer, headerOffset + sectionIdOffset);
                    int sectionOffset = coBaseOffset + sectionId * sectionSize;
                    const int fibScanSize = 0x10;
                    if (sectionOffset < 0 || sectionOffset + fibScanSize > stream.Length)
                        return false; // invalid document
                    var fibHeader = new byte[fibScanSize];
                    stream.Seek(sectionOffset, SeekOrigin.Begin);
                    ReadFromStream(stream, fibHeader);
                    short properties = BitConverter.ToInt16(fibHeader, 0x0A);
                    // check for fEncrypted FIB bit
                    const short fEncryptedBit = 0x0100;
                    return (properties & fEncryptedBit) == fEncryptedBit;
                }

                // check for Excel header
                const string workbookName = "W\0o\0r\0k\0b\0o\0o\0k" + afterNamePadding;
                headerOffset = bufferString.IndexOf(workbookName, StringComparison.InvariantCulture);
                if (headerOffset >= 0)
                {
                    sectionId = BitConverter.ToInt32(buffer, headerOffset + sectionIdOffset);
                    int sectionOffset = coBaseOffset + sectionId * sectionSize;
                    const int streamScanSize = 0x100;
                    if (sectionOffset < 0 || sectionOffset + streamScanSize > stream.Length)
                        return false; // invalid document
                    var workbookStream = new byte[streamScanSize];
                    stream.Seek(sectionOffset, SeekOrigin.Begin);
                    ReadFromStream(stream, workbookStream);
                    short record = BitConverter.ToInt16(workbookStream, 0);
                    short recordSize = BitConverter.ToInt16(workbookStream, sizeof(short));
                    const short bofMagic = 0x0809;
                    const short eofMagic = 0x000A;
                    const short filePassMagic = 0x002F;
                    if (record != bofMagic)
                        return false; // invalid BOF
                    // scan for FILEPASS record until the end of the buffer
                    int offset = sizeof(short) * 2 + recordSize;
                    int recordsLeft = 16; // simple infinite loop check just in case
                    do
                    {
                        record = BitConverter.ToInt16(workbookStream, offset);
                        if (record == filePassMagic)
                            return true;
                        recordSize = BitConverter.ToInt16(workbookStream, sizeof(short) + offset);
                        offset += sizeof(short) * 2 + recordSize;
                        recordsLeft--;
                    } while (record != eofMagic && recordsLeft > 0);
                }

                // check for PowerPoint user header
                const string currentUserName = "C\0u\0r\0r\0e\0n\0t\0 \0U\0s\0e\0r" + afterNamePadding;
                headerOffset = bufferString.IndexOf(currentUserName, StringComparison.InvariantCulture);
                if (headerOffset >= 0)
                {
                    sectionId = BitConverter.ToInt32(buffer, headerOffset + sectionIdOffset);
                    int sectionOffset = coBaseOffset + sectionId * sectionSize;
                    const int userAtomScanSize = 0x10;
                    if (sectionOffset < 0 || sectionOffset + userAtomScanSize > stream.Length)
                        return false; // invalid document
                    var userAtom = new byte[userAtomScanSize];
                    stream.Seek(sectionOffset, SeekOrigin.Begin);
                    ReadFromStream(stream, userAtom);
                    const int headerTokenOffset = 0x0C;
                    uint headerToken = BitConverter.ToUInt32(userAtom, headerTokenOffset);
                    // check for headerToken
                    const uint encryptedToken = 0xF3D1C4DF;
                    return headerToken == encryptedToken;
                }
            }
            catch (Exception ex)
            {
                // BitConverter exceptions may be related to document format problems
                // so we just treat them as "password not detected" result
                if (ex is ArgumentException)
                    return false;
                // respect all the rest exceptions
                throw;
            }

            return false;
        }
    }

    public class Application
    {
        private Pop3Client pop3Client;
        private Dictionary<int, Message> messages = new Dictionary<int, Message>();
        private SqlConnection ConnX;

        private EventLog eventLog1;
        private int eventId = 0;
        private string _MailSubject = "";



        private string sendMail(int port, string mailServer, string username, string password, string mailFrom, string mailTo, string mailSubject, string mailBody, string cc, string url)
        {
            try
            {
                //System.Net.Mail.MailMessage mail = new System.Net.Mail.MailMessage(mailFrom, mailTo);
                System.Net.Mail.MailMessage mail = new System.Net.Mail.MailMessage();

                string[] ToMuliId = mailTo.Split(';');
                foreach (string ToEMailId in ToMuliId)
                {
                    mail.To.Add(new MailAddress(ToEMailId)); //adding multiple TO Email Id
                }


                SmtpClient client = new SmtpClient();
                client.Port = port;
                client.DeliveryMethod = SmtpDeliveryMethod.Network;
                client.UseDefaultCredentials = false;
                client.Credentials = new NetworkCredential(username, password);
                client.Host = mailServer;
                mail.Subject = mailSubject;

                if (!string.IsNullOrEmpty(url))
                {
                    mail.IsBodyHtml = true;
                    mail.Body = HttpContent(url);
                }
                else
                {
                    mail.Body = mailBody;
                }



                mail.From = new MailAddress(mailFrom);

                if (!string.IsNullOrEmpty(cc))
                {
                    string[] CCMuliId = mailTo.Split(';');
                    foreach (string ToEMailId in CCMuliId)
                    {
                        mail.CC.Add(new MailAddress(ToEMailId)); //adding multiple TO Email Id
                    }

                }

                client.Send(mail);

                return "";
            }
            catch (Exception e)
            {
                return "Error occurred sending mail. " + e.Message;
            }
        }




        private string HttpContent(string url)
        {
            WebRequest objRequest = System.Net.HttpWebRequest.Create(url);
            StreamReader sr = new StreamReader(objRequest.GetResponse().GetResponseStream());
            string result = sr.ReadToEnd();
            sr.Close();
            return result;
        }



        public string SendFromQueue(string pickUpPath, string serverAddress, int serverPort, bool serverSSL, string username, string password)
        {
            try
            {
                SetStatus("Sending from Queue: " + pickUpPath, "");

                SmtpClient client = new SmtpClient();
                pop3Client = new Pop3Client();
                if (pop3Client.Connected)
                    pop3Client.Disconnect();
                pop3Client.Connect(serverAddress, serverPort, serverSSL);
                pop3Client.Authenticate(username, password);


                client.Port = 25;
                client.Host = serverAddress;
                client.EnableSsl = serverSSL;
                client.Timeout = 100000;
                client.DeliveryMethod = SmtpDeliveryMethod.Network;
                client.UseDefaultCredentials = false;
                client.Credentials = new System.Net.NetworkCredential(username, password);


                string donepath = Path.Combine(pickUpPath, "Done");
                if (!Directory.Exists(donepath))
                    Directory.CreateDirectory(donepath);


                string[] folders = Directory.GetFiles(pickUpPath, "*.eml");

                for (int f = 0; f < folders.Length; f++)
                {
                    string emlFile = new DirectoryInfo(folders[f] + "\\").Name;
                    File.Move(Path.Combine(pickUpPath, emlFile), Path.Combine(donepath, emlFile));
                    SetStatus("Sending: " + emlFile, "");

                    Message message = pop3Client.GetMessageFromFile(Path.Combine(donepath, emlFile));
                    
                    client.Send(message.ToMailMessage());
                }

                return "";
            }
            catch (Exception ex)
            {
                SetStatus("Error in sending: " + ex.Message, "");
                return ex.Message;
            }
        }


        private string KillProcess(string Executable)
        {
            //
            try
            {
                Process p = new Process();
                ProcessStartInfo s = new ProcessStartInfo("taskkill.exe", " /F /IM " + Executable);
                s.RedirectStandardOutput = true;
                s.RedirectStandardError = true;
                s.CreateNoWindow = true;
                s.UseShellExecute = false;
                s.WorkingDirectory = "C:\\";
                p.StartInfo = s;
                p.Start();
                p.WaitForExit();
                GC.Collect();
                return "";
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }



        public string ImportMail(string serverName, int portNumber, bool useSSL, string userName, string userPassword, bool deleteRead, string targetSavePath, string SqlConnection, string ConvertionParams, ref int messageCount, ref string retWarning)
        {
            KillProcess("winword.exe");

            SetStatus("Import mail event: " + serverName + " [" + userName + "]", SqlConnection);

            if (!ConvertionParams.Contains("norobotics"))
            {
                SetStatus("Processing robotics queue: " + serverName, SqlConnection);
                ProcessRobotics(SqlConnection);
            }
                

            if (!ConvertionParams.Contains("noforward"))
            {
                SetStatus("Processing forward queue: " + serverName, SqlConnection);
                ProcessForward(SqlConnection, targetSavePath);
            }


            try
            {
                if (!Directory.Exists(targetSavePath))
                    return "Invalid save target path;";

                //Logger
                if (!string.IsNullOrEmpty(SqlConnection))
                {
                    string ErrorMessage = "";
                    var ConnX = OpenSQLDatabase(SqlConnection, ref ErrorMessage);
                    if (!string.IsNullOrEmpty(ErrorMessage))
                        return ErrorMessage;
                }

                pop3Client = new Pop3Client();
                int mCount = 0;
                string Warning = "";
                string SuccessID = "";

                try
                {
                    Warning = "";
                    string Param = getParameter(ConvertionParams, "MailTempFolder");
                    if (String.IsNullOrEmpty(Param))
                    {
                        Param = @"c:\Temps";
                        if (!Directory.Exists(Param))
                        {
                            Directory.CreateDirectory(Param);
                        }
                    }

                    string tempMail = Param;

                    if (pop3Client.Connected)
                        pop3Client.Disconnect();
                    pop3Client.Connect(serverName, portNumber, useSSL);
                    pop3Client.Authenticate(userName, userPassword);
                    int count = pop3Client.GetMessageCount();
                    messageCount = count;


                    SetStatus("Available: " + messageCount + " messages", SqlConnection);
                    int success = 0;
                    int fail = 0;
                    int mCnt = 1;


                    for (int i = count; i >= 1; i -= 1)
                    {

                        Message message = pop3Client.GetMessage(i);
                        messages.Add(i, message);

                        String messageID = message.Headers.MessageId;

                        if (messageID == null)
                        {
                            messageID = Guid.NewGuid().ToString();
                        }
                        
                            if (messageID.Length > 100)
                                messageID = messageID.Substring(0, 99);


                            messageID = messageID.Trim(Path.GetInvalidFileNameChars());
                            messageID = messageID.Trim(Path.GetInvalidPathChars());
                            messageID = messageID.Replace(@"\", "");
                            messageID = messageID.Replace("/", "");

                            ///\:*?"<>|
                            SetStatus("Saving mail item [" + mCnt + "]: " + messageID + " - " + message.Headers.From, SqlConnection);

                            if (!File.Exists(Path.Combine(tempMail, messageID + ".msg")))
                            {
                                message.Save(File.Create(Path.Combine(tempMail, messageID + ".msg")));
                                pop3Client.DeleteMessage(i);
                            }
                            else
                            {
                                SetStatus("File exists: " + messageID + ".msg", SqlConnection);
                            }

                            mCnt++;
                            if (mCnt == 150)
                                break;
                        
                    }

                    pop3Client.Disconnect();

                    if (fail > 0)
                    {
                        Warning = "Since some of the emails were not parsed correctly (exceptions were thrown)\r\n" +
                                  "please consider sending your log file to the developer for fixing.\r\n" +
                                  "If you are able to include any extra information, please do so.";
                    }

                    try
                    {

                        DirectoryInfo d = new DirectoryInfo(tempMail);//Assuming Test is your Folder
                        FileInfo[] Files = d.GetFiles("*.msg"); //Getting Text files
                        string str = "";
                        foreach (FileInfo file in Files)
                        {
                            Message message = pop3Client.GetMessageFromFile(Path.Combine(tempMail, file.Name));

                            string GUID = Guid.NewGuid().ToString();
                            string Target = Path.Combine(targetSavePath, GUID);
                            Directory.CreateDirectory(Target);

                            string mailSubject = "";
                            string mailFrom = "";
                            string mailFromDisplayName = "";
                            string mailTo = "";
                            string mailReceived = "";
                            int mailAttachment = 0;
                            string Location = "";

                            SetStatus("Start saving: " + file.Name.ToString(), SqlConnection);
                            SetStatus("About to save message", SqlConnection);
                            string saveRes = SaveMessage(message, Target, GUID, SqlConnection, 0, ConvertionParams, deleteRead, userName, ref mailSubject, ref mailFrom, ref mailFromDisplayName, ref mailTo, ref mailReceived, ref mailAttachment);
                            SetStatus("SaveMessage response: " + saveRes, SqlConnection);

                            if (string.IsNullOrEmpty(saveRes))
                            {
                                
                                message = null;
                                file.Delete();
                            }
                        }

                        success++;
                    }
                    catch (Exception e)
                    {
                        SetStatus("Error: " + e.Message, SqlConnection);
                        //return e.Message
                        DefaultLogger.Log.LogError(
                            "TestForm: Message fetching failed: " + e.Message + "\r\n" +
                            "Stack trace:\r\n" +
                            e.StackTrace);
                        fail++;
                    }
                }

                catch (InvalidLoginException)
                {
                    return "The server did not accept the user credentials!";
                }
                catch (PopServerNotFoundException)
                {
                    return "The server could not be found";
                }
                catch (PopServerLockedException)
                {
                    return "The mailbox is locked. It might be in use or under maintenance. Are you connected elsewhere?";
                }
                catch (LoginDelayException)
                {
                    return "Login not allowed. Server enforces delay between logins. Have you connected recently?";
                }
                catch (Exception ex)
                {
                    SetStatus("Error: " + ex.Message, SqlConnection);
                    return "Error occurred retrieving mail. " + ex.Message;
                }
                finally
                {

                    // Enable the buttons again
                }


                return "";
            }
            catch (Exception ex)
            {
                SetStatus("Error: " + ex.Message, SqlConnection);
                return ex.Message;
            }
        }



        private SqlConnection OpenSQLDatabase(string ConnectionString, ref string ErrorMessage)
        {
            try
            {
                if (ConnX == null)
                {
                    ConnX = new SqlConnection();
                    ConnX.ConnectionString = ConnectionString;
                    ConnX.Open();
                }
                if (ConnX.State == 0)
                    ConnX.Open();

                return ConnX;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                return null;
            }
        }


        public void ExecuteSQL(string SqlString, string SqlConnection, ref string eMessage)
        {
            try
            {
                string ErrorMessage = "";
                var ConnX = OpenSQLDatabase(SqlConnection, ref ErrorMessage);
                var CmdX = new SqlCommand { CommandText = SqlString, Connection = ConnX };
                if (CmdX.Connection == null)
                    CmdX.Connection.Open();
                CmdX.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                eMessage = ex.Message;
            }
        }


        public DataSet ExecuteSQLSelect(string SqlString, string SqlConnection, ref string eMessage)
        {
            var data = new DataSet();

            try
            {
                string ErrorMessage = "";
                var ConnX = OpenSQLDatabase(SqlConnection, ref ErrorMessage);
                var adapterX = new SqlDataAdapter(SqlString, ConnX);
                adapterX.Fill(data);
                return data;
            }
            catch (Exception ex)
            {
                if (
                    ex.Message.Contains(
                        "A network-related or instance-specific error occurred while establishing a connection to SQL Server"))
                    eMessage = ex.Message;
                return null;
            }
        }



        private void processBodyX(Message message, string messagePath, string SqlConnection, string ConvertionParams, string targetPath, string identifier, ref string BodyPart, string messageType)
        {
            try
            {
                string e = "";
                string bodyHTMLText = "";
                string bodyPlainText = "";

                if (string.IsNullOrEmpty(_MailSubject))
                {
                    _MailSubject = message.Headers.Subject.ToString();
                }

                //BodyPart = FileTransform(messagePath, ConvertionParams, SqlConnection, 1, ref e);

                if (String.IsNullOrEmpty(BodyPart))
                {
                    string currentFile = "";
                    if (messageType == "main")
                        currentFile  = Path.Combine(targetPath, Guid.NewGuid() + "_original.msg");
                    else
                        currentFile = Path.Combine(targetPath, Guid.NewGuid() + "_attached.msg");


                    MessagePart htmlTextPart = message.FindFirstHtmlVersion();
                    if (htmlTextPart != null)
                    {
                        MessagePart html = message.FindFirstHtmlVersion();
                        bodyHTMLText = html.GetBodyAsText();
                    }

                    MessagePart plainTextPart = message.FindFirstPlainTextVersion();
                    if (plainTextPart != null)
                    {
                        bodyPlainText = plainTextPart.GetBodyAsText();
                    }

                 
                    if (htmlTextPart == null)
                        File.WriteAllText(currentFile, bodyPlainText);
                    else
                    {
                        if (ConvertionParams.Contains("RemoveExternalLink"))
                        {
                            bodyHTMLText = bodyHTMLText.Replace("src=\"http://outlook.", "\"");
                            bodyHTMLText = bodyHTMLText.Replace("src=\"http://www.", "\"");
                            bodyHTMLText = bodyHTMLText.Replace("src=3D\"http://", "\"");
                            bodyHTMLText = bodyHTMLText.Replace("https://", "\"");
                            
                        }
                        File.WriteAllText(currentFile, bodyHTMLText);
                    }

                    BodyPart = FileTransform(currentFile, ConvertionParams, SqlConnection, 1, ref e);
                    if (BodyPart.EndsWith(".msg"))
                    {
                        File.WriteAllText(currentFile, bodyPlainText);
                        BodyPart = FileTransform(currentFile, ConvertionParams, SqlConnection, 1, ref e);
                    }
                }

                if (!String.IsNullOrEmpty(BodyPart))
                {
                    string aFileName = "";
                    string fName = "";
                    long fLength = 0;

                    FileInfo newFile = new FileInfo(BodyPart);

                    if (messageType == "main")
                        aFileName = "Mail body.pdf";
                    else
                        aFileName = "Attachment body.pdf";

                    fName = Path.GetFileName(BodyPart);

                    fLength = newFile.Length;


                    string SqlString = "Insert into SYS_MailBoxAttachments(attachmentname, attachmentsavedname, attachmentsize, identifier, dateprocessed, deleted, attachmentpreview) values ('"
                                       + aFileName + "', '" + fName + "', " + fLength + ", '" + identifier +
                                       "', getdate(), 0, '')";
                    string errMessage = "";
                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                }
            }
            catch (Exception ex)
            {

            }
        }

        private void processBody(Message message, string SqlConnection, string ConvertionParams, string targetPath, string identifier, ref string BodyPart, string messageType)
        {
            string bodyType = "";
            string bodyPlainText = "";
            string currentFile = "";
            string htmlFile = "";
            string bodyHTMLText = "";

            string fGuid = Guid.NewGuid().ToString();
            string bodyPDF = "";
            string plainBody = currentFile;
            bodyPDF = Path.Combine(targetPath, fGuid + "_body.pdf");


            // ******* PROCESSING BODY
            MessagePart htmlTextPart = message.FindFirstHtmlVersion();
            if (htmlTextPart != null)
            {
                MessagePart html = message.FindFirstHtmlVersion();
                bodyHTMLText = html.GetBodyAsText();
                bodyType = "msg";
                html = null;
                currentFile = Path.Combine(targetPath, fGuid + "_body." + bodyType);
                htmlFile = currentFile;
                File.WriteAllText(currentFile, bodyHTMLText);
            }
            else
            {
                bodyType = "text";
                MessagePart plainTextPart = message.FindFirstPlainTextVersion();

                if (plainTextPart != null)
                {
                    // The message had a text/plain version - show that one
                    bodyPlainText = plainTextPart.GetBodyAsText();
                }
                else
                {
                    // Try to find a body to show in some of the other text versions
                    List<MessagePart> textVersions = message.FindAllTextVersions();
                    if (textVersions.Count >= 1)
                        bodyPlainText = textVersions[0].GetBodyAsText();
                    else
                        bodyPlainText =
                            "<<MailServer>> Cannot find a text version body in this message to show <<MailServer>>";

                currentFile = Path.Combine(targetPath, fGuid + "_body." + bodyType);
                File.WriteAllText(currentFile, bodyPlainText);
                }
            }
            

            SetStatus("Saving mail item:" + currentFile + ConvertionParams, SqlConnection);
            SetStatus("Saving mail item:" + ConvertionParams, SqlConnection);

            string Trans = "";
            string res = "";

            string tranResult = "";
            if (ConvertionParams.Contains("body=pdf"))
            {
                if (!string.IsNullOrEmpty(SqlConnection))
                {

                    res = "";
                    if (htmlTextPart == null)
                    {
                        File.Copy(plainBody, plainBody + ".doc");
                        res = FileTransform(plainBody + ".doc", ConvertionParams, SqlConnection, 1, ref tranResult);
                        BodyPart = res;
                    }
                    else
                    {
                        if (bodyType == "html")
                        {
                            bodyPDF = Path.Combine(Path.GetDirectoryName(currentFile), Path.GetFileNameWithoutExtension(currentFile) + ".pdf");
                            res = FileTransformHTML(htmlFile, ConvertionParams, SqlConnection, ref tranResult);
                            BodyPart = res;
                        }
                        else
                        {
                            File.Copy(currentFile, currentFile + ".html");
                            //res = FileTransform(currentFile + ".html", ConvertionParams, SqlConnection, 1, ref tranResult);
                            res = FileTransformHTML(currentFile + ".html", ConvertionParams, SqlConnection, ref tranResult);
                            BodyPart = res;
                        }
                    }
                }
            }
            else if (ConvertionParams.Contains("body=doc"))
            {
                res = FileTransform(currentFile, ConvertionParams, SqlConnection, 1, ref tranResult);
                BodyPart = res;
            }
            //*** END OF BODY PROCESSING
            
            
            SetStatus("res: " + res + "; tranResult:" + tranResult, SqlConnection);
            if (res != "")
            {
                bodyPDF = res;
                string aFileName = "";
                string fName = "";
                long fLength = 0;

                FileInfo newFile = new FileInfo(bodyPDF);

                if (messageType == "main")
                    aFileName = "Mail body";
                else
                    aFileName = "Mail attached body";

                fName = Path.GetFileName(bodyPDF);

                fLength = newFile.Length;


                string SqlString = "Insert into SYS_MailBoxAttachments(attachmentname, attachmentsavedname, attachmentsize, identifier, dateprocessed, deleted, attachmentpreview) values ('"
                                   + aFileName + "', '" + fName + "', " + fLength + ", '" + identifier +
                                   "', getdate(), 0, '')";
                string errMessage = "";
                ExecuteSQL(SqlString, SqlConnection, ref errMessage);
            }
        }


        private void processAttachments(Message message, string SqlConnection, string ConvertionParams, string targetPath, string identifier, string messageType)
        {
            try
            {
                string Trans = "";
                string currentFile = "";
                string tranResult = "";
                SetStatus("Processing attachment", SqlConnection);
                List<MessagePart> attachments = message.FindAllAttachments();
                foreach (MessagePart attachment in attachments)
                {
                    System.Net.Mime.ContentDisposition cDisp = attachment.ContentDisposition;
                    string[] Mime = attachment.FileName.Split('.');

                    if (Mime[0] == "(no name)" && attachment.ContentType.MediaType == "message/rfc822")
                        Mime[0] = "msg";

                    else if (Mime[0] == "(no name)" && attachment.ContentType.MediaType == "message/delivery-status")
                        Mime[0] = "txt";

                    currentFile = Path.Combine(targetPath, Guid.NewGuid() + "_attachment." + Mime[Mime.Length - 1]);
                    if (ConvertionParams.Contains("txt=csv") && currentFile.ToLower().EndsWith(".txt"))
                    {
                        currentFile = currentFile.ToLower().Replace(".txt", ".csv");
                    }

                    FileInfo file = new FileInfo(currentFile);
                    attachment.Save(file);

                    if (ConvertionParams.Contains("sabrix"))
                    {
                        string[] allvals = message.Headers.Subject.ToString().Split('+');
                        

                        string invnum = "";
                        string driver = "";
                        string fClass = "";

                        if (attachment.FileName.StartsWith("INV"))
                        {
                            fClass = "CLS_Invoices";
                            if (allvals.Length == 2)
                            {
                                invnum = allvals[0];
                                driver = allvals[1];
                            }
                            else
                            {
                                invnum = allvals[0];
                                driver = "";
                            }
                        }
                        else
                        {
                            fClass = "CLS_Purchase_Orders";
                            invnum = attachment.FileName.Substring(0, attachment.FileName.IndexOf("("));
                            driver = "";
                        }

                        string inFile = currentFile;
                        string outFile = "";
                        string ponumber = "";

                        if (fClass.Equals("CLS_Invoices"))
                        {
                            outFile = Path.GetTempFileName() + ".pdf";

                            string inputFile = inFile;
                            PdfReader reader = new PdfReader(inputFile);
                            string txt = iTextSharp.text.pdf.parser.PdfTextExtractor.GetTextFromPage(reader, 1, new iTextSharp.text.pdf.parser.LocationTextExtractionStrategy());
                            string[] filelines = txt.Split('\n');
                            
                            for (int fl = 1; fl < filelines.Length; fl++)
                            {
                                if (filelines[fl].Equals("Customer Order No"))
                                    ponumber = filelines[fl - 1];
                            }
                            

                            FileStream fs = new FileStream(outFile, FileMode.Create, FileAccess.Write);
                            PdfStamper stamper = new PdfStamper(reader, fs);

                            PdfContentByte cb = stamper.GetOverContent(1);

                            string fileName = "C:\\DoNOTDelete\\Blank.jpg";

                            Image image = Image.GetInstance(fileName);
                            image.SetAbsolutePosition(260, 293);

                            cb.AddImage(image);
                            //reader.Close();

                            stamper.Close();
                            fs.Close();
                        }
                        else
                        {
                            outFile = inFile;
                        }

                    }

                    if (attachment.ContentType.MediaType == "message/rfc822")
                    {
                        Message messageX = pop3Client.GetMessageFromFile(file.ToString());
                        string Blank = "";
                        processBodyX(messageX, currentFile, SqlConnection, ConvertionParams, targetPath, identifier, ref Blank, "attachment");

                        processAttachments(messageX, SqlConnection, ConvertionParams, targetPath, identifier, "attachment");
                    }


                    bool Inline = false;
                    if (cDisp != null)
                    {
                        Inline = cDisp.Inline;
                    }
                    else
                    {
                        Inline = true;

                    }

                    int FileSize = 0;
                    if (ConvertionParams.Contains("MinFileSize"))
                    {
                        string p = getParameter(ConvertionParams, "MinFileSize");
                        if (!string.IsNullOrEmpty(p))
                            FileSize = int.Parse(p);
                    }
                    else
                    {
                        FileSize = 0;
                    }



                    if ((ConvertionParams.Contains("inline-attachments=on")))
                    {
                        SetStatus("Saving attachment:" + file.Name, SqlConnection);

                        // Convert if needed, keep both, but reference converted only
                        //*************************************************************

                        SetStatus("Conversion:" + ConvertionParams, SqlConnection);

                        if (currentFile.ToLower().EndsWith("zip") && !string.IsNullOrEmpty(ConvertionParams))
                        {
                            if (ConvertionParams.Contains("zip=unzip"))
                            {
                                string ZipDest = Path.Combine(targetPath, Guid.NewGuid().ToString());
                                if (!Directory.Exists(ZipDest))
                                    Directory.CreateDirectory(ZipDest);

                                SetStatus("Unzipping:" + ZipDest, SqlConnection);

                                var ZIP = new ChilkatZip2Lib.ChilkatZip2();
                                ZIP.UnlockComponent("");
                                ZIP.OpenZip(currentFile);
                                ZIP.UnzipMatching(ZipDest, "*.*", 0);

                                string[] FileArray = Directory.GetFiles(ZipDest, "*.*");
                                for (int z = 0; z < FileArray.Length; z++)
                                {
                                    
                                    Trans = FileTransform(FileArray[z], ConvertionParams, SqlConnection, 0, ref tranResult);
                                    SetStatus("Conversion error:" + tranResult, SqlConnection);

                                    string aFileName = "";
                                    string fName = "";
                                    long fLength = 0;

                                    if (String.IsNullOrEmpty(Trans))
                                    {
                                        File.Copy(FileArray[z], Path.Combine(targetPath, Path.GetFileName(FileArray[z])));
                                        FileInfo newFile =
                                            new FileInfo(Path.Combine(targetPath, FileArray[z]));
                                        aFileName = Path.GetFileName(FileArray[z]);
                                        fName = newFile.Name;
                                        fLength = newFile.Length;

                                        string SqlStringX = "Insert into SYS_MailBoxAttachments(attachmentname, attachmentsavedname, attachmentsize, identifier, dateprocessed, deleted, attachmentpreview) values ('"
                                            + aFileName.Replace("'", "") + "', '" +
                                            fName.Replace("'", "") + "', " + fLength + ", '" + identifier +
                                            "', getdate(), 0, '')";
                                        string errMessageX = "";
                                        ExecuteSQL(SqlStringX, SqlConnection, ref errMessageX);
                                        SetStatus("SqlString: " + SqlStringX, SqlConnection);
                                    }
                                    else
                                    {
                                        string SqlString = "";

                                        if ((FileArray[z].ToLower().EndsWith(".jpg") || FileArray[z].ToLower().EndsWith(".jpeg") || FileArray[z].ToLower().EndsWith(".png") || FileArray[z].ToLower().EndsWith(".gif")))
                                        {
                                            File.Copy(Trans, Path.Combine(targetPath, Path.GetFileName(Trans)));
                                            File.Copy(FileArray[z], Path.Combine(targetPath, Path.GetFileName(FileArray[z])));
                                            FileInfo newFile = new FileInfo(Trans);
                                            aFileName = newFile.Name;
                                            fName = Path.GetFileName(Trans);
                                            fLength = newFile.Length;

                                            string attachmentpreview = Path.GetFileName(FileArray[z]);
                                            SqlString = "Insert into SYS_MailBoxAttachments(attachmentname, attachmentsavedname, attachmentsize, identifier, dateprocessed, deleted, attachmentpreview) values ('"
                                                + aFileName.Replace("'", "") + "', '" +
                                                fName.Replace("'", "") + "', " + fLength + ", '" + identifier +
                                                "', getdate(), 0, '" + attachmentpreview + "')";
                                        }
                                        else
                                        {
                                            File.Copy(FileArray[z], Path.Combine(targetPath, Path.GetFileName(FileArray[z])));
                                            FileInfo newFile = new FileInfo(FileArray[z]);
                                            aFileName = newFile.Name;
                                            fName = Path.GetFileName(FileArray[z]);
                                            fLength = newFile.Length;

                                            SqlString = "Insert into SYS_MailBoxAttachments(attachmentname, attachmentsavedname, attachmentsize, identifier, dateprocessed, deleted, attachmentpreview) values ('"
                                                                + aFileName.Replace("'", "") + "', '" +
                                                                fName.Replace("'", "") + "', " + fLength + ", '" + identifier +
                                                                "', getdate(), 0, '')";
                                        }
                                        string errMessage = "";
                                        ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                        SetStatus("SqlString: " + SqlString, SqlConnection);
                                    }
                                }
                            }

                                // Leave as ZIP file
                            else
                            {
                                if (!string.IsNullOrEmpty(SqlConnection))
                                {
                                    string aFileName = "";
                                    string fName = "";
                                    long fLength = 0;

                                    FileInfo newFile = new FileInfo(currentFile);
                                    aFileName = newFile.Name;
                                    fName = Path.GetFileName(currentFile);

                                    fLength = newFile.Length;
                                    string SqlString = "";

                                    SqlString = "Insert into SYS_MailBoxAttachments(attachmentname, attachmentsavedname, attachmentsize, identifier, dateprocessed, deleted, attachmentpreview) values ('"
                                                        + aFileName.Replace("'", "") + "', '" + fName.Replace("'", "") +
                                                        "', " + fLength + ", '" + identifier +
                                                        "', getdate(), 0, '')";
                                    
                                    string errMessage = "";
                                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                    SetStatus("SqlString: " + SqlString, SqlConnection);
                                }
                            }
                        }
                        else
                        {
                            if (attachment.ContentType.MediaType != "message/rfc822")
                            {
                                Trans = FileTransform(currentFile, ConvertionParams, SqlConnection, 0, ref tranResult);
                                SetStatus("Conversion error:" + tranResult, SqlConnection);

                                string Ext = "";
                                if (String.IsNullOrEmpty(Trans))
                                {
                                    Trans = currentFile;
                                    Ext = Path.GetExtension(Trans);
                                }
                                else
                                {
                                    Ext = ".pdf";
                                }

                                string aFileName = "";
                                string fName = "";
                                long fLength = 0;

                                SetStatus("File Transformation result:" + Trans, SqlConnection);

                                if (!String.IsNullOrEmpty(Trans))
                                {
                                    FileInfo newFile = new FileInfo(Trans);
                                    aFileName = Path.GetFileNameWithoutExtension(attachment.FileName) + Ext;
                                    fName = Path.GetFileName(Trans);
                                    fLength = newFile.Length;
                                }
                                else
                                {
                                    if (ConvertionParams.Contains("txt=csv") &&
                                        attachment.FileName.ToLower().EndsWith(".txt"))
                                    {
                                        aFileName = attachment.FileName.ToLower().Replace(".txt", ".csv");
                                    }
                                    else
                                    {
                                        aFileName = attachment.FileName; //Actual filename
                                    }

                                    fName = file.Name; //Saved name with GUID
                                    fLength = file.Length;
                                }

                                if (!string.IsNullOrEmpty(SqlConnection))
                                {
                                    if (aFileName == "(no name)")
                                    {
                                        aFileName = fName;
                                    }

                                    if (Mime[0] == "msg")
                                    {
                                        aFileName = "Message attachment.txt";
                                    }

                                    string attachmentpreview = "";
                                    if (currentFile.ToLower().EndsWith("png") || currentFile.ToLower().EndsWith("jpg") ||
                                        currentFile.ToLower().EndsWith("jpeg") || currentFile.ToLower().EndsWith("tif") ||
                                        currentFile.ToLower().EndsWith("tiff") && !string.IsNullOrEmpty(Trans))
                                    {
                                        attachmentpreview = Path.GetFileName(currentFile);
                                    }
                                    string SqlString = "";
                                    if (file.Length < FileSize && (file.Name.ToLower().EndsWith(".jpg") || file.Name.ToLower().EndsWith(".jpeg") || file.Name.ToLower().EndsWith(".png") || file.Name.ToLower().EndsWith(".gif")))
                                    {
                                        SqlString = "Insert into SYS_MailBoxAttachments(attachmentname, attachmentsavedname, attachmentsize, identifier, dateprocessed, deleted, attachmentpreview) values ('"
                                            + aFileName.Replace("'", "") + "', '" + fName.Replace("'", "") +
                                            "', " + fLength + ", '" + identifier +
                                            "', getdate(), 1, '" + attachmentpreview + "')";
                                    }
                                    else
                                    {
                                        SqlString = "Insert into SYS_MailBoxAttachments(attachmentname, attachmentsavedname, attachmentsize, identifier, dateprocessed, deleted, attachmentpreview) values ('"
                                            + aFileName.Replace("'", "") + "', '" + fName.Replace("'", "") +
                                            "', " + fLength + ", '" + identifier +
                                            "', getdate(), 0, '" + attachmentpreview + "')";
                                    }


                                    string errMessage = "";
                                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                    SetStatus("SqlString: " + SqlString, SqlConnection);
                                }
                            }
                            else
                            {
                                SetStatus("Attachment ignored as being attached MSG:" + Trans, SqlConnection);
                            }
                        }
                        //*************************************************************


                        file = null;
                    }
                }
            }
            catch (Exception ex)
            {
                SetStatus("Error saving attachment: " + ex.Message, SqlConnection);
            }
        }


        private string ProcessForward(string SqlConnection, string targetPath)
        {
            string smtp_ServerName = "";
            try
            {
                string errMessage = "";
                DataSet SQLDataX = null;
                // Get all mail from the robot
                string SqlString = "Select * from sys_Mailbox where Status in (21, 22, 23) order by id";
                SQLDataX = ExecuteSQLSelect(SqlString, SqlConnection, ref errMessage);

                smtp_ServerName = getProfileSetting("SMTP_Server", "smtp_ServerName", SqlConnection);
                string smtp_ServerPort = getProfileSetting("SMTP_Server", "smtp_ServerPort", SqlConnection);
                string smtp_ServerSSL = getProfileSetting("SMTP_Server", "smtp_ServerSSL", SqlConnection);
                string smtp_UserName = getProfileSetting("SMTP_Server", "smtp_UserName", SqlConnection);
                string smtp_Password = getProfileSetting("SMTP_Server", "smtp_Password", SqlConnection);
                string smtp_Rule21_recipient = getProfileSetting("SMTP_Server", "smtp_Rule21_recipient", SqlConnection);
                string smtp_Rule22_recipient = getProfileSetting("SMTP_Server", "smtp_Rule22_recipient", SqlConnection);
                string smtp_Rule23_recipient = getProfileSetting("SMTP_Server", "smtp_Rule23_recipient", SqlConnection);


                if (string.IsNullOrEmpty(smtp_ServerName))
                {
                    SetStatus("Unable to get smtp_ServerName setting from forward mail processing. Processing aborted", SqlConnection);
                    return "";
                }


                for (int dbRows = 0; dbRows < SQLDataX.Tables[0].Rows.Count; dbRows++)
                {
                    string identifier = SQLDataX.Tables[0].Rows[dbRows]["identifier"].ToString();
                    string Status = SQLDataX.Tables[0].Rows[dbRows]["Status"].ToString();

                    SmtpClient client = new SmtpClient();
                    pop3Client = new Pop3Client();
                    if (pop3Client.Connected)
                        pop3Client.Disconnect();


                    SetStatus("SMTP_Connect [server: " + smtp_ServerName  + "] [port: " + smtp_ServerPort + "] [ssl: " + smtp_ServerSSL  + "]", SqlConnection);


                    if (smtp_ServerName.Contains("hollard"))
                    {
                        pop3Client.Connect(smtp_ServerName, int.Parse(smtp_ServerPort), bool.Parse(smtp_ServerSSL), 0, 0, null, smtp_UserName, smtp_Password);
                        SetStatus("SMTP_Authenticate", SqlConnection);
                    }
                    else
                    {
                        pop3Client.Connect(smtp_ServerName, int.Parse(smtp_ServerPort), bool.Parse(smtp_ServerSSL));
                        SetStatus("SMTP_Authenticate", SqlConnection);
                        pop3Client.Authenticate(smtp_UserName, smtp_Password);
                    }


                    client.Port = 25;
                    client.Host = smtp_ServerName;
                    client.EnableSsl = Convert.ToBoolean( smtp_ServerSSL);
                    client.Timeout = 100000;
                    client.DeliveryMethod = SmtpDeliveryMethod.Network;
                    client.UseDefaultCredentials = false;
                    client.Credentials = new System.Net.NetworkCredential(smtp_UserName, smtp_Password);

                    SetStatus("All OK", SqlConnection);

                    string[] f = Directory.GetFiles(targetPath, identifier + @"\*_original.msg");

                    long large = 0;
                    int subsc = 0;
                    for (int flen = 0; flen < f.Length; flen++)
                    {
                        FileInfo fi = new FileInfo(f[flen]);
                        if (fi.Length > large)
                        {
                            large = fi.Length;
                            subsc = flen;
                        }
                            
                    }

                    if (f.Length == 0)
                    {
                        SqlString = "Update SYS_MailBox Set Status = 99, mailfinalised = getdate(), actiondate = getdate() Where identifier = '" + identifier + "'";
                        ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                        SetStatus("Unable to locate original message", SqlConnection);
                        return "";
                    }
                    else
                    {
                        //targetPath, identifier + @"\df29c0a0-b79e-44ad-b86a-daea34725524_original.msg")
                        Message ms = pop3Client.GetMessageFromFile(f[subsc]);
                        System.Net.Mail.MailMessage m = ms.ToMailMessage();

                        MailAddress to = null;
                        if (Status == "21")
                        {
                            to = new MailAddress(smtp_Rule21_recipient);
                        }
                        else if (Status == "22")
                        {
                            to = new MailAddress(smtp_Rule22_recipient);
                        }
                        else if (Status == "23")
                        {
                            to = new MailAddress(smtp_Rule23_recipient);
                        }

                        SetStatus("Subject: " + ms.Headers.Subject, SqlConnection);
                        SetStatus("Filename: " + f[subsc], SqlConnection); 

                        SetStatus("Processing TO/CC/BCC", SqlConnection);
                        for (int i = m.CC.Count; i > 0; i--)
                            m.To.Remove(m.CC[i - 1]);

                        for (int i = m.Bcc.Count; i > 0; i--)
                            m.To.Remove(m.Bcc[i - 1]);

                        for (int i = m.To.Count; i > 0; i--)
                            m.To.Remove(m.To[i - 1]);


                        SetStatus("To: " + to.Address, SqlConnection);
                        m.To.Add(to);

                        SetStatus("From: " + smtp_UserName, SqlConnection);
                        m.From = new MailAddress(smtp_UserName);

                        SetStatus("Sender: " + smtp_UserName, SqlConnection);
                        m.Sender = new MailAddress(smtp_UserName);

                        SetStatus("Sending", SqlConnection);
                        client.Send(m);

                        SqlString = "Update SYS_MailBox Set Status = 5, mailfinalised = getdate(), actiondate = getdate() Where identifier = '" + identifier + "'";
                        ExecuteSQL(SqlString, SqlConnection, ref errMessage);

                        SetStatus("Status set = 5", SqlConnection);
                    }
                }
            }
            catch (Exception ex)
            {
                SetStatus("Error processing forward emails: " + ex.Message  + "[" + smtp_ServerName  + "]", SqlConnection);
            }

            return "";
        }


        private string ProcessRobotics(string SqlConnection)
        {
            string errMessage = "";
            DataSet SQLDataX = null;
            // Get all mail from the robot
            string SqlString = "Select * from sys_Mailbox where Status = 12 order by id";
            SQLDataX = ExecuteSQLSelect(SqlString, SqlConnection, ref errMessage);
            bool autoIndexed = false;
            string document_class = "DCI_HPL_TIA";
            cls_TIAExport.Application TIA = new cls_TIAExport.Application();
            string RuleName = "Robot Indexing";

            string sys_TIAProxyUser = getProfileSetting(RuleName, "sys_TIAProxyUser", SqlConnection);
            string sys_TIAProxyPass = getProfileSetting(RuleName, "sys_TIAProxyPass", SqlConnection);
            string sys_TIAStoreFile = getProfileSetting(RuleName, "sys_TIAStoreFile", SqlConnection);

            if (string.IsNullOrEmpty(sys_TIAStoreFile))
            {
                SetStatus("Unable to get sys_TIAStoreFile setting from Robot Indexing rule. Processing aborted", SqlConnection);
                return "";
            }
            

            for (int dbRows = 0; dbRows < SQLDataX.Tables[0].Rows.Count; dbRows++)
            {
                string identifier = SQLDataX.Tables[0].Rows[dbRows]["identifier"].ToString();
                string custom1 = SQLDataX.Tables[0].Rows[dbRows]["custom1"].ToString();
                string document_group = SQLDataX.Tables[0].Rows[dbRows]["custom2"].ToString();
                string policy_number = SQLDataX.Tables[0].Rows[dbRows]["custom3"].ToString();
                string claim_number = SQLDataX.Tables[0].Rows[dbRows]["custom4"].ToString();
                string id_number = SQLDataX.Tables[0].Rows[dbRows]["custom5"].ToString();
                string initials = SQLDataX.Tables[0].Rows[dbRows]["custom6"].ToString();
                string surname = SQLDataX.Tables[0].Rows[dbRows]["custom7"].ToString();
                string email_address = SQLDataX.Tables[0].Rows[dbRows]["custom8"].ToString();
                string subject = SQLDataX.Tables[0].Rows[dbRows]["mailsubject"].ToString();
                if(subject.Length > 200)
                    subject = subject.Substring(0, 199);
                string itemID = SQLDataX.Tables[0].Rows[dbRows]["id"].ToString();
                string mailOwner = SQLDataX.Tables[0].Rows[dbRows]["mailOwner"].ToString();

                // Get all attachments per mail
                DataSet SQLDataY = null;
                SqlString = "Select * from sys_MailboxAttachments where identifier = '" + SQLDataX.Tables[0].Rows[dbRows]["identifier"].ToString()  + "' and deleted = 0 order by id";
                SQLDataY = ExecuteSQLSelect(SqlString, SqlConnection, ref errMessage);

                for (int dbRowsY = 0; dbRowsY < SQLDataY.Tables[0].Rows.Count; dbRowsY++)
                {

                    string doc_type = SQLDataY.Tables[0].Rows[dbRowsY]["acustom1"].ToString();
                    string doc_subclass = SQLDataY.Tables[0].Rows[dbRowsY]["acustom2"].ToString();
                    
                    //Index into SBimmage
                    SBImageSDK.Application SDK = new SBImageSDK.Application();
                    string sbiuser = getProfileSetting(RuleName, "SBimage_Username", SqlConnection);
                    string sbipass = getProfileSetting(RuleName, "SBimage_Password", SqlConnection);
                    SDK.Initialise(sbiuser, sbipass, "");

                    string sys_appPath = getProfileSetting(RuleName, "sys_AppPath", SqlConnection);
                    string DocKeys = "document_class=" + document_class + ";document_group=" +
                                     document_group + ";policy_number=" + policy_number + ";claim_number=" +
                                     claim_number + ";id_number=" + id_number + ";initials=" + initials +
                                     ";surname=" + surname + ";email_address=" + email_address + ";subject=" +
                                     subject + ";document_type=" + doc_type + ";Document_subclass=" +
                                     doc_subclass + ";Index_Identity={" + identifier + "}";
                    string FileString = sys_appPath + "\\Mailbox\\" + identifier + "\\" +
                                        SQLDataY.Tables[0].Rows[dbRowsY]["attachmentsavedname"];


                    string UniqueRef = "";
                    bool Resp = SDK.IndexFiles(document_class, FileString, DocKeys, "96", true);



                    SetStatus("SBimage indexing: " + Resp, SqlConnection);

                    if (!Resp)
                    {
                        autoIndexed = false;
                        string E = SDK.ErrorMessage;
                        SetStatus("SBimage indexing: " + E, SqlConnection);

                        SqlString = "Update SYS_MailBoxAttachments set sbimage_response = '" + E + "' where id = " + SQLDataY.Tables[0].Rows[dbRowsY]["id"];
                        ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                    }
                    else
                    {
                        string CASE_TYPE = "";
                        string LETTER_DESC = "";

                        SqlString = "select * From config_Lists where list_name = 'Document class." + custom1 + ".Document group." + doc_type + "' and list_value = '" + doc_subclass + "'";
                        SetStatus("Indexing criteria: " + SqlString, SqlConnection);

                        DataSet Classification = ExecuteSQLSelect(SqlString, SqlConnection, ref errMessage);
                        if (Classification.Tables[0].Rows.Count > 0)
                        {
                            string List_Description = Classification.Tables[0].Rows[0]["List_Description"].ToString();
                            string[] FileVals = List_Description.Split(',');

                            CASE_TYPE = FileVals[0];
                            LETTER_DESC = FileVals[1];
                        }
                        else
                        {
                            CASE_TYPE = "CDOC";
                            LETTER_DESC = "LETTER_GENERAL";
                        }


                        UniqueRef = SDK.GetUniqueRef;
                        SetStatus("SBimage indexing, UniqueRef: " + UniqueRef, SqlConnection);


                        SqlString = "Update SYS_MailBoxAttachments set sbimage_response = '" + UniqueRef + "' where id = " + SQLDataY.Tables[0].Rows[dbRowsY]["id"];
                        ExecuteSQL(SqlString, SqlConnection, ref errMessage);

                        string USER_ID = "SBI";
                        string LANGUAGE = "GB";
                        string SITE_NAME = "HPL";
                        string TRACE_YN = "N";
                        string COMMIT_YN = "Y";
                        string SOURCE_SYSTEM = "SBI";
                        string SOURCE_SYSTEM_REF = UniqueRef;
                        string SOURCE_SYSTEM_GROUP_ID = identifier;

                        string NAME_ID_NO = id_number;
                        string POLICY_NO = policy_number;
                        string CLAIM_NO = claim_number;
                        string INBOX = email_address;
                        string EMAIL_SUBJECT = subject;
                        string EMAIL_MESSAGE = "";
                        string EMAIL_FROM = email_address;
                        string EMAIL_CC = "";
                        string EMAIL_BCC = "";


                        string PassString =
                            "USER_ID=SBI;LANGUAGE=GB;SITE_NAME=HPL;TRACE_YN=N;COMMIT_YN=Y;SOURCE_SYSTEM=SBI;SOURCE_SYSTEM_REF=" +
                            SOURCE_SYSTEM_REF + ";SOURCE_SYSTEM_GROUP_ID=" + SOURCE_SYSTEM_GROUP_ID +
                            ";CASE_TYPE=" + CASE_TYPE + ";LETTER_DESC=" + LETTER_DESC + ";NAME_ID_NO=" +
                            NAME_ID_NO + ";POLICY_NO=" + POLICY_NO + ";CLAIM_NO=" + CLAIM_NO + ";INBOX=" +
                            INBOX + ";EMAIL_SUBJECT=" + EMAIL_SUBJECT + ";EMAIL_MESSAGE=" + EMAIL_MESSAGE +
                            ";EMAIL_FROM=" + EMAIL_FROM + ";EMAIL_CC=;EMAIL_BCC=";

                        SetStatus("TIA Values: " + PassString, SqlConnection);

                        SqlString =
                            "insert into SYS_Stats(s_Action, s_Comments, s_userid, s_recordedat, s_reccount, s_execution) values ('tia_index', '" +
                            PassString.Replace("'", "`") + "', 0, 'SERVER', 0, 0)";
                        ExecuteSQL(SqlString, SqlConnection, ref errMessage);

                        string ErrorMessage = "";
                        string res = TIA.StoreFile(USER_ID, LANGUAGE, SITE_NAME, TRACE_YN, COMMIT_YN,
                            SOURCE_SYSTEM, SOURCE_SYSTEM_REF, SOURCE_SYSTEM_GROUP_ID, CASE_TYPE, LETTER_DESC,
                            NAME_ID_NO, POLICY_NO, CLAIM_NO, INBOX, EMAIL_SUBJECT, EMAIL_MESSAGE, EMAIL_FROM,
                            EMAIL_CC, EMAIL_BCC, sys_TIAStoreFile, sys_TIAProxyUser, sys_TIAProxyPass,
                            ref ErrorMessage);

                        SetStatus("TIA store file, response: " + res, SqlConnection);
                        SetStatus("TIA store file, ErrorMessage: " + ErrorMessage, SqlConnection);

                        if (!string.IsNullOrEmpty(ErrorMessage))
                        {
                            SqlString = "insert into SYS_MailBoxNotes (item_id, note_text, user_id) values (" + itemID + ", '" + ErrorMessage + "', " + mailOwner + ")";
                            ExecuteSQL(SqlString, SqlConnection, ref errMessage);

                            SqlString = "Update SYS_MailBoxAttachments set tia_response = '" +
                                        ErrorMessage.Replace("'", "`") + "' where id = " +
                                        SQLDataY.Tables[0].Rows[dbRowsY]["id"];
                            ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                            autoIndexed = false;
                        }
                        else
                        {
                            SqlString = "insert into SYS_MailBoxNotes (item_id, note_text, user_id) values (" + itemID + ", '" + RuleName + "', " + mailOwner + ")";
                            ExecuteSQL(SqlString, SqlConnection, ref errMessage);

                            SqlString = "Update SYS_MailBoxAttachments set tia_response = '" + res +
                                        "' where id = " + SQLDataY.Tables[0].Rows[dbRowsY]["id"];
                            ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                            autoIndexed = true;
                        }
                    }
                }

                //Update mail status
                if (autoIndexed)
                {
                    SqlString = "Update SYS_MailBox Set Status = 5, mailfinalised = getdate(), actiondate = getdate() Where identifier = '" + identifier + "'";
                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                }
                else
                {
                    SqlString = "Update SYS_MailBox Set Status = 0 Where identifier = '" + identifier + "'";
                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                }
            }

            return "";
        }



        private string SaveMessage(Message message, string targetPath, string identifier, string SqlConnection, int messageID, string ConvertionParams, bool deleteRead, string MailBox, ref string mailSubject, ref string mailFrom, ref string mailFromDisplayName, ref string mailTo, ref string mailReceived, ref int mailAttachment)
        {
            try
            {
                string xMailBox = MailBox;

                SetStatus("Saving mail:" + identifier, SqlConnection);

                int mailOwner = 1;
                int spamOwner = 1;
                bool FailedMail = false;


                string BodyPart = "";
                string bodyHTMLText = "";
                string bodyPlainText = "";
                string currentFile = "";
                string itemID = "";
                string htmlFile = "";
                string Trans = "";
                string tranResult = "";

                currentFile = Path.Combine(targetPath, Guid.NewGuid() + "_original.msg");
                message.Save(File.Create(currentFile));

                try
                {
                    if (String.IsNullOrEmpty(message.Headers.Subject.ToString()))
                        _MailSubject = "";
                    else
                        _MailSubject = message.Headers.Subject.ToString();
                
                }
                catch (Exception ex)
                {
                    _MailSubject = "";
                }

                MessagePart htmlTextPart = message.FindFirstHtmlVersion();
                if (htmlTextPart != null)
                {
                    MessagePart html = message.FindFirstHtmlVersion();
                    bodyHTMLText = html.GetBodyAsText();
                }

                MessagePart plainTextPart = message.FindFirstPlainTextVersion();
                if (plainTextPart != null)
                {
                    bodyPlainText = plainTextPart.GetBodyAsText();
                }


                
                // Only show that attachmentPanel if there is attachments in the message
                //bool hadAttachments = attachments.Count > 0;
                //attachmentPanel.Visible = hadAttachments;

                // Generate header table
                DataSet dataSet = new DataSet();
                System.Data.DataTable table = dataSet.Tables.Add("Headers");
                table.Columns.Add("Header");
                table.Columns.Add("Value");

                DataRowCollection rows = table.Rows;

                SetStatus("Adding headers", SqlConnection);
                // Add all known headers
                rows.Add(new object[] { "Content-Description", message.Headers.ContentDescription });
                rows.Add(new object[] { "Content-Id", message.Headers.ContentId });
                foreach (string keyword in message.Headers.Keywords) rows.Add(new object[] { "Keyword", keyword });
                foreach (RfcMailAddress dispositionNotificationTo in message.Headers.DispositionNotificationTo)
                    rows.Add(new object[] { "Disposition-Notification-To", dispositionNotificationTo });
                foreach (Received received in message.Headers.Received)
                    rows.Add(new object[] { "Received", received.Raw });
                rows.Add(new object[] { "Importance", message.Headers.Importance });
                rows.Add(new object[] { "Content-Transfer-Encoding", message.Headers.ContentTransferEncoding });
                foreach (RfcMailAddress cc in message.Headers.Cc) rows.Add(new object[] { "Cc", cc });
                foreach (RfcMailAddress bcc in message.Headers.Bcc) rows.Add(new object[] { "Bcc", bcc });
                foreach (RfcMailAddress to in message.Headers.To) rows.Add(new object[] { "To", to });
                rows.Add(new object[] { "From", message.Headers.From });
                rows.Add(new object[] { "Reply-To", message.Headers.ReplyTo });
                foreach (string inReplyTo in message.Headers.InReplyTo)
                    rows.Add(new object[] { "In-Reply-To", inReplyTo });
                foreach (string reference in message.Headers.References)
                    rows.Add(new object[] { "References", reference });
                rows.Add(new object[] { "Sender", message.Headers.Sender });
                rows.Add(new object[] { "Content-Type", message.Headers.ContentType });
                rows.Add(new object[] { "Content-Disposition", message.Headers.ContentDisposition });
                rows.Add(new object[] { "Date", message.Headers.Date });
                rows.Add(new object[] { "Date", message.Headers.DateSent });
                rows.Add(new object[] { "Message-Id", message.Headers.MessageId });
                rows.Add(new object[] { "Mime-Version", message.Headers.MimeVersion });
                rows.Add(new object[] { "Return-Path", message.Headers.ReturnPath });
                rows.Add(new object[] { "Subject", message.Headers.Subject });
                SetStatus("Added headers", SqlConnection);

                string MessageId = message.Headers.MessageId;
                if (string.IsNullOrEmpty(MessageId))
                    MessageId = "NOID@" + Guid.NewGuid().ToString();

                if (MessageId.Length > 100)
                    MessageId = MessageId.Substring(0, 99);

                SetStatus("MessageId: " + MessageId, SqlConnection);


                if (!ConvertionParams.Contains("sabrix"))
                {
                    string eMessageY = "";
                    string SqlStringX = "Select MessageID From SYS_MailBox where MessageID = '" + MessageId + "'";
                    DataSet SQLDataID = ExecuteSQLSelect(SqlStringX, SqlConnection, ref eMessageY);

                    if (SQLDataID.Tables[0].Rows.Count > 0)
                    {
                        SetStatus("Mail Rejected already exists: " + MessageId, SqlConnection);
                        return "";
                    }
                }


                if (!ConvertionParams.Contains("sabrix"))
                {
                    processBodyX(message, currentFile, SqlConnection, ConvertionParams, targetPath, identifier, ref BodyPart, "main");
                }
                processAttachments(message, SqlConnection, ConvertionParams, targetPath, identifier, "main");


                try
                {
                    if (String.IsNullOrEmpty(_MailSubject))
                        mailSubject = "~~ No Subject ~~";
                    else
                        mailSubject = message.Headers.Subject.ToString();
                }
                catch (Exception ex)
                {
                    mailSubject = "~~ No Subject ~~";
                }

                try
                {
                    if (String.IsNullOrEmpty(message.Headers.From.Address.ToString()))
                        mailFrom = message.Headers.From.DisplayName;
                    else
                        mailFrom = message.Headers.From.Address.ToString();
                }
                catch (Exception ex)
                {
                    mailFrom = "~~ Unknown ~~";
                }

                SetStatus("Obtained mailfrom", SqlConnection);

                try
                {
                    if (String.IsNullOrEmpty(message.Headers.From.DisplayName.ToString()))
                        mailFromDisplayName = "~~ Unknown ~~";
                    else
                        mailFromDisplayName = message.Headers.From.DisplayName.ToString();

                }
                catch (Exception ex)
                {
                    mailFromDisplayName = "~~ Unknown ~~";
                }


                SetStatus("Obtained Displayname", SqlConnection);

                try
                {
                    if (String.IsNullOrEmpty(message.Headers.To[0].Address.ToString()))
                        mailTo = MailBox;
                    else
                    {
                        for (int z = 0; z < message.Headers.To.Count; z++)
                        {
                            mailTo = mailTo + message.Headers.To[z].Address.ToString() + ";";
                        }
                    }
                }
                catch (Exception ex)
                {
                    mailTo = MailBox;
                }


                if (!string.IsNullOrEmpty(mailTo))
                {
                    if (mailTo.EndsWith(";"))
                    {
                        mailTo = mailTo.Substring(0, mailTo.Length - 1);
                    }
                }

                string mailCC = "";
                try
                {
                    if (String.IsNullOrEmpty(message.Headers.Cc[0].Address.ToString()))
                        mailCC = "";
                    else
                    {
                        for (int z = 0; z < message.Headers.Cc.Count; z++)
                        {
                            mailCC = mailCC + message.Headers.Cc[z].Address.ToString() + ";";
                        }
                    }
                }
                catch (Exception ex)
                {
                    mailCC = "";
                }

                if (!string.IsNullOrEmpty(mailCC))
                {
                    if (mailCC.EndsWith(";"))
                    {
                        mailCC = mailCC.Substring(0, mailCC.Length - 1);
                    }
                }

                
                DateTime DT = Convert.ToDateTime(message.Headers.DateSent);
                DateTime tempDate = DateTime.Today;

                if (ConvertionParams.Contains("GMT=+2"))
                {
                    mailReceived = DT.AddHours(2).ToString("yyyy MMM dd HH:mm:ss");
                }
                else
                {
                    mailReceived = DT.ToString("yyyy MMM dd HH:mm:ss");
                }

                SetStatus("Mail received: " + mailReceived, SqlConnection);


                if (!DateTime.TryParse(mailReceived.ToString(), out tempDate))
                    mailReceived = DateTime.Today.ToString("yyyy MMM dd HH:mm:ss");


                if (mailReceived.ToString().StartsWith("0001"))
                    mailReceived = DateTime.Today.ToString("yyyy MMM dd HH:mm:ss");


                SetStatus("About to stamp: " + mailReceived, SqlConnection);

                if (ConvertionParams.ToLower().Contains("stampmailbody") && !string.IsNullOrEmpty(BodyPart) && BodyPart.EndsWith(".pdf"))
                {
                    SetStatus("Temp path: " + Path.GetTempPath().ToString(), SqlConnection);
                    String pathin = BodyPart;
                    String pathout = Path.GetTempFileName();
                    SetStatus("Stamping: " + pathout, SqlConnection);
                    SetStatus("Temp path:", pathout);
                    //create a document object
                    PdfReader reader = new PdfReader(pathin);
                    //select two pages from the original document
                    reader.SelectPages("1-" + reader.NumberOfPages + 1);
                    //create PdfStamper object to write to get the pages from reader 
                    PdfStamper stamper = new PdfStamper(reader, new FileStream(pathout, FileMode.Create));

                    PdfContentByte pbover = stamper.GetOverContent(1);
                    //add content to the page using ColumnText
                    iTextSharp.text.Rectangle mediabox = reader.GetPageSize(1);
                    int H = Convert.ToInt16(mediabox.Height);

                    H = Convert.ToInt32(mediabox.Height) - 15;
                    ColumnText.ShowTextAligned(pbover, Element.ALIGN_LEFT, new Phrase("Subject: " + mailSubject), 10, H, 0);
                    H = Convert.ToInt32(mediabox.Height) - 30;
                    ColumnText.ShowTextAligned(pbover, Element.ALIGN_LEFT, new Phrase("From: " + mailFrom), 10, H, 0);
                    H = Convert.ToInt32(mediabox.Height) - 45;
                    ColumnText.ShowTextAligned(pbover, Element.ALIGN_LEFT, new Phrase("CC: " + mailCC), 10, H, 0);
                    H = Convert.ToInt32(mediabox.Height) - 60;
                    ColumnText.ShowTextAligned(pbover, Element.ALIGN_LEFT, new Phrase("Received: " + mailReceived), 10, H, 0);

                    // PdfContentByte from stamper to add content to the pages under the original content
                    PdfContentByte pbunder = stamper.GetUnderContent(1);

                    stamper.Close();
                    reader.Close();

                    SetStatus("Delete:", BodyPart);
                    SetStatus("Replace from:", pathout);

                    File.Delete(BodyPart);
                    File.Move(pathout, BodyPart);
                }


                SetStatus("Obtained date: " + mailAttachment, SqlConnection);

                // Add all unknown headers
                foreach (string key in message.Headers.UnknownHeaders)
                {
                    string[] values = message.Headers.UnknownHeaders.GetValues(key);
                    if (values != null)
                        foreach (string value in values)
                        {
                            rows.Add(new object[] { key, value });
                        }
                }


                currentFile = Path.Combine(targetPath, Guid.NewGuid() + "_info.xml");
                dataSet.WriteXml(currentFile);
                dataSet.Dispose();
                

                string[] custom = new string[9];

                SetStatus("Checking conversion parameters: " + ConvertionParams, SqlConnection);

                string[] AllCustom = ConvertionParams.Split('+');
                for (int x = 0; x < AllCustom.Length; x++)
                {
                    string[] cVals = AllCustom[x].Split('=');

                    if (cVals[0].ToLower().Contains("custom") && cVals[1].ToLower() == "mailfrom")
                    {
                        int el = int.Parse(cVals[0].Substring(6, 1));
                        custom[el - 1] = mailFrom;
                    }
                    if (cVals[0].ToLower().Contains("custom") && cVals[1].ToLower() == "mailfromdisplayname")
                    {
                        int el = int.Parse(cVals[0].Substring(6, 1));
                        custom[el - 1] = mailFromDisplayName;
                    }
                    if (cVals[0].ToLower().Contains("custom") && cVals[1].ToLower() == "mailreceived")
                    {
                        int el = int.Parse(cVals[0].Substring(6, 1));
                        custom[el - 1] = mailReceived;
                    }
                    if (cVals[0].ToLower().Contains("custom") && cVals[1].ToLower() == "mailto")
                    {
                        int el = int.Parse(cVals[0].Substring(6, 1));
                        custom[el - 1] = mailTo;
                    }
                    if (cVals[0].ToLower().Contains("custom") && cVals[1].ToLower() == "mailsubject")
                    {
                        int el = int.Parse(cVals[0].Substring(6, 1));
                        custom[el - 1] = mailSubject;
                    }
                }


                SetStatus("Done processing conversion parameters", SqlConnection);

                message = null;
                if (!string.IsNullOrEmpty(SqlConnection))
                {
                    SetStatus("Enter SPAM rules", SqlConnection);

                    string eMessage = "";


                    string Param = getParameter(ConvertionParams, "AdminBoxID");
                    if (!String.IsNullOrEmpty(Param))
                    {
                        mailOwner = int.Parse(Param);
                    }

                    SetStatus("MailOwner: " + mailOwner, SqlConnection);

                    Param = getParameter(ConvertionParams, "SpamBoxID");
                    if (!String.IsNullOrEmpty(Param))
                    {
                        spamOwner = int.Parse(Param);
                        SetStatus("SpamOwner: " + spamOwner, SqlConnection);

                        // Check SPAM
                        string sys_SpamKeywords = getGeneralProfileSetting(MailBox, "sys_SpamKeywords", SqlConnection);
                        SetStatus("Spam Keys: " + sys_SpamKeywords, SqlConnection);

                        if (!String.IsNullOrEmpty(sys_SpamKeywords))
                        {
                            // Check if subject of body contain spam rule

                            string[] allWords = sys_SpamKeywords.Split(',');
                            foreach (string s in allWords)
                            {
                                if (mailSubject.Contains(s) || mailFrom.Contains(s) || bodyHTMLText.Contains(s) ||
                                    bodyPlainText.Contains(s))
                                {
                                    mailOwner = spamOwner;
                                    SetStatus("Spam Identified, Owner: " + mailOwner, SqlConnection);
                                    break;
                                }
                                
                            }
                        }
                    }



                    SetStatus("Check existing user: " + mailFrom, SqlConnection);
                    string SqlString = "Select s_id from sys_users where email = '" + mailFrom + "'";
                    DataSet SQLDataX = ExecuteSQLSelect(SqlString, SqlConnection, ref eMessage);

                    

                    if (SQLDataX.Tables[0].Rows.Count > 0)
                    {
                        mailOwner = int.Parse(SQLDataX.Tables[0].Rows[0].ItemArray[0].ToString());
                    }

                    SqlString =
                        "Insert into SYS_MailBox(mailsubject, mailowner, mailfrom, mailfromdisplayname, mailto, mailcc, mailreceived, mailprocessed, " +
                        "attachments, status, mailtextbody, mailhtmlbody, identifier, custom1, custom2, custom3, custom4, custom5, custom6, custom7, custom8, custom9, MessageID) values ('" +
                        mailSubject.Replace("'", "`") + "', " + mailOwner + ",'" + mailFrom.Replace("'", "`") + "', '" +
                        mailFromDisplayName.Replace("'", "`") + "', '" + MailBox.Replace("'", "`") + "', '" + mailCC.Replace("'", "`")  +
                        "', '" + mailReceived.Replace("'", "`") + "', getdate(), " + mailAttachment + ", 9, '" +
                        bodyPlainText.Replace("'", "`") + "', '" + bodyHTMLText.Replace("'", "`") + "', '" + identifier +
                        "', '" + custom[0] + "', '" + custom[1] + "', '" + custom[2] + "', '" +
                        custom[3] + "', '" + custom[4] + "', '" + custom[5] + "', '" + custom[6] + "', '" + custom[7] +
                        "', '" + custom[8] + "', '" + MessageId + "')";


                    SetStatus("SqlString: " + SqlString, SqlConnection);

                    string errMessage = "";
                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                    if (!string.IsNullOrEmpty(errMessage))
                        return errMessage;

                    string eMessageM = "";
                    SqlString = "Select * From SYS_MailBox where identifier = '" + identifier + "'";
                    DataSet SQLDataIDX = ExecuteSQLSelect(SqlString, SqlConnection, ref eMessageM);
                    itemID = SQLDataIDX.Tables[0].Rows[0]["id"].ToString();

                }

                SetStatus("About to construct a string", SqlConnection);

                string eMessageX = "";
                string SqlStringRules = "select * From sys_Settings where (setting = 'usr_FromAddress' and value = '') or (Setting = 'usr_FromAddress' and value = '" + xMailBox + "') order by value desc, section";


                SetStatus("SqlString: " + SqlStringRules, SqlConnection);
                DataSet SQLDataRules = ExecuteSQLSelect(SqlStringRules, SqlConnection, ref eMessageX);


                if (SQLDataRules.Tables[0].Rows.Count == 0)
                {
                    string errMessage = "";
                    string SqlString = "Update SYS_MailBox set Status = 0 where identifier = '" + identifier + "'";
                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                    SetStatus("No Auto Indexing configured", SqlConnection);

                    SqlString = "insert into SYS_MailBoxNotes (item_id, note_text, user_id) values (" + itemID + ", 'No rules configured for: [" + xMailBox + "]', " + mailOwner + ")";
                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                    return "";
                }

                string RuleName = "";
                bool mailValidated = false;
                bool autoIndexed = false;

                for (int Rule = 0; Rule < SQLDataRules.Tables[0].Rows.Count; Rule++)
                {
                    RuleName = SQLDataRules.Tables[0].Rows[Rule]["Section"].ToString();

                    SetStatus("Rule name: " + RuleName, SqlConnection);
                    if (RuleName.StartsWith("SFTP"))
                    {
                        string allParals = getProfileSetting(RuleName, "sys_ConversionParams", SqlConnection);

                        string fSubject = getParameter(allParals, "subject");
                        string fDomain = getParameter(allParals, "domain");
                        string fHost = getParameter(allParals, "host");
                        string fPort = getParameter(allParals, "port");
                        string fUsername = getParameter(allParals, "username");
                        string fPassword = getParameter(allParals, "password");
                        string fCertificate = getParameter(allParals, "certificate");
                        string fRemotePath = getParameter(allParals, "remotepath");
                        string fileName = getParameter(allParals, "filename");

                        SetStatus("Subject: " + fSubject + " Domain: " + fDomain, SqlConnection);
                        if (mailSubject.Contains(fSubject) & mailFrom.Contains(fDomain))
                        {
                            List<string> allFiles = Directory
                                .GetFiles(targetPath, "*.*")
                                .Where(file => file.ToLower().EndsWith("xls") || file.ToLower().EndsWith("xlsx"))
                                .ToList();

                            string[] txtFilename = allFiles.ToArray();

                            string sRes = "";
                            if (txtFilename.Length > 0)
                            {
                                sRes = startSFTP(fHost, fUsername, fPassword, fPort, txtFilename, fCertificate, fRemotePath, fileName);
                            }
                            else
                            {
                                sRes = "No XLS files attached";
                            }
                                
                            SetStatus("SFTP Result: " + sRes, SqlConnection);

                            if (sRes == "")
                            {
                                string errMessage = "";
                                string SqlString = "Update SYS_MailBox set Status = 5 where identifier = '" + identifier + "'";
                                ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                SetStatus("FTP Auto Indexed OK", SqlConnection);
                                return "";
                            }
                            else
                            {
                                string errMessage = "";
                                string SqlString = "Update SYS_MailBox set Status = 0 where identifier = '" + identifier + "'";
                                ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                SetStatus("FTP Auto Indexed aborted: " + sRes, SqlConnection);
                                return "";
                            }
                        }
                    }
                    else
                    {
                        bool ruleFound = false;
                        string CurrentRule = "";

                        // Set exception of From address
                        string usr_FromAddress = getProfileSetting(RuleName, "usr_FromAddress", SqlConnection);
                        if (!string.IsNullOrEmpty(usr_FromAddress))
                        {
                            if (!usr_FromAddress.ToLower().Contains(mailFrom.ToLower().Replace("'", "`")))
                            {
                                SetStatus("Rule [" + RuleName + "] ignored, rule is associated to " + mailFrom,
                                    SqlConnection);
                                continue;
                            }
                            else
                                SetStatus("Rule [" + RuleName + "] Accepted for Auto-indexing " + mailFrom, SqlConnection);
                        }
                        else
                        {
                            SetStatus("Generic rule [" + RuleName + "] Accepted for Auto-indexing " + mailFrom, SqlConnection);
                        }


                        //string SqlString = "";

                        cls_TIAExport.Application TIA = new cls_TIAExport.Application();
                        string sys_TIAValidateClaim = getProfileSetting(RuleName, "sys_TIAValidateClaim", SqlConnection);
                        string sys_TIAValidatePolicy = getProfileSetting(RuleName, "sys_TIAValidatePolicy", SqlConnection);

                        string sys_TIAProxyUser = getProfileSetting(RuleName, "sys_TIAProxyUser", SqlConnection);
                        string sys_TIAProxyPass = getProfileSetting(RuleName, "sys_TIAProxyPass", SqlConnection);
                        string sys_TIAStoreFile = getProfileSetting(RuleName, "sys_TIAStoreFile", SqlConnection);

                        string usr_ValidationType = getProfileSetting(RuleName, "usr_ValidationType", SqlConnection);

                        //Get claim number from header
                        string c_Initials = "";
                        string c_Name = "";
                        string c_Surname = "";


                        string claim = "";
                        string[] vSubject;

                        string SqlString = "";
                        string errMessage = "";


                        int validParam = 1;
                        string ErrorMessage = "";

                        //Check if subject qualifies for spam
                        bool returned = false;

                        if (RuleName == "Email Delivery Failure")
                        {
                            string usr_SubjectRule = "";


                            SqlString = "select * From sys_Settings where Section = '" + RuleName + "' and Setting = 'usr_SubjectRule'";
                            DataSet SQLDataR = ExecuteSQLSelect(SqlString, SqlConnection, ref errMessage);
                            for (int Row = 0; Row < SQLDataR.Tables[0].Rows.Count; Row++)
                            {
                                usr_SubjectRule = SQLDataR.Tables[0].Rows[Row]["value"].ToString();
                            }


                            string[] allOptions = usr_SubjectRule.Split('+');
                            for (int r = 0; r < allOptions.Length; r++)
                            {
                                if (mailSubject.Contains(allOptions[r]))
                                {
                                    returned = true;
                                    break;
                                }
                            }

                        }

                        if (returned)
                        {
                            FailedMail = true;
                            validParam = 0;
                            ErrorMessage = "";
                            SetStatus("Auto-indexing failed mail: ", SqlConnection);

                            string USER_ID = "SBI";
                            string LANGUAGE = "GB";
                            string SITE_NAME = "HPL";
                            string TRACE_YN = "N";
                            string COMMIT_YN = "Y";
                            string SOURCE_SYSTEM = "SBI";
                            string SOURCE_SYSTEM_REF = "";
                            string SOURCE_SYSTEM_GROUP_ID = identifier;

                            string NAME_ID_NO = "";
                            string POLICY_NO = "";
                            string CLAIM_NO = "";
                            string INBOX = MailBox;
                            string EMAIL_SUBJECT = "Email Delivery Failure";
                            string EMAIL_MESSAGE = "";
                            string EMAIL_FROM = mailFrom;
                            string EMAIL_CC = "";
                            string EMAIL_BCC = "";

                            if (!INBOX.Contains("@"))
                                INBOX = INBOX + "@hollard.co.za";


                            string LETTER_DESC = "DELIVERY_FAILURE";
                            string CASE_TYPE = "EMFL";
                            string MAILREF = "";

                            int pos1 = mailSubject.IndexOf("{");
                            int pos2 = mailSubject.IndexOf("}");

                            if (pos1 > -1 && pos2 > -1)
                            {
                                MAILREF = mailSubject.Substring(pos1 + 1, pos2 - pos1 - 1);
                            }
                            else
                            {
                                pos1 = bodyPlainText.IndexOf("{");
                                pos2 = bodyPlainText.IndexOf("}");
                                if (pos1 > -1 && pos2 > -1)
                                {
                                    MAILREF = bodyPlainText.Substring(pos1 + 1, pos2 - pos1 - 1);
                                }

                            }

                            if (!string.IsNullOrEmpty(MAILREF))
                            {
                                //string ErrorMessage = "";
                                string res = TIA.createWorkflowItem(USER_ID, LANGUAGE, SITE_NAME, TRACE_YN, COMMIT_YN,
                                    SOURCE_SYSTEM, SOURCE_SYSTEM_REF, SOURCE_SYSTEM_GROUP_ID, CASE_TYPE, LETTER_DESC,
                                    NAME_ID_NO, POLICY_NO, CLAIM_NO, INBOX, EMAIL_SUBJECT, EMAIL_MESSAGE, EMAIL_FROM,
                                    EMAIL_CC, EMAIL_BCC, "EMAIL_REQUEST_ID", MAILREF, "FAILURE_REASON",
                                    "[Postmaster] Email Delivery Failure", sys_TIAStoreFile, sys_TIAProxyUser,
                                    sys_TIAProxyPass,
                                    ref ErrorMessage);

                                SetStatus("TIA Create Work Item, response: " + res, SqlConnection);
                                SetStatus("TIA Create Work Item, ErrorMessage: " + ErrorMessage, SqlConnection);

                                if (string.IsNullOrEmpty(ErrorMessage))
                                {
                                    SqlString = "Update SYS_MailBox set Status = 5, TIA_Response = '" + res +
                                                "' where identifier = '" + identifier + "'";
                                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                    SetStatus("Auto Indexed OK", SqlConnection);
                                }
                                else
                                {
                                    SqlString = "Update SYS_MailBox set Status = 0,  TIA_Response = '" + ErrorMessage +
                                                "' where identifier = '" + identifier + "'";
                                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                    SetStatus("Auto Indexed Error: " + ErrorMessage, SqlConnection);
                                }

                                return "";
                                autoIndexed = true;
                            }
                            else
                            {
                                SqlString = "Update SYS_MailBox set Status = 0,  TIA_Response = '" + ErrorMessage +
                                                "' where identifier = '" + identifier + "'";
                                ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                SetStatus("Auto Indexed Error: Unable to get mail reference", SqlConnection);

                                return "";
                                autoIndexed = false;

                            }
                        }
                        else
                        {
                            if (mailSubject.Contains(","))
                                vSubject = mailSubject.Split(',');
                            else
                                vSubject = mailSubject.Split(' ');
                        }


                        for (int valid = 0; valid < vSubject.Length; valid++)
                        {
                            //2882

                            long n;
                            bool isNumeric = long.TryParse(vSubject[valid], out n);

                            if (isNumeric)
                            {

                                if (usr_ValidationType.ToLower() == "claim")
                                {
                                    SetStatus("Validating claim: " + n.ToString(), SqlConnection);
                                    bool res = TIA.getClaim(n, sys_TIAValidateClaim, sys_TIAProxyUser, sys_TIAProxyPass, ref ErrorMessage);
                                    if (!res)
                                        ErrorMessage = "Invalid claim";

                                    SetStatus("TIA Response: " + ErrorMessage, SqlConnection);
                                }

                                else if (usr_ValidationType.ToLower() == "policy")
                                {
                                    ulong pol = (ulong)n;

                                    SetStatus("Validating policy: " + pol.ToString(), SqlConnection);
                                    ErrorMessage = TIA.getPolicy(pol, ref c_Initials, ref c_Name, ref c_Surname, sys_TIAValidatePolicy, sys_TIAProxyUser, sys_TIAProxyPass);
                                    SetStatus("TIA Response: " + ErrorMessage, SqlConnection);
                                }


                                else if (usr_ValidationType.ToLower() == "subject")
                                {
                                    SqlString = "select * From sys_Settings where Section = '" + RuleName + "' and Setting = 'usr_SubjectRule'";
                                    DataSet SQLDataX = ExecuteSQLSelect(SqlString, SqlConnection, ref errMessage);

                                    ErrorMessage = "Unable to validate on subject rules";
                                    for (int Row = 0; Row < SQLDataX.Tables[0].Rows.Count; Row++)
                                    {
                                        string[] AllRules = SQLDataX.Tables[0].Rows[Row]["value"].ToString().Split(',');


                                        if (mailSubject.ToLower().Contains(AllRules[0].ToLower()))
                                        {
                                            ErrorMessage = "";
                                            CurrentRule = SQLDataX.Tables[0].Rows[Row]["value"].ToString();

                                            string usr_Custom1 = getProfileSetting(RuleName, "usr_Custom1", SqlConnection);
                                            if (usr_Custom1 == "TIA Claim")
                                            {
                                                bool res = TIA.getClaim(n, sys_TIAValidateClaim, sys_TIAProxyUser, sys_TIAProxyPass, ref ErrorMessage);
                                                usr_ValidationType = "claim";
                                                validParam = valid;
                                                if (!res)
                                                    ErrorMessage = "Invalid claim";
                                            }
                                            else if (usr_Custom1 == "TIA Policy")
                                            {
                                                ulong pol = (ulong)n;
                                                ErrorMessage = TIA.getPolicy(pol, ref c_Initials, ref c_Name, ref c_Surname,
                                                    sys_TIAValidatePolicy, sys_TIAProxyUser, sys_TIAProxyPass);
                                                usr_ValidationType = "policy";
                                                validParam = valid;
                                                //ErrorMessage = "";
                                            }
                                            else
                                            {
                                                break;
                                            }

                                            break;
                                        }
                                    }
                                }

                                if (string.IsNullOrEmpty(ErrorMessage))
                                {
                                    errMessage = "";
                                    mailValidated = true;
                                    SqlString = "";

                                    if (usr_ValidationType.ToLower() == "claim")
                                    {
                                        SqlString = "Update SYS_MailBox set custom1 = 'TIA Claim', custom4 = '" + vSubject[validParam] + "', custom8 = '" + mailFrom + "' where identifier = '" + identifier + "'";
                                        claim = vSubject[valid];
                                    }
                                    else
                                    {
                                        SqlString = "Update SYS_MailBox set custom1 = 'TIA Policy', custom3 = '" + vSubject[validParam] + "', custom6 = '" + c_Name + "', custom7 = '" + c_Surname + "', custom8 = '" + mailFrom + "' where identifier = '" + identifier + "'";
                                        claim = vSubject[validParam];
                                    }

                                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                    break;
                                }
                                else
                                    mailValidated = false;
                            }
                        }


                        if (mailValidated)
                        {
                            string CASE_TYPE = "";
                            string LETTER_DESC = "";
                            string[] TIAVals = { };

                            string usr_Custom1 = "";
                            string usr_aCustom1 = "";
                            string usr_aCustom2 = "";
                            string doc_type = "DCI_HPL_TIA";

                            TIAVals = getProfileSetting(RuleName, "usr_TIAValues", SqlConnection).Split(',');
                            CASE_TYPE = TIAVals[0];
                            LETTER_DESC = TIAVals[1];
                            usr_Custom1 = getProfileSetting(RuleName, "usr_Custom1", SqlConnection);
                            usr_aCustom1 = getProfileSetting(RuleName, "usr_aCustom1", SqlConnection);
                            usr_aCustom2 = getProfileSetting(RuleName, "usr_aCustom2", SqlConnection);


                            //Auto index
                            if (usr_ValidationType.ToLower() == "claim")
                            {
                                SetStatus("Auto indexing: claim no: " + claim, SqlConnection);
                            }
                            else
                            {
                                SetStatus("Auto indexing: policy no: " + claim, SqlConnection);
                            }

                            errMessage = "";
                            DataSet SQLDataX = null;
                            SqlString =
                                "select SYS_MailBox.id as m_id, SYS_MailBoxAttachments.id as a_id, * from SYS_MailBox left join SYS_MailBoxAttachments " +
                                    " on SYS_MailBox.Identifier = SYS_MailBoxAttachments.Identifier where SYS_MailBoxAttachments.deleted = 0 and SYS_MailBox.identifier = '" +
                                identifier + "' and SYS_MailBoxAttachments.identifier = '" + identifier + "'";
                            SQLDataX = ExecuteSQLSelect(SqlString, SqlConnection, ref errMessage);


                            for (int dbRows = 0; dbRows < SQLDataX.Tables[0].Rows.Count; dbRows++)
                            {

                                if (!string.IsNullOrEmpty(SQLDataX.Tables[0].Rows[dbRows]["attachmentsavedname"].ToString()))
                                {
                                    string document_class = SQLDataX.Tables[0].Rows[dbRows]["custom1"].ToString();
                                    string document_group = SQLDataX.Tables[0].Rows[dbRows]["custom2"].ToString();
                                    string policy_number = SQLDataX.Tables[0].Rows[dbRows]["custom3"].ToString();
                                    string claim_number = SQLDataX.Tables[0].Rows[dbRows]["custom4"].ToString();
                                    string id_number = SQLDataX.Tables[0].Rows[dbRows]["custom5"].ToString();
                                    string initials = SQLDataX.Tables[0].Rows[dbRows]["custom6"].ToString();
                                    string surname = SQLDataX.Tables[0].Rows[dbRows]["custom7"].ToString();
                                    string email_address = SQLDataX.Tables[0].Rows[dbRows]["custom8"].ToString();

                                    string subject = SQLDataX.Tables[0].Rows[dbRows]["mailsubject"].ToString();
                                    if (subject.Length > 200)
                                        subject = subject.Substring(1, 200);


                                    string sys_appPath = getProfileSetting(RuleName, "sys_AppPath", SqlConnection);
                                    string DocKeys = "document_class=" + document_class + ";document_group=" +
                                                     document_group + ";policy_number=" + policy_number + ";claim_number=" +
                                                     claim_number + ";id_number=" + id_number + ";initials=" + initials +
                                                     ";surname=" + surname + ";email_address=" + email_address + ";subject=" +
                                                     subject + ";document_type=" + usr_aCustom1 + ";Document_subclass=" +
                                                     usr_aCustom2 + ";Index_Identity=" + identifier;
                                    string FileString = sys_appPath + "\\Mailbox\\" + identifier + "\\" +
                                                        SQLDataX.Tables[0].Rows[dbRows]["attachmentsavedname"];

                                    if (FailedMail)
                                    {

                                    }
                                    else
                                    {

                                        SBImageSDK.Application SDK = new SBImageSDK.Application();
                                        string sbiuser = getProfileSetting(RuleName, "SBimage_Username", SqlConnection);
                                        string sbipass = getProfileSetting(RuleName, "SBimage_Password", SqlConnection);
                                        SDK.Initialise(sbiuser, sbipass, "");

                                        string UniqueRef = "";
                                        bool Resp = SDK.IndexFiles(doc_type, FileString, DocKeys, "95", true);



                                        SetStatus("SBimage indexing: " + Resp, SqlConnection);

                                        if (!Resp)
                                        {
                                            autoIndexed = false;
                                            string E = SDK.ErrorMessage;
                                            SetStatus("SBimage indexing: " + E, SqlConnection);

                                            SqlString = "Update SYS_MailBoxAttachments set sbimage_response = '" + E +
                                                        "' where id = " + SQLDataX.Tables[0].Rows[dbRows]["a_id"];
                                            ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                        }
                                        else
                                        {
                                            UniqueRef = SDK.GetUniqueRef;
                                            SetStatus("SBimage indexing, UniqueRef: " + UniqueRef, SqlConnection);


                                            SqlString = "Update SYS_MailBoxAttachments set sbimage_response = '" + UniqueRef +
                                                        "', acustom1 = '" + usr_aCustom1 + "', acustom2 = '" + usr_aCustom2 +
                                                        "' where id = " + SQLDataX.Tables[0].Rows[dbRows]["a_id"];
                                            ExecuteSQL(SqlString, SqlConnection, ref errMessage);

                                            string USER_ID = "SBI";
                                            string LANGUAGE = "GB";
                                            string SITE_NAME = "HPL";
                                            string TRACE_YN = "N";
                                            string COMMIT_YN = "Y";
                                            string SOURCE_SYSTEM = "SBI";
                                            string SOURCE_SYSTEM_REF = UniqueRef;
                                            string SOURCE_SYSTEM_GROUP_ID = identifier;

                                            string NAME_ID_NO = id_number;
                                            string POLICY_NO = policy_number;
                                            string CLAIM_NO = claim_number;
                                            string INBOX = email_address;
                                            string EMAIL_SUBJECT = subject;
                                            string EMAIL_MESSAGE = "";
                                            string EMAIL_FROM = email_address;
                                            string EMAIL_CC = "";
                                            string EMAIL_BCC = "";


                                            string PassString =
                                                "USER_ID=SBI;LANGUAGE=GB;SITE_NAME=HPL;TRACE_YN=N;COMMIT_YN=Y;SOURCE_SYSTEM=SBI;SOURCE_SYSTEM_REF=" +
                                                SOURCE_SYSTEM_REF + ";SOURCE_SYSTEM_GROUP_ID=" + SOURCE_SYSTEM_GROUP_ID +
                                                ";CASE_TYPE=" + CASE_TYPE + ";LETTER_DESC=" + LETTER_DESC + ";NAME_ID_NO=" +
                                                NAME_ID_NO + ";POLICY_NO=" + POLICY_NO + ";CLAIM_NO=" + CLAIM_NO + ";INBOX=" +
                                                INBOX + ";EMAIL_SUBJECT=" + EMAIL_SUBJECT + ";EMAIL_MESSAGE=" + EMAIL_MESSAGE +
                                                ";EMAIL_FROM=" + EMAIL_FROM + ";EMAIL_CC=;EMAIL_BCC=";

                                            SetStatus("TIA Values: " + PassString, SqlConnection);

                                            SqlString =
                                                "insert into SYS_Stats(s_Action, s_Comments, s_userid, s_recordedat, s_reccount, s_execution) values ('tia_index', '" +
                                                PassString.Replace("'", "`") + "', 0, 'SERVER', 0, 0)";
                                            ExecuteSQL(SqlString, SqlConnection, ref errMessage);

                                            //string ErrorMessage = "";
                                            string res = TIA.StoreFile(USER_ID, LANGUAGE, SITE_NAME, TRACE_YN, COMMIT_YN,
                                                SOURCE_SYSTEM, SOURCE_SYSTEM_REF, SOURCE_SYSTEM_GROUP_ID, CASE_TYPE, LETTER_DESC,
                                                NAME_ID_NO, POLICY_NO, CLAIM_NO, INBOX, EMAIL_SUBJECT, EMAIL_MESSAGE, EMAIL_FROM,
                                                EMAIL_CC, EMAIL_BCC, sys_TIAStoreFile, sys_TIAProxyUser, sys_TIAProxyPass,
                                                ref ErrorMessage);

                                            SetStatus("TIA store file, response: " + res, SqlConnection);
                                            SetStatus("TIA store file, ErrorMessage: " + ErrorMessage, SqlConnection);

                                            if (!string.IsNullOrEmpty(ErrorMessage))
                                            {
                                                SqlString = "insert into SYS_MailBoxNotes (item_id, note_text, user_id) values (" + itemID + ", '" + ErrorMessage + "', " + mailOwner + ")";
                                                ExecuteSQL(SqlString, SqlConnection, ref errMessage);

                                                SqlString = "Update SYS_MailBoxAttachments set tia_response = '" +
                                                            ErrorMessage.Replace("'", "`") + "' where id = " +
                                                            SQLDataX.Tables[0].Rows[dbRows]["a_id"];
                                                ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                                autoIndexed = false;
                                            }
                                            else
                                            {
                                                SqlString = "insert into SYS_MailBoxNotes (item_id, note_text, user_id) values (" + itemID + ", '" + RuleName + "', " + mailOwner + ")";
                                                ExecuteSQL(SqlString, SqlConnection, ref errMessage);

                                                SqlString = "Update SYS_MailBoxAttachments set tia_response = '" + res +
                                                            "' where id = " + SQLDataX.Tables[0].Rows[dbRows]["a_id"];
                                                ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                                                autoIndexed = true;
                                            }
                                        }
                                    }
                                }
                            }

                            if (autoIndexed)
                            {
                                break;
                            }
                        }                    
                    }
                }


                if (autoIndexed && mailValidated)
                {
                    string errMessage = "";
                    string SqlString = "Update SYS_MailBox set Status = 5 where identifier = '" + identifier + "'";
                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                    SetStatus("Auto Indexed OK", SqlConnection);
                }
                else
                {
                    string errMessage = "";
                    string SqlString = "Update SYS_MailBox set Status = 0 where identifier = '" + identifier + "'";
                    ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                    SetStatus("Auto Indexed ERROR, manual capture", SqlConnection);

                    if (!mailValidated)
                    {
                        SqlString = "insert into SYS_MailBoxNotes (item_id, note_text, user_id) values (" + itemID + ", 'Unable to validate mail to TIA', " + mailOwner + ")";
                        ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                    }
                    else if (!autoIndexed)
                    {
                        SqlString = "insert into SYS_MailBoxNotes (item_id, note_text, user_id) values (" + itemID + ", 'Unable to index attachments to SBimage', " + mailOwner + ")";
                        ExecuteSQL(SqlString, SqlConnection, ref errMessage);
                    }
                }

                SetStatus("Done", SqlConnection);
                return "";
            }
            catch (Exception ex)
            {
                string m = ex.Message;

                SetStatus("ERROR: " + m, SqlConnection);
                return ex.Message;
            }
        }



        private string startSFTP(string txtHost, string txtUsername, string txtPassword, string txtPort, string[] txtFilename , string certificate, string remotePath, string fileName)
        {
            try
            {
                string host = txtHost;
                string username = txtUsername;
                string password = txtPassword;
                string Port = txtPort;
                string certFile = certificate;


                var pk = new PrivateKeyFile(certFile);
                var keyFiles = new[] { pk };

                var methods = new List<Renci.SshNet.AuthenticationMethod>();
                methods.Add(new PrivateKeyAuthenticationMethod(username, keyFiles));
                var con = new ConnectionInfo(host, Convert.ToInt32(Port), username, methods.ToArray());

                using (var client = new SftpClient(con))
                {
                    client.Connect();
                    if (!String.IsNullOrEmpty(remotePath))
                        client.ChangeDirectory(remotePath);


                    for(int fl = 0; fl < txtFilename.Length; fl++)
                    {
                        using (var uplfileStream = System.IO.File.OpenRead(txtFilename[fl]))
                        {
                            client.UploadFile(uplfileStream, fileName, true);

                        }
                    }
   
                    client.Disconnect();
                }

                return "";
            }

            catch (Exception ex)
            {
                return ex.Message;
            }
        }

   
        private string getParameter(string ConversionString, string setting)
        {
            try
            {
                string[] allParams = ConversionString.Split('+');
                for (int i = 0; i < allParams.Length; i++)
                {
                    if (allParams[i].Contains(setting))
                    {
                        string[] param = allParams[i].Split('=');
                        return param[1];
                    }
                }
            }
            catch
            {
                return "";
            }

            return "";
        }




        private string getProfileSetting(string Section, string Setting, string SqlConnection)
        {
            try
            {
                string eMessage = "";
                string SqlString = "select * From sys_Settings where Section = '" + Section + "' and Setting = '" + Setting + "'";
                DataSet SQLDataX = ExecuteSQLSelect(SqlString, SqlConnection, ref eMessage);

                if (SQLDataX.Tables[0].Rows.Count > 0)
                {
                    return SQLDataX.Tables[0].Rows[0]["value"].ToString();
                }
            }
            catch (Exception ex)
            {
                return "";
            }

            return "";
        }

        private string getGeneralProfileSetting(string Section, string Setting, string SqlConnection)
        {
            try
            {
                string eMessage = "";
                string SqlString = "select * From sys_SettingsGeneral where Section = '" + Section + "' and Setting = '" + Setting + "'";
                DataSet SQLDataX = ExecuteSQLSelect(SqlString, SqlConnection, ref eMessage);

                if (SQLDataX.Tables[0].Rows.Count > 0)
                {
                    return SQLDataX.Tables[0].Rows[0]["value"].ToString();
                }
            }
            catch (Exception ex)
            {
                return "";
            }

            return "";
        }


        private void SetStatus(string Status, string SqlConnection)
        {
            try
            {

                string LogPath = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), "Logs");
                if (!Directory.Exists(LogPath))
                    Directory.CreateDirectory(LogPath);

                LogPath = Path.Combine(LogPath, DateTime.Now.ToString("yyyyMMdd") + ".txt");

                using (StreamWriter sw = File.AppendText(LogPath))
                {
                    sw.WriteLine(DateTime.Now.ToString("yyyyMMddHHmmss") + " " + Status);
                }
            }
            catch (Exception ex)
            {

            }
        }

        private string FileTransformHTML(string Filename, string ConvertionParams, string SqlConnection, ref string eMessage)
        {
            string NewName = Path.Combine(Path.GetDirectoryName(Filename), Path.GetFileNameWithoutExtension(Filename) + ".pdf");

            // create the HTML to PDF converter

            {
                HtmlToPdf htmlToPdfConverter = new HtmlToPdf();
                htmlToPdfConverter.BrowserWidth = 1200;
                htmlToPdfConverter.Document.FitPageWidth = true;
                htmlToPdfConverter.Document.ForceFitPageWidth = true;

                // set HTML Load timeout
                htmlToPdfConverter.HtmlLoadedTimeout = int.Parse("240");

                // set PDF page size and orientation
                htmlToPdfConverter.Document.PageSize = HiQPdf.PdfPageSize.A4;
                htmlToPdfConverter.Document.PageOrientation = HiQPdf.PdfPageOrientation.Landscape;

                // set PDF page margins
                htmlToPdfConverter.Document.Margins = new PdfMargins(0,0, 50, 0);

                // set a wait time before starting the conversion
                htmlToPdfConverter.WaitBeforeConvert = int.Parse("2");


                // convert HTML to PDF
                string pdfFile = null;
                try
                {
                    string url = Filename;
                    pdfFile = NewName;
                    htmlToPdfConverter.ConvertUrlToFile(url, pdfFile);
                }
                catch (Exception ex)
                {
                    //MessageBox.Show([String].Format("Conversion failed. {0}", ex.Message))
                    return ex.Message;

                }
                finally
                {
                }
            }

            return NewName;
        }


        private string FileTransform(string Filename, string ConvertionParams, string SqlConnection, int Orientation, ref string eMessage)
        {
            try
            {
                if (Path.GetExtension(Filename).ToLower() == ".pdf")
                {
                    return "";
                }

                string jpg = getParameter(ConvertionParams, "jpg");
                string png = getParameter(ConvertionParams, "png");
                string tif = getParameter(ConvertionParams, "tif");


                if ((Path.GetExtension(Filename).ToLower() == ".jpg" || Path.GetExtension(Filename).ToLower() == ".jpeg") && jpg == "pdf")
                {
                    string NewName = Path.Combine(Path.GetDirectoryName(Filename), Path.GetFileNameWithoutExtension(Filename) + ".pdf");
                    SetStatus("Converting file to: " + NewName, SqlConnection);

                    SBimage_FileConvert.FileConvertion.cls_FileConvert C = new SBimage_FileConvert.FileConvertion.cls_FileConvert();
                    string res = C.ConvertPDFFree(Filename, NewName, "");
                    SetStatus("Conversion result: " + res, SqlConnection);
                    return NewName;
                }



                if ((Path.GetExtension(Filename).ToLower() == ".tif" || Path.GetExtension(Filename).ToLower() == ".tiff") && tif == "pdf")
                {
                    string NewName = Path.Combine(Path.GetDirectoryName(Filename), Path.GetFileNameWithoutExtension(Filename) + ".pdf");
                    SetStatus("Converting file to: " + NewName, SqlConnection);

                    SBimage_FileConvert.FileConvertion.cls_FileConvert C = new SBimage_FileConvert.FileConvertion.cls_FileConvert();
                    string res = C.ConvertPDFFree(Filename, NewName, "");
                    SetStatus("Conversion result: " + res, SqlConnection);
                    return NewName;
                }


                if (Path.GetExtension(Filename).ToLower() == ".png" && png == "pdf")
                {
                    string NewName = Path.Combine(Path.GetDirectoryName(Filename), Path.GetFileNameWithoutExtension(Filename) + ".pdf");
                    SetStatus("Converting file to: " + NewName, SqlConnection);

                    SBimage_FileConvert.FileConvertion.cls_FileConvert C = new SBimage_FileConvert.FileConvertion.cls_FileConvert();
                    string res = C.ConvertPDFFree(Filename, NewName, "");
                    SetStatus("Conversion result: " + res, SqlConnection);
                    return NewName;
                }



                if (Path.GetExtension(Filename).ToLower() == ".csv" || Path.GetExtension(Filename).ToLower() == ".xls" || Path.GetExtension(Filename).ToLower() == ".xlsx")
                {
                    return "";
                }


                if ((Path.GetExtension(Filename).ToLower() == ".doc" || Path.GetExtension(Filename).ToLower() == ".docx" || Path.GetExtension(Filename).ToLower() == ".htm" || Path.GetExtension(Filename).ToLower() == ".html") && ConvertionParams.Contains("doc=pdf") || Path.GetExtension(Filename).ToLower() == ".msg")
                {
                    SetStatus("Executing WORD conversion", SqlConnection);
                    string wPassword = "";
                    object oMissingPass = null;
                    bool pRes = false;

                    if (ConvertionParams.Contains("password=subject"))
                    {
                        pRes = MsOfficeHelper.IsPasswordProtected(Filename);
                        if (pRes)
                        {
                            string[] subj = _MailSubject.Split(' ');
                            for (int x = 0; x < subj.Length; x++)
                            {
                                if (subj[x].StartsWith("PO"))
                                {
                                    wPassword = subj[x];
                                    break;
                                }
                            }
                        }

                        if (!String.IsNullOrEmpty(wPassword))
                        {
                            oMissingPass = wPassword;
                        }
                        else
                        {
                            oMissingPass = System.Reflection.Missing.Value;
                        }

                    }
                    else
                    {
                        oMissingPass = System.Reflection.Missing.Value;
                    }

                    object TRUE_VALUE = true;
                    object FALSE_VALUE = false;
                    object oMissing = System.Reflection.Missing.Value;

                    Microsoft.Office.Interop.Word.Application word = new Microsoft.Office.Interop.Word.Application();

                    // Get list of Word files in specified directory
                    word.Visible = false;
                    word.ScreenUpdating = false;


                    string NewName = Path.Combine(Path.GetDirectoryName(Filename), Path.GetFileNameWithoutExtension(Filename) + ".pdf");
                    SetStatus("Converting file to: " + NewName, SqlConnection);


                    // Cast as Object for word Open method
                    Object source_filename = (Object)Filename;
                    Object filename = (Object)NewName;


                    bool fExists = File.Exists(Filename);
                    SetStatus("Opening file: [" + fExists.ToString() + "] " + source_filename, SqlConnection);

                    Microsoft.Office.Interop.Word.Document doc = null;
                    bool retry = false;
                    try
                    {
                        if (pRes && string.IsNullOrEmpty(wPassword))
                        {
                            ((_Application)word).Quit(ref oMissing, ref oMissing, ref oMissing);
                            word = null;
                            return source_filename.ToString();
                        }
                        else

                            doc = word.Documents.Open(ref source_filename, ref FALSE_VALUE,
                                ref TRUE_VALUE, ref FALSE_VALUE, ref oMissingPass, ref oMissing, ref oMissing,
                                ref oMissing, ref oMissing, ref oMissing, ref oMissing, ref oMissing,
                                ref oMissing, ref oMissing, ref oMissing, ref oMissing);
                    }
                    catch (Exception e)
                    {
                        ((_Application)word).Quit(ref oMissing, ref oMissing, ref oMissing);
                        word = null;
                        return source_filename.ToString();
                    }

                    retry = false;
                    if (retry)
                    {
                        try
                        {
                            Object newHTMLName = (Object)Filename;
                            newHTMLName = source_filename + ".html";

                            File.Move(source_filename.ToString(), newHTMLName.ToString());
                            string res = FileTransformHTML(newHTMLName.ToString(), ConvertionParams, SqlConnection, ref eMessage);
                            return res;

                        }
                        catch (Exception e)
                        {
                            return "";
                        }
                    }


                    SetStatus("Activating document: ", SqlConnection);
                    doc.Activate();


                    SetStatus("Activated", SqlConnection);
                    //object outputFileName = wordFile.FullName.Replace(".doc", ".pdf");
                    object fileFormat = WdSaveFormat.wdFormatPDF;
                    SetStatus("Setting format", SqlConnection);

                    if (Orientation == 1 || Path.GetExtension(Filename).ToLower() == "msg")
                        doc.PageSetup.Orientation = WdOrientation.wdOrientLandscape;

                    SetStatus("About to save to PDF: " + filename, SqlConnection);
                    // Save document into PDF Format
                    //doc.FitToPages();
                    doc.SaveAs(ref filename,
                        ref fileFormat, ref oMissing, ref oMissing,
                        ref oMissing, ref oMissing, ref oMissing, ref oMissing,
                        ref oMissing, ref oMissing, ref oMissing, ref oMissing,
                        ref oMissing, ref oMissing, ref oMissing, ref oMissing);


                    SetStatus("Saved, closing", SqlConnection);
                    // Close the Word document, but leave the Word application open.
                    // doc has to be cast to type _Document so that it will find the
                    // correct Close method.                



                    object saveChanges = WdSaveOptions.wdDoNotSaveChanges;
                    ((_Document)doc).Close(ref saveChanges, ref oMissing, ref oMissing);
                    doc = null;


                    SetStatus("Saved, quitting", SqlConnection);
                    // word has to be cast to type _Application so that it will find
                    // the correct Quit method.
                    ((_Application)word).Quit(ref oMissing, ref oMissing, ref oMissing);
                    word = null;


                    SetStatus("Conversion complete: " + NewName, SqlConnection);
                    return NewName;
                }
                else
                {
                    return "";
                }
            }
            catch (Exception ex)
            {
                
                SetStatus("Conversion error: " + ex.Message, SqlConnection);
                eMessage = ex.Message;
                return Filename;
            }

            return "";
        }
    }
}
