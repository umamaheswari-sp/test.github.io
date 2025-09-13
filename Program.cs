
using PnP.Core.Model.SharePoint;
using PnP.Core.QueryModel;
using PnP.Core.Services;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Remoting.Contexts;
using System.Security;
using System.Text;
using System.Threading.Tasks;

namespace NAA_WebBilling_CreateMaitenanceInvoice_SPO
{
    public class Program
    {
        static String strSiteUrl = Convert.ToString(ConfigurationManager.AppSettings["OnPremSiteUrl"]);
        static String ConfigList = Convert.ToString(ConfigurationManager.AppSettings["ConfigList"]);
        static string driverPath = Convert.ToString(ConfigurationManager.AppSettings["driverPath"]);//for Stage it's D:/ and for prod its E:/
        
        static String ErrorLogList, FssArchivedUrl, strTempLocation, strSFTPDestinationLocation, strSFTPHost, strSFTPUserName, strSFTPPassword;
        static int SFTPPort;
        static bool blnLogging;
        static string logFileName = "OPS_Merchant_Maintenance_Invoice_Creation_" + DateTime.Now.ToString("dd_MM_yyyy_HH_mm_ss");

        static  void Main(string[] args)
        {
            Main_Async();
        }
        public static async Task Main_Async()
        {
            await LoadConfigurationList();
            await CreateInvoiceXML();
            Log("Process Finished");
            await UploadLogFile("OPS_Merchant_Maintenance_Invoice_Creation");
        }
        private static async Task<bool> UploadLogFile(string exceptionMessage)
        {
            bool retVal = false;

            try
            {
                using (PnPContext context = await PnPSharePointContext.GetPnPContext(strSiteUrl))
                {
                    if (blnLogging && !string.IsNullOrEmpty(exceptionMessage))
                    {
                        // Get the error log list
                        var list = await context.Web.Lists.GetByTitleAsync(ErrorLogList);

                        // Create new list item
                        var newItem = await list.Items.AddAsync(new Dictionary<string, object>
                        {
                            { "Title", exceptionMessage },
                            { "ExceptionLogs", exceptionMessage }
                        });

                        // Generate timestamped file name
                        string dt = DateTime.Now.ToString("yyyyMMdd-HHmm");
                        string fileName = $"{logFileName}_{dt}.txt";
                        string originalFilePath = Path.Combine(strTempLocation, $"{logFileName}.txt");
                        string renamedFilePath = Path.Combine(strTempLocation, fileName);

                        // Copy and rename the file
                        File.Copy(originalFilePath, renamedFilePath, true);

                        // Read file bytes
                        byte[] fileBytes = File.ReadAllBytes(renamedFilePath);

                        using (var stream = new MemoryStream(fileBytes))
                        {
                            await newItem.AttachmentFiles.AddAsync(fileName, stream);
                        }
                        File.Delete(renamedFilePath);

                        retVal = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error uploading log file: {ex.Message}");
            }

            return retVal;
        }

        private static async Task LoadConfigurationList()
        {
            try
            {
                Log("Load Configuration List");
                using (PnPContext srcContext = await PnPSharePointContext.GetPnPContext(strSiteUrl))
                {
                    string title, value;
                    string camlQuery = "";
                    IListItemCollection items = await GetListItems(strSiteUrl, ConfigList, camlQuery);
                    foreach (var item in items.AsRequested())
                    {

                        title = item["Title"].ToString();
                        value = item["Value"].ToString();
                        switch (title)
                        {
                            case "ErrorLogList":
                                ErrorLogList = value;
                                break;
                            case "TempFileLocation":
                                strTempLocation = value;
                                break;
                            case "logFileName":
                                logFileName = value;
                                break;
                            case "Logging":
                                blnLogging = bool.Parse(value);
                                break;
                            case "FssArchivedUrl":
                                FssArchivedUrl = value;
                                break;

                            case "SFTPDestinationLocation":
                                strSFTPDestinationLocation = value;
                                break;
                            case "SFTPHost":
                                strSFTPHost = value;
                                break;
                            case "SFTPUserName":
                                strSFTPUserName = value;
                                break;
                            case "SFTPPassword":
                                strSFTPPassword = value;
                                break;
                            case "SFTPPort":
                                SFTPPort = Convert.ToInt32(value);
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log("Error in Load Configuration List : " + ex.Message);
            }

        }
        public static async Task CreateInvoiceXML()
        {
            try
            {
                using (PnPContext srcContext = await PnPSharePointContext.GetPnPContext(strSiteUrl))
                {
                    Log("Started CreateInvoiceXML");
                    string query = "<View><Query><Where><And><Eq><FieldRef Name='Status' /><Value Type='Choice'>Pending Invoice Upload</Value></Eq><Eq><FieldRef Name='Portal' /><Value Type='Lookup'>OPS MERCHANT</Value></Eq></And></Where></Query></View>";
                    IListItemCollection items = await GetListItems(strSiteUrl, "CL_Maintenance_Invoice_Queue", query);


                    foreach (IListItem itm in items)
                    {

                        bool isValid = true;
                        MaintenanceInvoice objMI = new MaintenanceInvoice();

                        objMI.invoiceNumber = Convert.ToString(itm["InvoiceNumber"]);
                        DateTime dateInvoiceDate = (DateTime)itm["InvoiceDate"];
                        objMI.InvoiceDate = dateInvoiceDate.ToString("MM'/'dd'/'yyyy");
                        objMI.OrderDate = dateInvoiceDate.ToString("MM'/'dd'/'yyyy");
                        objMI.TaxTotal = Convert.ToString(itm["Tax"]);
                        objMI.SubTotal = Convert.ToString(itm["NetServiceContractAmount"]);
                        objMI.Freight = Convert.ToString(itm["Freight"]);
                        objMI.GrandTotal = Convert.ToString(itm["TotalAmountDue"]);
                        objMI.AccountNumber = Convert.ToString(itm["CustomerNumber"]);
                        objMI.CompanyName = Convert.ToString(itm["Company"]);
                        objMI.InvoiceURL = itm["InvoiceURL"] != null ? Convert.ToString(((FieldUrlValue)itm["InvoiceURL"]).Url) : "";



                        DateTime dateServiceFromDate = (DateTime)itm["ServiceFromDate"];
                        objMI.ServiceFromDate = dateServiceFromDate.ToString("MM'/'dd'/'yyyy");
                        DateTime dateServiceEndDate = (DateTime)itm["ServiceEndDate"];
                        objMI.ServiceEndDate = dateServiceEndDate.ToString("MM'/'dd'/'yyyy");


                        string statusExplanation = string.Empty;
                        if (
                               string.IsNullOrEmpty(objMI.invoiceNumber)
                            || string.IsNullOrEmpty(objMI.InvoiceDate)
                            || string.IsNullOrEmpty(objMI.ServiceFromDate)
                            || string.IsNullOrEmpty(objMI.ServiceEndDate)
                            || string.IsNullOrEmpty(objMI.OrderDate)
                            || string.IsNullOrEmpty(objMI.TaxTotal)
                            || string.IsNullOrEmpty(objMI.SubTotal)
                            || string.IsNullOrEmpty(objMI.GrandTotal)
                            || string.IsNullOrEmpty(objMI.AccountNumber)
                            || string.IsNullOrEmpty(objMI.CompanyName)
                            || string.IsNullOrEmpty(objMI.InvoiceURL)
                            )
                        {
                            isValid = false;
                            statusExplanation = "Values can't be blank";
                        }
                        if (!isValid)
                        {
                           await SetSentInvoiceStatus(objMI.invoiceNumber, "Need Attention", statusExplanation);
                        }

                        else
                        {
                            string xmlTemplate = System.IO.File.ReadAllText(driverPath + "OpsExcelFileTmpl\\XMLTemplateMaintenance.xml");
                            using (PnPContext fssArchiveCtx = await PnPSharePointContext.GetPnPContext(FssArchivedUrl))
                            {
                                

                                xmlTemplate = xmlTemplate.Replace("{Quantity}", "1");
                                xmlTemplate = xmlTemplate.Replace("{UnitPrice}", objMI.SubTotal);
                                xmlTemplate = xmlTemplate.Replace("{UnitofMeasure}", "Hr");
                                xmlTemplate = xmlTemplate.Replace("{Description}", "Maintenance Service from " + objMI.ServiceFromDate + " to " + objMI.ServiceEndDate);
                                xmlTemplate = xmlTemplate.Replace("{LineTotal}", objMI.SubTotal);
                                xmlTemplate = xmlTemplate.Replace("{InvoiceNumber}", objMI.invoiceNumber);
                                xmlTemplate = xmlTemplate.Replace("{InvoiceDate}", objMI.InvoiceDate);
                                xmlTemplate = xmlTemplate.Replace("{OrderDate}", objMI.OrderDate);
                                xmlTemplate = xmlTemplate.Replace("{TaxTotal}", objMI.TaxTotal);
                                xmlTemplate = xmlTemplate.Replace("{SubTotal}", objMI.SubTotal);
                                xmlTemplate = xmlTemplate.Replace("{Freight}", objMI.Freight);
                                xmlTemplate = xmlTemplate.Replace("{GrandTotal}", objMI.GrandTotal);
                                xmlTemplate = xmlTemplate.Replace("{AccountNumber}", objMI.AccountNumber);
                                //xmlTemplate = xmlTemplate.Replace("{ImageURL}", sharingFileLink);

                                string xmlFileName = strTempLocation + objMI.invoiceNumber + "_" + DateTime.Now.ToString("dd_MM_yyyy_HH_mm_ss") + ".xml";
                                FileStream fs = System.IO.File.Create(xmlFileName);
                                fs.Dispose();
                                System.IO.File.WriteAllText(xmlFileName, xmlTemplate);

                                FileInfo fileInfo = new System.IO.FileInfo(xmlFileName);
                                bool exists = fileInfo.Exists;

                                if (exists)
                                {
                                    bool isUploadedToSFTP;
                                    isUploadedToSFTP = UploadToSFTPServer(strSFTPHost, strSFTPUserName, strSFTPPassword, xmlFileName, strSFTPDestinationLocation, SFTPPort, objMI.CompanyName);
                                    if (isUploadedToSFTP)
                                    {
                                        Log("XML File: " + xmlFileName + " uploaded successfully to SFTP Server.");
                                        //write a code for uploading xml to sharepoint doc lib
                                        bool isUploadXMLToDocLib = false;
                                        isUploadXMLToDocLib = await UploadXMLToWebBillingSite(xmlFileName, objMI.invoiceNumber, objMI.InvoiceURL);
                                        if (isUploadXMLToDocLib)
                                        {
                                            Log("XML File: " + xmlFileName + " uploaded successfully to WebBilling Site.");

                                            await SetSentInvoiceStatus(objMI.invoiceNumber, "Invoice Sent to Ops Merchant", "Invoice Sent to Ops Merchant");
                                        }
                                     
                                    }
                                    else
                                    {
                                        //TODO:Set log -> File didn't upload on SPO Doc Lib
                                        //Log("Error in uploading Invoice File: " + fileName);
                                    }
                                }

                            }
                        }
                    }

                }
            }
            catch (Exception ex)
            {

                Log("Error in CreateInvoiceXML " + ex.Message);
            }

        }
        public static async Task<string> GetCustEmail(string CustomerNumber)
        {
            string custEmail = string.Empty;
            try
            {
                using (PnPContext srcContext = await PnPSharePointContext.GetPnPContext(strSiteUrl))
                {
                    string query = "<View><Query><Where><Eq><FieldRef Name='Title' /><Value Type='Text'>" + CustomerNumber + "</Value></Eq></Where></Query></View>";
                    IListItemCollection items =await GetListItems(strSiteUrl, "CL_CustomerMaster", query);
                    IListItem oItem;
                    oItem = items.FirstOrDefault();
                    custEmail = Convert.ToString(oItem["CustomerEmail"]);
                }
            }
            catch (Exception ex)
            {
                Log("Error in GetCustEmail " + ex.Message);
            }
            return custEmail;
        }
        public static async Task<bool> SetSentInvoiceStatus(string invoiceNumber, string status, string statusExplanation)
        {
            bool isInvoiceSent = false;
            try
            {
                using (PnPContext srcContext = await PnPSharePointContext.GetPnPContext(strSiteUrl))
                {
                 
                    string  query = "<View><Query><Where><And><Eq><FieldRef Name='InvoiceNumber' /><Value Type='Text'>" + invoiceNumber + "</Value></Eq><Eq><FieldRef Name='Portal' /><Value Type='Lookup'>OPS MERCHANT</Value></Eq></And></Where></Query></View>";

                    IListItemCollection items = await GetListItems(strSiteUrl, "CL_Maintenance_Invoice_Queue", query);

                    IListItem oItem;
                    oItem = items.FirstOrDefault();
                    //Rename from SFTPStatus to Status
                    oItem["Status"] = status;
                    oItem["StatusExplanation"] = statusExplanation;
                    oItem.Update();
                    srcContext.Execute();
                    Log("Invoice status updated for " + invoiceNumber);
                }
            }
            catch (Exception ex)
            {
                Log("Error in SetSentInvoiceStatus " + ex.Message);
            }
            return isInvoiceSent;
        }

        public static async Task<bool> UploadXMLToWebBillingSite(string fileName, string invoiceNumber, string invoiceURL)
        {
            bool isUploadXMLToDocLib = false;
            try
            {
                Log("Started UploadXMLToWebBillingSite");
                using (PnPContext srcContext = await PnPSharePointContext.GetPnPContext(strSiteUrl))
                {
                    var library = await srcContext.Web.Lists.GetByTitleAsync("DL_OpsMerchantXMLRepository");

                    byte[] fileContent = File.ReadAllBytes(fileName);
                    

                    using (var stream = new MemoryStream(fileContent))
                    {
                        string fileNameOnly = Path.GetFileName(fileName);

                        var uploadedFile = await library.RootFolder.Files.AddAsync(fileNameOnly, stream, true);
                        var listItem = uploadedFile.ListItemAllFields;

                        FieldUrlValue objFieldUrlValueInvoice = new FieldUrlValue(invoiceURL, invoiceNumber
                            );
                        listItem["InvoiceURL"] = objFieldUrlValueInvoice;
                        listItem["InvoiceType"] = "Maintenance";

                        // Commit changes
                        await listItem.UpdateAsync();

                        isUploadXMLToDocLib = true;
                    }

                }
            }
            catch (Exception ex)
            {
                Log("Error in UploadXMLToWebBillingSite : " + ex.Message);
            }
            return isUploadXMLToDocLib;
        }
        public static bool UploadToSFTPServer(string host, string username, string password, string sourcefile, string destinationpath, int port, string companyName)
        {
            bool isUploadedToSFTP = false;
            Log("Started UploadToSFTPServer");
            try
            {
                using (SftpClient client = new SftpClient(host, port, username, password))
                {
                    client.Connect();
                    destinationpath = destinationpath + companyName; //+ "Test_Invoices" ///For staging it's Test_Invoices
                    client.ChangeDirectory(destinationpath);
                    using (FileStream fs = new FileStream(sourcefile, FileMode.Open))
                    {
                        client.BufferSize = 4 * 1024;
                        client.UploadFile(fs, Path.GetFileName(sourcefile));
                        isUploadedToSFTP = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Log("Error in UploadToSFTPServer : " + ex.Message);
            }
            return isUploadedToSFTP;
        }
        public static async Task<IListItemCollection> GetListItems(string csiteUrl, string listName, string camlQuery)
        {
            IListItemCollection listItems = null;
            try
            {
                using (PnPContext ctx = await PnPSharePointContext.GetPnPContext(csiteUrl))
                {
                    ctx.Execute();
                    IList list = ctx.Web.Lists.GetByTitle(listName);
                    var output = await list.LoadListDataAsStreamAsync(new PnP.Core.Model.SharePoint.RenderListDataOptions()
                    {
                        ViewXml = camlQuery,
                        RenderOptions = RenderListDataOptionsFlags.ListData,

                    }).ConfigureAwait(false);

                    if (list.Items.Length > 0)
                    {
                        listItems = list.Items;
                    }

                    ctx.Execute();
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return listItems;
        }


        static void Log(string logMessage)
        {
            try
            {
                Console.WriteLine(logMessage);
                if (blnLogging)
                {

                    using (StreamWriter w = new StreamWriter(strTempLocation + logFileName + ".txt", true))
                    {
                        w.WriteLine($"{DateTime.Now.ToLongDateString()} {DateTime.Now.ToLongTimeString()}" + ": " + logMessage);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }

}
