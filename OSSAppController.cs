/// Name:CRM API to get Rates from Synapse DB 
/// Purpose:Getting the Rates for Special Handling & Standard Rates from Synapse DB using API and add the rates information Callback edit form & SH Web billing form
/// Getting Special handling data from CRM
/// 1066700 - Merging multi T-Bill/Primary Bill emails and adding into sharepoint Email column - Start
/// 1114234 - Include Status condition while retrieving billing rates for Callback
/// 1052312 - AR Support Workflow Request-Revised Invoice (No write off)
/// 1115241 - T&M Material Markup Integration
/// 1166858 - Email Recipient - Remove Inactive T-Billing Contact from Invoice Deliveries
/// 1213924,1336067: Retrive Consumable fee from CRM on contract level and add to callback
/// 1341118: Branch couldn't bill CMZ callbacks
/// Modified By: Kavya M
/// Modified Date: 2 Sep 2025
using Microsoft.IdentityModel.Tokens;
using NAA_OSS_Synapse_API;
using Newtonsoft.Json;
using PnP.Core;
using PnP.Core.Model.SharePoint;
using PnP.Core.QueryModel;
using PnP.Core.Services;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.Remoting.Contexts;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Cors;
using System.Web.UI.WebControls;
using System.Xml;

namespace NAA_OSS_Synapse_API.Controllers
{
    /// <summary>
    /// 1077457-Getting the contract status from CRM using JDE Number/Customer number in Collections
    /// </summary>
    public class ContractStatus
    {
        public string JDENo { get; set; }
    }
    /// <summary>
    /// 1213924 - Retrive Consumable fee from CRM on contract level and add to callback - Permanent solution
    /// </summary>
    public class ApplyConsumableFee
    {
        public string JDENo { get; set; }
    }
    /// <summary>
    /// 1115241 - T&M Material Markup Integration
    /// </summary>
    public class MaterialMarkup
    {
        public string CRMQuoteNumber { get; set; }
    }
    /// <summary>
    /// 1052312-Getting the billing address from CRM using JDE Number/Customer number
    /// </summary>
    public class BillingAddress
    {
        public string JDENo { get; set; }
    }
    /// <summary>
    /// Getting the Rates date from Synapse DB we need to pass JDE,ApplicatioName,OfficePrefix & Service Date
    /// </summary>
    public class SHRatesFromJDE
    {
        public string JDENo { get; set; }
        public string ApplicationName { get; set; }
        public string OfficePrefix { get; set; }
        public DateTime ServiceDate { get; set; }

    }
    /// <summary>
    /// API POST Input paraments for Web Billig Data
    /// </summary>
    public class WebBillingData
    {
        public string ContractId { get; set; }
        public string Contract { get; set; }
        public string JDE { get; set; }
        public string ApplicationName { get; set; }

    }
    /// <summary>
    /// API POST Input paraments for SH Data load for Callback FPTM & Inspection
    /// </summary>
    public class SHMCRMDetails
    {
        public string MachineNo { get; set; }
        // public string QuoteNumber { get; set; }
        public string JDE { get; set; }
        public string ApplicationName { get; set; }
        public string TempUnitNumber { get; set; }

    }

    /// <summary>
    /// Generate Open Order #'s
    /// </summary>
    public class OOGenerateDetails
    {
        public string OfficePrefix { get; set; }
        public string ApplicationType { get; set; }
        public string ClaimedBy { get; set; }
        public string CallDetailId { get; set; }

    }

    public class OSSAppController : ApiController
    {
        /// <summary>
        /// API is to get Special Rates & Standard rates data based on JDE, Service Date & Application Name
        /// </summary>
        /// <param name="SHRates"></param>
        /// <returns>Rates Data</returns>
        [Route("api/ReadSHRates")]
        [HttpPost]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public async Task<IHttpActionResult> ReadSHRatesAsync([FromBody] SHRatesFromJDE SHRates)
        {
            try
            {
                Helper hlp = new Helper();
                HttpStatusCode cCode;

                var CLConfiguration = ConfigurationManager.AppSettings["CLConfiguration"];
                DataTable dtAccountRates = new DataTable();
                DataTable dtAccountGroupRates = new DataTable();
                DataTable dtAccountGroupRatesNAA = new DataTable();
                DataTable dtStandardRates = new DataTable();
                DataTable dtFiltered = new DataTable();
                PnPContext ctx = await PnPSharePointContext.GetPnPContext(Convert.ToString(ConfigurationManager.AppSettings["OSSSiteURL"]));
                using (ctx)
                {

                    var list = await ctx.Web.Lists.GetByTitleAsync("CL_Configurations", p => p.Title);

                    //var camlQuery = new CamlQuery() { ViewXml = "<View><Query><Where><Eq><FieldRef Name='Title' /><Value Type='Text'>DatabaseConfig</Value></Eq></Where></Query></View>" };
                    string viewXml = "<View><Query><Where><Eq><FieldRef Name='Title' /><Value Type='Text'>DatabaseConfig</Value></Eq></Where></Query></View>";
                    var output = await list.LoadListDataAsStreamAsync(new PnP.Core.Model.SharePoint.RenderListDataOptions()
                    {
                        ViewXml = viewXml,
                        RenderOptions = RenderListDataOptionsFlags.ListData,

                    }).ConfigureAwait(false);
                    // Get the Connection Sting value from CL_Configurations list
                    var ConnectStringValue = "";
                    foreach (var itm in list.Items.AsRequested())
                    {
                        try
                        {
                            ConnectStringValue = Convert.ToString(itm["Value"]);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message.ToString());
                        }
                    }


                    //string ConnectStringValue = "Server=naa-otis-gss-test-db.database.windows.net;Database=sscrm-naa-sit-synapse-db;User ID=sscrm_synapse_readonly;Password=4NDroiWY0UdcHyN8fwV8;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
                    SqlConnection connRates = new SqlConnection(ConnectStringValue);
                    try
                    {
                        // Open the SQL connect if it is in close State
                        if (connRates.State != System.Data.ConnectionState.Open)
                        {
                            connRates.Open();
                        }
                        // Select command to get Rates data based on Application Name & JDE
                        string grySelectCRMAccountRates = "";
                        string qrySelectAccountGroupRates = "";
                        string qrySelectAccountGroupRatesNAA = "";

                        string qryStandradRates = "";
                        if (SHRates.ApplicationName.Trim().ToUpper() == "CALLBACK")
                        {
                            if (!string.IsNullOrEmpty(SHRates.JDENo))
                            {
                                SHRates.JDENo = SHRates.JDENo.Trim();
                            }
                            grySelectCRMAccountRates = "SELECT Account.[accountnumber] AS AccountNumber ,Rates.[gsscore_branchname] Branch, Rates.gsscore_consumablefeename AS ConsumableFee, Rates.[gsscore_laborrate] AS Rate,Rates.[gsscore_effectivestart] AS StartDate, Rates.[gsscore_effectiveend] AS EndDate"
                                           + ",Rates.[gsscore_ratetypename] AS RateType, Rates.[gsscore_workertypename] AS WorkerType, Rates.[gsscore_ratenotes] AS SpecialInstruction, Rates.[gsscore_account] AS AccountID"
                                           + ",Rates.[gsscore_accountname] AS AccountName,Rates.[gsscore_listtypename] AS ListType,Rates.[gsscore_materialmarkup] AS MaterialMarkupRate FROM [vw_ApprovedBillingRates] AS Rates"
                                           + " JOIN [account] AS Account ON Rates.[gsscore_account] = Account.[accountID] WHERE Account.[accountnumber] = '" + SHRates.JDENo + "' AND Rates.[gsscore_listtypename] IN ('Account Rate', 'Account') AND Rates.statuscodename = 'Active' ORDER BY EndDate DESC";

                            qrySelectAccountGroupRates = "SELECT Rates.[gsscore_listtypename] AS ListType, Account.[accountnumber] AS AccountNumber"
                                             + ", Rates.[gsscore_branchname] AS Branch,Rates.gsscore_consumablefeename AS ConsumableFee,Rates.[gsscore_laborrate] AS Rate, Rates.[gsscore_effectivestart] AS StartDate, Rates.[gsscore_effectiveend] AS EndDate, Rates.[gsscore_ratetypename] AS RateType, Rates.[gsscore_workertypename] AS WorkerType"
                                             + " , Rates.[gsscore_ratenotes] AS SpecialInstruction, Rates.[gsscore_accountgroup] AS AccountGroupID, Rates.[gsscore_accountname] AS AccountName,Rates.[gsscore_materialmarkup] AS MaterialMarkupRate  FROM[dbo].[vw_ApprovedBillingRates] Rates JOIN [dbo].[account] Account"
                                             + " ON Rates.[gsscore_accountgroup] = Account.[gsscore_accountgroups] WHERE Rates.[gsscore_listtypename] IN ('Account Group Rate', 'Account Group') And Account.[accountnumber] = '" + SHRates.JDENo + "'"
                                             + " AND Rates.[gsscore_servicebranchnumber] = '" + SHRates.OfficePrefix + "' AND Rates.statuscodename = 'Active' ORDER BY EndDate DESC";

                            qrySelectAccountGroupRatesNAA = "SELECT Rates.[gsscore_listtypename] AS ListType, Account.[accountnumber] AS AccountNumber"
                                             + ", Rates.[gsscore_branchname] AS Branch,Rates.gsscore_consumablefeename AS ConsumableFee, Rates.[gsscore_laborrate] AS Rate, Rates.[gsscore_effectivestart] AS StartDate, Rates.[gsscore_effectiveend] AS EndDate, Rates.[gsscore_ratetypename] AS RateType, Rates.[gsscore_workertypename] AS WorkerType"
                                             + " , Rates.[gsscore_ratenotes] AS SpecialInstruction, Rates.[gsscore_accountgroup] AS AccountGroupID, Rates.[gsscore_accountname] AS AccountName,Rates.[gsscore_materialmarkup] AS MaterialMarkupRate  FROM[dbo].[vw_ApprovedBillingRates] Rates JOIN [dbo].[account] Account"
                                             + " ON Rates.[gsscore_accountgroup] = Account.[gsscore_accountgroups] WHERE Rates.[gsscore_listtypename] IN ('Account Group Rate', 'Account Group') And Account.[accountnumber] = '" + SHRates.JDENo + "'"
                                             + " AND Rates.[gsscore_servicebranchnumber] = 'NAA' AND Rates.statuscodename = 'Active' ORDER BY EndDate DESC";


                            qryStandradRates = "SELECT Rates.[gsscore_listtypename] AS ListType, Rates.[gsscore_branchname] Branch,Rates.gsscore_consumablefeename AS ConsumableFee, Rates.[gsscore_laborrate] AS Rate, Rates.[gsscore_effectivestart] AS StartDate"
                                                          + ", Rates.[gsscore_effectiveend] AS EndDate, Rates.[gsscore_ratetypename] AS RateType, Rates.[gsscore_workertypename] AS WorkerType, Rates.[gsscore_ratenotes] AS SpecialInstruction"
                                                          + ", Rates.[gsscore_account] AS AccountID, Rates.[gsscore_accountname] AS AccountName,Rates.[gsscore_materialmarkup] AS MaterialMarkupRate  FROM[dbo].[vw_ApprovedBillingRates] Rates WHERE Rates.[gsscore_listtypename] = 'Standard Rate' AND "
                                                          + " Rates.[gsscore_servicebranchnumber] = '" + SHRates.OfficePrefix + "' AND Rates.statuscodename = 'Active' ORDER BY EndDate DESC";

                        }
                        using (SqlCommand cmdRates = new SqlCommand(grySelectCRMAccountRates, connRates))
                        {
                            SqlDataAdapter daRatesAll = new SqlDataAdapter(cmdRates);
                            daRatesAll.Fill(dtAccountRates);
                            // 1st checking the account rates
                            if (dtAccountRates.Rows.Count > 0)
                            {

                                DataView dvAccountRates = dtAccountRates.DefaultView;
                                dtFiltered = dvAccountRates.ToTable();
                                if (dtFiltered.Rows.Count > 0)
                                {
                                    dtAccountRates = dtFiltered;
                                    dvAccountRates = dtAccountRates.DefaultView;
                                    // Filter the Datatable data with Start Date, EndDate with Service Date
                                    dvAccountRates.RowFilter = "StartDate <= #" + SHRates.ServiceDate + "# AND EndDate >= #" + SHRates.ServiceDate + "#";
                                    dtAccountRates = dvAccountRates.ToTable();
                                    dvAccountRates.RowFilter = string.Empty;

                                    if (dtAccountRates.Rows.Count > 0)
                                    {
                                        dtFiltered = dtAccountRates;
                                    }
                                    else
                                    {
                                        DataView dvAccountRatesEndate = dtFiltered.DefaultView;

                                        dtFiltered = dvAccountRatesEndate.ToTable();
                                        dvAccountRatesEndate.RowFilter = string.Empty;
                                        DataView dvEndDate = dtFiltered.DefaultView;
                                        dvEndDate.RowFilter = "#" + SHRates.ServiceDate + "# >= EndDate";
                                        dtFiltered = dvEndDate.ToTable();
                                        dvEndDate.RowFilter = string.Empty;
                                    }

                                    if (dtFiltered.Rows.Count == 0)
                                    {
                                        using (SqlCommand cmdStandardRates = new SqlCommand(qryStandradRates, connRates))
                                        {
                                            dtStandardRates = new DataTable();
                                            dtFiltered = new DataTable();
                                            SqlDataAdapter daStandardRates = new SqlDataAdapter(cmdStandardRates);
                                            daStandardRates.Fill(dtStandardRates);
                                            dtFiltered = dtStandardRates;
                                            if (dtStandardRates.Rows.Count > 0)
                                            {
                                                DataView dvStandarRates = dtStandardRates.DefaultView;
                                                dvStandarRates.RowFilter = "StartDate <= #" + SHRates.ServiceDate + "# AND EndDate >= #" + SHRates.ServiceDate + "#";
                                                dtStandardRates = dvStandarRates.ToTable();
                                                if (dtStandardRates.Rows.Count > 0)
                                                {
                                                    dtFiltered = dtStandardRates;
                                                }
                                                else
                                                {
                                                    dtStandardRates = dtFiltered;
                                                    DataView dvSHRatesEndate = dtStandardRates.DefaultView;
                                                    dvSHRatesEndate.RowFilter = "#" + SHRates.ServiceDate + "# >= EndDate";
                                                    dtFiltered = dvSHRatesEndate.ToTable();
                                                    dvSHRatesEndate.RowFilter = string.Empty;
                                                }
                                            }
                                        }
                                    }

                                }
                                else
                                {
                                    dtFiltered = new DataTable();
                                }

                                Console.WriteLine(Convert.ToString(dtFiltered.Rows.Count));
                            }
                            //If account rates count 0 checking account group rates
                            else
                            {
                                //Account Group rates
                                using (SqlCommand cmdAccountGroupRates = new SqlCommand(qrySelectAccountGroupRates, connRates))
                                {
                                    dtAccountGroupRates = new DataTable();
                                    DataView dvAccountGroupRates = new DataView();
                                    SqlDataAdapter daAccountGroupRates = new SqlDataAdapter(cmdAccountGroupRates);
                                    daAccountGroupRates.Fill(dtAccountGroupRates);
                                    dtFiltered = dtAccountGroupRates;
                                    if (dtAccountGroupRates.Rows.Count > 0)
                                    {
                                        dvAccountGroupRates = dtAccountGroupRates.DefaultView;
                                        // Filter the Datatable data with Start Date, EndDate with Service Date
                                        //Account Group Rates with Prefix exists
                                        if (dtAccountGroupRates.Rows.Count > 0)
                                        {
                                            DataView dvAGRates = dvAccountGroupRates;
                                            DataTable dtAGRates = dvAGRates.ToTable();

                                            dvAccountGroupRates = dtAccountGroupRates.DefaultView;
                                            dvAccountGroupRates.RowFilter = "StartDate <= #" + SHRates.ServiceDate + "# AND EndDate >= #" + SHRates.ServiceDate + "#";
                                            dtAccountGroupRates = dvAccountGroupRates.ToTable();
                                            dvAccountGroupRates.RowFilter = string.Empty;
                                            if (dtAccountGroupRates.Rows.Count > 0)
                                            {
                                                dtFiltered = dtAccountGroupRates;
                                            }
                                            else
                                            {
                                                DataView dvEndDate = dtAGRates.DefaultView;
                                                dvEndDate.RowFilter = "#" + SHRates.ServiceDate + "# >= EndDate";
                                                dtFiltered = dvEndDate.ToTable();
                                                dvEndDate.RowFilter = string.Empty;

                                                dtAccountGroupRates = dtFiltered;
                                            }
                                        }
                                    }
                                    //Account Group Rates with Prefix not exists default NAA
                                    if (dtAccountGroupRates.Rows.Count == 0)
                                    {
                                        using (SqlCommand cmdAccountGroupRatesNAA = new SqlCommand(qrySelectAccountGroupRatesNAA, connRates))
                                        {
                                            dtAccountGroupRatesNAA = new DataTable();
                                            DataView dvAccountGroupRatesNAA = new DataView();
                                            SqlDataAdapter daAccountGroupRatesNAA = new SqlDataAdapter(cmdAccountGroupRatesNAA);
                                            daAccountGroupRatesNAA.Fill(dtAccountGroupRatesNAA);
                                            dtFiltered = dtAccountGroupRatesNAA;

                                            dvAccountGroupRatesNAA = dtAccountGroupRatesNAA.DefaultView;

                                            // Filter the Datatable data with Start Date, EndDate with Service Date
                                            //dvAccountGroupRates.RowFilter = "Branch like '%NAA'";
                                            if (dvAccountGroupRatesNAA.Count > 0)
                                            {
                                                DataView dvAGNAARates = dvAccountGroupRatesNAA;
                                                DataTable dtAGNAARates = dvAGNAARates.ToTable();

                                                dvAccountGroupRatesNAA = dtAGNAARates.DefaultView;
                                                dvAccountGroupRatesNAA.RowFilter = "StartDate <= #" + SHRates.ServiceDate + "# AND EndDate >= #" + SHRates.ServiceDate + "#";
                                                dtAccountGroupRatesNAA = dvAccountGroupRatesNAA.ToTable();
                                                dvAccountGroupRatesNAA.RowFilter = string.Empty;
                                                if (dtAccountGroupRatesNAA.Rows.Count > 0)
                                                {
                                                    dtFiltered = dtAccountGroupRatesNAA;
                                                }
                                                else
                                                {
                                                    DataView dvAccountGroupRatesNAAEndate = dtAGNAARates.DefaultView;
                                                    dvAccountGroupRatesNAAEndate.RowFilter = "#" + SHRates.ServiceDate + "# >= EndDate";

                                                    dtFiltered = dvAccountGroupRatesNAAEndate.ToTable();
                                                    dvAccountGroupRatesNAAEndate.RowFilter = string.Empty;

                                                    dtAccountGroupRatesNAA = dtFiltered;
                                                }
                                            }

                                        }
                                    }

                                    // Standard Rates if no Account group rates with Prefix & Account Group NAA Rates
                                    if (dtAccountGroupRatesNAA.Rows.Count == 0 && dtAccountGroupRates.Rows.Count == 0)
                                    {
                                        using (SqlCommand cmdStandardRates = new SqlCommand(qryStandradRates, connRates))
                                        {
                                            dtStandardRates = new DataTable();
                                            SqlDataAdapter daStandardRates = new SqlDataAdapter(cmdStandardRates);
                                            daStandardRates.Fill(dtStandardRates);
                                            dtFiltered = dtStandardRates;
                                            if (dtStandardRates.Rows.Count > 0)
                                            {
                                                DataView dvStandarRates = dtStandardRates.DefaultView;
                                                dvStandarRates.RowFilter = "StartDate <= #" + SHRates.ServiceDate + "# AND EndDate >= #" + SHRates.ServiceDate + "#";
                                                dtStandardRates = dvStandarRates.ToTable();
                                                if (dtStandardRates.Rows.Count > 0)
                                                {
                                                    dtFiltered = dtStandardRates;
                                                }
                                                else
                                                {
                                                    dtStandardRates = dtFiltered;
                                                    DataView dvSHRatesEndate = dtStandardRates.DefaultView;
                                                    dvSHRatesEndate.RowFilter = "#" + SHRates.ServiceDate + "# >= EndDate";
                                                    dtFiltered = dvSHRatesEndate.ToTable();
                                                    dvSHRatesEndate.RowFilter = string.Empty;
                                                }
                                            }
                                        }

                                    }
                                }
                            }

                        }
                        cCode = HttpStatusCode.OK;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(Convert.ToString(ex.Message) + "--" + Convert.ToString(ex.StackTrace));
                        cCode = HttpStatusCode.NotFound;
                        dtFiltered = new DataTable();
                    }
                    finally
                    {
                        // Close the SQL connect if it is in open State
                        if (connRates.State == System.Data.ConnectionState.Open)
                        {
                            connRates.Close();
                            connRates.Dispose();
                        }
                    }
                }
                // Send the reponse to API
                string json = JsonConvert.SerializeObject(dtFiltered);
                return Content(cCode, json);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return Content(HttpStatusCode.InternalServerError, JsonConvert.SerializeObject("Unknown error occured-402."));
            }


        }

        /// <summary>
        /// API is to get Unit no & Contract ERP other data based on Application Name & JDE
        /// </summary>
        /// <param name="sSHUnitContractNo"></param>
        /// <returns>Unit no details</returns>
        [Route("api/ReadWebBillingData")]
        [HttpPost]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public async Task<IHttpActionResult> ReadWebBillingData([FromBody] WebBillingData sWebBillingData)
        {
            try
            {
                Helper hlp = new Helper();
                HttpStatusCode cCode;

                var CLConfiguration = ConfigurationManager.AppSettings["CLConfiguration"];
                DataTable dtWebBillingDataAll = new DataTable();
                DataTable dtWebBillingDataFiltered = new DataTable();
                PnPContext ctx = await PnPSharePointContext.GetPnPContext(Convert.ToString(ConfigurationManager.AppSettings["OSSSiteURL"]));
                using (ctx)
                {
                    var list = await ctx.Web.Lists.GetByTitleAsync(CLConfiguration, p => p.Title);

                    //var camlQuery = new CamlQuery() { ViewXml = "<View><Query><Where><Eq><FieldRef Name='Title' /><Value Type='Text'>DatabaseConfig</Value></Eq></Where></Query></View>" };
                    string viewXml = "<View><Query><Where><Eq><FieldRef Name='Title' /><Value Type='Text'>DatabaseConfig</Value></Eq></Where></Query></View>";
                    var output = await list.LoadListDataAsStreamAsync(new PnP.Core.Model.SharePoint.RenderListDataOptions()
                    {
                        ViewXml = viewXml,
                        RenderOptions = RenderListDataOptionsFlags.ListData,

                    }).ConfigureAwait(false);
                    // Get the Connection Sting value from CL_Configurations list
                    var ConnectStringValue = "";
                    foreach (var itm in list.Items.AsRequested())
                    {
                        try
                        {
                            ConnectStringValue = Convert.ToString(itm["Value"]);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message.ToString());
                        }
                    }
                    SqlConnection connWebBilling = new SqlConnection(ConnectStringValue);
                    try
                    {
                        // Open the SQL connect if it is in close State
                        if (connWebBilling.State != System.Data.ConnectionState.Open)
                        {
                            connWebBilling.Open();
                        }
                        // Select command to get Web Billing data 
                        string qryWebBilling = "";
                        if (sWebBillingData.ApplicationName.Trim().ToUpper() == "WEBBILLING")
                        {
                            qryWebBilling = "select cntr.Id AS ContractID, cntr.gsscore_contracterpnumber AS ContractERPNumber, a.accountnumber AS JDENumber, a.name AS CustomerName, b.internalemailaddress AS AccountManagerEmail, cntr.gsscore_thirdparty_systemname AS ThirdPartySystem, ISNULL(c_los.LocalizedLabel, '') AS LineOfServiceRequiresUpload, ISNULL(cntr.gsscore_uploadstartdate, '') AS UploadStartDate, ISNULL(cntr.gsscore_nottoexceedincreasesrequiredpriortoupload, '') AS AreNTEIncreasesRequiredPriorUpload, ISNULL(cntr.gsscore_whorequestsnottoexceedincreases, '') AS WhoRequestsNTEIncreases, ISNULL(cntr.gsscore_whocompletesworkorder, '') AS WhoCompletesWorkOrder, ISNULL(cntr.gsscore_webbillingcomments, '') AS Comments, ISNULL(cntr.gsscore_topenorderpo, '') AS PONumber, cntr.gsscore_topenorderporequired AS POWORequired from salesorder cntr (NOLOCK) inner join account a (NOLOCK) on a.Id = cntr.customerid inner join team t (NOLOCK) on t.id = a.ownerid left join systemuser b (NOLOCK) on b.Id = t.gsscore_portfolioownerid left join GlobalOptionSetMetadata c_los (NOLOCK) on c_los.[Option] = cntr.gsscore_lineofservicethatrequiresupload and c_los.LocalizedLabelLanguageCode = 1033 and c_los.EntityName = 'salesorder' and c_los.OptionSetName = 'gsscore_lineofservicethatrequiresupload' "
                                + " where cntr.Id = '" + sWebBillingData.ContractId + "' and cntr.gsscore_contracterpnumber = '" + sWebBillingData.Contract + "' and a.accountnumber = '" + sWebBillingData.JDE + "' and cntr.gsscore_thirdparty_systemname IS NOT NULL";
                        }

                        using (SqlCommand cmrWebBilling = new SqlCommand(qryWebBilling, connWebBilling))
                        {
                            SqlDataAdapter daRatesAll = new SqlDataAdapter(cmrWebBilling);
                            daRatesAll.Fill(dtWebBillingDataAll);

                            if (dtWebBillingDataAll.Rows.Count > 0)
                            {
                                dtWebBillingDataAll.Columns.Add("PORequried");
                                if (!string.IsNullOrEmpty(Convert.ToString(dtWebBillingDataAll.Rows[0]["PONumber"])))
                                {
                                    dtWebBillingDataAll.Rows[0]["PONumber"] = dtWebBillingDataAll.Rows[0]["PONumber"];
                                }
                                else
                                {
                                    dtWebBillingDataAll.Rows[0]["PONumber"] = "";
                                }
                                if (!string.IsNullOrEmpty(Convert.ToString(dtWebBillingDataAll.Rows[0]["POWORequired"])))
                                {
                                    if (Convert.ToBoolean(dtWebBillingDataAll.Rows[0]["POWORequired"]))
                                    {
                                        dtWebBillingDataAll.Rows[0]["PORequried"] = "Yes";
                                    }
                                    else
                                    {
                                        dtWebBillingDataAll.Rows[0]["PORequried"] = "No";
                                    }
                                }
                                else
                                {
                                    dtWebBillingDataAll.Rows[0]["PORequried"] = "No";
                                }
                                dtWebBillingDataFiltered = dtWebBillingDataAll;
                                Console.WriteLine(Convert.ToString(dtWebBillingDataFiltered.Rows));
                            }
                            cCode = HttpStatusCode.OK;
                        }

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(Convert.ToString(ex.Message) + "--" + Convert.ToString(ex.StackTrace));
                        cCode = HttpStatusCode.NotFound;
                        dtWebBillingDataFiltered = new DataTable();
                        hlp.addToLog(2, "Information", "ReadWebBillingData(): " + ex.Message.ToString() + "--" + ex.StackTrace.ToString());

                    }
                    finally
                    {
                        // Close the SQL connect if it is in open State
                        if (connWebBilling.State == System.Data.ConnectionState.Open)
                        {
                            connWebBilling.Close();
                            connWebBilling.Dispose();
                        }
                    }
                }
                // Send the reponse to API
                string json = JsonConvert.SerializeObject(dtWebBillingDataFiltered);
                return Content(cCode, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return Content(HttpStatusCode.InternalServerError, JsonConvert.SerializeObject("Unknown error occured.-531"));
            }


        }


        /// <summary>
        /// API is to get Machine No and other details information based on Application Name & JDE
        /// </summary>
        /// <param name="sSHMCRMDetails"></param>
        /// <returns>Unit no details</returns>
        [Route("api/ReadSpecialHandlingData")]
        [HttpPost]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public async Task<IHttpActionResult> ReadSpecialHandlingData([FromBody] SHMCRMDetails sSHMCRMDetails)
        {
            Helper hlp = new Helper();
            try
            {
                HttpStatusCode cCode;
                var CLConfiguration = ConfigurationManager.AppSettings["CLConfiguration"];
                DataTable dtCallbackFPTMInspection = new DataTable();

                bool isPOFromCRM = false;

                PnPContext ctx = await PnPSharePointContext.GetPnPContext(Convert.ToString(ConfigurationManager.AppSettings["OSSSiteURL"]));
                using (ctx)
                {

                    var list = await ctx.Web.Lists.GetByTitleAsync(CLConfiguration);

                    var viewXml = "<View><Query><Where><Or><Eq><FieldRef Name='Title' /><Value Type='Text'>DatabaseConfig</Value></Eq><Or><Eq><FieldRef Name='Title' /><Value Type='Text'>EmailValidation</Value></Eq><Eq><FieldRef Name='Title' /><Value Type='Text'>GetPOFromCRM</Value></Eq></Or></Or></Where></Query></View>";
                    var output = await list.LoadListDataAsStreamAsync(new PnP.Core.Model.SharePoint.RenderListDataOptions()
                    {
                        ViewXml = viewXml,
                        RenderOptions = RenderListDataOptionsFlags.ListData,

                    }).ConfigureAwait(false);
                    // Get the Connection Sting value from CL_Configurations list
                    var ConnectStringValue = "";
                    String email_Validation = "";
                    foreach (var itm in list.Items.AsRequested())
                    {
                        try
                        {
                            if (Convert.ToString(itm["Title"]) == "DatabaseConfig")
                            {
                                ConnectStringValue = Convert.ToString(itm["Value"]);
                            }

                            if (Convert.ToString(itm["Title"]) == "EmailValidation")
                            {
                                email_Validation = Convert.ToString(itm["Value"]).Replace(",", string.Empty).Replace("{", string.Empty).Replace("}", string.Empty);
                            }
                            if (Convert.ToString(itm["Title"]) == "GetPOFromCRM")
                            {
                                if (!string.IsNullOrEmpty(Convert.ToString(itm["Value"])))
                                {
                                    isPOFromCRM = Convert.ToBoolean(itm["Value"]);
                                }
                            }


                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message.ToString());
                        }
                    }

                    char[] email_ValidationArr = email_Validation.ToCharArray();
                    SqlConnection connCallbackFPTMInspection = new SqlConnection(ConnectStringValue);
                    try
                    {
                        // Open the SQL connect if it is in close State
                        if (connCallbackFPTMInspection.State != System.Data.ConnectionState.Open)
                        {
                            connCallbackFPTMInspection.Open();
                        }
                        // Select command to get Unit no & Contract ERP other data based on Application Name & JDE
                        string qrySHDataForCallbackInsepction = "";
                        string qrySHDataForFPTM = "";

                        // Callback & Inspection SH details
                        if (sSHMCRMDetails.ApplicationName.Trim().ToUpper() == "CALLBACK" || sSHMCRMDetails.ApplicationName.Trim().ToUpper() == "INSPECTION")
                        {

                            if (!string.IsNullOrEmpty(sSHMCRMDetails.MachineNo))
                            {
                                sSHMCRMDetails.MachineNo = sSHMCRMDetails.MachineNo.Trim();
                            }
                            if (!string.IsNullOrEmpty(sSHMCRMDetails.JDE))
                            {
                                sSHMCRMDetails.JDE = sSHMCRMDetails.JDE.Trim();
                            }

                            // If both Machine # & JDE no available from POST call
                            if (!string.IsNullOrEmpty(sSHMCRMDetails.MachineNo) && !string.IsNullOrEmpty(sSHMCRMDetails.JDE))
                            {
                                if (isPOFromCRM)
                                {
                                    qrySHDataForCallbackInsepction = " select top(1) cntr.Id AS ContractId,a.accountnumber AS JDENumber,u.gsscore_uniterpnumber AS UnitERPNumber,u.gsscore_legacycontractnumber AS LegacyContractNumber,cntr.gsscore_contracterpnumber AS ContractERPNumber,br.gsscore_name AS Branch,cntr.gsscore_materialitemizationrequired AS MaterialItemizationRequired,cntr.gsscore_nottoexceedhours AS AmountOfHours,cntr.gsscore_entrapmentsbillable AS EntrapmentsBillable,cntr.gsscore_billforexpenses AS BillForExpenses,cntr.gsscore_billfortraveltime AS BillForTravelTime,cntr.gsscore_cannotbill_overamount AS CannotBillOverAmount,cntr.gsscore_elevatornamedesignationrequired AS ElevatorNameDesignationRequired,cntr.gsscore_CustomerReferenceNumber AS CustomerReferenceNumber,cntr.gsscore_totalinvoice_discountrate AS TotalInvoiceDiscountRate,cntr.gsscore_vehiclecharge AS VehicleChargeAmount,cntr.gsscore_date1paymentpcnt AS ConsumableFee,cntr.gsscore_vandalismbillable AS VandalismBillable,cntr.gsscore_AccountGroupCode AS AccountGroupCode,cntr.gsscore_taxexempt AS TaxExempt,cntr.gsscore_timeticketrequired AS TimeTicketRequired,c_tt.LocalizedLabel AS TimeTicketType,cntr.gsscore_topenorderpo AS PONumber,cntr.gsscore_topenorderporequired AS POWORequired,c_id.LocalizedLabel AS InvoiceDelivery,cntr.gsscore_traveltimecaphrs AS TravelTime,cntr.gsscore_specificwordingontopenorderinvoice AS SpecificWording,c_st.LocalizedLabel AS ContractStatus,c_bs.LocalizedLabel AS BusinessStream"
                                                                    + " from salesorder cntr (NOLOCK) inner join gsscore_unit u (NOLOCK) on u.gsscore_contract = cntr.Id inner join gsscore_Branch br (NOLOCK) on br.gsscore_branchid = cntr.gsscore_Branch inner join account a (NOLOCK) on a.Id = u.gsscore_account inner join StatusMetadata c_st (NOLOCK) on c_st.[Status] = cntr.statuscode and c_st.LocalizedLabelLanguageCode = 1033 and c_st.EntityName = 'salesorder' left join GlobalOptionSetMetadata c_tt (NOLOCK) on c_tt.[Option] = cntr.gsscore_timetickettype and c_tt.LocalizedLabelLanguageCode = 1033 and c_tt.EntityName = 'salesorder' and c_tt.OptionSetName = 'gsscore_timetickettype' left join GlobalOptionSetMetadata c_bs (NOLOCK) on c_bs.[Option] = cntr.gsscore_businessstream and c_bs.LocalizedLabelLanguageCode = 1033 and c_bs.EntityName = 'salesorder' and c_bs.OptionSetName = 'gsscore_businessstream' left join GlobalOptionSetMetadata c_id (NOLOCK) on c_id.[Option] = cntr.gsscore_tinvoicedeliverymethod and c_id.LocalizedLabelLanguageCode = 1033 and c_id.EntityName = 'salesorder' and c_id.OptionSetName = 'gsscore_tinvoicedeliverymethod' where c_st.LocalizedLabel IN('In Service','Expired - In Service','Suspended','Booked') and u.gsscore_uniterpnumber ='" + sSHMCRMDetails.MachineNo + "';"
                                                                  + " select a.Id AS AccountID, a.name AS AccountName,a.gsscore_portfolioownername AS AccountManager,b.internalemailaddress AS AccountManagerEmail,cr.[gsscore_englishname] AS EmailType,c.emailaddress1 AS PrimaryEmail,c.emailaddress2 AS InvoiceEmail,c.emailaddress3 AS StatementEmail from account a (NOLOCK) inner join team t (NOLOCK) on t.id=a.ownerid left join systemuser b (NOLOCK) on b.Id = t.gsscore_portfolioownerid left join [connection] cc (NOLOCK) on a.Id = cc.[record1id] and cc.[record1id_entitytype] ='account' left join contact c (NOLOCK) on c.Id = cc.[record2id] and cc.[record2id_entitytype] ='contact' left join [gsscore_connectionrole] cr (NOLOCK) on cr.Id = cc.[gsscore_connectionrole] left join StatusMetadata c_cc (NOLOCK) on c_cc.[Status] = cc.statuscode and c_cc.LocalizedLabelLanguageCode = 1033 and c_cc.EntityName = 'gsscore_connectionrole' where a.accountnumber = '" + sSHMCRMDetails.JDE + "' and cc.gsscore_invoicerecipient = '1' and c_cc.LocalizedLabel = 'Active'";
                                }
                                else
                                {
                                    qrySHDataForCallbackInsepction = " select top(1) cntr.Id AS ContractId,a.accountnumber AS JDENumber,u.gsscore_uniterpnumber AS UnitERPNumber,u.gsscore_legacycontractnumber AS LegacyContractNumber,cntr.gsscore_contracterpnumber AS ContractERPNumber,br.gsscore_name AS Branch,cntr.gsscore_materialitemizationrequired AS MaterialItemizationRequired,cntr.gsscore_nottoexceedhours AS AmountOfHours,cntr.gsscore_entrapmentsbillable AS EntrapmentsBillable,cntr.gsscore_billforexpenses AS BillForExpenses,cntr.gsscore_billfortraveltime AS BillForTravelTime,cntr.gsscore_cannotbill_overamount AS CannotBillOverAmount,cntr.gsscore_elevatornamedesignationrequired AS ElevatorNameDesignationRequired,cntr.gsscore_CustomerReferenceNumber AS CustomerReferenceNumber,cntr.gsscore_totalinvoice_discountrate AS TotalInvoiceDiscountRate,cntr.gsscore_vehiclecharge AS VehicleChargeAmount,cntr.gsscore_date1paymentpcnt AS ConsumableFee,cntr.gsscore_vandalismbillable AS VandalismBillable,cntr.gsscore_AccountGroupCode AS AccountGroupCode,cntr.gsscore_taxexempt AS TaxExempt,cntr.gsscore_timeticketrequired AS TimeTicketRequired,c_tt.LocalizedLabel AS TimeTicketType,cntr.gsscore_traveltimecaphrs AS TravelTime,cntr.gsscore_specificwordingontopenorderinvoice AS SpecificWording,c_st.LocalizedLabel AS ContractStatus,c_bs.LocalizedLabel AS BusinessStream"
                                                                + " from salesorder cntr (NOLOCK) inner join gsscore_unit u (NOLOCK) on u.gsscore_contract = cntr.Id inner join gsscore_Branch br (NOLOCK) on br.gsscore_branchid = cntr.gsscore_Branch inner join account a (NOLOCK) on a.Id = u.gsscore_account inner join StatusMetadata c_st (NOLOCK) on c_st.[Status] = cntr.statuscode and c_st.LocalizedLabelLanguageCode = 1033 and c_st.EntityName = 'salesorder' left join GlobalOptionSetMetadata c_tt (NOLOCK) on c_tt.[Option] = cntr.gsscore_timetickettype and c_tt.LocalizedLabelLanguageCode = 1033 and c_tt.EntityName = 'salesorder' and c_tt.OptionSetName = 'gsscore_timetickettype' left join GlobalOptionSetMetadata c_bs (NOLOCK) on c_bs.[Option] = cntr.gsscore_businessstream and c_bs.LocalizedLabelLanguageCode = 1033 and c_bs.EntityName = 'salesorder' and c_bs.OptionSetName = 'gsscore_businessstream' where c_st.LocalizedLabel IN('In Service','Expired - In Service','Suspended','Booked') and u.gsscore_uniterpnumber = '" + sSHMCRMDetails.MachineNo + "';"
                                                                + " select top(1) a.Id AS AccountID,a.name AS AccountName,a.gsscore_portfolioownername AS AccountManager,b.internalemailaddress AS AccountManagerEmail,cr.[gsscore_englishname] AS EmailType,c.emailaddress1 AS PrimaryEmail,c.emailaddress2 AS InvoiceEmail,c.emailaddress3 AS StatementEmail from account a (NOLOCK) inner join team t on t.id=a.ownerid left join systemuser b (NOLOCK) on b.Id = t.gsscore_portfolioownerid left join [connection] cc (NOLOCK) on a.Id = cc.[record1id] and cc.[record1id_entitytype] ='account' left join contact c (NOLOCK) on c.Id = cc.[record2id] and cc.[record2id_entitytype] ='contact' left join [gsscore_connectionrole] cr (NOLOCK) on cr.Id = cc.[gsscore_connectionrole] left join StatusMetadata c_cc (NOLOCK) on c_cc.[Status] = cc.statuscode and c_cc.LocalizedLabelLanguageCode = 1033 and c_cc.EntityName = 'gsscore_connectionrole' where a.accountnumber = '" + sSHMCRMDetails.JDE + "' and cc.gsscore_invoicerecipient = '1' and c_cc.LocalizedLabel = 'Active'";
                                }

                                DataSet dsetCallbackFPTMInspection = new DataSet();
                                using (SqlCommand crmCallbackInspection = new SqlCommand(qrySHDataForCallbackInsepction, connCallbackFPTMInspection))
                                {
                                    crmCallbackInspection.CommandTimeout = 90;
                                    SqlDataAdapter daCallbackFPTMInspection = new SqlDataAdapter(crmCallbackInspection);



                                    dtCallbackFPTMInspection = new DataTable();

                                    daCallbackFPTMInspection.Fill(dsetCallbackFPTMInspection);


                                    if (dsetCallbackFPTMInspection.Tables.Count > 1)
                                    {
                                        dtCallbackFPTMInspection = new DataTable();
                                        if (dsetCallbackFPTMInspection.Tables[0].Rows.Count > 0)
                                        {
                                            dtCallbackFPTMInspection.Merge(dsetCallbackFPTMInspection.Tables[0]);
                                            dtCallbackFPTMInspection = hlp.getDBValuesSet(dsetCallbackFPTMInspection.Tables[0], isPOFromCRM);
                                            dtCallbackFPTMInspection.Rows[0]["SHDataFound"] = true;
                                        }
                                    }

                                    //Set Default if Machine# no record found in DB
                                    if (dsetCallbackFPTMInspection.Tables.Count > 0)
                                    {
                                        if ((dsetCallbackFPTMInspection.Tables[0].Rows.Count == 0))
                                        {
                                            dtCallbackFPTMInspection = new DataTable();
                                            dtCallbackFPTMInspection.Rows.Add();
                                            dtCallbackFPTMInspection = hlp.getDefaultColumns(dtCallbackFPTMInspection);
                                            dtCallbackFPTMInspection.Rows[0]["SHDataFound"] = false;
                                        }

                                        //Update NonBillableHours to Output
                                        if (!string.IsNullOrEmpty(sSHMCRMDetails.JDE))
                                        {
                                            dtCallbackFPTMInspection.Rows[0]["NonBillableHours"] = GetNonBillableHours(connCallbackFPTMInspection, sSHMCRMDetails.JDE);
                                        }
                                        else
                                        {
                                            dtCallbackFPTMInspection.Rows[0]["NonBillableHours"] = false;
                                        }
                                    }

                                    //Apply filters for Inv devilery type and Email type
                                    if (dsetCallbackFPTMInspection.Tables.Count > 1)
                                    {
                                        DataTable dtAccountData = dsetCallbackFPTMInspection.Tables[1];
                                        if (dtAccountData.Rows.Count > 0 && isPOFromCRM)
                                        {
                                            DataTable dtGetAccountData = hlp.getDBValuesSetAccount(dtAccountData, dtCallbackFPTMInspection, isPOFromCRM, email_ValidationArr);
                                            if (dtGetAccountData.Rows.Count > 0)
                                            {
                                                dtCallbackFPTMInspection = dtGetAccountData;
                                            }
                                        }
                                        else
                                        {
                                            if (dtAccountData.Rows.Count > 0)
                                            {
                                                dtCallbackFPTMInspection.Rows[0]["AccountName"] = dtAccountData.Rows[0]["AccountName"];
                                                dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = dtAccountData.Rows[0]["AccountManagerEmail"];
                                            }
                                            else
                                            {
                                                dtCallbackFPTMInspection.Rows[0]["AccountName"] = "";
                                                dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = "";
                                            }
                                        }

                                    }
                                    DataTable dtCallbackFPTMInspectionFilter = new DataTable();
                                    dtCallbackFPTMInspectionFilter = dtCallbackFPTMInspection;

                                    DataView dvCallbackFPTMInspection = new DataView(dtCallbackFPTMInspectionFilter);

                                    dtCallbackFPTMInspection = new DataTable();
                                    if (isPOFromCRM)
                                    {
                                        dtCallbackFPTMInspection = dvCallbackFPTMInspection.ToTable("Selected", false,
                                            "JDENumber", "AccountName", "UnitERPNumber", "LegacyContractNumber", "ContractERPNumber", "Branch", "AmountOfHours", "NotToExceedOnHoursBilled",
                                            "CannotBillOverAmount", "CustomerReferenceNumber", "OtherCustomerReferenceRequired",
                                            "TotalInvoiceDiscountRate", "OverallDiscountOnTotalInvoiceAmount", "VehicleChargeAmount", "VehicleCharge", "ConsumableFee", "AccountGroupCode", "IsNSA", "TimeTicketType", "PONumber", "PORequried",
                                            "TravelTime", "DoesTheCustomerHaveATravelTimeRestriction", "ContractStatus", "AccountManager", "AccountManagerEmail", "EmailType", "InvoiceEmail",
                                            "InvoiceDelivery", "SpecificWording", "SpecificWordingOrAdditionalLanguageRequiredOnInvoice", "MaterialItemizationRequiredOnInvoice",
                                             "EntrapmentsBillableVal", "BillForExpensesVal", "BillForTravelTimeVal", "MaterialItemizationRequiredVal",
                                            "ElevatorNameDesignationRequiredVal", "VandalismBillableVal", "TaxExemptVal", "TimeTicketRequiredVal", "NonBillableHours", "SHDataFound", "BusinessStream", "AccountID", "ContractId");
                                    }
                                    else
                                    {
                                        dtCallbackFPTMInspection = dvCallbackFPTMInspection.ToTable("Selected", false,
                                           "JDENumber", "AccountName", "UnitERPNumber", "LegacyContractNumber", "ContractERPNumber", "Branch", "AmountOfHours", "NotToExceedOnHoursBilled",
                                           "CannotBillOverAmount", "CustomerReferenceNumber", "OtherCustomerReferenceRequired",
                                           "TotalInvoiceDiscountRate", "OverallDiscountOnTotalInvoiceAmount", "VehicleChargeAmount", "VehicleCharge", "ConsumableFee", "AccountGroupCode", "IsNSA", "TimeTicketType",
                                           "TravelTime", "DoesTheCustomerHaveATravelTimeRestriction", "ContractStatus", "AccountManager", "AccountManagerEmail",
                                           "SpecificWording", "SpecificWordingOrAdditionalLanguageRequiredOnInvoice", "MaterialItemizationRequiredOnInvoice",
                                            "EntrapmentsBillableVal", "BillForExpensesVal", "BillForTravelTimeVal", "MaterialItemizationRequiredVal",
                                           "ElevatorNameDesignationRequiredVal", "VandalismBillableVal", "TaxExemptVal", "TimeTicketRequiredVal", "NonBillableHours", "SHDataFound", "BusinessStream", "AccountID", "ContractId");
                                    }

                                }

                                cCode = HttpStatusCode.OK;
                            }


                            // If Machine # available & JDE no not available from POST call
                            else if (!string.IsNullOrEmpty(sSHMCRMDetails.MachineNo) && string.IsNullOrEmpty(sSHMCRMDetails.JDE))
                            {
                                if (isPOFromCRM)
                                {
                                    qrySHDataForCallbackInsepction = " select cntr.Id AS ContractId, a.accountnumber AS JDENumber,u.gsscore_uniterpnumber AS UnitERPNumber,u.gsscore_legacycontractnumber AS LegacyContractNumber,cntr.gsscore_contracterpnumber AS ContractERPNumber,br.gsscore_name AS Branch,cntr.gsscore_materialitemizationrequired AS MaterialItemizationRequired,cntr.gsscore_nottoexceedhours AS AmountOfHours,cntr.gsscore_entrapmentsbillable AS EntrapmentsBillable,cntr.gsscore_billforexpenses AS BillForExpenses,cntr.gsscore_billfortraveltime AS BillForTravelTime,cntr.gsscore_cannotbill_overamount AS CannotBillOverAmount,cntr.gsscore_elevatornamedesignationrequired AS ElevatorNameDesignationRequired,cntr.gsscore_CustomerReferenceNumber AS CustomerReferenceNumber,cntr.gsscore_totalinvoice_discountrate AS TotalInvoiceDiscountRate,cntr.gsscore_vehiclecharge AS VehicleChargeAmount,cntr.gsscore_vandalismbillable AS VandalismBillable,cntr.gsscore_AccountGroupCode AS AccountGroupCode,cntr.gsscore_taxexempt AS TaxExempt,cntr.gsscore_timeticketrequired AS TimeTicketRequired,c_tt.LocalizedLabel AS TimeTicketType,cntr.gsscore_topenorderpo AS PONumber,cntr.gsscore_topenorderporequired AS POWORequired,c_id.LocalizedLabel AS InvoiceDelivery,cntr.gsscore_traveltimecaphrs AS TravelTime,cntr.gsscore_specificwordingontopenorderinvoice AS SpecificWording,c_st.LocalizedLabel AS ContractStatus,c_bs.LocalizedLabel AS BusinessStream"
                                                                     + " from salesorder cntr (NOLOCK) inner join gsscore_unit u (NOLOCK) on u.gsscore_contract = cntr.Id inner join gsscore_Branch br (NOLOCK) on br.gsscore_branchid = cntr.gsscore_Branch inner join account a (NOLOCK) on a.Id = u.gsscore_account inner join StatusMetadata c_st (NOLOCK) on c_st.[Status] = cntr.statuscode and c_st.LocalizedLabelLanguageCode = 1033 and c_st.EntityName = 'salesorder' left join GlobalOptionSetMetadata c_tt (NOLOCK) on c_tt.[Option] = cntr.gsscore_timetickettype and c_tt.LocalizedLabelLanguageCode = 1033 and c_tt.EntityName = 'salesorder' and c_tt.OptionSetName = 'gsscore_timetickettype' left join GlobalOptionSetMetadata c_bs (NOLOCK) on c_bs.[Option] = cntr.gsscore_businessstream and c_bs.LocalizedLabelLanguageCode = 1033 and c_bs.EntityName = 'salesorder' and c_bs.OptionSetName = 'gsscore_businessstream' left join GlobalOptionSetMetadata c_id (NOLOCK) on c_id.[Option] = cntr.gsscore_tinvoicedeliverymethod and c_id.LocalizedLabelLanguageCode = 1033 and c_id.EntityName = 'salesorder' and c_id.OptionSetName = 'gsscore_tinvoicedeliverymethod' where c_st.LocalizedLabel IN('In Service','Expired - In Service','Suspended','Booked') and u.gsscore_uniterpnumber ='" + sSHMCRMDetails.MachineNo + "'";
                                }
                                else
                                {
                                    qrySHDataForCallbackInsepction = " select cntr.Id AS ContractId, a.accountnumber AS JDENumber,u.gsscore_uniterpnumber AS UnitERPNumber,u.gsscore_legacycontractnumber AS LegacyContractNumber,cntr.gsscore_contracterpnumber AS ContractERPNumber,br.gsscore_name AS Branch,cntr.gsscore_materialitemizationrequired AS MaterialItemizationRequired,cntr.gsscore_nottoexceedhours AS AmountOfHours,cntr.gsscore_entrapmentsbillable AS EntrapmentsBillable,cntr.gsscore_billforexpenses AS BillForExpenses,cntr.gsscore_billfortraveltime AS BillForTravelTime,cntr.gsscore_cannotbill_overamount AS CannotBillOverAmount,cntr.gsscore_elevatornamedesignationrequired AS ElevatorNameDesignationRequired,cntr.gsscore_CustomerReferenceNumber AS CustomerReferenceNumber,cntr.gsscore_totalinvoice_discountrate AS TotalInvoiceDiscountRate,cntr.gsscore_vehiclecharge AS VehicleChargeAmount,cntr.gsscore_vandalismbillable AS VandalismBillable,cntr.gsscore_AccountGroupCode AS AccountGroupCode,cntr.gsscore_taxexempt AS TaxExempt,cntr.gsscore_timeticketrequired AS TimeTicketRequired,c_tt.LocalizedLabel AS TimeTicketType,cntr.gsscore_traveltimecaphrs AS TravelTime,cntr.gsscore_specificwordingontopenorderinvoice AS SpecificWording,c_st.LocalizedLabel AS ContractStatus,c_bs.LocalizedLabel AS BusinessStream"
                                                                + " from salesorder cntr (NOLOCK) inner join gsscore_unit u (NOLOCK) on u.gsscore_contract = cntr.Id inner join gsscore_Branch br (NOLOCK) on br.gsscore_branchid = cntr.gsscore_Branch inner join account a (NOLOCK) on a.Id = u.gsscore_account inner join StatusMetadata c_st (NOLOCK) on c_st.[Status] = cntr.statuscode and c_st.LocalizedLabelLanguageCode = 1033 and c_st.EntityName = 'salesorder' left join GlobalOptionSetMetadata c_tt (NOLOCK) on c_tt.[Option] = cntr.gsscore_timetickettype and c_tt.LocalizedLabelLanguageCode = 1033 and c_tt.EntityName = 'salesorder' and c_tt.OptionSetName = 'gsscore_timetickettype' left join GlobalOptionSetMetadata c_bs (NOLOCK) on c_bs.[Option] = cntr.gsscore_businessstream and c_bs.LocalizedLabelLanguageCode = 1033 and c_bs.EntityName = 'salesorder' and c_bs.OptionSetName = 'gsscore_businessstream' where c_st.LocalizedLabel IN('In Service','Expired - In Service','Suspended','Booked') and u.gsscore_uniterpnumber = '" + sSHMCRMDetails.MachineNo + "'";

                                }

                                DataTable dtcallbackInspection = new DataTable();
                                DataTable dtAllData = new DataTable();
                                using (SqlCommand crmCallbackInspection = new SqlCommand(qrySHDataForCallbackInsepction, connCallbackFPTMInspection))
                                {
                                    crmCallbackInspection.CommandTimeout = 90;
                                    SqlDataAdapter daCallbackFPTMInspection = new SqlDataAdapter(crmCallbackInspection);

                                    dtCallbackFPTMInspection = new DataTable();

                                    daCallbackFPTMInspection.Fill(dtAllData);

                                    daCallbackFPTMInspection.Fill(dtcallbackInspection);

                                    DataTable dtOMaintenance = new DataTable();
                                    DataView dvOMaintenance = new DataView();

                                    DataTable dtTOpenOrder = new DataTable();
                                    DataView dvTOpenOrder = new DataView();

                                    DataTable dtDefault = new DataTable();
                                    DataView dvDefault = new DataView();


                                    dtOMaintenance = dtAllData;

                                    // get the JDE no based on Machine #
                                    if (dtcallbackInspection.Rows.Count > 0)
                                    {
                                        dtCallbackFPTMInspection = new DataTable();

                                        dtCallbackFPTMInspection = dtcallbackInspection;

                                        dtCallbackFPTMInspection = hlp.getDBValuesSet(dtcallbackInspection, isPOFromCRM);

                                        dvOMaintenance = dtOMaintenance.DefaultView;
                                        dvOMaintenance.RowFilter = "BusinessStream='O-Maintenance'";
                                        dtOMaintenance = dvOMaintenance.ToTable();
                                        dvOMaintenance.RowFilter = string.Empty;

                                        if (dtOMaintenance.Rows.Count > 0)
                                        {
                                            dtCallbackFPTMInspection = dtOMaintenance;
                                        }
                                        else
                                        {
                                            dtTOpenOrder = dtAllData;
                                            dvTOpenOrder = dtTOpenOrder.DefaultView;
                                            dvTOpenOrder.RowFilter = "BusinessStream='T-Open Order'";
                                            dtTOpenOrder = dvTOpenOrder.ToTable();
                                            dvTOpenOrder.RowFilter = string.Empty;

                                            if (dtTOpenOrder.Rows.Count > 0)
                                            {
                                                dtCallbackFPTMInspection = dtTOpenOrder;
                                            }
                                        }

                                        if (dtOMaintenance.Rows.Count > 0 || dtTOpenOrder.Rows.Count > 0)
                                        {
                                            if (dtOMaintenance.Rows.Count > 0)
                                            {
                                                dtOMaintenance = hlp.getDBValuesSet(dtOMaintenance, isPOFromCRM);
                                                dtCallbackFPTMInspection = dtOMaintenance;
                                            }
                                            else if (dtTOpenOrder.Rows.Count > 0)
                                            {
                                                dtOMaintenance = hlp.getDBValuesSet(dtTOpenOrder, isPOFromCRM);
                                                dtCallbackFPTMInspection = dtTOpenOrder;
                                            }

                                            dtCallbackFPTMInspection.Rows[0]["SHDataFound"] = true;

                                            string JDENumber = Convert.ToString(dtcallbackInspection.Rows[0]["JDENumber"]);

                                            if (string.IsNullOrEmpty(JDENumber))
                                            {
                                                JDENumber = "";
                                                dtCallbackFPTMInspection.Rows[0]["NonBillableHours"] = false;
                                            }
                                            else
                                            {
                                                JDENumber = JDENumber.Trim();
                                                //Update NonBillableHours to Output
                                                dtCallbackFPTMInspection.Rows[0]["NonBillableHours"] = GetNonBillableHours(connCallbackFPTMInspection, JDENumber);

                                            }
                                            string qrySHDataForCallbackInsepctionJDE = " select a.Id AS AccountID, a.name AS AccountName,a.gsscore_portfolioownername AS AccountManager,b.internalemailaddress AS AccountManagerEmail,cr.[gsscore_englishname] AS EmailType,c.emailaddress1 AS PrimaryEmail,c.emailaddress2 AS InvoiceEmail,c.emailaddress3 AS StatementEmail from account a (NOLOCK) inner join team t (NOLOCK) on t.id=a.ownerid left join systemuser b (NOLOCK) on b.Id = t.gsscore_portfolioownerid left join [connection] cc (NOLOCK) on a.Id = cc.[record1id] and cc.[record1id_entitytype] ='account' left join contact c (NOLOCK) on c.Id = cc.[record2id] and cc.[record2id_entitytype] ='contact' left join [gsscore_connectionrole] cr (NOLOCK) on cr.Id = cc.[gsscore_connectionrole] left join StatusMetadata c_cc (NOLOCK) on c_cc.[Status] = cc.statuscode and c_cc.LocalizedLabelLanguageCode = 1033 and c_cc.EntityName = 'gsscore_connectionrole' where a.accountnumber = '" + JDENumber + "' and cc.gsscore_invoicerecipient = '1' and c_cc.LocalizedLabel = 'Active'";
                                            using (SqlCommand crmCallbackInspectionNoJDE = new SqlCommand(qrySHDataForCallbackInsepctionJDE, connCallbackFPTMInspection))
                                            {
                                                SqlDataAdapter daCallbackFPTMInspectionNoJDE = new SqlDataAdapter(crmCallbackInspectionNoJDE);

                                                DataTable dtcallbackInspectionJDE = new DataTable();

                                                daCallbackFPTMInspectionNoJDE.Fill(dtcallbackInspectionJDE);

                                                if (dtcallbackInspectionJDE.Rows.Count > 0 && isPOFromCRM)
                                                {
                                                    DataTable dtAccountData = dtcallbackInspectionJDE;

                                                    DataTable dtGetAccountData = hlp.getDBValuesSetAccount(dtAccountData, dtCallbackFPTMInspection, isPOFromCRM, email_ValidationArr);
                                                    if (dtGetAccountData.Rows.Count > 0)
                                                    {
                                                        dtCallbackFPTMInspection = dtGetAccountData;
                                                    }
                                                    else
                                                    {
                                                        dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"] = "Standard mail";
                                                        dtCallbackFPTMInspection.Rows[0]["InvoiceEmail"] = "";
                                                        dtCallbackFPTMInspection.Rows[0]["AccountName"] = dtAccountData.Rows[0]["AccountName"];
                                                        dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = dtAccountData.Rows[0]["AccountManagerEmail"];
                                                    }
                                                }
                                                else
                                                {
                                                    if (dtcallbackInspectionJDE.Rows.Count > 0)
                                                    {
                                                        dtCallbackFPTMInspection.Rows[0]["AccountName"] = dtcallbackInspectionJDE.Rows[0]["AccountName"];
                                                        dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = dtcallbackInspectionJDE.Rows[0]["AccountManagerEmail"];
                                                    }
                                                    else
                                                    {
                                                        dtCallbackFPTMInspection.Rows[0]["AccountName"] = "";
                                                        dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = "";
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            dtCallbackFPTMInspection = hlp.getDBValuesSet(dtCallbackFPTMInspection, isPOFromCRM);
                                            dtCallbackFPTMInspection.Rows[0]["SHDataFound"] = true;
                                            dtCallbackFPTMInspection.Rows[0]["JDENumber"] = "";
                                        }
                                    }

                                    //no recound found based jde then sending default output 
                                    else
                                    {
                                        dtCallbackFPTMInspection = new DataTable();
                                        dtCallbackFPTMInspection.Rows.Add();
                                        dtCallbackFPTMInspection = hlp.getDefaultColumns(dtCallbackFPTMInspection);
                                        dtCallbackFPTMInspection.Rows[0]["SHDataFound"] = false;
                                        dtCallbackFPTMInspection.Rows[0]["JDENumber"] = "";
                                    }
                                }


                                //final output for selective columns

                                DataTable dtCallbackFPTMInspectionFilter = new DataTable();
                                dtCallbackFPTMInspectionFilter = dtCallbackFPTMInspection;

                                DataView dvCallbackFPTMInspection = new DataView(dtCallbackFPTMInspectionFilter);

                                dtCallbackFPTMInspection = new DataTable();
                                if (isPOFromCRM)
                                {
                                    dtCallbackFPTMInspection = dvCallbackFPTMInspection.ToTable("Selected", false,
                                        "JDENumber", "AccountName", "UnitERPNumber", "LegacyContractNumber", "ContractERPNumber", "Branch", "AmountOfHours", "NotToExceedOnHoursBilled",
                                        "CannotBillOverAmount", "CustomerReferenceNumber", "OtherCustomerReferenceRequired",
                                        "TotalInvoiceDiscountRate", "OverallDiscountOnTotalInvoiceAmount", "VehicleChargeAmount", "VehicleCharge", "AccountGroupCode", "IsNSA", "TimeTicketType", "PONumber", "PORequried",
                                        "TravelTime", "DoesTheCustomerHaveATravelTimeRestriction", "ContractStatus", "AccountManager", "AccountManagerEmail", "EmailType", "InvoiceEmail",
                                        "InvoiceDelivery", "SpecificWording", "SpecificWordingOrAdditionalLanguageRequiredOnInvoice", "MaterialItemizationRequiredOnInvoice",
                                         "EntrapmentsBillableVal", "BillForExpensesVal", "BillForTravelTimeVal", "MaterialItemizationRequiredVal",
                                        "ElevatorNameDesignationRequiredVal", "VandalismBillableVal", "TaxExemptVal", "TimeTicketRequiredVal", "NonBillableHours", "SHDataFound", "BusinessStream", "AccountID", "ContractId");
                                }
                                else
                                {
                                    dtCallbackFPTMInspection = dvCallbackFPTMInspection.ToTable("Selected", false,
                                       "JDENumber", "AccountName", "UnitERPNumber", "LegacyContractNumber", "ContractERPNumber", "Branch", "AmountOfHours", "NotToExceedOnHoursBilled",
                                       "CannotBillOverAmount", "CustomerReferenceNumber", "OtherCustomerReferenceRequired",
                                       "TotalInvoiceDiscountRate", "OverallDiscountOnTotalInvoiceAmount", "VehicleChargeAmount", "VehicleCharge", "AccountGroupCode", "IsNSA", "TimeTicketType",
                                       "TravelTime", "DoesTheCustomerHaveATravelTimeRestriction", "ContractStatus", "AccountManager", "AccountManagerEmail",
                                       "SpecificWording", "SpecificWordingOrAdditionalLanguageRequiredOnInvoice", "MaterialItemizationRequiredOnInvoice",
                                        "EntrapmentsBillableVal", "BillForExpensesVal", "BillForTravelTimeVal", "MaterialItemizationRequiredVal",
                                       "ElevatorNameDesignationRequiredVal", "VandalismBillableVal", "TaxExemptVal", "TimeTicketRequiredVal", "NonBillableHours", "SHDataFound", "BusinessStream", "AccountID", "ContractId");
                                }

                            }

                        }

                        //FPTM API Code
                        if (sSHMCRMDetails.ApplicationName.Trim().ToUpper() == "FPTM")
                        {
                            if (!string.IsNullOrEmpty(sSHMCRMDetails.MachineNo))
                            {
                                sSHMCRMDetails.MachineNo = sSHMCRMDetails.MachineNo.Trim();
                            }
                            if (!string.IsNullOrEmpty(sSHMCRMDetails.JDE))
                            {
                                sSHMCRMDetails.JDE = sSHMCRMDetails.JDE.Trim();
                            }
                            if (!string.IsNullOrEmpty(sSHMCRMDetails.TempUnitNumber))
                            {
                                sSHMCRMDetails.TempUnitNumber = sSHMCRMDetails.TempUnitNumber.Trim();
                            }
                            if ((!string.IsNullOrEmpty(sSHMCRMDetails.MachineNo) || !string.IsNullOrEmpty(sSHMCRMDetails.TempUnitNumber)) && !string.IsNullOrEmpty(sSHMCRMDetails.JDE))
                            {
                                if (isPOFromCRM)
                                {
                                    qrySHDataForFPTM = "select top(1) cntr.Id AS ContractId,'' AS QuoteNumber,br.gsscore_name AS Branch, cntr.gsscore_timeticketrequired AS TimeTicketRequired,c_tt.LocalizedLabel AS TimeTicketType,cntr.gsscore_AccountGroupCode AS AccountGroupCode,cntr.gsscore_topenorderpo AS PONumber,cntr.gsscore_topenorderporequired AS POWORequired,c_id.LocalizedLabel AS InvoiceDelivery,cntr.gsscore_vehiclecharge AS VehicleChargeAmount,c_st.LocalizedLabel AS ContractStatus from salesorder cntr (NOLOCK) inner join gsscore_unit u (NOLOCK) on u.gsscore_contract = cntr.Id inner join gsscore_Branch br (NOLOCK) on br.gsscore_branchid = cntr.gsscore_Branch inner join account a (NOLOCK) on a.Id = u.gsscore_account inner join StatusMetadata c_st (NOLOCK) on c_st.[Status] = cntr.statuscode and c_st.LocalizedLabelLanguageCode = 1033 and c_st.EntityName = 'salesorder' left join GlobalOptionSetMetadata c_tt (NOLOCK) on c_tt.[Option] = cntr.gsscore_timetickettype and c_tt.LocalizedLabelLanguageCode = 1033 and c_tt.EntityName = 'salesorder' and c_tt.OptionSetName = 'gsscore_timetickettype' left join GlobalOptionSetMetadata c_bs (NOLOCK) on c_bs.[Option] = cntr.gsscore_businessstream and c_bs.LocalizedLabelLanguageCode = 1033 and c_bs.EntityName = 'salesorder' and c_bs.OptionSetName = 'gsscore_businessstream' left join GlobalOptionSetMetadata c_id (NOLOCK) on c_id.[Option] = cntr.gsscore_tinvoicedeliverymethod and c_id.LocalizedLabelLanguageCode = 1033 and c_id.EntityName = 'salesorder' and c_id.OptionSetName = 'gsscore_tinvoicedeliverymethod' "
                                        + " where c_st.LocalizedLabel IN ('In Service','Expired - In Service','Suspended','Booked') and u.gsscore_uniterpnumber ='" + sSHMCRMDetails.MachineNo + "' OR u.gsscore_name ='" + sSHMCRMDetails.TempUnitNumber + "';"
                                                      + " select a.Id AS AccountID, a.name AS AccountName,a.gsscore_portfolioownername AS AccountManager,b.internalemailaddress AS AccountManagerEmail,cr.[gsscore_englishname] AS EmailType,c.emailaddress1 AS PrimaryEmail,c.emailaddress2 AS InvoiceEmail,c.emailaddress3 AS StatementEmail from account a (NOLOCK) inner join team t (NOLOCK) on t.id=a.ownerid left join systemuser b (NOLOCK) on b.Id = t.gsscore_portfolioownerid left join [connection] cc (NOLOCK) on a.Id = cc.[record1id] and cc.[record1id_entitytype] ='account' left join contact c (NOLOCK) on c.Id = cc.[record2id] and cc.[record2id_entitytype] ='contact' left join [gsscore_connectionrole] cr (NOLOCK) on cr.Id = cc.[gsscore_connectionrole] left join StatusMetadata c_cc (NOLOCK) on c_cc.[Status] = cc.statuscode and c_cc.LocalizedLabelLanguageCode = 1033 and c_cc.EntityName = 'gsscore_connectionrole' where a.accountnumber = '" + sSHMCRMDetails.JDE + "' and cc.gsscore_invoicerecipient = '1' and c_cc.LocalizedLabel = 'Active'";
                                }
                                else
                                {
                                    qrySHDataForFPTM = "select top(1) cntr.Id AS ContractId,'' AS QuoteNumber,br.gsscore_name AS Branch, cntr.gsscore_timeticketrequired AS TimeTicketRequired,c_tt.LocalizedLabel AS TimeTicketType,cntr.gsscore_AccountGroupCode AS AccountGroupCode,c_id.LocalizedLabel AS InvoiceDelivery,cntr.gsscore_vehiclecharge AS VehicleChargeAmount,c_st.LocalizedLabel AS ContractStatus from salesorder cntr (NOLOCK) inner join gsscore_unit u (NOLOCK) on u.gsscore_contract = cntr.Id inner join gsscore_Branch br (NOLOCK) on br.gsscore_branchid = cntr.gsscore_Branch inner join account a (NOLOCK) on a.Id = u.gsscore_account inner join StatusMetadata c_st (NOLOCK) on c_st.[Status] = cntr.statuscode and c_st.LocalizedLabelLanguageCode = 1033 and c_st.EntityName = 'salesorder' left join GlobalOptionSetMetadata c_tt (NOLOCK) on c_tt.[Option] = cntr.gsscore_timetickettype and c_tt.LocalizedLabelLanguageCode = 1033 and c_tt.EntityName = 'salesorder' and c_tt.OptionSetName = 'gsscore_timetickettype' left join GlobalOptionSetMetadata c_bs (NOLOCK) on c_bs.[Option] = cntr.gsscore_businessstream and c_bs.LocalizedLabelLanguageCode = 1033 and c_bs.EntityName = 'salesorder' and c_bs.OptionSetName = 'gsscore_businessstream' left join GlobalOptionSetMetadata c_id (NOLOCK) on c_id.[Option] = cntr.gsscore_tinvoicedeliverymethod and c_id.LocalizedLabelLanguageCode = 1033 and c_id.EntityName = 'salesorder' and c_id.OptionSetName = 'gsscore_tinvoicedeliverymethod'  "
                                        + " where c_st.LocalizedLabel IN ('In Service','Expired - In Service','Suspended','Booked') and u.gsscore_uniterpnumber ='" + sSHMCRMDetails.MachineNo + "' OR u.gsscore_name ='" + sSHMCRMDetails.TempUnitNumber + "';"
                                                          + " select top(1)  a.Id AS AccountID, a.name AS AccountName,a.gsscore_portfolioownername AS AccountManager,b.internalemailaddress AS AccountManagerEmail,cr.[gsscore_englishname] AS EmailType,c.emailaddress1 AS PrimaryEmail,c.emailaddress2 AS InvoiceEmail,c.emailaddress3 AS StatementEmail	 from account a (NOLOCK) inner join team t (NOLOCK) on t.id=a.ownerid left join systemuser b (NOLOCK) on b.Id = t.gsscore_portfolioownerid left join [connection] cc (NOLOCK) on a.Id = cc.[record1id] and cc.[record1id_entitytype] ='account' left join contact c (NOLOCK) on c.Id = cc.[record2id] and cc.[record2id_entitytype] ='contact' left join [gsscore_connectionrole] cr (NOLOCK) on cr.Id = cc.[gsscore_connectionrole] left join StatusMetadata c_cc (NOLOCK) on c_cc.[Status] = cc.statuscode and c_cc.LocalizedLabelLanguageCode = 1033 and c_cc.EntityName = 'gsscore_connectionrole' where a.accountnumber =  '" + sSHMCRMDetails.JDE + "' and cc.gsscore_invoicerecipient = '1' and c_cc.LocalizedLabel = 'Active'";
                                }

                                DataSet dsetCallbackFPTMInspection = new DataSet();
                                using (SqlCommand crmCallbackInspection = new SqlCommand(qrySHDataForFPTM, connCallbackFPTMInspection))
                                {
                                    crmCallbackInspection.CommandTimeout = 90;
                                    SqlDataAdapter daCallbackFPTMInspection = new SqlDataAdapter(crmCallbackInspection);


                                    dtCallbackFPTMInspection = new DataTable();
                                    daCallbackFPTMInspection.Fill(dsetCallbackFPTMInspection);

                                    if (dsetCallbackFPTMInspection.Tables.Count > 1)
                                    {
                                        dtCallbackFPTMInspection = new DataTable();

                                        dtCallbackFPTMInspection.Columns.Add("AccountName");
                                        dtCallbackFPTMInspection.Columns.Add("AccountManager");
                                        dtCallbackFPTMInspection.Columns.Add("AccountManagerEmail");
                                        dtCallbackFPTMInspection.Columns.Add("InvoiceEmail");
                                        dtCallbackFPTMInspection.Columns.Add("EmailType");
                                        dtCallbackFPTMInspection.Columns.Add("AccountID");
                                        if (isPOFromCRM)
                                        {
                                            dtCallbackFPTMInspection.Columns.Add("PONumber");
                                            dtCallbackFPTMInspection.Columns.Add("PORequried");
                                            dtCallbackFPTMInspection.Columns.Add("InvoiceDelivery");
                                        }
                                        dtCallbackFPTMInspection.Columns.Add("SHDataFound", typeof(System.Boolean));

                                        if (dsetCallbackFPTMInspection.Tables[0].Rows.Count > 0)
                                        {
                                            dtCallbackFPTMInspection.Merge(dsetCallbackFPTMInspection.Tables[0]);
                                            if (isPOFromCRM)
                                            {
                                                if (!string.IsNullOrEmpty(Convert.ToString(dtCallbackFPTMInspection.Rows[0]["PONumber"])))
                                                {
                                                    dtCallbackFPTMInspection.Rows[0]["PONumber"] = dtCallbackFPTMInspection.Rows[0]["PONumber"];
                                                }
                                                else
                                                {
                                                    dtCallbackFPTMInspection.Rows[0]["PONumber"] = "";
                                                }
                                                if (!string.IsNullOrEmpty(Convert.ToString(dtCallbackFPTMInspection.Rows[0]["POWORequired"])))
                                                {
                                                    if (Convert.ToBoolean(dtCallbackFPTMInspection.Rows[0]["POWORequired"]))
                                                    {
                                                        dtCallbackFPTMInspection.Rows[0]["PORequried"] = "Yes";
                                                    }
                                                    else
                                                    {
                                                        dtCallbackFPTMInspection.Rows[0]["PORequried"] = "No";
                                                    }
                                                }
                                                else
                                                {
                                                    dtCallbackFPTMInspection.Rows[0]["PORequried"] = "No";
                                                }
                                            }
                                        }
                                        else
                                        {
                                            dtCallbackFPTMInspection.Rows.Add();
                                            dtCallbackFPTMInspection.Columns.Add("Branch");
                                            dtCallbackFPTMInspection.Columns.Add("QuoteNumber");
                                            dtCallbackFPTMInspection.Columns.Add("AccountGroupCode");
                                            dtCallbackFPTMInspection.Columns.Add("TimeTicketRequired");
                                            dtCallbackFPTMInspection.Columns.Add("TimeTicketType");
                                            dtCallbackFPTMInspection.Columns.Add("ContractStatus");
                                            dtCallbackFPTMInspection.Columns.Add("VehicleChargeAmount");
                                            dtCallbackFPTMInspection.Columns.Add("ContractId");


                                            dtCallbackFPTMInspection.Rows[0]["Branch"] = "";
                                            dtCallbackFPTMInspection.Rows[0]["QuoteNumber"] = "";
                                            dtCallbackFPTMInspection.Rows[0]["AccountGroupCode"] = "";
                                            dtCallbackFPTMInspection.Rows[0]["TimeTicketRequired"] = false;
                                            dtCallbackFPTMInspection.Rows[0]["TimeTicketType"] = "";
                                            dtCallbackFPTMInspection.Rows[0]["ContractStatus"] = "";
                                            dtCallbackFPTMInspection.Rows[0]["ContractId"] = "";
                                            if (isPOFromCRM)
                                            {
                                                dtCallbackFPTMInspection.Rows[0]["PONumber"] = "";
                                                dtCallbackFPTMInspection.Rows[0]["PORequried"] = "No";
                                                dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"] = "Standard mail";
                                            }
                                        }

                                        dtCallbackFPTMInspection.Columns.Add("IsNSA");
                                        dtCallbackFPTMInspection.Columns.Add("VehicleCharge");
                                        dtCallbackFPTMInspection.Columns.Add("TimeTicketRequiredVal");
                                        if (!string.IsNullOrEmpty(Convert.ToString(dtCallbackFPTMInspection.Rows[0]["TimeTicketRequired"])))
                                        {
                                            if (Convert.ToBoolean(Convert.ToString(dtCallbackFPTMInspection.Rows[0]["TimeTicketRequired"])))
                                            {
                                                dtCallbackFPTMInspection.Rows[0]["TimeTicketRequiredVal"] = "Yes";
                                            }
                                            else
                                            {
                                                dtCallbackFPTMInspection.Rows[0]["TimeTicketRequiredVal"] = "No";
                                            }
                                        }
                                        else
                                        {
                                            dtCallbackFPTMInspection.Rows[0]["TimeTicketRequiredVal"] = "No";
                                        }
                                        if (!string.IsNullOrEmpty(Convert.ToString(dtCallbackFPTMInspection.Rows[0]["AccountGroupCode"])))
                                        {
                                            dtCallbackFPTMInspection.Rows[0]["IsNSA"] = "Yes";
                                        }
                                        else
                                        {
                                            dtCallbackFPTMInspection.Rows[0]["IsNSA"] = "No";
                                        }
                                        if (!string.IsNullOrEmpty(Convert.ToString(dtCallbackFPTMInspection.Rows[0]["VehicleChargeAmount"])))
                                        {
                                            dtCallbackFPTMInspection.Rows[0]["VehicleCharge"] = "Yes";
                                        }
                                        else
                                        {
                                            dtCallbackFPTMInspection.Rows[0]["VehicleCharge"] = "No";
                                            dtCallbackFPTMInspection.Rows[0]["VehicleChargeAmount"] = 0;
                                        }

                                        if (dsetCallbackFPTMInspection.Tables.Count > 1)
                                        {
                                            DataTable dtAccountData = dsetCallbackFPTMInspection.Tables[1];
                                            if (dtAccountData.Rows.Count > 0 && isPOFromCRM)
                                            {
                                                DataTable dtGetAccountData = hlp.getDBValuesSetAccount(dtAccountData, dtCallbackFPTMInspection, isPOFromCRM, email_ValidationArr);
                                                if (dtGetAccountData.Rows.Count > 0)
                                                {
                                                    dtCallbackFPTMInspection = dtGetAccountData;
                                                }
                                                else
                                                {
                                                    dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"] = "Standard mail";
                                                    dtCallbackFPTMInspection.Rows[0]["InvoiceEmail"] = "";
                                                    dtCallbackFPTMInspection.Rows[0]["AccountID"] = "";
                                                    dtCallbackFPTMInspection.Rows[0]["AccountName"] = dtAccountData.Rows[0]["AccountName"];
                                                    dtCallbackFPTMInspection.Rows[0]["AccountManager"] = dtAccountData.Rows[0]["AccountManager"];
                                                    dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = dtAccountData.Rows[0]["AccountManagerEmail"];
                                                }
                                            }
                                            else
                                            {
                                                if (dtAccountData.Rows.Count > 0)
                                                {
                                                    dtCallbackFPTMInspection.Rows[0]["AccountName"] = dtAccountData.Rows[0]["AccountName"];
                                                    dtCallbackFPTMInspection.Rows[0]["AccountManager"] = dtAccountData.Rows[0]["AccountManager"];
                                                    dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = dtAccountData.Rows[0]["AccountManagerEmail"];
                                                    dtCallbackFPTMInspection.Rows[0]["AccountID"] = dtAccountData.Rows[0]["AccountID"];
                                                }
                                                else
                                                {
                                                    dtCallbackFPTMInspection.Rows[0]["AccountName"] = "";
                                                    dtCallbackFPTMInspection.Rows[0]["AccountManager"] = "";
                                                    dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = "";
                                                    dtCallbackFPTMInspection.Rows[0]["AccountID"] = "";
                                                }
                                            }
                                        }
                                    }
                                    if (dsetCallbackFPTMInspection.Tables.Count > 1)
                                    {
                                        if (dsetCallbackFPTMInspection.Tables[0].Rows.Count > 0)
                                        {
                                            dtCallbackFPTMInspection.Rows[0]["SHDataFound"] = true;
                                        }
                                        else
                                        {
                                            dtCallbackFPTMInspection.Rows[0]["SHDataFound"] = false;
                                        }
                                    }
                                    else
                                    {
                                        dtCallbackFPTMInspection.Rows[0]["SHDataFound"] = false;
                                    }
                                    //final output


                                    DataTable dtCallbackFPTMInspectionFilter = new DataTable();
                                    dtCallbackFPTMInspectionFilter = dtCallbackFPTMInspection;

                                    DataView dvCallbackFPTMInspection = new DataView(dtCallbackFPTMInspectionFilter);

                                    dtCallbackFPTMInspection = new DataTable();
                                    if (isPOFromCRM)
                                    {
                                        dtCallbackFPTMInspection = dvCallbackFPTMInspection.ToTable("Selected", false,
                                            "AccountName", "AccountManager", "AccountManagerEmail", "InvoiceDelivery", "InvoiceEmail", "TimeTicketRequiredVal", "QuoteNumber", "Branch", "AccountGroupCode",
                                             "TimeTicketType", "ContractStatus", "PONumber", "PORequried", "IsNSA", "VehicleCharge", "VehicleChargeAmount", "SHDataFound", "AccountID", "ContractId");
                                    }
                                    else
                                    {
                                        dtCallbackFPTMInspection = dvCallbackFPTMInspection.ToTable("Selected", false,
                                           "AccountName", "AccountManager", "AccountManagerEmail", "TimeTicketRequiredVal", "QuoteNumber", "Branch", "AccountGroupCode", "TimeTicketType", "ContractStatus", "IsNSA",
                                            "VehicleCharge", "VehicleChargeAmount", "SHDataFound", "AccountID", "ContractId");
                                    }

                                    cCode = HttpStatusCode.OK;
                                }
                            }
                        }
                        cCode = HttpStatusCode.OK;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message.ToString() + "--" + ex.StackTrace.ToString());

                        hlp.addToLog(1, "Error", "ReadSpecialHandlingData(): " + ex.Message.ToString() + "--" + ex.StackTrace.ToString());

                        cCode = HttpStatusCode.NotFound;
                        dtCallbackFPTMInspection = new DataTable();

                        //Logging error part

                    }
                    finally
                    {
                        // Close the SQL connect if it is in open State
                        if (connCallbackFPTMInspection.State == System.Data.ConnectionState.Open)
                        {
                            connCallbackFPTMInspection.Close();
                            connCallbackFPTMInspection.Dispose();
                        }
                    }
                }

                // Send the reponse to API
                string json = JsonConvert.SerializeObject(dtCallbackFPTMInspection);
                return Content(cCode, json);
            }
            catch (ServiceException ex)
            {
                Console.WriteLine(ex.Message);
                var error = ex.Error as SharePointRestError;
                hlp.addToLog(2, "Information", "ReadSpecialHandlingData(): " + ex.Message.ToString() + "--" + ex.StackTrace.ToString());
                return Content(HttpStatusCode.InternalServerError, JsonConvert.SerializeObject("Unknown error occured.-1219"));
            }
        }

        /// <summary>
        /// 1213924 - Retrive Consumable fee from CRM on contract level and add to callback - Permanent solution
        /// </summary>
        /// <param name="JDENo"></param>
        /// <returns>Apply Consumable Fee value</returns>
        [Route("api/GetApplyConsumableFee")]
        [HttpPost]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public async Task<IHttpActionResult> GetApplyConsumableFeeAsync([FromBody] ApplyConsumableFee applyConsumableFee)
        {
            try
            {
                Helper hlp = new Helper();
                HttpStatusCode cCode;

                var CLConfiguration = ConfigurationManager.AppSettings["CLConfiguration"];
                DataTable dtApplyConsumbaleFeeAll = new DataTable();
                DataTable dtApplyConsumbaleFeeFiltered = new DataTable();
                PnPContext ctx = await PnPSharePointContext.GetPnPContext(Convert.ToString(ConfigurationManager.AppSettings["OSSSiteURL"]));
                using (ctx)
                {
                    var list = await ctx.Web.Lists.GetByTitleAsync("CL_Configurations", p => p.Title);

                    string viewXml = "<View><Query><Where><Eq><FieldRef Name='Title' /><Value Type='Text'>DatabaseConfig</Value></Eq></Where></Query></View>";
                    var output = await list.LoadListDataAsStreamAsync(new PnP.Core.Model.SharePoint.RenderListDataOptions()
                    {
                        ViewXml = viewXml,
                        RenderOptions = RenderListDataOptionsFlags.ListData,

                    }).ConfigureAwait(false);
                    // Get the Connection Sting value from CL_Configurations list
                    var ConnectStringValue = "";
                    foreach (var itm in list.Items.AsRequested())
                    {
                        try
                        {
                            ConnectStringValue = Convert.ToString(itm["Value"]);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message.ToString());
                        }
                    }
                    SqlConnection connApplyConsumbaleFee = new SqlConnection(ConnectStringValue);
                    try
                    {
                        // Open the SQL connect if it is in close State
                        if (connApplyConsumbaleFee.State != System.Data.ConnectionState.Open)
                        {
                            connApplyConsumbaleFee.Open();
                        }
                        // Select command to get qryApplyConsumbaleFee 
                        string qryApplyConsumbaleFee = "SELECT Account.[accountnumber] AS AccountNumber, cf.LocalizedLabel AS ApplyConsumableFee,"
                                                + "Account.Id AS AccountID,Account.name AS AccountName FROM [dbo].[account] Account"
                                           + " LEFT JOIN OptionSetMetadata (nolock) cf on cf.[Option] = Account.gsscore_applyconsumablefee and cf.EntityName = 'account' and cf.OptionSetName = 'gsscore_applyconsumablefee' and cf.LocalizedLabelLanguageCode = 1033"
                                           + " WHERE Account.[accountnumber] = '" + applyConsumableFee.JDENo.Trim() + "'";

                        using (SqlCommand cmdApplyConsumbaleFee = new SqlCommand(qryApplyConsumbaleFee, connApplyConsumbaleFee))
                        {
                            SqlDataAdapter daACFAll = new SqlDataAdapter(cmdApplyConsumbaleFee);
                            daACFAll.Fill(dtApplyConsumbaleFeeAll);

                            if (dtApplyConsumbaleFeeAll.Rows.Count > 0)
                            {
                                dtApplyConsumbaleFeeFiltered = dtApplyConsumbaleFeeAll;
                            }
                            cCode = HttpStatusCode.OK;
                        }
                    }
                    catch (Exception ex)
                    {
                        cCode = HttpStatusCode.NotFound;
                        dtApplyConsumbaleFeeFiltered = new DataTable();
                        hlp.addToLog(1, "Information", "GetApplyConsumableFee(): " + ex.Message.ToString() + "--" + ex.StackTrace.ToString());
                    }
                    finally
                    {
                        // Close the SQL connect if it is in open State
                        if (connApplyConsumbaleFee.State == System.Data.ConnectionState.Open)
                        {
                            connApplyConsumbaleFee.Close();
                            connApplyConsumbaleFee.Dispose();
                        }
                    }
                }
                // Send the reponse to API
                string json = JsonConvert.SerializeObject(dtApplyConsumbaleFeeFiltered);
                return Content(cCode, json);
            }
            catch (Exception ex)
            {
                Helper hlp = new Helper();
                hlp.addToLog(2, "Information", "ReadSpecialHandlingData(): " + ex.Message.ToString() + "--" + ex.StackTrace.ToString());
                return Content(HttpStatusCode.InternalServerError, JsonConvert.SerializeObject("Unknown error occured.-1219"));
            }
        }

        /// <summary>
        ///1052312-ReadBillingAddress based on JDE for ARSupport
        /// </summary>
        /// <param name="SHRates"></param>
        /// <returns></returns>
        [Route("api/ReadBillingAddress")]
        [HttpPost]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public async Task<IHttpActionResult> ReadBillingAddress([FromBody] BillingAddress sBillingAddress)
        {
            try
            {
                Helper hlp = new Helper();
                HttpStatusCode cCode;
                var CLConfiguration = ConfigurationManager.AppSettings["CLConfiguration"];
                DataTable dtFilterBillingAddress = new DataTable();
                PnPContext ctx = await PnPSharePointContext.GetPnPContext(Convert.ToString(ConfigurationManager.AppSettings["OSSSiteURL"]));
                using (ctx)
                {

                    var list = await ctx.Web.Lists.GetByTitleAsync("CL_Configurations", p => p.Title);

                    string viewXml = "<View><Query><Where><Eq><FieldRef Name='Title' /><Value Type='Text'>DatabaseConfig</Value></Eq></Where></Query></View>";
                    var output = await list.LoadListDataAsStreamAsync(new PnP.Core.Model.SharePoint.RenderListDataOptions()
                    {
                        ViewXml = viewXml,
                        RenderOptions = RenderListDataOptionsFlags.ListData,

                    }).ConfigureAwait(false);
                    // Get the Connection Sting value from CL_Configurations list
                    var ConnectStringValue = "";
                    foreach (var itm in list.Items.AsRequested())
                    {
                        try
                        {
                            ConnectStringValue = Convert.ToString(itm["Value"]);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message.ToString());
                        }
                    }


                    SqlConnection connBillindAddress = new SqlConnection(ConnectStringValue);
                    try
                    {
                        // Open the SQL connect if it is in close State
                        if (connBillindAddress.State != System.Data.ConnectionState.Open)
                        {
                            connBillindAddress.Open();
                        }
                        //command
                        string grySelectBillingAddress = "";
                        if (!string.IsNullOrEmpty(sBillingAddress.JDENo))
                        {
                            sBillingAddress.JDENo = sBillingAddress.JDENo.Trim();
                        }
                        grySelectBillingAddress = "SELECT A.GSSCORE_BILLINGNAME AS BILLINGNAME,A.ADDRESS2_LINE1 AS ADDRESSLINE1,A.ADDRESS2_LINE2 AS ADDRESSLINE2,A.ADDRESS2_CITY AS CITY,A.GSSCORE_BILLINGSTATEPROVINCE AS STATEID,S.GSSCORE_NAME AS STATE,A.ADDRESS2_POSTALCODE AS POSTALCODE,A.GSSCORE_BILLINGADDRESSCOUNTRY AS COUNTRYID,C.GSSCORE_NAME AS COUNTRY FROM ACCOUNT A (NOLOCK) INNER JOIN GSSCORE_STATE S (NOLOCK) ON A.GSSCORE_BILLINGSTATEPROVINCE = S.ID INNER JOIN GSSCORE_COUNTRY C (NOLOCK) ON A.GSSCORE_BILLINGADDRESSCOUNTRY = C.ID WHERE A.ACCOUNTNUMBER = '" + sBillingAddress.JDENo + "'";
                        DataTable dtBillingAddress = new DataTable();
                        using (SqlCommand cmdBillingAddress = new SqlCommand(grySelectBillingAddress, connBillindAddress))
                        {
                            SqlDataAdapter daRatesAll = new SqlDataAdapter(cmdBillingAddress);
                            daRatesAll.Fill(dtBillingAddress);
                            if (dtBillingAddress.Rows.Count > 0)
                            {
                                dtFilterBillingAddress = dtBillingAddress;
                            }
                            else
                            {
                                dtFilterBillingAddress = new DataTable();
                            }
                        }
                        cCode = HttpStatusCode.OK;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(Convert.ToString(ex.Message) + "--" + Convert.ToString(ex.StackTrace));

                        cCode = HttpStatusCode.NotFound;

                        dtFilterBillingAddress = new DataTable();
                        hlp.addToLog(1, "Information", "ReadBillingAddress(): " + ex.Message.ToString() + "--" + ex.StackTrace.ToString());
                    }
                    finally
                    {
                        // Close the SQL connect if it is in open State
                        if (connBillindAddress.State == System.Data.ConnectionState.Open)
                        {
                            connBillindAddress.Close();
                            connBillindAddress.Dispose();
                        }
                    }
                }
                // Send the reponse to API
                string json = JsonConvert.SerializeObject(dtFilterBillingAddress);
                return Content(cCode, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return Content(HttpStatusCode.InternalServerError, JsonConvert.SerializeObject("Unknown error occured.1326"));
            }
        }

        /// <summary>
        /// 1115241 - T&M Material Markup Integration
        /// </summary>
        /// <param name="CRMQuoteNumber"></param>
        /// <returns>Material Markup value</returns>
        [Route("api/GetMaterialMarkup")]
        [HttpPost]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public async Task<IHttpActionResult> GetMaterialMarkup([FromBody] MaterialMarkup materialMarkup)
        {
            try
            {
                Helper hlp = new Helper();
                HttpStatusCode cCode;

                var CLConfiguration = ConfigurationManager.AppSettings["CLConfiguration"];
                DataTable dtMaterialMarkupAll = new DataTable();
                DataTable dtMaterialMarkupFiltered = new DataTable();
                PnPContext ctx = await PnPSharePointContext.GetPnPContext(Convert.ToString(ConfigurationManager.AppSettings["OSSSiteURL"]));
                using (ctx)
                {

                    var list = await ctx.Web.Lists.GetByTitleAsync("CL_Configurations", p => p.Title);

                    string viewXml = "<View><Query><Where><Eq><FieldRef Name='Title' /><Value Type='Text'>DatabaseConfig</Value></Eq></Where></Query></View>";
                    var output = await list.LoadListDataAsStreamAsync(new PnP.Core.Model.SharePoint.RenderListDataOptions()
                    {
                        ViewXml = viewXml,
                        RenderOptions = RenderListDataOptionsFlags.ListData,

                    }).ConfigureAwait(false);
                    // Get the Connection Sting value from CL_Configurations list
                    var ConnectStringValue = "";
                    foreach (var itm in list.Items.AsRequested())
                    {
                        try
                        {
                            ConnectStringValue = Convert.ToString(itm["Value"]);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message.ToString());
                        }
                    }


                    SqlConnection connMaterialMarkup = new SqlConnection(ConnectStringValue);
                    try
                    {
                        // Open the SQL connect if it is in close State
                        if (connMaterialMarkup.State != System.Data.ConnectionState.Open)
                        {
                            connMaterialMarkup.Open();
                        }
                        // Select command to get qryMaterialMarkup 
                        string qryMaterialMarkup = "";
                        qryMaterialMarkup = "SELECT Rates.[gsscore_materialmarkup] AS MaterialMarkupRate " +
                                            "FROM [dbo].[vw_ApprovedBillingRates] Rates " +
                                            "WHERE Rates.[gsscore_autoid] = (SELECT top(1) gsscore_optionset FROM quotedetail qd " +
                                        "INNER JOIN quote q(NOLOCK) ON q.quoteid = qd.quoteid " +
                                        "WHERE q.gsscore_autoid = '" + materialMarkup.CRMQuoteNumber.Trim() + "')";

                        using (SqlCommand cmdMaterialMarkup = new SqlCommand(qryMaterialMarkup, connMaterialMarkup))
                        {
                            SqlDataAdapter daMMAll = new SqlDataAdapter(cmdMaterialMarkup);
                            daMMAll.Fill(dtMaterialMarkupAll);

                            if (dtMaterialMarkupAll.Rows.Count > 0)
                            {
                                dtMaterialMarkupFiltered = dtMaterialMarkupAll;
                            }
                            cCode = HttpStatusCode.OK;
                        }
                    }
                    catch (Exception ex)
                    {
                        cCode = HttpStatusCode.NotFound;
                        dtMaterialMarkupFiltered = new DataTable();
                        hlp.addToLog(2, "Information", "GetMaterialMarkup(): " + ex.Message.ToString() + "--" + ex.StackTrace.ToString());

                    }
                    finally
                    {
                        // Close the SQL connect if it is in open State
                        if (connMaterialMarkup.State == System.Data.ConnectionState.Open)
                        {
                            connMaterialMarkup.Close();
                            connMaterialMarkup.Dispose();
                        }
                    }
                }
                // Send the reponse to API
                string json = JsonConvert.SerializeObject(dtMaterialMarkupFiltered);
                return Content(cCode, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return Content(HttpStatusCode.InternalServerError, JsonConvert.SerializeObject("Unknown error occured.-1429"));
            }
        }
        /// <summary>
        /// Get non billable hours from Syanpase DB
        /// </summary>
        /// <param name="connCallbackFPTMInspection">SQL connection string</param>
        /// <param name="JDE">JDE #</param>
        /// <returns>boolean value indicating if non billable hours available or not for the respective customer</returns>
        public Boolean GetNonBillableHours(SqlConnection connCallbackFPTMInspection, string JDE)
        {
            bool isNonBillableHours = false;
            string qryNonBillableHours = "select qo.gsscore_optioncode AS Options,qo.gsscore_value AS Val1,qo.gsscore_value2 AS Val2,c_st.LocalizedLabel AS StatusVal from quote q inner join account a on q.customerid = a.Id inner join gsscore_quoteoption qo on qo.gsscore_quote = q.Id inner join StatusMetadata c_st on c_st.[Status] = q.statuscode and c_st.LocalizedLabelLanguageCode = 1033 and c_st.EntityName = 'quote' where a.accountnumber = '" + JDE + "' and qo.gsscore_optioncode like '%NBH%' and c_st.LocalizedLabel = 'Won'";

            DataTable dtNonBillableHours = new DataTable();
            using (SqlCommand crmNonBillableHours = new SqlCommand(qryNonBillableHours, connCallbackFPTMInspection))
            {
                crmNonBillableHours.CommandTimeout = 90;
                SqlDataAdapter daNonBillableHours = new SqlDataAdapter(crmNonBillableHours);
                daNonBillableHours.Fill(dtNonBillableHours);
                if (dtNonBillableHours.Rows.Count > 0)
                {
                    isNonBillableHours = true;
                }
            }
            return isNonBillableHours;
        }
        /// <summary>
        /// 1077457-Getting the contract status from CRM using JDE Number/Customer number in Collections
        /// </summary>
        /// <param name="contractStatus">gets the customer number from UI</param>
        /// <returns></returns>
        [Route("api/ReadContractStatus")]
        [HttpPost]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public async Task<IHttpActionResult> ReadContractStatus([FromBody] ContractStatus contractStatus)
        {
            try
            {
                Helper hlp = new Helper();
                HttpStatusCode cCode;
                var CLConfiguration = ConfigurationManager.AppSettings["CLConfiguration"];
                DataTable dtFilterContractStatus = new DataTable();

                PnPContext ctx = await PnPSharePointContext.GetPnPContext(Convert.ToString(ConfigurationManager.AppSettings["siteUrl"]));
                using (ctx)
                {

                    var list = await ctx.Web.Lists.GetByTitleAsync("CL_Configuration", p => p.Title);

                    string viewXml = "<View><Query><Where><Eq><FieldRef Name='Title' /><Value Type='Text'>DatabaseConfig</Value></Eq></Where></Query></View>";
                    var output = await list.LoadListDataAsStreamAsync(new PnP.Core.Model.SharePoint.RenderListDataOptions()
                    {
                        ViewXml = viewXml,
                        RenderOptions = RenderListDataOptionsFlags.ListData,

                    }).ConfigureAwait(false);
                    // Get the Connection Sting value from CL_Configurations list
                    var ConnectStringValue = "";
                    foreach (var itm in list.Items.AsRequested())
                    {
                        try
                        {
                            ConnectStringValue = Convert.ToString(itm["Value"]);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message.ToString());
                        }
                    }


                    SqlConnection connContractStatus = new SqlConnection(ConnectStringValue);
                    try
                    {
                        // Open the SQL connect if it is in close State
                        if (connContractStatus.State != System.Data.ConnectionState.Open)
                        {
                            connContractStatus.Open();
                        }
                        //command
                        string grySelectContractStatus = "";
                        if (!string.IsNullOrEmpty(contractStatus.JDENo))
                        {
                            contractStatus.JDENo = contractStatus.JDENo.Trim();
                        }
                        grySelectContractStatus = @"select ch.gsscore_account as AccountID, ac.accountnumber as AccountNumber, ch.gsscore_contractnamename AS ContractName,ch.subject as HistoryID, ch.gsscore_effectivedate as EffectiveDate,
                            osm.Localizedlabel as StatusReason, c_st.Localizedlabel as ContractStatusReason, sm.Localizedlabel as ContractStatus,
                            cntr.gsscore_contracterpnumber as ContractNumber from gsscore_contracthistory ch
                            left
                                                                             join salesorder cntr ON cntr.customerid = ch.gsscore_account
                                                                        left
                                                                             join account ac ON ac.Id = ch.gsscore_account
                                                                        left
                                                                             join [OptionSetMetadata] osm (NOLOCK) on osm.[Option] = ch.gsscore_statusreason and osm.LocalizedLabelLanguageCode = 1033 and osm.EntityName = 'gsscore_contracthistory'
                            left join StatusMetadata c_st(NOLOCK) on c_st.[Status] = cntr.statuscode and c_st.LocalizedLabelLanguageCode = 1033 and c_st.EntityName = 'salesorder'
                            left join[StateMetadata] sm(NOLOCK) on sm.[State] = cntr.statecode and sm.[EntityName] = 'salesorder' and sm.[LocalizedLabelLanguageCode] = '1033'
                            where accountnumber = '" + contractStatus.JDENo + @"'
                            order by ch.gsscore_effectivedate desc";
                        DataTable dtContractStatus = new DataTable();
                        using (SqlCommand cmdContractStatus = new SqlCommand(grySelectContractStatus, connContractStatus))
                        {
                            cmdContractStatus.CommandTimeout = 180;
                            SqlDataAdapter daRatesAll = new SqlDataAdapter(cmdContractStatus);
                            daRatesAll.Fill(dtContractStatus);
                            if (dtContractStatus.Rows.Count > 0)
                            {
                                dtFilterContractStatus = dtContractStatus;
                            }
                            else
                            {
                                dtFilterContractStatus = new DataTable();
                            }
                        }
                        cCode = HttpStatusCode.OK;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(Convert.ToString(ex.Message) + "--" + Convert.ToString(ex.StackTrace));

                        cCode = HttpStatusCode.NotFound;

                        dtFilterContractStatus = new DataTable();
                        hlp.addToLog(1, "Information", "ReadBillingAddress(): " + ex.Message.ToString() + "--" + ex.StackTrace.ToString());
                    }
                    finally
                    {
                        // Close the SQL connect if it is in open State
                        if (connContractStatus.State == System.Data.ConnectionState.Open)
                        {
                            connContractStatus.Close();
                            connContractStatus.Dispose();
                        }
                    }
                }
                // Send the reponse to API
                string json = JsonConvert.SerializeObject(dtFilterContractStatus);
                return Content(cCode, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return Content(HttpStatusCode.InternalServerError, JsonConvert.SerializeObject("Unknown error occured.1567"));
            }
        }

        /// <summary>
        /// Generate Open Order #'s
        /// </summary>
        /// <param name="oOGenerateDetails"></param>
        /// <returns>Open Order Number</returns>
        [Route("api/GetOpenOrderNumber")]
        [HttpPost]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public async Task<IHttpActionResult> GetOpenOrderNumber([FromBody] OOGenerateDetails oOGenerateDetails)
        {
            try
            {
                Helper hlp = new Helper();
                HttpStatusCode cCode;

                string OONumber = "";

                DataTable DtOONumber = new DataTable();
                DtOONumber.Columns.Add("OpenOrderNumber");
                DataRow dtRow = DtOONumber.NewRow();

                var strETTServicetURL = Convert.ToString(ConfigurationManager.AppSettings["ETTServiceUrl"]);

                try
                {
                    var handler = new HttpClientHandler();
                    handler.ClientCertificateOptions = ClientCertificateOption.Manual;
                    handler.ServerCertificateCustomValidationCallback =
                    (httpRequestMessage, cert, cetChain, policyErrors) =>
                    {
                        return true;
                    };

                    var client = new HttpClient(handler);
                    var request = new HttpRequestMessage(HttpMethod.Post, strETTServicetURL);
                    request.Headers.Add("SOAPAction", "http://tempuri.org/IMF/GetNextOpenOrderNumber");
                    var Inputcontent = "<s:Envelope xmlns:s='http://schemas.xmlsoap.org/soap/envelope/'>"
                                      + "<s:Body>"
                                      + "<GetNextOpenOrderNumber xmlns='http://tempuri.org/'>"
                                      + "<officePrefix>" + oOGenerateDetails.OfficePrefix + "</officePrefix>"
                                      + "<applicationType>" + oOGenerateDetails.ApplicationType +"</applicationType>"
                                      + "<claimed_By>" + oOGenerateDetails.ClaimedBy +"</claimed_By>"
                                      + "<callDetailId>" + oOGenerateDetails.CallDetailId + "</callDetailId>"
                                      + "</GetNextOpenOrderNumber>"
                                      + "</s:Body>"
                                      + "</s:Envelope>";
                    var content = new StringContent(Inputcontent, null, "text/xml");

                    request.Content = content;
                    var response = await client.SendAsync(request);
                    response.EnsureSuccessStatusCode();
                    var responseval = await response.Content.ReadAsStringAsync();

                    var xmlDoc = new XmlDocument();
                    xmlDoc.LoadXml(responseval);

                    var orderNumberNode = xmlDoc.InnerXml;
                    if (orderNumberNode != null)
                    {
                        OONumber = xmlDoc.InnerText;
                    }
                    dtRow["OpenOrderNumber"] = OONumber;
                    DtOONumber.Rows.Add(dtRow);
                    cCode = HttpStatusCode.OK;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return Content(HttpStatusCode.InternalServerError, JsonConvert.SerializeObject("Unknown error occured.1567"));
                }

                string json = JsonConvert.SerializeObject(DtOONumber);
                return Content(cCode, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return Content(HttpStatusCode.InternalServerError, JsonConvert.SerializeObject("Unknown error occured.-1429"));
            }
        }
    }
}
