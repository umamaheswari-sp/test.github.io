
using NAA_OSS_Synapse_API;
using PnP.Core.Model.SharePoint;
using PnP.Core.Services;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Runtime.Remoting.Contexts;
using System.Web;


namespace NAA_OSS_Synapse_API
{
    public class Helper
    {

       
        /// <summary>
        /// 
        /// </summary>
        /// <param name="strKey"></param>
        /// <returns></returns>
        public string GetConfigValue(string strKey)
        {
            string strRetVal = "";

            strRetVal = Convert.ToString(ConfigurationManager.AppSettings[strKey]);

            return strRetVal;
        }

        /// <summary>
        /// Setting the default values for API reponse is not returning the data
        /// </summary>
        /// <param name="dtFPTMCallback"></param>
        /// <returns></returns>
        public DataTable getDefaultColumns(DataTable dtFPTMCallback)
        {
            DataTable dtUpdated = dtFPTMCallback;

            dtUpdated.Columns.Add("PONumber");
            dtUpdated.Columns.Add("VehicleChargeAmount");
            dtUpdated.Columns.Add("TotalInvoiceDiscountRate");
            dtUpdated.Columns.Add("TravelTime");

            dtUpdated.Columns.Add("AmountOfHours");
            dtUpdated.Columns.Add("TimeTicketType");
            dtUpdated.Columns.Add("CustomerReferenceNumber");
            dtUpdated.Columns.Add("SpecificWording");
            dtUpdated.Columns.Add("CannotBillOverAmount");
            dtUpdated.Columns.Add("AccountGroupCode");
            dtUpdated.Columns.Add("JDENumber");
            dtUpdated.Columns.Add("UnitERPNumber");
            dtUpdated.Columns.Add("LegacyContractNumber");
            dtUpdated.Columns.Add("ContractERPNumber");
            dtUpdated.Columns.Add("Branch");
            dtUpdated.Columns.Add("ContractStatus");
            dtUpdated.Columns.Add("BusinessStream");
            dtUpdated.Columns.Add("PORequried");
            dtUpdated.Columns.Add("AccountName");
            dtUpdated.Columns.Add("AccountManager");
            dtUpdated.Columns.Add("AccountManagerEmail");
            dtUpdated.Columns.Add("InvoiceDelivery");

            dtUpdated.Columns.Add("ConsumableFee");

            dtUpdated.Columns.Add("VehicleCharge");
            dtUpdated.Columns.Add("OtherCustomerReferenceRequired");
            dtUpdated.Columns.Add("OverallDiscountOnTotalInvoiceAmount");
            dtUpdated.Columns.Add("DoesTheCustomerHaveATravelTimeRestriction");
            dtUpdated.Columns.Add("MaterialItemizationRequiredOnInvoice");
            dtUpdated.Columns.Add("SpecificWordingOrAdditionalLanguageRequiredOnInvoice");
            dtUpdated.Columns.Add("NotToExceedOnHoursBilled");


            dtUpdated.Columns.Add("EntrapmentsBillableVal");
            dtUpdated.Columns.Add("BillForExpensesVal");
            dtUpdated.Columns.Add("BillForTravelTimeVal");
            dtUpdated.Columns.Add("MaterialItemizationRequiredVal");
            dtUpdated.Columns.Add("ElevatorNameDesignationRequiredVal");
            dtUpdated.Columns.Add("VandalismBillableVal");
            dtUpdated.Columns.Add("TaxExemptVal");
            dtUpdated.Columns.Add("TimeTicketRequiredVal");

            dtUpdated.Columns.Add("EmailType");
            dtUpdated.Columns.Add("AccountID");
            dtUpdated.Columns.Add("ContractId");
            dtUpdated.Columns.Add("InvoiceEmail");

            dtUpdated.Columns.Add("IsNSA");

            dtUpdated.Columns.Add("NonBillableHours", typeof(System.Boolean));
            dtUpdated.Columns.Add("SHDataFound", typeof(System.Boolean));

            dtUpdated.Rows[0]["PONumber"] = "";

            dtUpdated.Rows[0]["AccountID"] = "";
            dtUpdated.Rows[0]["ContractId"] = "";

            dtUpdated.Rows[0]["PORequried"] = "No";
            dtUpdated.Rows[0]["VandalismBillableVal"] = "No";
            dtUpdated.Rows[0]["EntrapmentsBillableVal"] = "No";
            dtUpdated.Rows[0]["ConsumableFee"] = "0";
            dtUpdated.Rows[0]["VehicleChargeAmount"] = "0";
            dtUpdated.Rows[0]["VehicleCharge"] = "No";
            dtUpdated.Rows[0]["TotalInvoiceDiscountRate"] = "0";
            dtUpdated.Rows[0]["InvoiceDelivery"] = "Standard mail";
            dtUpdated.Rows[0]["InvoiceEmail"] = "";
            dtUpdated.Rows[0]["TimeTicketType"] = "";
            dtUpdated.Rows[0]["BusinessStream"] = "";
            dtUpdated.Rows[0]["BillForExpensesVal"] = "No";
            dtUpdated.Rows[0]["BillForTravelTimeVal"] = "Yes";
            dtUpdated.Rows[0]["TaxExemptVal"] = "No";
            dtUpdated.Rows[0]["MaterialItemizationRequiredVal"] = "No";
            dtUpdated.Rows[0]["MaterialItemizationRequiredOnInvoice"] = "No";
            dtUpdated.Rows[0]["TravelTime"] = "0";
            dtUpdated.Rows[0]["DoesTheCustomerHaveATravelTimeRestriction"] = "No";
            dtUpdated.Rows[0]["AmountOfHours"] = "";
            dtUpdated.Rows[0]["NotToExceedOnHoursBilled"] = "No";
            dtUpdated.Rows[0]["CustomerReferenceNumber"] = "";
            dtUpdated.Rows[0]["OtherCustomerReferenceRequired"] = "No";
            dtUpdated.Rows[0]["TimeTicketRequiredVal"] = "No";
            dtUpdated.Rows[0]["SpecificWording"] = "";
            dtUpdated.Rows[0]["SpecificWordingOrAdditionalLanguageRequiredOnInvoice"] = "No";
            dtUpdated.Rows[0]["CannotBillOverAmount"] = "";
            dtUpdated.Rows[0]["OverallDiscountOnTotalInvoiceAmount"] = "No";
            dtUpdated.Rows[0]["AccountGroupCode"] = "";
            dtUpdated.Rows[0]["AccountManager"] = "";
            dtUpdated.Rows[0]["AccountManagerEmail"] = "";
            dtUpdated.Rows[0]["EmailType"] = "";
            dtUpdated.Rows[0]["ElevatorNameDesignationRequiredVal"] = "No";
            dtUpdated.Rows[0]["IsNSA"] = "No";
            dtUpdated.Rows[0]["JDENumber"] = "";
            dtUpdated.Rows[0]["NonBillableHours"] = false;
            dtUpdated.Rows[0]["SHDataFound"] = false;

            return dtUpdated;
        }

        /// <summary>
        /// Setting update the conditional based values for True/Flase and dependant vlaues
        /// </summary>
        /// <param name="dtFPTMCallbackValues"></param>
        /// <param name="isPOFromCRM"></param>
        /// <returns></returns>
        public DataTable getDBValuesSet(DataTable dtFPTMCallbackValues, bool isPOFromCRM)
        {
            DataTable dtSetCallback = dtFPTMCallbackValues;

            dtSetCallback.Columns.Add("AccountName");
            dtSetCallback.Columns.Add("AccountManager");
            dtSetCallback.Columns.Add("AccountManagerEmail");



            dtSetCallback.Columns.Add("VehicleCharge");
            dtSetCallback.Columns.Add("OtherCustomerReferenceRequired");
            dtSetCallback.Columns.Add("OverallDiscountOnTotalInvoiceAmount");
            dtSetCallback.Columns.Add("DoesTheCustomerHaveATravelTimeRestriction");
            dtSetCallback.Columns.Add("MaterialItemizationRequiredOnInvoice");
            dtSetCallback.Columns.Add("SpecificWordingOrAdditionalLanguageRequiredOnInvoice");
            dtSetCallback.Columns.Add("NotToExceedOnHoursBilled");
            dtSetCallback.Columns.Add("PORequried");

            dtSetCallback.Columns.Add("EntrapmentsBillableVal");
            dtSetCallback.Columns.Add("BillForExpensesVal");
            dtSetCallback.Columns.Add("BillForTravelTimeVal");
            dtSetCallback.Columns.Add("MaterialItemizationRequiredVal");
            dtSetCallback.Columns.Add("ElevatorNameDesignationRequiredVal");
            dtSetCallback.Columns.Add("VandalismBillableVal");
            dtSetCallback.Columns.Add("TaxExemptVal");
            dtSetCallback.Columns.Add("TimeTicketRequiredVal");
            dtSetCallback.Columns.Add("EmailType");
            dtSetCallback.Columns.Add("AccountID");
            dtSetCallback.Columns.Add("InvoiceEmail");

            dtSetCallback.Columns.Add("IsNSA");

            dtSetCallback.Columns.Add("NonBillableHours", typeof(System.Boolean));
            dtSetCallback.Columns.Add("SHDataFound", typeof(System.Boolean));





            // Set Yes/No values based on dependant columns

            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["VehicleChargeAmount"])))
            {
                dtSetCallback.Rows[0]["VehicleCharge"] = "Yes";
            }
            else
            {
                dtSetCallback.Rows[0]["VehicleCharge"] = "No";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["CustomerReferenceNumber"])))
            {
                dtSetCallback.Rows[0]["OtherCustomerReferenceRequired"] = "Yes";
            }
            else
            {
                dtSetCallback.Rows[0]["OtherCustomerReferenceRequired"] = "No";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["TotalInvoiceDiscountRate"])))
            {
                dtSetCallback.Rows[0]["OverallDiscountOnTotalInvoiceAmount"] = "Yes";
            }
            else
            {
                dtSetCallback.Rows[0]["OverallDiscountOnTotalInvoiceAmount"] = "No";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["TravelTime"])))
            {
                dtSetCallback.Rows[0]["DoesTheCustomerHaveATravelTimeRestriction"] = "Yes";
            }
            else
            {
                dtSetCallback.Rows[0]["DoesTheCustomerHaveATravelTimeRestriction"] = "No";
            }


            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["MaterialItemizationRequired"])))
            {
                dtSetCallback.Rows[0]["MaterialItemizationRequiredOnInvoice"] = "Yes";
            }
            else
            {
                dtSetCallback.Rows[0]["MaterialItemizationRequiredOnInvoice"] = "No";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["SpecificWording"])))
            {
                dtSetCallback.Rows[0]["SpecificWordingOrAdditionalLanguageRequiredOnInvoice"] = "Yes";
            }
            else
            {
                dtSetCallback.Rows[0]["SpecificWordingOrAdditionalLanguageRequiredOnInvoice"] = "No";
            }
            if (isPOFromCRM)
            {
                if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["PONumber"])))
                {
                    dtSetCallback.Rows[0]["PONumber"] = dtSetCallback.Rows[0]["PONumber"];
                }
                else
                {
                    dtSetCallback.Rows[0]["PONumber"] = "";
                }
                if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["POWORequired"])))
                {
                    if (Convert.ToBoolean(dtSetCallback.Rows[0]["POWORequired"]))
                    {
                        dtSetCallback.Rows[0]["PORequried"] = "Yes";
                    }
                    else
                    {
                        dtSetCallback.Rows[0]["PORequried"] = "No";
                    }
                }
                else
                {
                    dtSetCallback.Rows[0]["PORequried"] = "No";
                }
            }

            if ((!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["AmountOfHours"]))) && (Convert.ToString(dtSetCallback.Rows[0]["AmountOfHours"]) != "0"))
            {
                dtSetCallback.Rows[0]["NotToExceedOnHoursBilled"] = "Yes";
            }
            else
            {
                dtSetCallback.Rows[0]["NotToExceedOnHoursBilled"] = "No";
            }


            //True or false set to Yes/No

            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["EntrapmentsBillable"])))
            {
                if (Convert.ToBoolean(dtSetCallback.Rows[0]["EntrapmentsBillable"]))
                {
                    dtSetCallback.Rows[0]["EntrapmentsBillableVal"] = "Yes";
                }
                else
                {
                    dtSetCallback.Rows[0]["EntrapmentsBillableVal"] = "No";
                }
            }
            else
            {
                dtSetCallback.Rows[0]["EntrapmentsBillableVal"] = "No";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["BillForExpenses"])))
            {
                if (Convert.ToBoolean(dtSetCallback.Rows[0]["BillForExpenses"]))
                {
                    dtSetCallback.Rows[0]["BillForExpensesVal"] = "Yes";
                }
                else
                {
                    dtSetCallback.Rows[0]["BillForExpensesVal"] = "No";
                }
            }
            else
            {
                dtSetCallback.Rows[0]["BillForExpensesVal"] = "No";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["BillForTravelTime"])))
            {
                if (Convert.ToBoolean(dtSetCallback.Rows[0]["BillForTravelTime"]))
                {
                    dtSetCallback.Rows[0]["BillForTravelTimeVal"] = "Yes";
                }
                else
                {
                    dtSetCallback.Rows[0]["BillForTravelTimeVal"] = "No";
                }
            }
            else
            {
                dtSetCallback.Rows[0]["BillForTravelTimeVal"] = "No";
            }


            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["MaterialItemizationRequired"])))
            {
                if (Convert.ToBoolean(dtSetCallback.Rows[0]["MaterialItemizationRequired"]))
                {
                    dtSetCallback.Rows[0]["MaterialItemizationRequiredVal"] = "Yes";
                }
                else
                {
                    dtSetCallback.Rows[0]["MaterialItemizationRequiredVal"] = "No";
                }
            }
            else
            {
                dtSetCallback.Rows[0]["MaterialItemizationRequiredVal"] = "No";
            }


            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["ElevatorNameDesignationRequired"])))
            {
                if (Convert.ToBoolean(dtSetCallback.Rows[0]["ElevatorNameDesignationRequired"]))
                {
                    dtSetCallback.Rows[0]["ElevatorNameDesignationRequiredVal"] = "Yes";
                }
                else
                {
                    dtSetCallback.Rows[0]["ElevatorNameDesignationRequiredVal"] = "No";
                }
            }
            else
            {
                dtSetCallback.Rows[0]["ElevatorNameDesignationRequiredVal"] = "No";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["VandalismBillable"])))
            {
                if (Convert.ToBoolean(dtSetCallback.Rows[0]["VandalismBillable"]))
                {
                    dtSetCallback.Rows[0]["VandalismBillableVal"] = "Yes";
                }
                else
                {
                    dtSetCallback.Rows[0]["VandalismBillableVal"] = "No";
                }
            }
            else
            {
                dtSetCallback.Rows[0]["VandalismBillableVal"] = "No";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["TaxExempt"])))
            {
                if (Convert.ToBoolean(dtSetCallback.Rows[0]["TaxExempt"]))
                {
                    dtSetCallback.Rows[0]["TaxExemptVal"] = "Yes";
                }
                else
                {
                    dtSetCallback.Rows[0]["TaxExemptVal"] = "No";
                }
            }
            else
            {
                dtSetCallback.Rows[0]["TaxExemptVal"] = "No";
            }


            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["TimeTicketRequired"])))
            {
                if (Convert.ToBoolean(dtSetCallback.Rows[0]["TimeTicketRequired"]))
                {
                    dtSetCallback.Rows[0]["TimeTicketRequiredVal"] = "Yes";
                }
                else
                {
                    dtSetCallback.Rows[0]["TimeTicketRequiredVal"] = "No";
                }
            }
            else
            {
                dtSetCallback.Rows[0]["TimeTicketRequiredVal"] = "No";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["AccountGroupCode"])))
            {
                dtSetCallback.Rows[0]["IsNSA"] = "Yes";
            }
            else
            {
                dtSetCallback.Rows[0]["IsNSA"] = "No";
            }

            if (isPOFromCRM)
            {
                if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["InvoiceDelivery"])))
                {
                    dtSetCallback.Rows[0]["InvoiceDelivery"] = Convert.ToString(dtSetCallback.Rows[0]["InvoiceDelivery"]);
                }
                else
                {
                    dtSetCallback.Rows[0]["InvoiceDelivery"] = "Standard mail";
                }
            }

            if (!string.IsNullOrEmpty(Convert.ToString(dtSetCallback.Rows[0]["ContractId"])))
            {
                dtSetCallback.Rows[0]["ContractId"] = Convert.ToString(dtSetCallback.Rows[0]["ContractId"]);
            }
            else
            {
                dtSetCallback.Rows[0]["ContractId"] = "";
            }


            return dtSetCallback;
        }

        /// <summary>
        /// Set the logic for getting the Invocie Mail based on Email Type : T Billing Contact or Primary Billing Contact
        /// </summary>
        /// <param name="dtAccuontData"></param>
        /// <param name="dtCallbackFPTMInspection"></param>
        /// <param name="isPOFromCRM"></param>
        /// <returns></returns>
        public DataTable getDBValuesSetAccount(DataTable dtAccuontData, DataTable dtCallbackFPTMInspection, bool isPOFromCRM, char[] email_ValidationArr)
        {
            DataTable dtSetAccountData = dtAccuontData;

            string InvoiceDelivery = Convert.ToString(dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"]);


            if (dtSetAccountData.Rows.Count >= 1)
            {
                if (!string.IsNullOrEmpty(InvoiceDelivery))
                {
                    if (InvoiceDelivery == "Regular Mail - Default")
                    {
                        dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"] = "Standard mail";
                    }
                    else if (InvoiceDelivery == "Email")
                    {
                        dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"] = "Email";
                    }
                    else if (InvoiceDelivery == "Email + Regular Mail")
                    {
                        dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"] = "Both Standard Mail and Email";
                    }
                    else
                    {
                        dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"] = "Standard mail";
                    }

                    if (InvoiceDelivery == "Email" || InvoiceDelivery == "Email + Regular Mail" || InvoiceDelivery == "Standard mail")
                    {
                        DataView dvEmailTypeTBill = new DataView();
                        DataTable dtEmailTypeTBill = new DataTable();

                        DataView dvEmailTypePBill = new DataView();
                        DataTable dtEmailTypePBill = new DataTable();

                        DataView dvInvoiceEmail = new DataView();
                        DataTable dtInvoiceEmail = new DataTable();

                        DataTable dtFilteredEmailType = new DataTable();



                        dtFilteredEmailType = dtAccuontData;

                        dvEmailTypeTBill = dtFilteredEmailType.DefaultView;
                        dvEmailTypeTBill.RowFilter = "EmailType='T Billing Contact' AND Isnull(InvoiceEmail,'') <> ''";
                        dtEmailTypeTBill = dvEmailTypeTBill.ToTable();
                        dvEmailTypeTBill.RowFilter = string.Empty;

                       //1066700 - Merging multi T-Bill/Primary Bill emails and adding into sharepoint Email column - Start

                        if (dtEmailTypeTBill.Rows.Count > 0)
                        {
                            
                            dtSetAccountData = dtEmailTypeTBill.Rows[0].Table;
                            DataTable dtSetAccountData_Mail = dtEmailTypeTBill.DefaultView.ToTable(true, "InvoiceEmail");
                            if (dtSetAccountData_Mail.Rows.Count > 1)
                            {
                                var lstUniqueData = dtSetAccountData_Mail.AsEnumerable().Select(r => r["InvoiceEmail"].ToString());
                                string emailInformation = string.Join(";", lstUniqueData);
                                dtCallbackFPTMInspection.Rows[0]["InvoiceEmail"] = emailInformation;
                            }
                            else
                            {
                                string customerEmail = Convert.ToString(dtSetAccountData.Rows[0]["InvoiceEmail"]).Trim();
                               
                                foreach (char i in email_ValidationArr)
                                {
                                   
                                    
                                    if(customerEmail.IndexOf(i) > -1)
                                    {
                                        customerEmail = customerEmail.Replace(i, ';') ;
                                    }
                                }
                                customerEmail = customerEmail.Replace(";;;", ";");
                                customerEmail =customerEmail.Replace(";;", ";");
                                dtCallbackFPTMInspection.Rows[0]["InvoiceEmail"] = customerEmail.Trim();

                            }
                        }
                        else
                        {
                            dtFilteredEmailType = dtAccuontData;
                            dvEmailTypePBill = dtFilteredEmailType.DefaultView;
                            dvEmailTypePBill.RowFilter = "EmailType='Primary Billing Contact'  AND Isnull(InvoiceEmail,'') <> ''";
                            dtEmailTypePBill = dvEmailTypePBill.ToTable();
                            dvEmailTypePBill.RowFilter = string.Empty;

                            if (dtEmailTypePBill.Rows.Count > 0)
                            {
                               
                                dtSetAccountData = dtEmailTypePBill.Rows[0].Table;
                                DataTable dtSetAccountData_Mail = dtEmailTypePBill.DefaultView.ToTable(true, "InvoiceEmail");
                                if (dtSetAccountData_Mail.Rows.Count > 1)
                                {
                                    var lst_UniqueData = dtSetAccountData_Mail.AsEnumerable().Select(r => r["InvoiceEmail"].ToString());
                                    string emailInformation = string.Join(";", lst_UniqueData);
                                    dtCallbackFPTMInspection.Rows[0]["InvoiceEmail"] = emailInformation;
                                }
                                else
                                {
                                    string customer_email = Convert.ToString(dtSetAccountData.Rows[0]["InvoiceEmail"]).Trim();
                                    foreach (char i in email_ValidationArr)
                                    {
                                        Console.WriteLine(i);
                                        if (customer_email.IndexOf(i) > -1)
                                        {
                                            customer_email = customer_email.Replace(i, ';');
                                        }
                                    }
                                    customer_email.Replace(";;;", ";");
                                    customer_email.Replace(";;", ";");
                                    dtCallbackFPTMInspection.Rows[0]["InvoiceEmail"] = customer_email.Trim();
                                }

                            } 
                        }
                    } //1066700 - End
                }
                else
                {
                    dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"] = "Standard mail";
                    dtCallbackFPTMInspection.Rows[0]["InvoiceEmail"] = "";
                }
                dtCallbackFPTMInspection.Rows[0]["AccountManager"] = dtSetAccountData.Rows[0]["AccountManager"];
                dtCallbackFPTMInspection.Rows[0]["EmailType"] = dtSetAccountData.Rows[0]["EmailType"];
                dtCallbackFPTMInspection.Rows[0]["AccountID"] = dtSetAccountData.Rows[0]["AccountID"];
                dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = dtSetAccountData.Rows[0]["AccountManagerEmail"];
                dtCallbackFPTMInspection.Rows[0]["AccountName"] = dtSetAccountData.Rows[0]["AccountName"];
            }
            else
            {
                dtCallbackFPTMInspection.Rows[0]["InvoiceDelivery"] = "Standard mail";
                dtCallbackFPTMInspection.Rows[0]["InvoiceEmail"] = "";
                dtCallbackFPTMInspection.Rows[0]["AccountManager"] = "";
                dtCallbackFPTMInspection.Rows[0]["AccountManagerEmail"] = "";
                dtCallbackFPTMInspection.Rows[0]["AccountName"] = "";
                dtCallbackFPTMInspection.Rows[0]["AccountID"] = "";
            }

            return dtCallbackFPTMInspection;
        }


        /// <summary>
        /// Add the expection message to SharePoint List
        /// </summary>
        /// <param name="strLogCode"></param>
        /// <param name="strLogCategory"></param>
        /// <param name="strLogMessage"></param>
        public async void addToLog(int strLogCode, string strLogCategory, string strLogMessage)
        {
            try
            {
                PnPContext objAddToLog = await PnPSharePointContext.GetPnPContext(Convert.ToString(ConfigurationManager.AppSettings["OSSSiteURL"]));
                using (objAddToLog)
                {
                    // ClientContext objAddToLog = GetContext();
                    IList lstLog = objAddToLog.Web.Lists.GetByTitle("CL_CRMDBCallbackLog");
                    Dictionary<string, object> values = new Dictionary<string, object>
                    {
                        { "Code", strLogCode },
                        { "Title", strLogCategory },
                        { "Message", strLogMessage },                     
                    };

                    // Use the AddBatch method to add the request to the current batch
                    await lstLog.Items.AddBatchAsync(values);

                    await objAddToLog.ExecuteAsync();
                }
            }
            catch (Exception exaddToLog)
            {
                Console.WriteLine(exaddToLog.Message.ToString() + "--" + exaddToLog.StackTrace.ToString());
            }
        }
    }
}