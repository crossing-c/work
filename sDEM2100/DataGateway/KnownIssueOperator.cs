using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using sDEM2100.Utils;
using System.Globalization;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class KnownIssueOperator
    {
        private InfoLightDBTools mDBTools;
        private InfoLightDBTools mDCDBTools;
        private object[] mClientInfo;
        public KnownIssueOperator(object[] clientInfo, string dbName)
        {
            mDBTools = new InfoLightDBTools(clientInfo, dbName);
            mDCDBTools = new InfoLightDBTools(clientInfo, "DC");
            mClientInfo = clientInfo;
        }
        public DataTable GetKnownIssueTable(string vDateType, string vQCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();

            string sql = "", sql1 = "";
            if (vDateType == "BETWEEN" || vDateType == "Weekly")
                sql1 += " and t.work_date between :qcond and :qcond2 ";
            else if (vDateType == "Daily")
                sql1 += " and t.work_date = :qcond  ";
            else if (vDateType == "Monthly")
                sql1 += " and t.work_date like :qcond  ";

            sql = @"    select t.plant_code,t.model_name as Model,t.part_type as Part,t.issue_name as Issue,t.Vendor,
                                      nvl(sum(t.input_qty),0) as Input,
                                      nvl(sum(t.qc_fail_qty),0) as Fail,
                                      case
                                         when nvl(sum(t.input_qty),0) = 0 then
                                          0
                                         when nvl(sum(t.input_qty),0) < nvl(sum(t.qc_fail_qty),0) then
                                          0
                                         else
                                          round((nvl(sum(t.qc_fail_qty),0) / nvl(sum(t.input_qty),0)) * 100, 2)
                                       end fr ,                                    
                                      nvl(sum(t.mfg_vid_qty),0) as vid_qty,
                                      nvl(sum(t.sqe_osv_qty),0) as osv_qty,
                                      nvl(sum(t.mfg_vid_qty),0)-nvl(sum(t.sqe_osv_qty),0) as gap 
                                      from SFISM4.R_KNOWN_ISSUE_T t where 1=1 " + sql1 + @"
                                      group by t.plant_code,t.model_name,t.part_type,t.issue_name,t.Vendor
                                      order by t.plant_code,t.model_name,t.part_type,t.issue_name,t.Vendor";

            ht.Clear();

            if (vDateType == "Daily")
                ht.Add("qcond", vQCond);
            else if (vDateType == "Weekly")
            {
                string strYear, strWeek;
                vQCond = vQCond.Replace("W", "");
                strYear = vQCond.Substring(0, 4);
                strWeek = vQCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                ht.Add("qcond", sTime.ToString("yyyyMMdd"));
                ht.Add("qcond2", eTime.ToString("yyyyMMdd"));
            }
            else if (vDateType == "Monthly")
                ht.Add("qcond", vQCond.Replace("M", "") + "%");
            else if (vDateType == "BETWEEN")
            {
                string[] qcond = vQCond.Split('_');
                ht.Add("qcond", qcond[0].ToString().Replace("-", ""));
                ht.Add("qcond2", qcond[1].ToString().Replace("-", ""));
            }

            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetKnownIssueDPPM(string vPlant, string vModel, string vPart, string vIssue, string vVendor, string vDateType, string vSDate, string vEDate)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlType = "";

            if (vDateType == "Daily")
                sqlType = " t.work_date ";
            else if (vDateType == "Weekly")
                sqlType = " 'W'|| to_char(to_date(t.work_date,'YYYYMMDD'),'IW') ";
            else
                sqlType = "substr(t.work_date,1,4)||  'M'|| substr(t.work_date,5,2) ";

            string sql = @" select " + sqlType + @" as catalogy,      
                                      nvl(sum(t.input_qty),0) as Input,
                                      nvl(sum(t.qc_fail_qty),0) as Fail,
                                      case
                                         when nvl(sum(t.input_qty),0) = 0 then
                                          0
                                         when nvl(sum(t.input_qty),0) < nvl(sum(t.qc_fail_qty),0) then
                                          0
                                         else
                                          round((nvl(sum(t.qc_fail_qty),0) / nvl(sum(t.input_qty),0)) * 1000000)
                                       end DPPM
                                  from SFISM4.R_KNOWN_ISSUE_T t 
                                  where t.work_date between :qcond and :qcond2   
                                        and t.plant_code=:plant and t.model_name=:model 
                                        and t.part_type=:part and t.issue_name=:issue 
                                        and t.vendor=:vendor
                                 group by " + sqlType + @"
                                 order by " + sqlType;

            ht.Clear();
            ht.Add("plant", vPlant);
            ht.Add("model", vModel);
            ht.Add("part", vPart);
            ht.Add("issue", vIssue);
            ht.Add("vendor", vVendor);
            ht.Add("qcond", vSDate);
            ht.Add("qcond2", vEDate);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }
    }
}
