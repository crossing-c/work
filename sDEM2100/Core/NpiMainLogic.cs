using Compal.MESComponent;
using sDEM2100.DataGateway;
using sDEM2100.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Serialization;

namespace sDEM2100.Core
{
    class NpiMainLogic
    {
        private object[] mClientInfo;
        private MESLog mesLog;
        private string mDbName;
        private NpiSqlOperater npiSqlOperater;
        private static Hashtable htPlantCode;
        public NpiMainLogic(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.mesLog = new MESLog("Dqms");
            npiSqlOperater = new NpiSqlOperater(mClientInfo, dbName);
            if (htPlantCode == null)
            {
                htPlantCode = new Hashtable();
                htPlantCode.Add("KSP2", "A32");
                htPlantCode.Add("KSP3", "A31(KS)");
                htPlantCode.Add("KSP4", "C38");
                htPlantCode.Add("CDP1", "A31(CD)");
                htPlantCode.Add("CQP1", "ABO");
                htPlantCode.Add("TW01", "A31(TW01)");
            }
        }

        public ExecutionResult ExecuteNpiCleanLaunch(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            List<DataSet> lists = new List<DataSet>();
            string[] titles = new string[] { "NPIPHASEGATE", "SAFELAUNCH" };
            foreach (string title in titles)
            {
                DataSet dsRes = null;
                string url = GetUrl(title);
                string plantCode = htPlantCode[plant].ToString();
                if (!string.IsNullOrEmpty(url))
                {
                    dsRes = GetDataFromWS(url, plantCode);
                }
                lists.Add(dsRes);
            }
            //CleanLaunch
            DataSet dsCL = new DataSet();
            DataTable dtResult = new DataTable();
            string fromTime = WorkDateConvert.GetWorkDate("WEEKLY", -13);
            string endTime = WorkDateConvert.GetWorkDate("WEEKLY", 0);
            dtResult = npiSqlOperater.GetModelData(htPlantCode[plant].ToString(), fromTime, endTime);
            #region Merge
            //double now = 0, pre = 0;
            foreach(DataRow dr in dtResult.Rows)
            {
                string modelName = dr["model_name"].ToString();
                dr["total_fail"] = npiSqlOperater.GetWeekTotalValue(plant, modelName);
                #region 箭头颜色逻辑
                //DataTable dtTemp = npiSqlOperater.GetWeekValue(plant, modelName);
                //if (dtTemp != null && dtTemp.Rows.Count > 0)
                //{
                //if (dtTemp.Rows.Count == 2)
                //{
                //    string nowData = dtTemp.Rows[0]["total_fail"].ToString();
                //    string preData = dtTemp.Rows[1]["total_fail"].ToString();
                //    double.TryParse(nowData, out now);
                //    double.TryParse(preData, out pre);
                //    dr["total_fail"] = now;
                //    //参照FPY
                //    if (now>pre)
                //    {
                //        dr["trend"] = "1";//上
                //        dr["color"] = "1";//Blue
                //    }else if (now<pre)
                //    {
                //        dr["trend"] = "-1";//下
                //        dr["color"] = "-1";//Red
                //    }
                //    else
                //    {
                //        dr["trend"] = "0";//->
                //        dr["color"] = "0";//Orange
                //    }
                //}
                //else
                //{
                //    dr["total_fail"] = dtTemp.Rows[0]["total_fail"].ToString();
                //}
                //}
                #endregion
            }
            #endregion
            dsCL.Tables.Add(dtResult.Copy());
            lists.Add(dsCL);
            exeRes.Anything = lists;
            return exeRes;
            #endregion
        }

        private DataSet GetDataFromWS(string url, string plantCode)
        {
            DataSet dsRes = new DataSet();
            Hashtable ht = new Hashtable();
            ht.Add("CustomerCode", plantCode);
            string res = Utilities.HttpHelper.PostXmlResponse(url, ht);
            StringReader strReader = new StringReader(res);
            using (XmlReader xmlReader = XmlReader.Create(strReader))
            {
                XmlSerializer serializer = new XmlSerializer(typeof(DataSet), "http://tempuri.org/");
                if (serializer.CanDeserialize(xmlReader))
                {
                    dsRes = serializer.Deserialize(xmlReader) as DataSet;
                }
            }
            return dsRes;
        }

        private string GetUrl(string title)
        {
            string url = string.Empty;
            title = title ?? "".ToUpper();
            switch (title)
            {
                case "NPIPHASEGATE":
                    url = "http://ksrdap1/wsotbm/WS_QueryNPIPhaseGateReport.asmx/uf_qry_NPIPhaseGate";
                    break;
                case "SAFELAUNCH":
                    url = "http://ksrdap1/wsotbm/WS_QueryNPIPhaseGateReport.asmx/uf_qry_SafeLaunch";
                    break;
                case "CLEANLAUNCH":
                    url = "";
                    break;
                default:
                    break;
            }
            return url;
        }
    }
}
