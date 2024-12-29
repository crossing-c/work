using sDEM2100.Beans;
using Compal.MESComponent;
using sDEM2100.DataGateway;
using sDEM2100.Utils;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;

namespace sDEM2100.Core
{
    class KnownIssueControl
    {
        private DateTime dtNow;
        private static KnownIssueOperator knownIssueOperator;
        private object[] mClientInfo;
        public KnownIssueControl(object[] clientInfo, string DBName)
        {
            dtNow = DateTime.Now;
            knownIssueOperator = new KnownIssueOperator(clientInfo, DBName);
            mClientInfo = clientInfo;
        }

        public object[] GetKnownIssueTable(object[] objParam)
        {
            string bydateType = "", qCond = "";

            bydateType = objParam[0].ToString();
            qCond = objParam[1].ToString();

            DataTable dtResult = new DataTable();
            dtResult = knownIssueOperator.GetKnownIssueTable(bydateType, qCond);

            return new object[] { dtResult };
        }

        public ExecutionResult GetKnownIssueDPPMData(object[] objParam)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            string strPlant = "", strModel = "", strPart = "", strIssue = "", strVendor = "", strbydateType = "", strqCond = "";
            strPlant = objParam[0].ToString();
            strModel = objParam[1].ToString();
            strPart = objParam[2].ToString();
            strIssue = objParam[3].ToString();
            strVendor = objParam[4].ToString();
            strbydateType = objParam[5].ToString();
            strqCond = objParam[6].ToString();
            DateTime dBaseDate = DateTime.Now;
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            if (strbydateType == "Daily")
                dBaseDate = DateTime.ParseExact(strqCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
            else if (strbydateType == "Weekly")
            {
                string strYear, strWeek;
                string strCond = strqCond.Replace("W", "");
                strYear = strCond.Substring(0, 4);
                strWeek = strCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                dBaseDate = sTime;
            }
            else if (strbydateType == "Monthly")
                dBaseDate = DateTime.ParseExact(strqCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
            else if (strbydateType == "BETWEEN")
            {
                string[] qcond = strqCond.Split('_');
                dBaseDate = DateTime.ParseExact(qcond[1].ToString().Replace("-", ""), "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
            }
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            dtDay = knownIssueOperator.GetKnownIssueDPPM(strPlant, strModel, strPart, strIssue, strVendor, "Daily", dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"));
            dicResult.Add("FpyDay", dtDay);
            dtWeek = knownIssueOperator.GetKnownIssueDPPM(strPlant, strModel, strPart, strIssue, strVendor, "Weekly", dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"));
            dicResult.Add("FpyWeek", dtWeek);
            dtMonth = knownIssueOperator.GetKnownIssueDPPM(strPlant, strModel, strPart, strIssue, strVendor, "Monthly", dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"));
            dicResult.Add("FpyMonth", dtMonth);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
    }
}
