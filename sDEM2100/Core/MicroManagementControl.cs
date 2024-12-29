using sDEM2100.Beans;
using sDEM2100.DataGateway;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using Newtonsoft.Json;
using sDEM2100.Utils;
namespace sDEM2100.Core
{
    class MicroManagementControl
    {
        private DateTime dtNow;
        private static MicroManagementOperator micromanagementOperator;
        private static MpSqlOperater mpsqlOperator;
        private object[] mClientInfo;
        public MicroManagementControl(object[] clientInfo, string DBName)
        {
            dtNow = DateTime.Now;
            micromanagementOperator = new MicroManagementOperator(clientInfo, DBName);
            mpsqlOperator = new MpSqlOperater(clientInfo, DBName);
            mClientInfo = clientInfo;
        }

        public string getCustList(object[] objParam)
        {
            string plant = objParam[0].ToString();
            plant = plant.Replace("TW01", "CTY1");
            DataTable dtResult = new DataTable();
            DataTable dt = micromanagementOperator.getCustList(plant);
            if (dt.Rows.Count > 0)
            {
                string strCust = dt.Rows[0][0].ToString();
                if (strCust.IndexOf(",") > -1)
                {
                    string[] aa = strCust.Split(',');
                    dtResult.Columns.Add("CUSTOMER");
                    for (int i = 0; i < aa.Length; i++)
                    {
                        dtResult.Rows.Add(aa[i]);
                    }
                }
                else
                    dtResult = dt;
            }
            return JsonConvert.SerializeObject(dtResult);
        }


        public object[] GetData(object[] objParam)
        {
            if (objParam.Length == 5)
            {
                #region table
                List<MicroManagementModels> modellist = new List<MicroManagementModels>();
                string plant = "", qCond = "", qCust = "", qDate = "", qType = "", sTime = "", eTime = "";
                string categories = "";
                int iTop = 5;
                plant = objParam[0].ToString();
                qCond = objParam[1].ToString();
                qCust = objParam[2].ToString();
                qDate = objParam[3].ToString();
                qType = objParam[4].ToString();
                plant = plant.Replace("TW01", "CTY1");
                if (qType.IndexOf("_") > -1)
                {
                    string[] cc = qType.Split('_');
                    qType = cc[0];
                    iTop = Int32.Parse(cc[1]);
                }
                plant = plant.Replace("TW01", "CTY1");
                if (qDate.IndexOf("M") > -1)
                {
                    string strDate = qDate.Replace("M", "");
                    sTime = strDate + "01";
                    eTime = DateTime.ParseExact(strDate + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).AddDays(-1).ToString("yyyyMMdd");
                }
                else if (qDate.IndexOf("W") > -1)
                {
                    DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, qDate.Replace("W", ""), qDate.Replace("W", ""), mClientInfo);
                    if (dtWeeks.Rows.Count > 0)
                    {
                        sTime = dtWeeks.Rows[0]["fromday"].ToString().Replace("/", "");
                        eTime = dtWeeks.Rows[0]["today"].ToString().Replace("/", "");
                    }
                }
                else
                {
                    sTime = qDate;
                    eTime = qDate;
                }
                DataTable dtError = new DataTable();
                dtError = micromanagementOperator.GetErrorTable(plant, qType, sTime, eTime, qCond, qCust, iTop.ToString());
                DataTable dtResult = new DataTable();
                dtResult.Columns.Add("TOP");
                dtResult.Columns.Add("ERROR_CODE");
                dtResult.Columns.Add("QTY");
                dtResult.Columns.Add("FR");
                dtResult.Columns.Add("PERCENTS");
                if (dtError.Rows.Count < iTop)
                    iTop = dtError.Rows.Count;
                for (int i = 0; i < dtError.Rows.Count; i++)
                {
                    dtResult.Rows.Clear();
                    MicroManagementModels model = new MicroManagementModels();
                    model.ID = "ChartE" + i;
                    DataRow dr = dtResult.NewRow();
                    dr["TOP"] = "TOP" + (i + 1).ToString();
                    dr["ERROR_CODE"] = dtError.Rows[i]["catalogy"];
                    dr["QTY"] = dtError.Rows[i]["qty"];
                    dr["FR"] = dtError.Rows[i]["FR"];
                    dr["PERCENTS"] = dtError.Rows[i]["percents"];
                    dtResult.Rows.Add(dr);
                    model.DtError = dtResult.Copy();
                    modellist.Add(model);
                }
                return new object[] { categories, modellist };
                #endregion
            }
            else
            {
                #region default
                List<MicroManagementModels> modellist = new List<MicroManagementModels>();
                string plant = objParam[0].ToString();
                string bydate = objParam[1].ToString();
                string fdate = objParam[2].ToString();
                string edate = objParam[3].ToString();
                string Indexs = objParam[4].ToString();
                string Cust = objParam[5].ToString();
                plant = plant.Replace("TW01", "CTY1");
                if (Indexs != null && Indexs != "")
                {
                    string categories = GetCategories(bydate.Substring(0, 1), fdate, edate);
                    for (int i = 0; i < Indexs.Split(';').Length; i++)
                    {
                        string index = Indexs.Split(';')[i];
                        if (index != "")
                        {
                            MicroManagementModels model = new MicroManagementModels();
                            model.IndexName = index;
                            model.ID = "Chart" + i;
                            DataTable dt = new DataTable();
                            Dictionary<string, string> dicTime;
                            Dictionary<string, string> dicTime1 = null;
                            Dictionary<string, string> dicTime2 = null;
                            if (bydate == "Daily")
                                dicTime = GetTime("D", fdate, edate);
                            else if (bydate == "Weekly")
                                dicTime = GetTime("W", fdate, edate);
                            else if (bydate == "Monthly")
                                dicTime = GetTime("M", fdate, edate);
                            else
                            {
                                dicTime = GetTime("M", DateTime.Now.AddMonths(-5).ToString("yyyy-MM-dd"), DateTime.Now.ToString("yyyy-MM-dd"));
                                dicTime1 = GetTime("W", DateTime.Now.AddDays(-21).ToString("yyyy-MM-dd"), DateTime.Now.ToString("yyyy-MM-dd"));
                                dicTime2 = GetTime("D", DateTime.Now.AddDays(-2).ToString("yyyy-MM-dd"), DateTime.Now.ToString("yyyy-MM-dd"));
                            }

                            switch (index)
                            {
                                case "Detail":
                                    break;
                                default:
                                    #region
                                    model.Unit = "%";
                                    model.Goal = "/";
                                    if (bydate == "Daily")
                                    {
                                        string sTime = dicTime["fromTime"].ToString();
                                        string eTime = dicTime["endTime"].ToString();
                                        DataTable dtTemp = micromanagementOperator.GetFailRateTrend(plant, bydate, sTime, eTime, index, Cust);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                    if (bydate == "Weekly")
                                    {
                                        DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, dicTime["fromTime"].Replace("W", ""), dicTime["endTime"].Replace("W", ""), mClientInfo);
                                        if (dtWeeks.Rows.Count > 0)
                                        {
                                            string sTime = dtWeeks.Rows[0]["fromday"].ToString().Replace("/", "");
                                            string eTime = dtWeeks.Rows[0]["today"].ToString().Replace("/", "");
                                            DataTable dtTemp = micromanagementOperator.GetFailRateTrend(plant, bydate, sTime, eTime, index, Cust);
                                            if (dtTemp.Rows.Count > 0)
                                                dt.Merge(dtTemp);
                                        }
                                    }
                                    if (bydate == "Monthly")
                                    {
                                        string sTime = dicTime["fromTime"].ToString().Replace("M", "") + "01";
                                        string eTime = DateTime.ParseExact(dicTime["endTime"].ToString().Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).AddDays(-1).ToString("yyyyMMdd");
                                        DataTable dtTemp = micromanagementOperator.GetFailRateTrend(plant, bydate, sTime, eTime, index, Cust);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);

                                    }
                                    if (bydate == "Xed")
                                    {
                                        string sTime = dicTime["fromTime"].ToString().Replace("M", "") + "01";
                                        string eTime = DateTime.ParseExact(dicTime["endTime"].ToString().Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).AddDays(-1).ToString("yyyyMMdd");
                                        DataTable dtTemp = micromanagementOperator.GetFailRateTrend(plant, "Monthly", sTime, eTime, index, Cust);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                        DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, dicTime1["fromTime"].Replace("W", ""), dicTime1["endTime"].Replace("W", ""), mClientInfo);
                                        if (dtWeeks.Rows.Count > 0)
                                        {
                                            sTime = dtWeeks.Rows[0]["fromday"].ToString().Replace("/", "");
                                            eTime = dtWeeks.Rows[0]["today"].ToString().Replace("/", "");
                                            dtTemp = micromanagementOperator.GetFailRateTrend(plant, "Weekly", sTime, eTime, index, Cust);
                                            if (dtTemp.Rows.Count > 0)
                                                dt.Merge(dtTemp);
                                        }
                                        sTime = dicTime2["fromTime"].ToString();
                                        eTime = dicTime2["endTime"].ToString();
                                        dtTemp = micromanagementOperator.GetFailRateTrend(plant, "Daily", sTime, eTime, index, Cust);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                    if (dt.Rows.Count > 0)
                                        dt.Columns["date1"].ColumnName = "date";
                                    break;
                                    #endregion
                            }

                            if (dt.Rows.Count > 0)
                            {
                                for (int k = 0; k < categories.Split(';').Length; k++)
                                {
                                    DataRow[] drli = dt.Select(" date='" + categories.Split(';')[k] + "' ");
                                    if (drli.Length > 0)
                                        model.data.Add(Math.Round(Convert.ToDouble(drli[0]["value"]), 2));
                                    else
                                        model.data.Add(0);
                                }
                            }
                            else
                            {
                                for (int k = 0; k < categories.Split(';').Length; k++)
                                {
                                    if (categories.Split(';')[k] != "")
                                        model.data.Add(0);
                                }
                            }
                            modellist.Add(model);
                        }
                    }
                }
                return new object[] { GetCategories(bydate.Substring(0, 1), fdate, edate), modellist };
                #endregion
            }
        }
        public Dictionary<string, string> GetTime(string dateType)
        {
            string fromTime = "", endTime = "";
            Dictionary<string, string> dicTime = new Dictionary<string, string>();
            switch (dateType)
            {
                case "D":
                    fromTime = dtNow.AddDays(-6).ToString("yyyyMMdd");
                    endTime = dtNow.ToString("yyyyMMdd");
                    break;
                case "W":
                    DateTime dtPre = dtNow.AddDays(-42);
                    GregorianCalendar gc = new GregorianCalendar();
                    int wp = gc.GetWeekOfYear(dtPre, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    fromTime = dtPre.ToString("yyyy") + "W" + wp.ToString().PadLeft(2, '0');
                    int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    endTime = dtNow.ToString("yyyy") + "W" + w.ToString().PadLeft(2, '0');
                    break;
                case "M":
                    fromTime = dtNow.AddMonths(-6).ToString("yyyyMM");
                    endTime = dtNow.ToString("yyyyMM");
                    fromTime = fromTime.Insert(fromTime.Length - 2, "M");
                    endTime = endTime.Insert(endTime.Length - 2, "M");
                    break;
                case "Y":
                    fromTime = dtNow.AddYears(-6).ToString("yyyy");
                    endTime = dtNow.ToString("yyyy");
                    break;
            }
            dicTime.Add("fromTime", fromTime);
            dicTime.Add("endTime", endTime);
            return dicTime;
        }
        public Dictionary<string, string> GetTime(string dateType, string fdate, string edate)
        {
            string fromTime = "", endTime = "";
            Dictionary<string, string> dicTime = new Dictionary<string, string>();
            switch (dateType)
            {
                case "D":
                    fromTime = fdate.Replace("-", "");// dtNow.AddDays(-6).ToString("yyyyMMdd");
                    endTime = edate.Replace("-", "");// dtNow.ToString("yyyyMMdd");
                    break;
                case "W":
                    DateTime dtFrom = Convert.ToDateTime(fdate.Replace("-", "/"));// dtNow.AddDays(-42);
                    DateTime dtEnd = Convert.ToDateTime(edate.Replace("-", "/"));
                    GregorianCalendar gc = new GregorianCalendar();
                    int wp = gc.GetWeekOfYear(dtFrom, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    int w = gc.GetWeekOfYear(dtEnd, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    if (wp > w && dtFrom.Year <= dtEnd.Year)
                        fromTime = dtFrom.AddYears(-1).ToString("yyyy") + "W" + wp.ToString().PadLeft(2, '0');
                    else
                        fromTime = dtFrom.ToString("yyyy") + "W" + wp.ToString().PadLeft(2, '0');
                    endTime = dtEnd.ToString("yyyy") + "W" + w.ToString().PadLeft(2, '0');
                    break;
                case "M":
                    fromTime = fdate.Substring(0, 7).Replace("-", "M");
                    endTime = edate.Substring(0, 7).Replace("-", "M");
                    break;
                case "Y":
                    fromTime = fdate.Replace("-", "").Substring(0, 4);
                    endTime = fdate.Replace("-", "").Substring(0, 4);
                    break;
            }
            dicTime.Add("fromTime", fromTime);
            dicTime.Add("endTime", endTime);
            return dicTime;
        }
        public string GetCategories(string dateType, string fdate, string edate)
        {
            string Categories = "";
            string work_date = string.Empty;
            DateTime dtFrom = Convert.ToDateTime(fdate.Replace("-", "/"));// dtNow.AddDays(-42);
            DateTime dtEnd = Convert.ToDateTime(edate.Replace("-", "/"));
            TimeSpan ts = dtEnd - dtFrom;

            switch (dateType)
            {
                case "D":
                    for (int i = 0; i < ts.Days + 1; i++)
                    {
                        work_date = dtFrom.AddDays(i).ToString("yyyyMMdd");
                        Categories += work_date + ";";
                        if (work_date == dtEnd.ToString("yyyyMMdd"))
                            break;
                    }
                    break;
                case "W":
                    GregorianCalendar gc = new GregorianCalendar();
                    dtFrom = GetMondayOfDay(dtFrom);
                    for (int i = 0; i <= ts.Days; i = i + 7)
                    {
                        if (dtFrom.AddDays(i) <= dtEnd)
                        {
                            int we = gc.GetWeekOfYear(dtFrom.AddDays(i), CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                            //if (gc.GetWeekOfYear(dtFrom.AddDays(i + 7), CalendarWeekRule.FirstDay, DayOfWeek.Monday) == 2)
                            //{ 
                            //    work_date = dtFrom.AddDays(i + 7).ToString("yyyy") + "W01";
                            //    Categories += work_date + ";";
                            //}
                            //else
                            //{
                            work_date = dtFrom.AddDays(i).ToString("yyyy") + "W" + we.ToString().PadLeft(2, '0');
                            Categories += work_date + ";";
                            //  }
                        }
                    }
                    //if (ts.Days == 0)
                    //    Categories += dtFrom.ToString("yyyy") + "W" + gc.GetWeekOfYear(dtFrom, CalendarWeekRule.FirstDay, DayOfWeek.Monday).ToString().PadLeft(2, '0');
                    break;
                case "M":
                    while (dtFrom.ToString("yyyyMM") != dtEnd.ToString("yyyyMM"))
                    {
                        work_date = dtFrom.ToString("yyyyMM");
                        Categories += work_date.Substring(0, 4) + "M" + work_date.Substring(4) + ";";
                        dtFrom = dtFrom.AddMonths(1);
                    }
                    Categories += dtEnd.ToString("yyyyMM").Substring(0, 4) + "M" + dtEnd.ToString("yyyyMM").Substring(4) + ";";
                    break;
                case "X":
                    dtFrom = DateTime.Now.AddMonths(-5);
                    dtEnd = DateTime.Now;
                    while (dtFrom.ToString("yyyyMM") != dtEnd.ToString("yyyyMM"))
                    {
                        work_date = dtFrom.ToString("yyyyMM");
                        Categories += work_date.Substring(0, 4) + "M" + work_date.Substring(4) + ";";
                        dtFrom = dtFrom.AddMonths(1);
                    }
                    Categories += dtEnd.ToString("yyyyMM").Substring(0, 4) + "M" + dtEnd.ToString("yyyyMM").Substring(4) + ";";
                    dtFrom = DateTime.Now.AddDays(-21);
                    gc = new GregorianCalendar();
                    dtFrom = GetMondayOfDay(dtFrom);
                    ts = dtEnd - dtFrom;
                    for (int i = 0; i <= ts.Days; i = i + 7)
                    {
                        if (dtFrom.AddDays(i) <= dtEnd)
                        {
                            int we = gc.GetWeekOfYear(dtFrom.AddDays(i), CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                            work_date = dtFrom.AddDays(i).ToString("yyyy") + "W" + we.ToString().PadLeft(2, '0');
                            Categories += work_date + ";";
                        }
                    }
                    dtFrom = DateTime.Now.AddDays(-3);
                    ts = dtEnd - dtFrom;
                    for (int i = 0; i < ts.Days + 1; i++)
                    {
                        work_date = dtFrom.AddDays(i).ToString("yyyyMMdd");
                        Categories += work_date + ";";
                    }
                    break;
            }
            return Categories.Substring(0, Categories.Length - 1);
        }
        public static int GetWeekOfYear(DateTime dt)
        {
            System.Globalization.GregorianCalendar gc = new System.Globalization.GregorianCalendar();
            int weekOfYear = gc.GetWeekOfYear(dt, System.Globalization.CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
            return weekOfYear;
        }
        public DateTime GetMondayOfDay(DateTime date)
        {
            DateTime date0 = date;
            switch (date.DayOfWeek)
            {
                case System.DayOfWeek.Monday:
                    date0 = date;
                    break;
                case System.DayOfWeek.Tuesday:
                    date0 = date.AddDays(-1);
                    break;
                case System.DayOfWeek.Wednesday:
                    date0 = date.AddDays(-2);
                    break;
                case System.DayOfWeek.Thursday:
                    date0 = date.AddDays(-3);
                    break;
                case System.DayOfWeek.Friday:
                    date0 = date.AddDays(-4);
                    break;
                case System.DayOfWeek.Saturday:
                    date0 = date.AddDays(-5);
                    break;
                case System.DayOfWeek.Sunday:
                    date0 = date.AddDays(-6);
                    break;
            }
            return date0;
        }

        public DataRow GetDataRow(DataTable dt, string filter)
        {
            foreach (DataRow dr in dt.Rows)
            {
                if (filter.Contains(dr["date"].ToString().Replace("/", "")))
                {
                    return dr;
                }
            }
            return null;
        }
    }
}
