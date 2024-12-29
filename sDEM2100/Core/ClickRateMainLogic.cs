using Compal.MESComponent;
using sDEM2100.DataGateway;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using sDEM2100.Beans;

namespace sDEM2100.Core
{
    class ClickRateMainLogic
    {
        private object[] mClientInfo;
        private MESLog mesLog;
        private string mDbName;
        private DateTime dtNow;
        private ClickRateOperator ClickRateSqlOperater;

        public ClickRateMainLogic(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.mesLog = new MESLog("Dqms");
            ClickRateSqlOperater = new ClickRateOperator(mClientInfo, dbName);
        }

        public Dictionary<string, string> GetTime(string dateType)
        {
            string fromTime = "", endTime = "";
            dtNow = DateTime.Now;
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

        public ExecutionResult ExecuteClickRateIndex(string plant, string dateType, string date, string dateSpan)
        {
            var exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            ClickRate model = new Beans.ClickRate();
            ClickRateChart1 chart1 = new ClickRateChart1();
            DataTable dtPie = new DataTable();
            DataTable dtChart1 = new DataTable();
            DataTable dtChart2 = new DataTable();
            try
            {
                var funcList = GetFuncList();
                var dateRange = GetTime(dateType, date, Convert.ToInt32(dateSpan));
                string funcid = string.Empty, categories = "";
                foreach (string str in funcList)
                {
                    if (str.Split(',')[0] == "撞件北斗图")
                        funcid += ",N'" + str.Split(',')[0] + "'";
                    else
                        funcid += ",'" + str.Split(',')[0] + "'";
                    categories += str.Split(',')[1] + ",";
                }
                funcid = funcid.Substring(1);
                categories = categories.Substring(0, categories.LastIndexOf(','));
                switch (dateType)
                {
                    case "W":
                        foreach (string strfunc in funcList)
                        {
                            #region pieChart
                            ClickRatePie pie = new Beans.ClickRatePie();
                            pie.subtitle = strfunc.Split(',')[1];
                            dtPie = ClickRateSqlOperater.GetClickRateByDeptWeekly(strfunc.Split(',')[0], plant, dateRange["fromTime"], dateRange["endTime"]);
                            if (dtPie.Rows.Count > 0)
                            {
                                foreach (DataRow dr in dtPie.Rows)
                                {
                                    ClickRatePieData data0 = new ClickRatePieData();
                                    data0.name = dr["value_str2"].ToString();
                                    double? y = null;
                                    if (!string.IsNullOrEmpty(dr["qty"].ToString()))
                                        y = Convert.ToDouble(dr["qty"].ToString());
                                    data0.y = y;
                                    data0.qty = y;
                                    pie.data.Add(data0);
                                }
                            }
                            model.PieChartList.Add(pie);
                            #endregion
                        }
                        dtChart1 = ClickRateSqlOperater.GetClickRateByDateWeekly(funcid, plant, dateRange["fromTime"], dateRange["endTime"]);
                        dtChart2 = ClickRateSqlOperater.GetClickRateByPlantWeekly(funcid, dateRange["fromTime"], dateRange["endTime"]);
                        break;
                    case "D":
                        foreach (string strfunc in funcList)
                        {
                            #region pieChart
                            ClickRatePie pie = new Beans.ClickRatePie();
                            pie.subtitle = strfunc.Split(',')[1];
                            dtPie = ClickRateSqlOperater.GetClickRateByDeptDaily(strfunc.Split(',')[0], plant, dateRange["fromTime"], dateRange["endTime"]);
                            if (dtPie.Rows.Count > 0)
                            {
                                foreach (DataRow dr in dtPie.Rows)
                                {
                                    ClickRatePieData data0 = new ClickRatePieData();
                                    data0.name = dr["value_str2"].ToString();
                                    data0.y = string.IsNullOrEmpty(dr["qty"].ToString()) ? 0 : Convert.ToDouble(dr["qty"].ToString());
                                    data0.qty = string.IsNullOrEmpty(dr["qty"].ToString()) ? 0 : Convert.ToDouble(dr["qty"].ToString());
                                    pie.data.Add(data0);
                                }
                            }
                            model.PieChartList.Add(pie);
                            #endregion
                        }
                        dtChart1 = ClickRateSqlOperater.GetClickRateByDateDaily(funcid, plant, dateRange["fromTime"], dateRange["endTime"]);
                        dtChart2 = ClickRateSqlOperater.GetClickRateByPlantDaily(funcid, dateRange["fromTime"], dateRange["endTime"]);
                        break;
                    default:
                        break;
                }

                #region ColumnChart1
                model.chart1.categories = categories;
                if (dtChart1.Rows.Count > 0)
                {
                    DataTable dtDate = dtChart1.DefaultView.ToTable(true, "work_date");
                    foreach (DataRow drDate in dtDate.Rows)
                    {
                        string work_date = drDate[0].ToString();
                        ClickRateSeries series = new ClickRateSeries();
                        series.name = dateType == "D" ? work_date.Substring(4).Insert(2, "/") : work_date;
                        foreach (string strfunc in funcList)
                        {
                            double? y = null;
                            var drli = dtChart1.Select(" work_date='" + work_date + "' and value_str1='" + strfunc.Split(',')[0] + "'");
                            if (drli.Length > 0)
                                y = Convert.ToDouble(drli[0]["qty"].ToString());
                            series.data.Add(y);
                        }
                        model.chart1.series.Add(series);
                    }
                }
                #endregion
                #region ColumnChart2
                if (dtChart2.Rows.Count > 0)
                {
                    DataTable dtDate = dtChart2.DefaultView.ToTable(true, "work_date");
                    List<string> plant_color = new List<string>();
                    foreach (DataRow drDate in dtDate.Rows)
                    {
                        ClickRateChart2 chart2 = new Beans.ClickRateChart2();
                        chart2.categories = categories;
                        string work_date = drDate[0].ToString();
                        chart2.subtitle = dateType == "D" ? work_date.Substring(4).Insert(2, "/") : work_date;
                        DataTable dtPlant = dtChart2.DefaultView.ToTable(true, "plant_code");
                        foreach (DataRow drPlant in dtPlant.Rows)
                        {
                            ClickRateSeries series = new ClickRateSeries();
                            series.color = getColor(drPlant[0].ToString());
                            series.name = drPlant[0].ToString();
                            if (!plant_color.Contains(drPlant[0].ToString() + "_" + getColor(drPlant[0].ToString())))
                                plant_color.Add(drPlant[0].ToString() + "_" + getColor(drPlant[0].ToString()));
                            foreach (string strfunc in funcList)
                            {
                                double? y = null;
                                var drli = dtChart2.Select(" work_date='" + work_date + "' and value_str1='" + strfunc.Split(',')[0] + "' and plant_code='" + drPlant[0].ToString() + "'");
                                if (drli.Length > 0)
                                    y = Convert.ToDouble(drli[0]["qty"].ToString());
                                series.data.Add(y);
                            }
                            chart2.series.Add(series);
                        }
                        model.chart2.Add(chart2);
                    }
                    model.plant_color = plant_color;
                }
                #endregion
                exeRes.Anything = model;
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = "Meet exception:" + ex.Message;
            }

            return exeRes;
        }

        public List<string> GetFuncList()
        {
            List<string> funclist = new List<string>();
            funclist.Add("KBN3142,KBN3142 CTQ葡萄樹系統");
            funclist.Add("KBN4406,KBN4406 FPY管理系統");
            funclist.Add("RPT2601SMT,RPT2601SMT YR管理系統");
            funclist.Add("RPT2169,RPT2169 SMT AIO FA Tool");
            funclist.Add("撞件北斗图,QMS撞件北斗圖");
            return funclist;
        }

        public Dictionary<string, string> GetTime(string dateType, string date, int ts)
        {
            string fromTime = "", endTime = "";
            Dictionary<string, string> dicTime = new Dictionary<string, string>();
            switch (dateType)
            {
                case "D":
                    DateTime dated = Convert.ToDateTime(date.Replace("-", "/"));
                    endTime = dated.ToString("yyyyMMdd");
                    fromTime = dated.AddDays(-ts).ToString("yyyyMMdd");
                    break;
                case "W":
                    GregorianCalendar gc = new GregorianCalendar();
                    DateTime datew = gc.AddWeeks(Convert.ToDateTime(date.Substring(0, 4) + "/01/01"), Convert.ToInt32(date.Substring(5)) - 1);
                    DateTime dtfrom = datew.AddDays(-7 * ts);
                    int wp = gc.GetWeekOfYear(dtfrom, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    fromTime = dtfrom.ToString("yyyy") + "W" + wp.ToString().PadLeft(2, '0');
                    endTime = date;
                    //int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstDay, DayOfWeek.Monday);
                    //endTime = dtNow.ToString("yyyy") + "W" + w.ToString().PadLeft(2, '0');
                    break;
            }
            dicTime.Add("fromTime", fromTime);
            dicTime.Add("endTime", endTime);
            return dicTime;
        }

        public string getColor(string plant)
        {
            string color = string.Empty;
            switch (plant)
            {
                case "KSP1-AEP":
                    color = "#6D4AEE";
                    break;
                case "KSP1-SVR":
                    color = "#8863ED";
                    break;
                case "KSP2":
                    color = "#4F81BD";
                    break;
                case "KSP3":
                    color = "#00B0F0";
                    break;
                case "KSP4":
                    color = "#9BBB59";
                    break;
                case "CDP1":
                    color = "#DB843D";
                    break;
                case "CQP1":
                    color = "#71588F";
                    break;
                case "TW01":
                    color = "#22F80A";
                    break;
            }
            return color;
        }

    }
}
