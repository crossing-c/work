using Compal.MESComponent;
using sDEM2100.Beans;
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
    class MpMainLogic
    {
        private object[] mClientInfo;
        private MESLog mesLog;
        private string mDbName;
        private DateTime dtNow;
        private MpSqlOperater mpSqlOperater;

        public MpMainLogic(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.mesLog = new MESLog("Dqms");
            dtNow = DateTime.Now;
            mpSqlOperater = new MpSqlOperater(mClientInfo, dbName);
        }

        public ExecutionResult ExecuteMpIndex(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            DataTable dtResult = mpSqlOperater.GetMpIndexData(plant);
            exeRes.Anything = dtResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMpOffline(string plant, string type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            plant = plant.Replace("TW01", "CTY1");
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string fromTime, endTime;

            GregorianCalendar gc = new GregorianCalendar();
            DateTime dtNow0 = dtNow.AddDays(-7 * 5);
            fromTime = dtNow0.AddDays(1 - (int)(gc.GetDayOfWeek(dtNow0))).ToString("yyyyMMdd");
            endTime = dtNow.AddDays(8 - (int)(gc.GetDayOfWeek(dtNow))).ToString("yyyyMMdd");
            DataTable dtOfflinePlantAnalysis = mpSqlOperater.GetReflowPlantAnalysis(type, fromTime, endTime);
            dtOfflinePlantAnalysis.Columns["work_date"].ColumnName = "date";

            fromTime = dtNow.AddDays(-6).ToString("yyyyMMdd");
            endTime = dtNow.AddDays(1).ToString("yyyyMMdd");
            DataTable dtOfflineDaily = mpSqlOperater.GetReflowDailyTrend(type, plant, fromTime, endTime);
            dtOfflineDaily.Columns["work_date"].ColumnName = "date";

            dtNow0 = dtNow.AddDays(-7 * 5);
            fromTime = dtNow0.AddDays(1 - (int)(gc.GetDayOfWeek(dtNow0))).ToString("yyyyMMdd");
            endTime = dtNow.AddDays(8 - (int)(gc.GetDayOfWeek(dtNow))).ToString("yyyyMMdd");
            DataTable dtOfflineWeekly = mpSqlOperater.GetReflowWeeklyTrend(type, plant, fromTime, endTime);
            dtOfflineWeekly.Columns["work_date"].ColumnName = "date";

            dtNow0 = dtNow.AddMonths(-5);
            fromTime = dtNow0.ToString("yyyyMM") + "01";
            endTime = dtNow.AddMonths(1).ToString("yyyyMM") + "01";
            DataTable dtOfflineMonthly = mpSqlOperater.GetReflowMonthlyTrend(type, plant, fromTime, endTime);
            dtOfflineMonthly.Columns["work_date"].ColumnName = "date";

            dicResult.Add(type + "PlantAnalysis", dtOfflinePlantAnalysis);
            dicResult.Add(type + "Daily", dtOfflineDaily);
            //dicResult.Add(type + "Analysis", dtOfflineAnalysis);
            dicResult.Add(type + "Weekly", dtOfflineWeekly);
            dicResult.Add(type + "Monthly", dtOfflineMonthly);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMpWfr(string plant, string type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            Dictionary<string, string> dicTime;

            dicTime = GetTime("D");
            DataTable dtWfrDaily = mpSqlOperater.GetWfrDailyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            if (plant == "CDP1" && type == "FATP")
            {
                DataTable dt = mpSqlOperater.GetWfrDailyTrend(type, "CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                if (dt.Rows.Count > 0)
                    dtWfrDaily.Merge(dt);
            }
            else if (plant == "TW01" && type == "FATP")
            {
                DataTable dt = mpSqlOperater.GetWfrDailyTrend(type, "TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                if (dt.Rows.Count > 0)
                    dtWfrDaily.Merge(dt);
            }
            DataView dv = dtWfrDaily.DefaultView;
            dv.Sort = "data_day ";
            dtWfrDaily = dv.ToTable();
            dicTime = GetTime("W");
            DataTable dtWfrWeekly = mpSqlOperater.GetWfrWeeklyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            if (plant == "CDP1" && type == "FATP")
            {
                DataTable dt = mpSqlOperater.GetWfrWeeklyTrend(type, "CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                if (dt.Rows.Count > 0)
                    dtWfrWeekly.Merge(dt);
            }
            else if (plant == "TW01" && type == "FATP")
            {
                DataTable dt = mpSqlOperater.GetWfrWeeklyTrend(type, "TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                if (dt.Rows.Count > 0)
                    dtWfrWeekly.Merge(dt);
            }
            dv = dtWfrWeekly.DefaultView;
            dv.Sort = "date";
            dtWfrWeekly = dv.ToTable();
            dicTime = GetTime("M");
            DataTable dtWfrMonthly = mpSqlOperater.GetWfrMonthlyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            if (plant == "CDP1" && type == "FATP")
            {
                DataTable dt = mpSqlOperater.GetWfrMonthlyTrend(type, "CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                if (dt.Rows.Count > 0)
                    dtWfrMonthly.Merge(dt);
            }
            else if (plant == "TW01" && type == "FATP")
            {
                DataTable dt = mpSqlOperater.GetWfrMonthlyTrend(type, "TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                if (dt.Rows.Count > 0)
                    dtWfrMonthly.Merge(dt);
            }
            dv = dtWfrMonthly.DefaultView;
            dv.Sort = "date";
            dtWfrMonthly = dv.ToTable();
            dicResult.Add(type + "Daily", dtWfrDaily);
            dicResult.Add(type + "Weekly", dtWfrWeekly);
            dicResult.Add(type + "Monthly", dtWfrMonthly);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMpCtq(string plant, string type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            Dictionary<string, string> dicTime;
            dicTime = GetTime("W");
            DataTable dtCtqPlantAnalysis = mpSqlOperater.GetCtqPlantAnalysis(type, dicTime["fromTime"], dicTime["endTime"]);
            dicTime = GetTime("D");
            DataTable dtCtqDaily = mpSqlOperater.GetCtqDailyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            dicTime = GetTime("M");
            DataTable dtCtqAnalysis = mpSqlOperater.GetCtqAnalysis(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            dicTime = GetTime("W");
            DataTable dtCtqWeekly = mpSqlOperater.GetCtqWeeklyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            dicTime = GetTime("M");
            DataTable dtCtqMonthly = mpSqlOperater.GetCtqMonthlyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            dicResult.Add(type + "PlantAnalysis", dtCtqPlantAnalysis);
            dicResult.Add(type + "Daily", dtCtqDaily);
            dicResult.Add(type + "Analysis", dtCtqAnalysis);
            dicResult.Add(type + "Weekly", dtCtqWeekly);
            dicResult.Add(type + "Monthly", dtCtqMonthly);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMpPid(string plant, string type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            Dictionary<string, string> dicTime;
            if (type == "LCD")
            {
                dicTime = GetTime("D");
                DataTable dtPidLcdDaily = mpSqlOperater.GetPidLcdDailyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                dicTime = GetTime("W");
                DataTable dtPidLcdWeekly = mpSqlOperater.GetPidLcdWeeklyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                dicTime = GetTime("M");
                DataTable dtPidLcdMonthly = mpSqlOperater.GetPidLcdMonthlyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                dicResult.Add("LCDDaily", dtPidLcdDaily);
                dicResult.Add("LCDWeekly", dtPidLcdWeekly);
                dicResult.Add("LCDMonthly", dtPidLcdMonthly);
            }
            else if (type == "ME")
            {
                dicTime = GetTime("D");
                DataTable dtPidMeDaily = mpSqlOperater.GetPidMeDailyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                dicTime = GetTime("W");
                DataTable dtPidMeWeekly = mpSqlOperater.GetPidMeWeeklyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                dicTime = GetTime("M");
                DataTable dtPidMeMonthly = mpSqlOperater.GetPidMeMonthlyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                dicResult.Add("MEDaily", dtPidMeDaily);
                dicResult.Add("MEWeekly", dtPidMeWeekly);
                dicResult.Add("MEMonthly", dtPidMeMonthly);
            }
            else if (type == "KPS")
            {
                dicTime = GetTime("D");
                DataTable dtPidKpsDaily = mpSqlOperater.GetPidKpsDailyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                dicTime = GetTime("W");
                DataTable dtPidKpsWeekly = mpSqlOperater.GetPidKpsWeeklyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                dicTime = GetTime("M");
                DataTable dtPidKpsMonthly = mpSqlOperater.GetPidKpsMonthlyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                dicResult.Add("KPSDaily", dtPidKpsDaily);
                dicResult.Add("KPSWeekly", dtPidKpsWeekly);
                dicResult.Add("KPSMonthly", dtPidKpsMonthly);
            }
            else if (type == "RECYCLING_RATE")
            {
                plant = plant.Replace("TW01", "CTY1");
                dicTime = GetTime("D");
                DataTable dtPidReRateDaily = mpSqlOperater.GetPidReRateDailyTrend(plant, dicTime["fromTime"], dicTime["endTime"], "");
                dicTime = GetTime("W");
                DataTable dtPidReRateWeekly = mpSqlOperater.GetPidReRateWeeklyTrend(plant, dicTime["fromTime"], dicTime["endTime"], "");
                dicTime = GetTime("M");
                DataTable dtPidReRateMonthly = mpSqlOperater.GetPidReRateMonthlyTrend(plant, dicTime["fromTime"], dicTime["endTime"], "");
                dicResult.Add(type + "Daily", dtPidReRateDaily);
                dicResult.Add(type + "Weekly", dtPidReRateWeekly);
                dicResult.Add(type + "Monthly", dtPidReRateMonthly);
                DataTable dtTotal = new DataTable();
                dicTime = GetTimeRecylineRate("M");
                DataTable dtTotalReRateMonthly = mpSqlOperater.GetPidReRateMonthlyTrend(plant, dicTime["fromTime"], dicTime["endTime"], "");
                if (dtTotalReRateMonthly != null)
                    dtTotal = dtTotalReRateMonthly;
                dicTime = GetTimeRecylineRate("W");
                DataTable dtTotalReRateWeekly = mpSqlOperater.GetPidReRateWeeklyTrend(plant, dicTime["fromTime"], dicTime["endTime"], "");
                if (dtTotalReRateWeekly != null)
                    dtTotal.Merge(dtTotalReRateWeekly);
                dicTime = GetTimeRecylineRate("D");
                DataTable dtTotalReRateDaily = mpSqlOperater.GetPidReRateDailyTrend(plant, dicTime["fromTime"], dicTime["endTime"], "");
                if (dtTotalReRateDaily != null)
                    dtTotal.Merge(dtTotalReRateDaily);
                dicResult.Add(type + "Total", dtTotal);
                DataTable dtType = mpSqlOperater.GetReRateType(plant);
                if (dtType.Rows.Count > 0)
                {
                    dicResult.Add(type + "Type", dtType);
                    for (int i = 0; i < dtType.Rows.Count; i++)
                    {
                        string strType = dtType.Rows[i]["ptype"].ToString();
                        DataTable dtTotalType = new DataTable();
                        dicTime = GetTimeRecylineRate("M");
                        DataTable dtTotalReRateMonthlyType = mpSqlOperater.GetPidReRateMonthlyTrend(plant, dicTime["fromTime"], dicTime["endTime"], strType);
                        if (dtTotalReRateMonthlyType != null)
                            dtTotalType = dtTotalReRateMonthlyType;
                        dicTime = GetTimeRecylineRate("W");
                        DataTable dtTotalReRateWeeklyType = mpSqlOperater.GetPidReRateWeeklyTrend(plant, dicTime["fromTime"], dicTime["endTime"], strType);
                        if (dtTotalReRateWeeklyType != null)
                            dtTotalType.Merge(dtTotalReRateWeeklyType);
                        dicTime = GetTimeRecylineRate("D");
                        DataTable dtTotalReRateDailyType = mpSqlOperater.GetPidReRateDailyTrend(plant, dicTime["fromTime"], dicTime["endTime"], strType);
                        if (dtTotalReRateDailyType != null)
                            dtTotalType.Merge(dtTotalReRateDailyType);
                        dicResult.Add(type + strType, dtTotalType);
                    }
                }
                else
                {
                    dtType = new DataTable();
                    dtType.Columns.Add("ptype");
                    dicResult.Add(type + "Type", dtType);
                }
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMpScrap()
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            Dictionary<string, string> dicTime;
            dicTime = GetTime("M");
            DataTable dtScrap = mpSqlOperater.GetScrap(dicTime["fromTime"], dicTime["endTime"]);
            dicResult.Add("Scrap", dtScrap);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMpFpy(string plant, string type, string fpytype)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            if (fpytype == "MES FPY")
            {
                #region
                Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
                Dictionary<string, string> dicTime;
                dicTime = GetTime("W");
                DataTable dtFpyPlantAnalysis = mpSqlOperater.GetFpyPlantAnalysis(type, dicTime["fromTime"], dicTime["endTime"]);
                //dicTime = GetTime("M");
                //DataTable dtFpyAnalysis = mpSqlOperater.GetFpyAnalysis(type,plant, dicTime["fromTime"], dicTime["endTime"]);
                //dicTime = GetTime("M");
                //DataTable dtFpyDutyCategory = mpSqlOperater.GetFpyDutyCategory(type,plant, dicTime["fromTime"], dicTime["endTime"]);
                dicTime = GetTime("D");
                DataTable dtFpyDaily = mpSqlOperater.GetFpyDailyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                if (plant == "TW01")
                {
                    DataTable dt = mpSqlOperater.GetFpyDailyTrend(type, "TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyDaily.Merge(dt);
                }
                else if (plant == "CDP1")
                {
                    DataTable dt = mpSqlOperater.GetFpyDailyTrend(type, "CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyDaily.Merge(dt);
                }
                DataView dv = dtFpyDaily.DefaultView;
                dv.Sort = "data_day";
                dtFpyDaily = dv.ToTable();
                dicTime = GetTime("W");
                DataTable dtFpyWeekly = mpSqlOperater.GetFpyWeeklyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                if (plant == "TW01")
                {
                    DataTable dt = mpSqlOperater.GetFpyWeeklyTrend(type, "TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyWeekly.Merge(dt);
                }
                else if (plant == "CDP1")
                {
                    DataTable dt = mpSqlOperater.GetFpyWeeklyTrend(type, "CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyWeekly.Merge(dt);
                }
                dv = dtFpyWeekly.DefaultView;
                dv.Sort = "data_week";
                dtFpyWeekly = dv.ToTable();
                dicTime = GetTime("M");
                DataTable dtFpyMonthly = mpSqlOperater.GetFpyMonthlyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                if (plant == "TW01")
                {
                    DataTable dt = mpSqlOperater.GetFpyMonthlyTrend(type, "TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyMonthly.Merge(dt);
                }
                else if (plant == "CDP1")
                {
                    DataTable dt = mpSqlOperater.GetFpyMonthlyTrend(type, "CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyMonthly.Merge(dt);
                }
                dv = dtFpyMonthly.DefaultView;
                dv.Sort = "data_month";
                dtFpyMonthly = dv.ToTable();
                //   string time = WorkDateConvert.GetWorkDate("WEEKLY", 0);
                //DataTable dtFpyLine = mpSqlOperater.GetFpyByLine(type, plant, time);
                //DataTable dtFpyModel = mpSqlOperater.GetFpyByModel(type, plant, time);

                dicResult.Add(type + "PlantAnalysis", dtFpyPlantAnalysis);
                //dicResult.Add(type+"Analysis", dtFpyAnalysis);
                //dicResult.Add(type+"DutyCategory", dtFpyDutyCategory);
                dicResult.Add(type + "Daily", dtFpyDaily);
                dicResult.Add(type + "Weekly", dtFpyWeekly);
                dicResult.Add(type + "Monthly", dtFpyMonthly);
                //dicResult.Add(type + "Line", dtFpyLine);
                //dicResult.Add(type + "Model", dtFpyModel);
                exeRes.Anything = dicResult;
                #endregion
            }
            if (fpytype == "Insight FPY")
            {
                #region
                Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
                Dictionary<string, string> dicTime;
                dicTime = GetTime("W");
                DataTable dtFpyPlantAnalysis = mpSqlOperater.GetInsightPlantAnalysis(type, dicTime["fromTime"], dicTime["endTime"]);
                dicTime = GetTime("D");
                DataTable dtFpyDaily = mpSqlOperater.GetInsightDailyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                if (plant == "TW01")
                {
                    DataTable dt = mpSqlOperater.GetInsightDailyTrend(type, "TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyDaily.Merge(dt);
                }
                else if (plant == "CDP1")
                {
                    DataTable dt = mpSqlOperater.GetInsightDailyTrend(type, "CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyDaily.Merge(dt);
                }
                DataView dv = dtFpyDaily.DefaultView;
                dv.Sort = "data_day";
                dtFpyDaily = dv.ToTable();
                dicTime = GetTime("W");
                DataTable dtFpyWeekly = mpSqlOperater.GetInsightWeeklyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                if (plant == "TW01")
                {
                    DataTable dt = mpSqlOperater.GetInsightWeeklyTrend(type, "TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyWeekly.Merge(dt);
                }
                else if (plant == "CDP1")
                {
                    DataTable dt = mpSqlOperater.GetInsightWeeklyTrend(type, "CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyWeekly.Merge(dt);
                }
                dv = dtFpyWeekly.DefaultView;
                dv.Sort = "data_week";
                dtFpyWeekly = dv.ToTable();
                dicTime = GetTime("M");
                DataTable dtFpyMonthly = mpSqlOperater.GetInsightMonthlyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                if (plant == "TW01")
                {
                    DataTable dt = mpSqlOperater.GetInsightMonthlyTrend(type, "TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyMonthly.Merge(dt);
                }
                else if (plant == "CDP1")
                {
                    DataTable dt = mpSqlOperater.GetInsightMonthlyTrend(type, "CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                    dtFpyMonthly.Merge(dt);
                }
                dv = dtFpyMonthly.DefaultView;
                dv.Sort = "data_month";
                dtFpyMonthly = dv.ToTable();
                //   string time = WorkDateConvert.GetWorkDate("WEEKLY", 0);
                //DataTable dtFpyLine = mpSqlOperater.GetFpyByLine(type, plant, time);
                //DataTable dtFpyModel = mpSqlOperater.GetFpyByModel(type, plant, time);

                dicResult.Add(type + "PlantAnalysis", dtFpyPlantAnalysis);
                dicResult.Add(type + "Daily", dtFpyDaily);
                dicResult.Add(type + "Weekly", dtFpyWeekly);
                dicResult.Add(type + "Monthly", dtFpyMonthly);
                //dicResult.Add(type + "Line", dtFpyLine);
                //dicResult.Add(type + "Model", dtFpyModel);
                exeRes.Anything = dicResult;
                #endregion
            }
            if (fpytype == "FPY GAP" || fpytype == "FPY GAP (VNNB1)" || fpytype == "FPY GAP (VNDOCK)" || fpytype == "FPY GAP (CDP1)" || fpytype == "FPY GAP (CDDOCK)"
                        || fpytype == "FPY GAP (TW01DOCK)" || fpytype == "FPY GAP (TW01)")
            {
                #region
                Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
                Dictionary<string, string> dicTime;
                string strProcess = "SMT";
                if (type == "AP")
                    strProcess = "FATP";
                dicTime = GetTime("D");
                if (fpytype.IndexOf("NB1") > -1)
                    plant += @"_NB1";
                else if (fpytype.IndexOf("VNDOCK") > -1)
                    plant += @"_DOCK";
                else if (fpytype.IndexOf("CDP1") > -1)
                    plant = "CDP1";
                else if (fpytype.IndexOf("CDDOCK") > -1)
                    plant = "CDDOCK";
                else if (fpytype.IndexOf("TW01DOCK") > -1)
                    plant = "TW01DOCK";
                else if (fpytype.IndexOf("TW01") > -1)
                    plant = "TW01";
                DataTable dtFpyDaily = mpSqlOperater.GetGapDailyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                DataTable dtDaily = dtFpyDaily.DefaultView.ToTable(true, "date");
                for (int i = 0; i < dtDaily.Rows.Count; i++)
                {
                    string strDate = dtDaily.Rows[i]["date"].ToString();
                    double dFPY = 0, dInsight = 0, dGap = 0;
                    DataRow[] dr = dtFpyDaily.Select("plant_code='FPY' and date='" + strDate + "'");
                    if (dr.Length > 0)
                    {
                        dFPY = double.Parse(dr[0]["value"].ToString());
                        dr[0]["plant_code"] = dr[0]["plant_code"].ToString() + " " + strProcess;
                    }

                    dr = dtFpyDaily.Select("plant_code='INSIGHT' and date='" + strDate + "'");
                    if (dr.Length > 0)
                    {
                        dInsight = double.Parse(dr[0]["value"].ToString());
                        dr[0]["plant_code"] = dr[0]["plant_code"].ToString() + " YR " + strProcess;
                    }
                    dGap = Math.Round(dFPY - dInsight, 2);
                    if (dGap < 0)
                        dGap = 0;
                    dtFpyDaily.Rows.Add("GAP", strDate, dGap);
                }
                dicTime = GetTime("W");
                DataTable dtFpyWeekly = mpSqlOperater.GetGapWeeklyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                dtDaily = dtFpyWeekly.DefaultView.ToTable(true, "date");
                for (int i = 0; i < dtDaily.Rows.Count; i++)
                {
                    string strDate = dtDaily.Rows[i]["date"].ToString();
                    double dFPY = 0, dInsight = 0, dGap = 0;
                    DataRow[] dr = dtFpyWeekly.Select("plant_code='FPY' and date='" + strDate + "'");
                    if (dr.Length > 0)
                    {
                        dFPY = double.Parse(dr[0]["value"].ToString());
                        dr[0]["plant_code"] = dr[0]["plant_code"].ToString() + " " + strProcess;
                    }

                    dr = dtFpyWeekly.Select("plant_code='INSIGHT' and date='" + strDate + "'");
                    if (dr.Length > 0)
                    {
                        dInsight = double.Parse(dr[0]["value"].ToString());
                        dr[0]["plant_code"] = dr[0]["plant_code"].ToString() + " YR " + strProcess;
                    }
                    dGap = Math.Round(dFPY - dInsight, 2);
                    if (dGap < 0)
                        dGap = 0;
                    dtFpyWeekly.Rows.Add("GAP", strDate, dGap);
                }
                dicTime = GetTime("M");
                DataTable dtFpyMonthly = mpSqlOperater.GetGapMonthlyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                dtDaily = dtFpyMonthly.DefaultView.ToTable(true, "date");
                for (int i = 0; i < dtDaily.Rows.Count; i++)
                {
                    string strDate = dtDaily.Rows[i]["date"].ToString();
                    double dFPY = 0, dInsight = 0, dGap = 0;
                    DataRow[] dr = dtFpyMonthly.Select("plant_code='FPY' and date='" + strDate + "'");
                    if (dr.Length > 0)
                    {
                        dFPY = double.Parse(dr[0]["value"].ToString());
                        dr[0]["plant_code"] = dr[0]["plant_code"].ToString() + " " + strProcess;
                    }

                    dr = dtFpyMonthly.Select("plant_code='INSIGHT' and date='" + strDate + "'");
                    if (dr.Length > 0)
                    {
                        dInsight = double.Parse(dr[0]["value"].ToString());
                        dr[0]["plant_code"] = dr[0]["plant_code"].ToString() + " YR " + strProcess;
                    }
                    dGap = Math.Round(dFPY - dInsight, 2);
                    if (dGap < 0)
                        dGap = 0;
                    dtFpyMonthly.Rows.Add("GAP", strDate, dGap);
                }
                dicResult.Add(type + "Daily", dtFpyDaily);
                dicResult.Add(type + "Weekly", dtFpyWeekly);
                dicResult.Add(type + "Monthly", dtFpyMonthly);
                exeRes.Anything = dicResult;
                #endregion
            }
            return exeRes;
        }

        public ExecutionResult ExecuteMpLrr(object[] objs)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            string plant = objs[0].ToString();
            string tabType = objs[1].ToString();
            string chartType = objs[2].ToString();
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string fromTime = "";
            string endTime = "";
            string str3Type = "";
            string strExType = "";
            switch (chartType)
            {
                case "ALL":
                    #region
                    if (tabType == "PCBALRR" || (tabType.Length > 7 && tabType.Substring(0, 7) == "PCBALRR"))
                    {
                        plant = plant.Replace("TW01", "CTY1");
                        str3Type = objs[3].ToString();
                        if (plant == "VNP1" && tabType.Contains("_"))
                            strExType = tabType.Substring(7);
                        GregorianCalendar gc = new GregorianCalendar();
                        DateTime dtNow0 = dtNow.AddDays(-7 * 3);
                        fromTime = dtNow0.AddDays(1 - (int)(gc.GetDayOfWeek(dtNow0))).ToString("yyyyMMdd");
                        endTime = dtNow.AddDays(8 - (int)(gc.GetDayOfWeek(dtNow))).ToString("yyyyMMdd");
                        DataTable dtPcbaLrrPlantAnalysis = mpSqlOperater.GetPcbaLrrPlantAnalysisWeek(fromTime, endTime, str3Type, strExType);
                        dtPcbaLrrPlantAnalysis.Columns["work_date"].ColumnName = "date";

                        dtNow0 = dtNow.AddMonths(-2);
                        fromTime = dtNow0.ToString("yyyyMM") + "01";
                        endTime = dtNow.AddMonths(1).ToString("yyyyMM") + "01";
                        DataTable dtPcbaLrrPlantAnalysis2 = mpSqlOperater.GetPcbaLrrPlantAnalysisMonth(fromTime, endTime, str3Type, strExType);
                        dtPcbaLrrPlantAnalysis2.Columns["work_date"].ColumnName = "date";

                        dtPcbaLrrPlantAnalysis2.Merge(dtPcbaLrrPlantAnalysis);

                        dtNow0 = dtNow.AddMonths(-5);
                        fromTime = dtNow0.ToString("yyyyMM") + "01";
                        endTime = dtNow.AddMonths(1).ToString("yyyyMM") + "01";
                        DataTable dtPcbaLrrMonthly = mpSqlOperater.GetPcbaLrrMonthlyTrend(plant, fromTime, endTime, strExType);
                        dtPcbaLrrMonthly.Columns["work_date"].ColumnName = "date";

                        dtNow0 = dtNow.AddDays(-7 * 5);
                        fromTime = dtNow0.AddDays(1 - (((int)(gc.GetDayOfWeek(dtNow0))) == 0 ? 7 : ((int)(gc.GetDayOfWeek(dtNow0))))).ToString("yyyyMMdd");
                        endTime = dtNow.AddDays(8 - (((int)(gc.GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)(gc.GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                        DataTable dtPcbaLrrWeekly = mpSqlOperater.GetPcbaLrrWeeklyTrend(plant, fromTime, endTime, strExType);
                        dtPcbaLrrWeekly.Columns["work_date"].ColumnName = "date";

                        fromTime = dtNow.AddDays(-6).ToString("yyyyMMdd");
                        endTime = dtNow.AddDays(1).ToString("yyyyMMdd");
                        DataTable dtPcbaLrrDaily = mpSqlOperater.GetPcbaLrrDailyTrend(plant, fromTime, endTime, strExType);
                        dtPcbaLrrDaily.Columns["work_date"].ColumnName = "date";

                        dtNow0 = dtNow.AddDays(-1);
                        fromTime = dtNow0.AddDays(1 - (((int)(gc.GetDayOfWeek(dtNow0))) == 0 ? 7 : ((int)(gc.GetDayOfWeek(dtNow0))))).ToString("yyyyMMdd");
                        endTime = dtNow0.AddDays(8 - (((int)(gc.GetDayOfWeek(dtNow0))) == 0 ? 7 : ((int)(gc.GetDayOfWeek(dtNow0))))).ToString("yyyyMMdd");
                        DataTable dtPcbaLrrDutyTypeCategory = mpSqlOperater.GetPcbaLrrDutyTypeCategory(plant, fromTime, endTime, strExType);
                        DataTable dtPcbaLrrDutyTypeCategory2 = mpSqlOperater.GetPcbaLrrDutyTypeCategory2(plant, fromTime, endTime, strExType);

                        dicResult.Add(tabType + "PlantAnalysis", dtPcbaLrrPlantAnalysis2);
                        dicResult.Add(tabType + "Monthly", dtPcbaLrrMonthly);
                        dicResult.Add(tabType + "Weekly", dtPcbaLrrWeekly);
                        dicResult.Add(tabType + "Daily", dtPcbaLrrDaily);
                        dicResult.Add(tabType + "DutyTypeCategory", dtPcbaLrrDutyTypeCategory);
                        dicResult.Add(tabType + "DutyTypeCategory2", dtPcbaLrrDutyTypeCategory2);
                    }
                    else
                    {
                        DataTable dtLrrPlantAnalysis = GetLrrPlantAnalysis(tabType, "ALL");
                        DataTable dtLrrMonthly = mpSqlOperater.GetLrrMonthlyTrend(tabType, plant, WorkDateConvert.GetWorkDate("MONTHLY", -5), WorkDateConvert.GetWorkDate("MONTHLY", 0));
                        DataTable dtLrrWeekly = mpSqlOperater.GetLrrWeeklyTrend(tabType, plant, WorkDateConvert.GetWorkDate("WEEKLY", -5), WorkDateConvert.GetWorkDate("WEEKLY", 1));
                        DataTable dtDutyTypeCategory = mpSqlOperater.GetDutyTypeCategory(tabType, plant, WorkDateConvert.GetWorkDate("WEEKLY", 0));

                        dicResult.Add(tabType + "PlantAnalysis", dtLrrPlantAnalysis);
                        dicResult.Add(tabType + "Monthly", dtLrrMonthly);
                        dicResult.Add(tabType + "Weekly", dtLrrWeekly);
                        dicResult.Add(tabType + "DutyTypeCategory", dtDutyTypeCategory);
                    }
                    #endregion
                    break;
                case "CPA":
                    #region
                    str3Type = "ALL";
                    if (objs.Length > 3)
                        str3Type = objs[3].ToString();
                    if (tabType == "PCBALRR" || (tabType.Length > 7 && tabType.Substring(0, 7) == "PCBALRR"))
                    {
                        if (plant == "VNP1" && tabType.Contains("_"))
                            strExType = tabType.Substring(7);
                        DateTime dtNow0 = dtNow.AddDays(-7 * 3);
                        fromTime = dtNow0.AddDays(1 - (int)((new GregorianCalendar()).GetDayOfWeek(dtNow0))).ToString("yyyyMMdd");
                        endTime = dtNow.AddDays(8 - (int)((new GregorianCalendar()).GetDayOfWeek(dtNow))).ToString("yyyyMMdd");
                        DataTable dtLrrCpa = mpSqlOperater.GetPcbaLrrPlantAnalysisWeek(fromTime, endTime, str3Type, strExType);
                        dtLrrCpa.Columns["work_date"].ColumnName = "date";

                        dtNow0 = dtNow.AddMonths(-2);
                        fromTime = dtNow0.ToString("yyyyMM") + "01";
                        endTime = dtNow.AddMonths(1).ToString("yyyyMM") + "01";
                        DataTable dtLrrCpa2 = mpSqlOperater.GetPcbaLrrPlantAnalysisMonth(fromTime, endTime, str3Type, strExType);
                        dtLrrCpa2.Columns["work_date"].ColumnName = "date";

                        dtLrrCpa2.Merge(dtLrrCpa);

                        dicResult.Add(tabType + "PlantAnalysis", dtLrrCpa2);
                    }
                    else if (tabType == "TTL")
                    {
                        DataTable dtLrrCpa = mpSqlOperater.GetLrrRMSPlantAnalysisWeek("AP", WorkDateConvert.GetWorkDate("WEEKLY", -2), WorkDateConvert.GetWorkDate("WEEKLY", 1), "PCBA LRR");
                        DataTable dtLrrCpa2 = mpSqlOperater.GetLrrRMSPlantAnalysisMonth("AP", WorkDateConvert.GetWorkDate("MONTHLY", -2), WorkDateConvert.GetWorkDate("MONTHLY", 0), "PCBA LRR");
                        dtLrrCpa2.Merge(dtLrrCpa);

                        DateTime dtNow0 = dtNow.AddDays(-7 * 5);
                        fromTime = dtNow0.AddDays(1 - (int)((new GregorianCalendar()).GetDayOfWeek(dtNow0))).ToString("yyyyMMdd");
                        endTime = dtNow.AddDays(8 - (int)((new GregorianCalendar()).GetDayOfWeek(dtNow))).ToString("yyyyMMdd");

                        DataTable dtWFR = mpSqlOperater.GetPcbaLrrPlantAnalysisWFRWeek(fromTime, endTime);
                        dtWFR.Columns["work_date"].ColumnName = "date";

                        fromTime = dtNow.AddMonths(-2).ToString("yyyyMM") + "01";
                        endTime = dtNow.AddMonths(1).ToString("yyyyMM") + "01";
                        DataTable dtWFR2 = mpSqlOperater.GetPcbaLrrPlantAnalysisWFRMonth(fromTime, endTime);
                        dtWFR2.Columns["work_date"].ColumnName = "date";
                        dtWFR2.Merge(dtWFR);

                        dicResult.Add(tabType + "PlantAnalysis", dtLrrCpa2);
                        dicResult.Add(tabType + "PlantAnalysisWFR", dtWFR2);
                    }
                    else
                    {
                        DataTable dtLrrCpa = GetLrrPlantAnalysis(tabType, str3Type);
                        dicResult.Add(tabType + "PlantAnalysis", dtLrrCpa);
                    }
                    #endregion
                    break;
                case "PCBALRR":
                    #region
                    strExType = ""; 
                    plant = plant.Replace("TW01", "CTY1");
                    if (plant.IndexOf("_") > -1)
                    {
                        string[] aa = plant.Split('_');
                        plant = aa[0].ToString();
                        strExType = "_" + aa[1].ToString();
                    }
                    DataTable dtL2 = new DataTable();
                    DataTable dtL3 = new DataTable();
                    DataTable dtL4 = new DataTable();
                    fromTime = WorkDateConvert.GetWorkDate("WEEKLY", 0);
                    endTime = WorkDateConvert.GetWorkDate("WEEKLY", 1);
                    DataTable reason_codelist;
                    string reason_codes;
                    if (objs.Length > 3)
                    {
                        if (objs[3].ToString().Contains("DEEPDIVE"))
                        { 
                            string strTemp = objs[3].ToString();
                            string[] aa = strTemp.Split('_');
                            if (aa[1].Substring(0, 1) == "D")
                            {
                                fromTime = aa[3];
                                endTime = DateTime.ParseExact(aa[3], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(1).ToString("yyyyMMdd"); ;
                            }
                            else if (aa[1].Substring(0, 1) == "W")
                            {
                                string strYear, strWeek;
                                aa[3] = aa[3].Replace("W", "");
                                strYear = aa[3].Substring(0, 4);
                                strWeek = aa[3].Substring(4, 2);
                                DateTime sTime, eTime;
                                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                                {
                                    sTime = DateTime.Now.AddDays(-7);
                                    eTime = DateTime.Now;
                                }
                                fromTime = sTime.ToString("yyyyMMdd");
                                endTime = eTime.AddDays(1).ToString("yyyyMMdd");
                            }
                            else if (aa[1].Substring(0, 1) == "M")
                            {
                                fromTime = aa[3].Replace("M", "") + "01";
                                endTime = DateTime.ParseExact(fromTime, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMMdd");
                            }
                        }
                        else
                        {
                            if (objs[3].ToString().ToLower().Contains("m"))
                            {
                                fromTime = WorkDateConvert.GetWorkDate("MONTHLY", -Convert.ToInt32(objs[3].ToString().Substring(2))).Replace("M", "") + "01";
                                endTime = WorkDateConvert.GetWorkDate("MONTHLY", -Convert.ToInt32(objs[3].ToString().Substring(2)) + 1).Replace("M", "") + "01";
                            }
                            if (objs[3].ToString().Contains("w"))
                            {
                                if ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow)) == 1)
                                    dtNow = dtNow.AddDays(-7 - 7 * Convert.ToInt32(objs[3].ToString().Substring(2)));
                                else
                                    dtNow = dtNow.AddDays(-7 * Convert.ToInt32(objs[3].ToString().Substring(2)));
                                fromTime = dtNow.AddDays(1 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                                endTime = dtNow.AddDays(8 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                            }
                            if (objs[3].ToString().Contains("d"))
                            {
                                fromTime = dtNow.AddDays(-Convert.ToInt32(objs[3].ToString().Substring(2))).ToString("yyyyMMdd");
                                endTime = dtNow.AddDays(-Convert.ToInt32(objs[3].ToString().Substring(2)) + 1).ToString("yyyyMMdd");
                            }
                        }
                    }

                    dtL2 = mpSqlOperater.GetPcbaLrrL2Data(tabType, plant, fromTime, endTime, strExType);
                    reason_codelist = dtL2.DefaultView.ToTable(true, "reason_code");
                    reason_codes = "('";
                    foreach (DataRow drreason_code in reason_codelist.Rows)
                    {
                        reason_codes += drreason_code[0].ToString() + "','";
                    }
                    reason_codes += "')";

                    DataTable dtTop5model_serials = mpSqlOperater.GetPcbaLrrTop5Model_serials(tabType, plant, fromTime, endTime, strExType);
                    string Top5model_serials = "('";
                    foreach (DataRow drmodel_serials in dtTop5model_serials.Rows)
                    {
                        Top5model_serials += drmodel_serials[0].ToString() + "','";
                    }
                    Top5model_serials += "')";

                    dtL3 = mpSqlOperater.GetPcbaLrrL3orL4Data(tabType, plant, fromTime, endTime, reason_codes, Top5model_serials, strExType, "L3");
                    dtL4 = mpSqlOperater.GetPcbaLrrL3orL4Data(tabType, plant, fromTime, endTime, reason_codes, Top5model_serials, strExType, "L4");
                    dicResult.Add(tabType + "L2", dtL2);
                    dicResult.Add(tabType + "L3", dtL3);
                    dicResult.Add(tabType + "L4", dtL4);
                    #endregion
                    break;
                case "PCBALRR_CHART_MODEL":
                    #region
                    strExType = "";
                    plant = plant.Replace("TW01", "CTY1");
                    if (plant.IndexOf("_") > -1)
                    {
                        string[] aa = plant.Split('_');
                        plant = aa[0].ToString();
                        strExType = "_" + aa[1].ToString();
                    }
                    if (objs.Length > 7)
                    {
                        string dateType = objs[3].ToString();
                        string dataType = objs[4].ToString();
                        string reason_code = objs[5].ToString();
                        string location_code = objs[6].ToString();
                        string model_serial = objs[7].ToString();
                        DateTime dtNow0 = dtNow.AddDays(-7);
                        fromTime = dtNow0.ToString("yyyyMMdd");
                        endTime = dtNow.AddDays(1).ToString("yyyyMMdd");
                        DataTable dtDaily;
                        if (dataType == "L2")
                            dtDaily = mpSqlOperater.GetL2ByReasonDailyTrend(tabType, plant, fromTime, endTime, reason_code, strExType);
                        else
                            dtDaily = mpSqlOperater.GetL4ByReasonDailyTrend(tabType, plant, fromTime, endTime, reason_code, location_code, model_serial);
                        dtDaily.Columns["work_date"].ColumnName = "date";

                        dtNow0 = dtNow.AddMonths(-4);
                        fromTime = dtNow0.ToString("yyyyMM") + "01";
                        endTime = dtNow.AddMonths(1).ToString("yyyyMM") + "01";
                        DataTable dtMonthly;
                        if (dataType == "L2")
                            dtMonthly = mpSqlOperater.GetL2ByReasonMonthlyTrend(tabType, plant, fromTime, endTime, reason_code, strExType);
                        else
                            dtMonthly = mpSqlOperater.GetL4ByReasonMonthlyTrend(tabType, plant, fromTime, endTime, reason_code, location_code, model_serial);
                        dtMonthly.Columns["work_date"].ColumnName = "date";

                        dtNow0 = dtNow.AddDays(-7 * 5);
                        GregorianCalendar gc = new GregorianCalendar();
                        fromTime = dtNow0.AddDays(1 - (int)(gc.GetDayOfWeek(dtNow0))).ToString("yyyyMMdd");
                        endTime = dtNow.AddDays(8 - (int)(gc.GetDayOfWeek(dtNow))).ToString("yyyyMMdd");
                        DataTable dtWeekly;
                        if (dataType == "L2")
                            dtWeekly = mpSqlOperater.GetL2ByReasonWeeklyTrend(tabType, plant, fromTime, endTime, reason_code, strExType);
                        else
                            dtWeekly = mpSqlOperater.GetL4ByReasonWeeklyTrend(tabType, plant, fromTime, endTime, reason_code, location_code, model_serial);
                        dtWeekly.Columns["work_date"].ColumnName = "date";

                        dicResult.Add("Daily", dtDaily);
                        dicResult.Add("Monthly", dtMonthly);
                        dicResult.Add("Weekly", dtWeekly);
                    }
                    #endregion
                    break;
                case "PCBALRR_MODEL":
                    #region
                    strExType = "";
                    plant = plant.Replace("TW01", "CTY1");
                    if (plant.IndexOf("_") > -1)
                    {
                        string[] aa = plant.Split('_');
                        plant = aa[0].ToString();
                        strExType = "_" + aa[1].ToString();
                    }
                    if (objs.Length > 5)
                    {
                        string dateType = objs[3].ToString();
                        string dataType = objs[4].ToString();
                        string model_serial = objs[5].ToString();
                        GregorianCalendar gc = new GregorianCalendar();
                        if (objs[3].ToString().Contains("DEEPDIVE"))
                        { 
                            string strTemp = objs[3].ToString();
                            string[] aa = strTemp.Split('_');
                            if (aa[1].Substring(0, 1) == "D")
                            {
                                fromTime = aa[3];
                                endTime = DateTime.ParseExact(aa[3], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(1).ToString("yyyyMMdd"); ;
                            }
                            else if (aa[1].Substring(0, 1) == "W")
                            {
                                string strYear, strWeek;
                                aa[3] = aa[3].Replace("W", "");
                                strYear = aa[3].Substring(0, 4);
                                strWeek = aa[3].Substring(4, 2);
                                DateTime sTime, eTime;
                                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                                {
                                    sTime = DateTime.Now.AddDays(-7);
                                    eTime = DateTime.Now;
                                }
                                fromTime = sTime.ToString("yyyyMMdd");
                                endTime = eTime.AddDays(1).ToString("yyyyMMdd");
                            }
                            else if (aa[1].Substring(0, 1) == "M")
                            {
                                fromTime = aa[3].Replace("M", "") + "01";
                                endTime = DateTime.ParseExact(fromTime, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMMdd");
                            }
                        }
                        else
                        {
                            if (objs[3].ToString().ToLower().Contains("m"))
                            {
                                fromTime = WorkDateConvert.GetWorkDate("MONTHLY", -Convert.ToInt32(objs[3].ToString().Substring(2))).Replace("M", "") + "01";
                                endTime = WorkDateConvert.GetWorkDate("MONTHLY", -Convert.ToInt32(objs[3].ToString().Substring(2)) + 1).Replace("M", "") + "01";
                            }
                            if (objs[3].ToString().Contains("w"))
                            {

                                if ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow)) == 1)
                                    dtNow = dtNow.AddDays(-7 - 7 * Convert.ToInt32(objs[3].ToString().Substring(2)));
                                else
                                    dtNow = dtNow.AddDays(-7 * Convert.ToInt32(objs[3].ToString().Substring(2)));
                                fromTime = dtNow.AddDays(1 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                                endTime = dtNow.AddDays(8 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                            }
                            if (objs[3].ToString().Contains("d"))
                            {
                                fromTime = dtNow.AddDays(-Convert.ToInt32(objs[3].ToString().Substring(2))).ToString("yyyyMMdd");
                                endTime = dtNow.AddDays(-Convert.ToInt32(objs[3].ToString().Substring(2)) + 1).ToString("yyyyMMdd");
                            }
                        }                     
                        //switch (dateType)
                        //{
                        //    case "上月":
                        //        fromTime = WorkDateConvert.GetWorkDate("MONTHLY", -1).Replace("M", "") + "01";
                        //        endTime = WorkDateConvert.GetWorkDate("MONTHLY", 0).Replace("M", "") + "01";
                        //        break;
                        //    case "本月":
                        //        fromTime = WorkDateConvert.GetWorkDate("MONTHLY", 0).Replace("M", "") + "01";
                        //        endTime = WorkDateConvert.GetWorkDate("MONTHLY", 1).Replace("M", "") + "01";
                        //        break;
                        //    case "上周":
                        //    case "本周":
                        //        if (dateType == "上周")
                        //            dtNow = dtNow.AddDays(-7);
                        //        fromTime = dtNow.AddDays(1 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                        //        endTime = dtNow.AddDays(8 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                        //        break;
                        //}
                        dtL2 = mpSqlOperater.GetPcbaLrrL2Data(tabType, plant, fromTime, endTime, strExType);
                        reason_codelist = dtL2.DefaultView.ToTable(true, "reason_code");
                        reason_codes = "('";
                        foreach (DataRow drreason_code in reason_codelist.Rows)
                        {
                            reason_codes += drreason_code[0].ToString() + "','";
                        }
                        reason_codes += "')";
                        DataTable dtResult = mpSqlOperater.GetPcbaLrrL3L4ModelData(tabType, plant, fromTime, endTime, reason_codes, model_serial, dataType);
                        dicResult.Add(tabType + "L2", dtL2);
                        dicResult.Add(tabType + "MODEL", dtResult);
                    }
                    #endregion
                    break;
                case "PCBALRR_EXCEL":
                    #region
                    strExType = "";
                    plant = plant.Replace("TW01", "CTY1");
                    if (plant.IndexOf("_") > -1)
                    {
                        string[] aa = plant.Split('_');
                        plant = aa[0].ToString();
                        strExType = "_" + aa[1].ToString();
                    }
                    if (objs.Length > 3)
                    {
                        if (objs[3].ToString().Contains("DEEPDIVE"))
                        { 
                            string strTemp = objs[3].ToString();
                            string[] aa = strTemp.Split('_');
                            if (aa[1].Substring(0, 1) == "D")
                            {
                                fromTime = aa[3];
                                endTime = DateTime.ParseExact(aa[3], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(1).ToString("yyyyMMdd"); ;
                            }
                            else if (aa[1].Substring(0, 1) == "W")
                            {
                                string strYear, strWeek;
                                aa[3] = aa[3].Replace("W", "");
                                strYear = aa[3].Substring(0, 4);
                                strWeek = aa[3].Substring(4, 2);
                                DateTime sTime, eTime;
                                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                                {
                                    sTime = DateTime.Now.AddDays(-7);
                                    eTime = DateTime.Now;
                                }
                                fromTime = sTime.ToString("yyyyMMdd");
                                endTime = eTime.AddDays(1).ToString("yyyyMMdd");
                            }
                            else if (aa[1].Substring(0, 1) == "M")
                            {
                                fromTime = aa[3].Replace("M", "") + "01";
                                endTime = DateTime.ParseExact(fromTime, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMMdd");
                            }
                        }
                        else
                        {
                            string date = objs[3].ToString();
                            GregorianCalendar gc = new GregorianCalendar();
                            if (objs[3].ToString().ToLower().Contains("m"))
                            {
                                fromTime = WorkDateConvert.GetWorkDate("MONTHLY", -Convert.ToInt32(objs[3].ToString().Substring(2))).Replace("M", "") + "01";
                                endTime = WorkDateConvert.GetWorkDate("MONTHLY", -Convert.ToInt32(objs[3].ToString().Substring(2)) + 1).Replace("M", "") + "01";
                            }
                            if (objs[3].ToString().Contains("w"))
                            {

                                if ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow)) == 1)
                                    dtNow = dtNow.AddDays(-7 - 7 * Convert.ToInt32(objs[3].ToString().Substring(2)));
                                else
                                    dtNow = dtNow.AddDays(-7 * Convert.ToInt32(objs[3].ToString().Substring(2)));
                                fromTime = dtNow.AddDays(1 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                                endTime = dtNow.AddDays(8 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                            }
                            if (objs[3].ToString().Contains("d"))
                            {
                                fromTime = dtNow.AddDays(-Convert.ToInt32(objs[3].ToString().Substring(2))).ToString("yyyyMMdd");
                                endTime = dtNow.AddDays(-Convert.ToInt32(objs[3].ToString().Substring(2)) + 1).ToString("yyyyMMdd");
                            }
                        }
                        DataTable dtResult = mpSqlOperater.GetPcbaLrrExcelData(tabType, plant, fromTime, endTime, strExType);
                        dicResult.Add("EXCEL", dtResult);
                    }
                    #endregion
                    break;
                case "PCBALRR_L4":
                case "PCBALRR_L4_PE":
                    #region
                    strExType = "";
                    plant = plant.Replace("TW01", "CTY1");
                    if (plant.IndexOf("_") > -1)
                    {
                        string[] aa = plant.Split('_');
                        plant = aa[0].ToString();
                        strExType = "_" + aa[1].ToString();
                    }
                    DataTable dtL2_ = new DataTable();
                    DataTable dtL4_ = new DataTable();
                    fromTime = WorkDateConvert.GetWorkDate("WEEKLY", 0);
                    endTime = WorkDateConvert.GetWorkDate("WEEKLY", 1);
                    DataTable reason_codelist_;
                    string reason_codes_;
                    if (objs.Length > 3)
                    {
                        if (objs[3].ToString().Contains("DEEPDIVE"))
                        { 
                            string strTemp = objs[3].ToString();
                            string[] aa = strTemp.Split('_');
                            if (aa[1].Substring(0, 1) == "D")
                            {
                                fromTime = aa[3];
                                endTime = DateTime.ParseExact(aa[3], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(1).ToString("yyyyMMdd"); ;
                            }
                            else if (aa[1].Substring(0, 1) == "W")
                            {
                                string strYear, strWeek;
                                aa[3] = aa[3].Replace("W", "");
                                strYear = aa[3].Substring(0, 4);
                                strWeek = aa[3].Substring(4, 2);
                                DateTime sTime, eTime;
                                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                                {
                                    sTime = DateTime.Now.AddDays(-7);
                                    eTime = DateTime.Now;
                                }
                                fromTime = sTime.ToString("yyyyMMdd");
                                endTime = eTime.AddDays(1).ToString("yyyyMMdd");
                            }
                            else if (aa[1].Substring(0, 1) == "M")
                            {
                                fromTime = aa[3].Replace("M", "") + "01";
                                endTime = DateTime.ParseExact(fromTime, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMMdd");
                            }
                        }
                        else
                        {
                            if (objs[3].ToString().ToLower().Contains("m"))
                            {
                                fromTime = WorkDateConvert.GetWorkDate("MONTHLY", -Convert.ToInt32(objs[3].ToString().Substring(2))).Replace("M", "") + "01";
                                endTime = WorkDateConvert.GetWorkDate("MONTHLY", -Convert.ToInt32(objs[3].ToString().Substring(2)) + 1).Replace("M", "") + "01";
                            }
                            if (objs[3].ToString().Contains("w"))
                            {
                                if ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow)) == 1)//周一取上周
                                    dtNow = dtNow.AddDays(-7 - 7 * Convert.ToInt32(objs[3].ToString().Substring(2)));
                                else
                                    dtNow = dtNow.AddDays(-7 * Convert.ToInt32(objs[3].ToString().Substring(2)));
                                fromTime = dtNow.AddDays(1 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                                endTime = dtNow.AddDays(8 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                            }
                            if (objs[3].ToString().Contains("d"))
                            {
                                fromTime = dtNow.AddDays(-Convert.ToInt32(objs[3].ToString().Substring(2))).ToString("yyyyMMdd");
                                endTime = dtNow.AddDays(-Convert.ToInt32(objs[3].ToString().Substring(2)) + 1).ToString("yyyyMMdd");
                            }
                        }
                    }
                    dtL2_ = mpSqlOperater.GetPcbaLrrL2Data(tabType, plant, fromTime, endTime, strExType);
                    reason_codelist_ = dtL2_.DefaultView.ToTable(true, "reason_code");
                    reason_codes_ = "('";
                    foreach (DataRow drreason_code in reason_codelist_.Rows)
                    {
                        reason_codes_ += drreason_code[0].ToString() + "','";
                    }
                    reason_codes_ += "')";

                    DataTable dtTop5model_serials_ = mpSqlOperater.GetPcbaLrrTop5Model_serials(tabType, plant, fromTime, endTime, strExType);
                    string Top5model_serials_ = "('";
                    foreach (DataRow drmodel_serials in dtTop5model_serials_.Rows)
                    {
                        Top5model_serials_ += drmodel_serials[0].ToString() + "','";
                    }
                    Top5model_serials_ += "')";
                    if (chartType == "PCBALRR_L4")
                        dtL4_ = mpSqlOperater.GetPcbaLrrL3orL4Data(tabType, plant, fromTime, endTime, reason_codes_, Top5model_serials_, strExType, "L4");
                    if (chartType == "PCBALRR_L4_PE")
                        dtL4_ = mpSqlOperater.GetPcbaLrrL3orL4Data(tabType, plant, fromTime, endTime, reason_codes_, Top5model_serials_, strExType, "L4", "", true);
                    dicResult.Add(tabType + "L2", dtL2_);
                    dicResult.Add(tabType + "L4", dtL4_);
                    #endregion
                    break;
                default:
                    break;
            }

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMpHsr(object[] objs)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            string plant = objs[0].ToString();
            string tabType = objs[1].ToString();
            string chartType = objs[2].ToString();

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            Dictionary<string, string> dicTime;
            switch (chartType)
            {
                case "ALL":
                    dicTime = GetTime("W");
                    DataTable dtHsrPlantAnalysis = mpSqlOperater.GetHsrPlantAnalysis(tabType, plant, dicTime["fromTime"], dicTime["endTime"], "HOLD QTY");
                    dicTime = GetTime("D");
                    DataTable dtHsrDaily = mpSqlOperater.GetHsrDailyTrend(tabType, plant, dicTime["fromTime"], dicTime["endTime"]);
                    dicTime = GetTime("W");
                    DataTable dtHsrWeekly = mpSqlOperater.GetHsrWeeklyTrend(tabType, plant, dicTime["fromTime"], dicTime["endTime"]);
                    dicTime = GetTime("M");
                    DataTable dtHsrMonthly = mpSqlOperater.GetHsrMonthlyTrend(tabType, plant, dicTime["fromTime"], dicTime["endTime"]);
                    dicResult.Add(tabType + "PlantAnalysis", dtHsrPlantAnalysis);
                    dicResult.Add(tabType + "Daily", dtHsrDaily);
                    dicResult.Add(tabType + "Weekly", dtHsrWeekly);
                    dicResult.Add(tabType + "Monthly", dtHsrMonthly);
                    break;
                case "CPA":
                    string str3Type = "HOLD QTY";
                    if (objs.Length > 3)
                        str3Type = objs[3].ToString();
                    dicTime = GetTime("W");
                    DataTable dtHsrCpa = mpSqlOperater.GetHsrPlantAnalysis(tabType, plant, dicTime["fromTime"], dicTime["endTime"], str3Type);
                    dicResult.Add(tabType + "PlantAnalysis", dtHsrCpa);
                    break;
                default:
                    break;
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMpIndex(string plant, string type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            DataTable dtResult = new DataTable();
            Dictionary<string, string> dicTime;
            switch (type)
            {
                case "TREND":
                    dtResult = mpSqlOperater.GetMpIndexData(plant);
                    break;
                case "FPY_CHARTDATA":
                    dicTime = GetTime("D");
                    dtResult = mpSqlOperater.GetFpyIndexDailyData(plant, dicTime["fromTime"], dicTime["endTime"]);
                    if (plant == "CDP1")
                    {
                        DataTable dt = mpSqlOperater.GetFpyIndexDailyData("CDDOCK", dicTime["fromTime"], dicTime["endTime"]);
                        if (dt.Rows.Count > 0)
                            dtResult.Merge(dt);
                    }
                    else if (plant == "TW01")
                    {
                        DataTable dt = mpSqlOperater.GetFpyIndexDailyData("TW01DOCK", dicTime["fromTime"], dicTime["endTime"]);
                        if (dt.Rows.Count > 0)
                            dtResult.Merge(dt);
                    }
                    break;
                case "REFLOW_CHARTDATA":
                    dicTime = GetTime("D");
                    dtResult = mpSqlOperater.GetReflowIndexDailyData(plant.Replace("TW01", "CTY1"), dicTime["fromTime"], dicTime["endTime"]);
                    dtResult.Columns["work_date"].ColumnName = "date";
                    break;
                default:
                    break;
            }
            exeRes.Anything = dtResult;
            #endregion

            return exeRes;
        }

        public DataTable GetLrrPlantAnalysis(string type, string str3Type)
        {
            DataTable dtLrrPlantAnalysis = new DataTable();
            dtLrrPlantAnalysis = mpSqlOperater.GetLrrPlantAnalysis(type, WorkDateConvert.GetWorkDate("WEEKLY", -5), WorkDateConvert.GetWorkDate("WEEKLY", 1), str3Type);
            return dtLrrPlantAnalysis;
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

        public Dictionary<string, string> GetTimeRecylineRate(string dateType)
        {
            string fromTime = "", endTime = "";
            Dictionary<string, string> dicTime = new Dictionary<string, string>();
            switch (dateType)
            {
                case "D":
                    fromTime = dtNow.AddDays(-7).ToString("yyyyMMdd");
                    endTime = dtNow.ToString("yyyyMMdd");
                    break;
                case "W":
                    DateTime dtPre = dtNow.AddDays(-28);
                    GregorianCalendar gc = new GregorianCalendar();
                    int wp = gc.GetWeekOfYear(dtPre, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    fromTime = dtPre.ToString("yyyy") + "W" + wp.ToString().PadLeft(2, '0');
                    int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    endTime = dtNow.ToString("yyyy") + "W" + w.ToString().PadLeft(2, '0');
                    break;
                case "M":
                    fromTime = dtNow.AddMonths(-2).ToString("yyyyMM");
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
    }
}
