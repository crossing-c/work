using sDEM2100.Beans;
using sDEM2100.DataGateway;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using sDEM2100.Utils;
using System.Text.RegularExpressions;
using System.Net;
using System.IO;

namespace sDEM2100.Core
{
    class CrossCheckControl
    {
        private DateTime dtNow;
        private static CrossCheckOperator crosscheckOperator;
        private static MpSqlOperater mpsqlOperator;
        private object[] mClientInfo;
        public CrossCheckControl(object[] clientInfo, string DBName)
        {
            dtNow = DateTime.Now;
            crosscheckOperator = new CrossCheckOperator(clientInfo, DBName);
            mpsqlOperator = new MpSqlOperater(clientInfo, DBName);
            mClientInfo = clientInfo;
        }
        public object[] GetData(object[] objParam)
        {
            List<CrossCheckModels> modellist = new List<CrossCheckModels>();
            string plant = objParam[0].ToString();
            string bydate = objParam[1].ToString();
            string fdate = objParam[2].ToString();
            string edate = objParam[3].ToString();
            string Indexs = objParam[4].ToString();
            if (Indexs != null && Indexs != "")
            {
                string categories = GetCategories(bydate.Substring(0, 1), fdate, edate);
                for (int i = 0; i < Indexs.Split(';').Length; i++)
                {
                    string index = Indexs.Split(';')[i];
                    if (index != "")
                    {
                        CrossCheckModels model = new CrossCheckModels();
                        model.IndexName = index;
                        model.ID = "Chart" + i;
                        DataTable dt = new DataTable();
                        Dictionary<string, string> dicTime;
                        if (bydate == "Daily" || (index == "Base AOI Y/R" && plant.IndexOf("CQ") == -1))
                            dicTime = GetTime("D", fdate, edate);
                        else if (bydate == "Weekly")
                            dicTime = GetTime("W", fdate, edate);
                        else
                            dicTime = GetTime("M", fdate, edate);
                        switch (index)
                        {
                            case "FPY FATP":
                            case "FPY SMT":
                                #region
                                model.Unit = "%";//單位
                                model.Goal = crosscheckOperator.GetGoal(index);
                                string process1 = "";
                                if (index.Contains("SMT"))
                                    process1 = "SMT";
                                else
                                    process1 = "AP";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetFpyDailyTrend(process1, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetFpyWeeklyTrend(process1, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetFpyMonthlyTrend(process1, plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "FATP INPUT":
                            case "SMT INPUT":
                                #region
                                model.Unit = "Pcs";//單位
                                model.Goal = crosscheckOperator.GetGoal(index);
                                string processINPUT = "";
                                if (index.Contains("SMT"))
                                    processINPUT = "SMT";
                                else
                                    processINPUT = "AP";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetOutputDailyTrend(processINPUT, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetOutputWeeklyTrend(processINPUT, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetOutputMonthlyTrend(processINPUT, plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "品質119 Burn Out":
                            case "品質119 掉螺絲":
                                #region
                                model.Unit = "Pcs";//單位
                                model.Goal = crosscheckOperator.GetGoal(index);
                                string q119type = "";
                                if (index == "品質119 Burn Out")
                                {
                                    q119type = "BURN OUT";
                                }
                                else if (index == "品質119 掉螺絲")
                                    q119type = "掉螺絲";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetQ119DailyTrend(q119type, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetQ119WeeklyTrend(q119type, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetQ119MonthlyTrend(q119type, plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "OFFLINE 整機":
                            case "OFFLINE 半成品":
                                #region
                                model.Unit = "%";//單位
                                model.Goal = crosscheckOperator.GetGoal(index);
                                string offlinetype = "";
                                if (index == "OFFLINE 整機")
                                {
                                    offlinetype = "FG";
                                }
                                else
                                    offlinetype = "SFG";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetOfflineDailyTrend(offlinetype, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetOfflineWeeklyTrend(offlinetype, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetOfflineMonthlyTrend(offlinetype, plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "REFLOW ASSY":
                            case "REFLOW PACK":
                                #region
                                model.Unit = "%";//單位
                                model.Goal = crosscheckOperator.GetGoal(index);
                                string reflowtype = "";
                                if (index == "REFLOW ASSY")
                                {
                                    reflowtype = "ASSY";
                                }
                                else
                                    reflowtype = "PACK";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetReflowDailyTrend(reflowtype, plant.Replace("TW01", "CTY1"), dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetReflowWeeklyTrend(reflowtype, plant.Replace("TW01", "CTY1"), dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetReflowMonthlyTrend(reflowtype, plant.Replace("TW01", "CTY1"), dicTime["fromTime"], dicTime["endTime"]);
                                dt.Columns["work_date"].ColumnName = "date";
                                break;
                            #endregion
                            case "CTQ":
                                #region
                                model.Unit = "%";//單位
                                model.Goal = "100";//crosscheckOperator.GetGoal(index);
                                string process2 = "AP";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetCtqDailyTrend(process2, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetCtqWeeklyTrend(process2, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetCtqMonthlyTrend(process2, plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "PID LCD":
                                #region 
                                model.Unit = "DPPM";//單位
                                model.Goal = "350";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetPidLcdDailyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetPidLcdWeeklyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetPidLcdMonthlyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "PID ME":
                                #region 
                                model.Unit = "DPPM";//單位
                                model.Goal = "8000";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetPidMeDailyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetPidMeWeeklyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetPidMeMonthlyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "PID KEY PARTS":
                                #region 
                                model.Unit = "DPPM";//單位
                                model.Goal = "500";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetPidKpsDailyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetPidKpsWeeklyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetPidKpsMonthlyTrend(plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "WFR FATP":
                            case "WFR SMT":
                                #region
                                model.Unit = "Piece";//單位
                                if (index.Contains("SMT"))
                                    model.Goal = "450";
                                else
                                    model.Goal = "200"; 
                                string process3 = "";
                                if (index.Contains("SMT"))
                                    process3 = "SMT";
                                else
                                    process3 = "FATP";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetWfrDailyTrend(process3, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetWfrWeeklyTrend(process3, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetWfrMonthlyTrend(process3, plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "MB SCRAP":
                                #region
                                model.Unit = "DPPM";//單位
                                model.Goal = "200";//crosscheckOperator.GetGoal(index);
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetMBScrapDaily(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetMBScrapWeekly(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetMBScrapMonthly(plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "CPU REPLACE RATE":
                                #region
                                model.Unit = "DPPM";//單位
                                model.Goal = "1500";//crosscheckOperator.GetGoal(index);
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetCPUReplaceRateDaily(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetCPUReplaceRateWeekly(plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetCPUReplaceRateMonthly(plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "SVLRR EE":
                                #region
                                model.Unit = "DPPM";//單
                                model.Goal = "800";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetSVLRRFRRDaily("SVLRR", "EE", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetSVLRRFRRWeekly("SVLRR", "EE", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetSVLRRFRRMonthly("SVLRR", "EE", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "SVLRR ME":
                                #region
                                model.Unit = "DPPM";//單
                                model.Goal = "880";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetSVLRRFRRDaily("SVLRR", "ME", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetSVLRRFRRWeekly("SVLRR", "ME", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetSVLRRFRRMonthly("SVLRR", "ME", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "FRR EE":
                                #region
                                model.Unit = "DPPM";//單
                                model.Goal = "100";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetSVLRRFRRDaily("FRR", "EE", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetSVLRRFRRWeekly("FRR", "EE", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetSVLRRFRRMonthly("FRR", "EE", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "FRR ME":
                                #region
                                model.Unit = "DPPM";//單
                                model.Goal = "1500";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetSVLRRFRRDaily("FRR", "ME", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetSVLRRFRRWeekly("FRR", "ME", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetSVLRRFRRMonthly("FRR", "ME", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "LRR FATP MATERIAL":
                                #region 
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetLrrDailyTrend("AP", "材料", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetLrrWeeklyTrend("AP", "材料", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetLrrMonthlyTrend("AP", "材料", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "LRR FATP PCBA":
                                #region
                                model.Unit = "%";
                                model.Goal = "0.5";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetLrrDailyTrend("AP", "PCBA LRR", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetLrrWeeklyTrend("AP", "PCBA LRR", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetLrrMonthlyTrend("AP", "PCBA LRR", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "LRR FATP WORKMANSHIP":
                                #region
                                model.Unit = "%";
                                model.Goal = "0.5";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetLrrDailyTrend("AP", "作業", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetLrrWeeklyTrend("AP", "作業", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetLrrMonthlyTrend("AP", "作業", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "LRR SMT 置件":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetLrrDailyTrend("SMT", "置件", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetLrrWeeklyTrend("SMT", "置件", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetLrrMonthlyTrend("SMT", "置件", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "LRR SMT 印刷":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetLrrDailyTrend("SMT", "印刷", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetLrrWeeklyTrend("SMT", "印刷", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetLrrMonthlyTrend("SMT", "印刷", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "LRR SMT 材料":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetLrrDailyTrend("SMT", "材料", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetLrrWeeklyTrend("SMT", "材料", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetLrrMonthlyTrend("SMT", "材料", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "FATP 修護回鍋率":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetRepairBackPotRateDaily("AP", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetRepairBackPotRateWeekly("AP", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetRepairBackPotRateMonthly("AP", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "SMT 修護回鍋率":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetRepairBackPotRateDaily("SMT", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetRepairBackPotRateWeekly("SMT", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetRepairBackPotRateMonthly("SMT", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "UPPH(FATP)":
                                #region
                                model.Unit = "set";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetUPPHDaily("FATP", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetUPPHWeekly("FATP", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetUPPHMonthly("FATP", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "UPPH(SMT)":
                                #region
                                model.Unit = "set";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetUPPHDaily("SMT", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetUPPHWeekly("SMT", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetUPPHMonthly("SMT", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "新人比例(＜1个月)":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetNewStaffRateDaily("A10", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetNewStaffRateWeekly("A10", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetNewStaffRateMonthly("A10", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "新人比例(1~3个月)":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetNewStaffRateDaily("A20", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetNewStaffRateWeekly("A20", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetNewStaffRateMonthly("A20", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "新人比例(3~6个月)":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetNewStaffRateDaily("A30", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetNewStaffRateWeekly("A30", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetNewStaffRateMonthly("A30", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "新人比例(6~12个月)":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetNewStaffRateDaily("A40", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetNewStaffRateWeekly("A40", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetNewStaffRateMonthly("A40", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "新人比例(＞12个月)":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetNewStaffRateDaily("A50", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetNewStaffRateWeekly("A50", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetNewStaffRateMonthly("A50", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "INSIGHT YR FATP":
                            case "INSIGHT YR SMT":
                                #region
                                model.Unit = "%";//單位

                                string process4 = "";
                                if (index.Contains("SMT"))
                                {
                                    model.Goal = "96.5";
                                    process4 = "SMT";
                                }
                                else
                                {
                                    model.Goal = "96";
                                    process4 = "AP";
                                }
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetInsightYRDailyTrend(process4, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetInsightYRWeeklyTrend(process4, plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetInsightYRMonthlyTrend(process4, plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "高速機拋料率":
                                #region
                                model.Unit = "‰";
                                model.Goal = "0.35";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetRjtRateDaily("H", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetRjtRateWeekly("H", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetRjtRateMonthly("H", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "泛用機拋料率":
                                #region
                                model.Unit = "‰";
                                model.Goal = "0.7";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetRjtRateDaily("L", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetRjtRateWeekly("L", plant, dicTime["fromTime"], dicTime["endTime"]);
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetRjtRateMonthly("L", plant, dicTime["fromTime"], dicTime["endTime"]);
                                break;
                            #endregion
                            case "HOLD QTY(FATP)":
                                #region
                                model.Unit = "Piece";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetHsrDailyTrend("AP", plant, dicTime["fromTime"], dicTime["endTime"], "Hold");
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetHsrWeeklyTrend("AP", plant, dicTime["fromTime"], dicTime["endTime"], "Hold");
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetHsrMonthlyTrend("AP", plant, dicTime["fromTime"], dicTime["endTime"], "Hold");
                                break;
                            #endregion
                            case "HOLD QTY(SMT)":
                                #region
                                model.Unit = "Piece";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetHsrDailyTrend("SMT", plant, dicTime["fromTime"], dicTime["endTime"], "Hold");
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetHsrWeeklyTrend("SMT", plant, dicTime["fromTime"], dicTime["endTime"], "Hold");
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetHsrMonthlyTrend("SMT", plant, dicTime["fromTime"], dicTime["endTime"], "Hold");
                                break;
                            #endregion
                            case "HOLD DPPM(FATP)":
                                #region
                                model.Unit = "DPPM";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetHsrDailyTrend("AP", plant, dicTime["fromTime"], dicTime["endTime"], "Hold DPPM");
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetHsrWeeklyTrend("AP", plant, dicTime["fromTime"], dicTime["endTime"], "Hold DPPM");
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetHsrMonthlyTrend("AP", plant, dicTime["fromTime"], dicTime["endTime"], "Hold DPPM");
                                break;
                            #endregion
                            case "HOLD DPPM(SMT)":
                                #region
                                model.Unit = "DPPM";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                    dt = crosscheckOperator.GetHsrDailyTrend("SMT", plant, dicTime["fromTime"], dicTime["endTime"], "Hold DPPM");
                                if (bydate == "Weekly")
                                    dt = crosscheckOperator.GetHsrWeeklyTrend("SMT", plant, dicTime["fromTime"], dicTime["endTime"], "Hold DPPM");
                                if (bydate == "Monthly")
                                    dt = crosscheckOperator.GetHsrMonthlyTrend("SMT", plant, dicTime["fromTime"], dicTime["endTime"], "Hold DPPM");
                                break;
                            #endregion
                            case "SMT Insight次數":
                                #region
                                model.Unit = "Times";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                {
                                    TimeSpan t1 = (TimeSpan)(DateTime.ParseExact(dicTime["endTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture) - DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture));
                                    for (int idx = 0; idx < t1.TotalDays; idx++)
                                    {
                                        string sTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx).ToString("yyyyMMdd") + "080000";
                                        string eTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx + 1).ToString("yyyyMMdd") + "075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageDailyTrend("SMTInsight", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                if (bydate == "Weekly")
                                {
                                    DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, dicTime["fromTime"].Replace("W", ""), dicTime["endTime"].Replace("W", ""), mClientInfo);
                                    if (dtWeeks.Rows.Count > 0)
                                    {
                                        for (int idx = 0; idx < dtWeeks.Rows.Count; idx++)
                                        {
                                            string strWeek = dtWeeks.Rows[i]["WEEKS"].ToString();

                                            string sTime = dtWeeks.Rows[idx]["fromday"].ToString().Replace("/", "") + "080000";
                                            string eTime = DateTime.Parse(dtWeeks.Rows[idx]["today"].ToString()).AddDays(1).ToString("yyyyMMdd") + "075959";
                                            DataTable dtTemp = crosscheckOperator.GetUsageWeeklyTrend("SMTInsight", plant, sTime, eTime);
                                            if (dtTemp.Rows.Count > 0)
                                                dt.Merge(dtTemp);
                                        }
                                    }
                                }
                                if (bydate == "Monthly")
                                {
                                    DataTable dtMonth = new DataTable();
                                    dtMonth.Columns.Add("M");
                                    dtMonth.Rows.Add(dicTime["fromTime"]);
                                    string strNow = DateTime.Now.ToString("yyyy") + "M" + DateTime.Now.ToString("MM");
                                    string strTemp = dicTime["fromTime"];
                                    if (dicTime["fromTime"] != dicTime["endTime"]) 
                                    {
                                        do
                                        {
                                            string[] aa = strTemp.Split('M');
                                            if (Int32.Parse(aa[1].ToString()) + 1 > 12)
                                                strTemp = Int32.Parse(aa[0].ToString()) + 1 + "M" + "01";
                                            else
                                                strTemp = aa[0].ToString() + "M" + (Int32.Parse(aa[1].ToString()) + 1).ToString("00");
                                            dtMonth.Rows.Add(strTemp);
                                        }
                                        while (strTemp != strNow && strTemp != dicTime["endTime"]);
                                    }
                                    for (int idx = 0; idx < dtMonth.Rows.Count; idx++)
                                    {
                                        string sTime = dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01080000";
                                        string eTime = DateTime.ParseExact(dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMM") + "01075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageMonthlyTrend("SMTInsight", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                dt.Columns["date1"].ColumnName = "date";
                                break;
                            #endregion
                            case "FATP Insight次數":
                                #region
                                model.Unit = "Times";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                {
                                    TimeSpan t1 = (TimeSpan)(DateTime.ParseExact(dicTime["endTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture) - DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture));
                                    for (int idx = 0; idx < t1.TotalDays; idx++)
                                    {
                                        string sTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx).ToString("yyyyMMdd") + "080000";
                                        string eTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx + 1).ToString("yyyyMMdd") + "075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageDailyTrend("FATPInsight", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                if (bydate == "Weekly")
                                {
                                    DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, dicTime["fromTime"].Replace("W", ""), dicTime["endTime"].Replace("W", ""), mClientInfo);
                                    if (dtWeeks.Rows.Count > 0)
                                    {
                                        for (int idx = 0; idx < dtWeeks.Rows.Count; idx++)
                                        {
                                            string strWeek = dtWeeks.Rows[i]["WEEKS"].ToString();

                                            string sTime = dtWeeks.Rows[idx]["fromday"].ToString().Replace("/", "") + "080000";
                                            string eTime = DateTime.Parse(dtWeeks.Rows[idx]["today"].ToString()).AddDays(1).ToString("yyyyMMdd") + "075959";
                                            DataTable dtTemp = crosscheckOperator.GetUsageWeeklyTrend("FATPInsight", plant, sTime, eTime);
                                            if (dtTemp.Rows.Count > 0)
                                                dt.Merge(dtTemp);
                                        }
                                    }
                                }
                                if (bydate == "Monthly")
                                {
                                    DataTable dtMonth = new DataTable();
                                    dtMonth.Columns.Add("M");
                                    dtMonth.Rows.Add(dicTime["fromTime"]);
                                    string strNow = DateTime.Now.ToString("yyyy") + "M" + DateTime.Now.ToString("MM");
                                    string strTemp = dicTime["fromTime"];
                                    if (dicTime["fromTime"] != dicTime["endTime"]) 
                                    {
                                        do
                                        {
                                            string[] aa = strTemp.Split('M');
                                            if (Int32.Parse(aa[1].ToString()) + 1 > 12)
                                                strTemp = Int32.Parse(aa[0].ToString()) + 1 + "M" + "01";
                                            else
                                                strTemp = aa[0].ToString() + "M" + (Int32.Parse(aa[1].ToString()) + 1).ToString("00");
                                            dtMonth.Rows.Add(strTemp);
                                        }
                                        while (strTemp != strNow && strTemp != dicTime["endTime"]);
                                    }
                                    for (int idx = 0; idx < dtMonth.Rows.Count; idx++)
                                    {
                                        string sTime = dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01080000";
                                        string eTime = DateTime.ParseExact(dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMM") + "01075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageMonthlyTrend("FATPInsight", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                dt.Columns["date1"].ColumnName = "date";
                                break;
                            #endregion
                            case "SMT FPY次數":
                                #region
                                model.Unit = "Times";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                {
                                    TimeSpan t1 = (TimeSpan)(DateTime.ParseExact(dicTime["endTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture) - DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture));
                                    for (int idx = 0; idx < t1.TotalDays; idx++)
                                    {
                                        string sTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx).ToString("yyyyMMdd") + "080000";
                                        string eTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx + 1).ToString("yyyyMMdd") + "075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageDailyTrend("SMT", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                if (bydate == "Weekly")
                                {
                                    DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, dicTime["fromTime"].Replace("W", ""), dicTime["endTime"].Replace("W", ""), mClientInfo);
                                    if (dtWeeks.Rows.Count > 0)
                                    {
                                        for (int idx = 0; idx < dtWeeks.Rows.Count; idx++)
                                        {
                                            string strWeek = dtWeeks.Rows[i]["WEEKS"].ToString();

                                            string sTime = dtWeeks.Rows[idx]["fromday"].ToString().Replace("/", "") + "080000";
                                            string eTime = DateTime.Parse(dtWeeks.Rows[idx]["today"].ToString()).AddDays(1).ToString("yyyyMMdd") + "075959";
                                            DataTable dtTemp = crosscheckOperator.GetUsageWeeklyTrend("SMT", plant, sTime, eTime);
                                            if (dtTemp.Rows.Count > 0)
                                                dt.Merge(dtTemp);
                                        }
                                    }
                                }
                                if (bydate == "Monthly")
                                {
                                    DataTable dtMonth = new DataTable();
                                    dtMonth.Columns.Add("M");
                                    dtMonth.Rows.Add(dicTime["fromTime"]);
                                    string strNow = DateTime.Now.ToString("yyyy") + "M" + DateTime.Now.ToString("MM");
                                    string strTemp = dicTime["fromTime"];
                                    if (dicTime["fromTime"] != dicTime["endTime"]) 
                                    {
                                        do
                                        {
                                            string[] aa = strTemp.Split('M');
                                            if (Int32.Parse(aa[1].ToString()) + 1 > 12)
                                                strTemp = Int32.Parse(aa[0].ToString()) + 1 + "M" + "01";
                                            else
                                                strTemp = aa[0].ToString() + "M" + (Int32.Parse(aa[1].ToString()) + 1).ToString("00");
                                            dtMonth.Rows.Add(strTemp);
                                        }
                                        while (strTemp != strNow && strTemp != dicTime["endTime"]);
                                    }
                                    for (int idx = 0; idx < dtMonth.Rows.Count; idx++)
                                    {
                                        string sTime = dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01080000";
                                        string eTime = DateTime.ParseExact(dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMM") + "01075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageMonthlyTrend("SMT", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                dt.Columns["date1"].ColumnName = "date";
                                break;
                            #endregion
                            case "FATP FPY次數":
                                #region
                                model.Unit = "Times";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                {
                                    TimeSpan t1 = (TimeSpan)(DateTime.ParseExact(dicTime["endTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture) - DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture));
                                    for (int idx = 0; idx < t1.TotalDays; idx++)
                                    {
                                        string sTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx).ToString("yyyyMMdd") + "080000";
                                        string eTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx + 1).ToString("yyyyMMdd") + "075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageDailyTrend("FATP", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                if (bydate == "Weekly")
                                {
                                    DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, dicTime["fromTime"].Replace("W", ""), dicTime["endTime"].Replace("W", ""), mClientInfo);
                                    if (dtWeeks.Rows.Count > 0)
                                    {
                                        for (int idx = 0; idx < dtWeeks.Rows.Count; idx++)
                                        {
                                            string strWeek = dtWeeks.Rows[i]["WEEKS"].ToString();

                                            string sTime = dtWeeks.Rows[idx]["fromday"].ToString().Replace("/", "") + "080000";
                                            string eTime = DateTime.Parse(dtWeeks.Rows[idx]["today"].ToString()).AddDays(1).ToString("yyyyMMdd") + "075959";
                                            DataTable dtTemp = crosscheckOperator.GetUsageWeeklyTrend("FATP", plant, sTime, eTime);
                                            if (dtTemp.Rows.Count > 0)
                                                dt.Merge(dtTemp);
                                        }
                                    }
                                }
                                if (bydate == "Monthly")
                                {
                                    DataTable dtMonth = new DataTable();
                                    dtMonth.Columns.Add("M");
                                    dtMonth.Rows.Add(dicTime["fromTime"]);
                                    string strNow = DateTime.Now.ToString("yyyy") + "M" + DateTime.Now.ToString("MM");
                                    string strTemp = dicTime["fromTime"];
                                    if (dicTime["fromTime"] != dicTime["endTime"])  
                                    {
                                        do
                                        {
                                            string[] aa = strTemp.Split('M');
                                            if (Int32.Parse(aa[1].ToString()) + 1 > 12)
                                                strTemp = Int32.Parse(aa[0].ToString()) + 1 + "M" + "01";
                                            else
                                                strTemp = aa[0].ToString() + "M" + (Int32.Parse(aa[1].ToString()) + 1).ToString("00");
                                            dtMonth.Rows.Add(strTemp);
                                        }
                                        while (strTemp != strNow && strTemp != dicTime["endTime"]);
                                    }
                                    for (int idx = 0; idx < dtMonth.Rows.Count; idx++)
                                    {
                                        string sTime = dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01080000";
                                        string eTime = DateTime.ParseExact(dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMM") + "01075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageMonthlyTrend("FATP", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                dt.Columns["date1"].ColumnName = "date";
                                break;
                            #endregion
                            case "PCBA LRR次數":
                                #region
                                model.Unit = "Times";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                {
                                    TimeSpan t1 = (TimeSpan)(DateTime.ParseExact(dicTime["endTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture) - DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture));
                                    for (int idx = 0; idx < t1.TotalDays; idx++)
                                    {
                                        string sTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx).ToString("yyyyMMdd") + "080000";
                                        string eTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx + 1).ToString("yyyyMMdd") + "075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageDailyTrend("P", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                if (bydate == "Weekly")
                                {
                                    DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, dicTime["fromTime"].Replace("W", ""), dicTime["endTime"].Replace("W", ""), mClientInfo);
                                    if (dtWeeks.Rows.Count > 0)
                                    {
                                        for (int idx = 0; idx < dtWeeks.Rows.Count; idx++)
                                        {
                                            string strWeek = dtWeeks.Rows[i]["WEEKS"].ToString();

                                            string sTime = dtWeeks.Rows[idx]["fromday"].ToString().Replace("/", "") + "080000";
                                            string eTime = DateTime.Parse(dtWeeks.Rows[idx]["today"].ToString()).AddDays(1).ToString("yyyyMMdd") + "075959";
                                            DataTable dtTemp = crosscheckOperator.GetUsageWeeklyTrend("P", plant, sTime, eTime);
                                            if (dtTemp.Rows.Count > 0)
                                                dt.Merge(dtTemp);
                                        }
                                    }
                                }
                                if (bydate == "Monthly")
                                {
                                    DataTable dtMonth = new DataTable();
                                    dtMonth.Columns.Add("M");
                                    dtMonth.Rows.Add(dicTime["fromTime"]);
                                    string strNow = DateTime.Now.ToString("yyyy") + "M" + DateTime.Now.ToString("MM");
                                    string strTemp = dicTime["fromTime"];
                                    if (dicTime["fromTime"] != dicTime["endTime"]) 
                                    {
                                        do
                                        {
                                            string[] aa = strTemp.Split('M');
                                            if (Int32.Parse(aa[1].ToString()) + 1 > 12)
                                                strTemp = Int32.Parse(aa[0].ToString()) + 1 + "M" + "01";
                                            else
                                                strTemp = aa[0].ToString() + "M" + (Int32.Parse(aa[1].ToString()) + 1).ToString("00");
                                            dtMonth.Rows.Add(strTemp);
                                        }
                                        while (strTemp != strNow && strTemp != dicTime["endTime"]);
                                    }
                                    for (int idx = 0; idx < dtMonth.Rows.Count; idx++)
                                    {
                                        string sTime = dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01080000";
                                        string eTime = DateTime.ParseExact(dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMM") + "01075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageMonthlyTrend("P", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                dt.Columns["date1"].ColumnName = "date";
                                break;
                            #endregion
                            case "PID次數":
                                #region
                                model.Unit = "Times";
                                model.Goal = "/";
                                if (bydate == "Daily")
                                {
                                    TimeSpan t1 = (TimeSpan)(DateTime.ParseExact(dicTime["endTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture) - DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture));
                                    for (int idx = 0; idx < t1.TotalDays; idx++)
                                    {
                                        string sTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx).ToString("yyyyMMdd") + "080000";
                                        string eTime = DateTime.ParseExact(dicTime["fromTime"], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(idx + 1).ToString("yyyyMMdd") + "075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageDailyTrend("PID", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                if (bydate == "Weekly")
                                {
                                    DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, dicTime["fromTime"].Replace("W", ""), dicTime["endTime"].Replace("W", ""), mClientInfo);
                                    if (dtWeeks.Rows.Count > 0)
                                    {
                                        for (int idx = 0; idx < dtWeeks.Rows.Count; idx++)
                                        {
                                            string strWeek = dtWeeks.Rows[i]["WEEKS"].ToString();

                                            string sTime = dtWeeks.Rows[idx]["fromday"].ToString().Replace("/", "") + "080000";
                                            string eTime = DateTime.Parse(dtWeeks.Rows[idx]["today"].ToString()).AddDays(1).ToString("yyyyMMdd") + "075959";
                                            DataTable dtTemp = crosscheckOperator.GetUsageWeeklyTrend("PID", plant, sTime, eTime);
                                            if (dtTemp.Rows.Count > 0)
                                                dt.Merge(dtTemp);
                                        }
                                    }
                                }
                                if (bydate == "Monthly")
                                {
                                    DataTable dtMonth = new DataTable();
                                    dtMonth.Columns.Add("M");
                                    dtMonth.Rows.Add(dicTime["fromTime"]);
                                    string strNow = DateTime.Now.ToString("yyyy") + "M" + DateTime.Now.ToString("MM");
                                    string strTemp = dicTime["fromTime"];
                                    if (dicTime["fromTime"] != dicTime["endTime"])  
                                    {
                                        do
                                        {
                                            string[] aa = strTemp.Split('M');
                                            if (Int32.Parse(aa[1].ToString()) + 1 > 12)
                                                strTemp = Int32.Parse(aa[0].ToString()) + 1 + "M" + "01";
                                            else
                                                strTemp = aa[0].ToString() + "M" + (Int32.Parse(aa[1].ToString()) + 1).ToString("00");
                                            dtMonth.Rows.Add(strTemp);
                                        }
                                        while (strTemp != strNow && strTemp != dicTime["endTime"]);
                                    }
                                    for (int idx = 0; idx < dtMonth.Rows.Count; idx++)
                                    {
                                        string sTime = dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01080000";
                                        string eTime = DateTime.ParseExact(dtMonth.Rows[idx]["M"].ToString().Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddMonths(1).ToString("yyyyMM") + "01075959";
                                        DataTable dtTemp = crosscheckOperator.GetUsageMonthlyTrend("PID", plant, sTime, eTime);
                                        if (dtTemp.Rows.Count > 0)
                                            dt.Merge(dtTemp);
                                    }
                                }
                                dt.Columns["date1"].ColumnName = "date";
                                break;
                            #endregion
                            case "Base AOI Y/R":
                                #region
                                model.Unit = "%";
                                model.Goal = "/";
                                string strPlant = plant;
                                if (plant.IndexOf("TW01") > -1)
                                    strPlant = "CTY1";
                                else if (plant.IndexOf("VNP1") > -1)
                                    strPlant = "VNP1";
                                if (plant.IndexOf("CQ") > -1)
                                {
                                    if (bydate == "Daily")
                                        dt = crosscheckOperator.GetBaseAoiDailyTrend(dicTime["fromTime"], dicTime["endTime"]);
                                    if (bydate == "Weekly")
                                        dt = crosscheckOperator.GetBaseAoiWeeklyTrend(dicTime["fromTime"], dicTime["endTime"]);
                                    if (bydate == "Monthly")
                                        dt = crosscheckOperator.GetBaseAoiMonthlyTrend(dicTime["fromTime"], dicTime["endTime"]);
                                    dt.Columns["work_date"].ColumnName = "date";
                                }
                                else
                                {
                                    string strValue = crosscheckOperator.GetDataDictionary("CLOUD_AOI", strPlant);
                                    if (strValue != "")
                                    {
                                        try
                                        {
                                            String url = strValue + @"/common_boardpattern/query_qms_data?date_start=" + dicTime["fromTime"] + "&date_end=" + dicTime["endTime"] + "&mode=" + bydate.Replace("Daily", "day").Replace("Weekly", "week").Replace("Monthly", "month");
                                            DataTable dtCloud = new DataTable();
                                            dtCloud.Columns.Add("date");
                                            dtCloud.Columns.Add("value");
                                            HttpWebRequest req = (HttpWebRequest)WebRequest.Create(url);
                                            req.KeepAlive = false;
                                            req.ProtocolVersion = HttpVersion.Version10;
                                            req.Method = "GET";
                                            req.UserAgent = "WinForm";
                                            //   req.ContentType = "application/json";
                                            req.Timeout = 15000;//设置请求超时时间，单位为毫秒            
                                            req.Headers.Set("Pragma", "no-cache");
                                            HttpWebResponse response = (HttpWebResponse)req.GetResponse();
                                            Stream streamReceive = response.GetResponseStream();
                                            Encoding encoding = Encoding.UTF8;
                                            StreamReader streamReader = new StreamReader(streamReceive, encoding);
                                            string strResult = streamReader.ReadToEnd();
                                            streamReceive.Dispose();
                                            streamReader.Dispose();
                                            strResult = strResult.Replace("\\", "").Replace("{", "");
                                            string[] tmpStr = Regex.Split(strResult, @"},");
                                            for (int x = 0; x < tmpStr.Length; x++)
                                            {
                                                string[] tmpRowStr = Regex.Split(tmpStr[x], @"\n");
                                                DataRow dr = dtCloud.NewRow();
                                                for (int j = 0; j < tmpRowStr.Length; j++)
                                                {
                                                    if (tmpRowStr[j].ToString().Trim() != "")
                                                    {
                                                        if (tmpRowStr[j].ToString().IndexOf("DataDate") > -1)
                                                        {
                                                            string strTemp = tmpRowStr[j].Substring(tmpRowStr[j].ToString().IndexOf("DataDate") + 9, tmpRowStr[j].ToString().IndexOf("\":") - (tmpRowStr[j].ToString().IndexOf("DataDate") + 9));
                                                            if (bydate == "Monthly")
                                                                strTemp = strTemp.Replace("-", "M");
                                                            else if (bydate == "Daily")
                                                                strTemp = strTemp.Replace("-", "");
                                                            dr["date"] = strTemp;

                                                        }
                                                        if (tmpRowStr[j].ToString().IndexOf("FPY") > -1)
                                                        {
                                                            dr["value"] = tmpRowStr[j].Substring(tmpRowStr[j].ToString().IndexOf("FPY") + 6, tmpRowStr[j].ToString().IndexOf(",") - (tmpRowStr[j].ToString().IndexOf("FPY") + 6));
                                                        }

                                                        //if (tmpRowStr[j].ToString().IndexOf("FR") > -1)
                                                        //    dr["FR"] = tmpRowStr[j].Substring(tmpRowStr[j].ToString().IndexOf("FR") + 5, tmpRowStr[j].ToString().IndexOf(",") - (tmpRowStr[j].ToString().IndexOf("FR") + 5));
                                                        //if (tmpRowStr[j].ToString().IndexOf("Total") > -1)
                                                        //    dr["Total"] = tmpRowStr[j].Substring(tmpRowStr[j].ToString().IndexOf("Total") + 8);
                                                    }
                                                }
                                                dtCloud.Rows.Add(dr);
                                            }
                                            if (dtCloud.Rows.Count > 0)
                                                dt = dtCloud;
                                        }
                                        catch
                                        {

                                        }
                                    }
                                }
                                break;
                                #endregion
                        }

                        if (dt.Rows.Count > 0)
                        {
                            for (int k = 0; k < categories.Split(';').Length; k++)
                            {
                                DataRow[] drli = dt.Select(" date='" + categories.Split(';')[k] + "' ");
                                //DataRow drli = GetDataRow(dt, categories.Split(';')[k]);
                                if (drli.Length > 0)
                                    model.data.Add(Math.Round(Convert.ToDouble(drli[0]["value"]), 2));
                                else
                                {
                                    if (index == "品質119 掉螺絲" || index == "品質119 Burn Out" || index == "OFFLINE 半成品" || index == "OFFLINE 整機")
                                        model.data.Add(0);
                                    else
                                        model.data.Add(null);
                                }
                            }
                        }
                        else
                        {
                            for (int k = 0; k < categories.Split(';').Length; k++)
                            {
                                if (categories.Split(';')[k] != "")
                                {
                                    if (index == "品質119 掉螺絲" || index == "品質119 Burn Out" || index == "OFFLINE 半成品" || index == "OFFLINE 整機")
                                        model.data.Add(0);
                                    else
                                        model.data.Add(null);
                                }
                            }
                        }
                        modellist.Add(model);
                    }
                }
            }
            return new object[] { GetCategories(bydate.Substring(0, 1), fdate, edate), modellist };
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
