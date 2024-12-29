using Compal.MESComponent;
using sDEM2100.DataGateway;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;

namespace sDEM2100.Core
{
    class MatrixMainLogic
    {
        private object[] mClientInfo;
        private MESLog mesLog;
        private string mDbName;
        private DateTime dtNow;
        private MatrixSqlOperater matrixSqlOperater;

        public MatrixMainLogic(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.mesLog = new MESLog("Dqms");
            dtNow = DateTime.Now;
            matrixSqlOperater = new MatrixSqlOperater(mClientInfo, dbName);
        }

        public ExecutionResult ExecuteMatrixIndex(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            DataTable dtResult = matrixSqlOperater.GetMatrixIndexData(plant);
            exeRes.Anything = dtResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMatrixQuality(string plant, string type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            Dictionary<string, string> dicTime;
            dicTime = GetTime("W");
            DataTable dtSvlrrPlantAnalysis = matrixSqlOperater.GetSvlrrPlantAnalysis(type, dicTime["fromTime"], dicTime["endTime"]);
            DataTable dtFrrPlantAnalysis = matrixSqlOperater.GetFrrPlantAnalysis(type, dicTime["fromTime"], dicTime["endTime"]);
            DataTable dtSvlrrWeekly = matrixSqlOperater.GetSvlrrWeeklyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            DataTable dtFrrWeekly = matrixSqlOperater.GetFrrWeeklyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            
            dicTime = GetTime("M");
            DataTable dtSvlrrMonthly = matrixSqlOperater.GetSvlrrMonthlyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            DataTable dtFrrMonthly = matrixSqlOperater.GetFrrMonthlyTrend(type, plant, dicTime["fromTime"], dicTime["endTime"]);

            dicResult.Add(type + "SvlrrPlantAnalysis", dtSvlrrPlantAnalysis);
            dicResult.Add(type + "FrrPlantAnalysis", dtFrrPlantAnalysis);
            dicResult.Add(type + "SvlrrWeekly", dtSvlrrWeekly);
            dicResult.Add(type + "FrrWeekly", dtFrrWeekly); 
            dicResult.Add(type + "SvlrrMonthly", dtSvlrrMonthly);
            dicResult.Add(type + "FrrMonthly", dtFrrMonthly);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
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
                    fromTime = dtPre.ToString("yyyy") + "W" + wp.ToString().PadLeft(2,'0');
                    int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    endTime = dtNow.ToString("yyyy") + "W" + w.ToString().PadLeft(2,'0');
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

        public ExecutionResult ExecuteMatrixQuality2(string plant)
        {
            var exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            var dicResult = new Dictionary<string, DataTable>();
            var dicTime = GetTime2("W",21);
            var dtMESvlrrWeekly = matrixSqlOperater.GetSvlrrWeeklyTrend("ME", plant, dicTime["fromTime"], dicTime["endTime"]);
            var dtMEFrrWeekly = matrixSqlOperater.GetFrrWeeklyTrend("ME", plant, dicTime["fromTime"], dicTime["endTime"]);
            var dtEESvlrrWeekly = matrixSqlOperater.GetSvlrrWeeklyTrend("EE", plant, dicTime["fromTime"], dicTime["endTime"]);
            var dtEEFrrWeekly = matrixSqlOperater.GetFrrWeeklyTrend("EE", plant, dicTime["fromTime"], dicTime["endTime"]);
            var dtOPWeekly = dtOP(plant,"W");
            var dtOPMonthly = dtOP(plant, "M");
            var dtOPQuarterly = dtOP(plant, "Q");
            //H高速機  L泛用機
            dicTime = GetTime2("W", 14);
            var dtHRejectWeekly = matrixSqlOperater.GetRejectRate("H", plant, dicTime["fromTime"], dicTime["endTime"]);
            var dtLRejectWeekly = matrixSqlOperater.GetRejectRate("L", plant, dicTime["fromTime"], dicTime["endTime"]);

            dicResult.Add("MESvlrrWeekly", dtMESvlrrWeekly);
            dicResult.Add("MEFrrWeekly", dtMEFrrWeekly);
            dicResult.Add("EESvlrrWeekly", dtEESvlrrWeekly);
            dicResult.Add("EEFrrWeekly", dtEEFrrWeekly);
            dicResult.Add("OPWeekly", dtOPWeekly);
            dicResult.Add("OPMonthly", dtOPMonthly);
            dicResult.Add("OPQuarterly", dtOPQuarterly);
            dicResult.Add("HRejectWeekly", dtHRejectWeekly);
            dicResult.Add("LRejectWeekly", dtLRejectWeekly);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public Dictionary<string, string> GetTime2(string dateType,int week)
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
                    DateTime dtPre = dtNow.AddDays(-week);
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

        public string[] GetTime3(string dateType)
        {
            var dates = new string[3];

            for(int i=0;i<dates.Count();i++)
            {
                switch (dateType)
                {
                    case "W":
                        var dtPre = dtNow.AddDays(-((i+1)*7));
                        var gc = new GregorianCalendar();
                        var wp = gc.GetWeekOfYear(dtPre, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                        dates[i] = dtPre.ToString("yyyy") + "W" + wp.ToString().PadLeft(2, '0');
                        break;
                    case "M":
                        var Monthly = dtNow.AddMonths(-(i+1)).ToString("yyyyMM");
                        dates[i] = Monthly.Insert(Monthly.Length - 2, "M");
                        break;
                    case "Q":
                        var Quarterly = dtNow.AddMonths(-((i+1)*3));
                        var Month = Quarterly.Month;
                        dates[i] = Quarterly.ToString("yyyy") + "Q" + (Month % 3 == 0 ? Month / 3 : (Month / 3 + 1));
                        break;
                }
            }
            return dates;
        }

        public DataTable dtOP(string plant,string dateType)
        {
            var dt = new DataTable();
            var strs = GetTime3(dateType);
            for (int i = strs.Count(); i >0; i--)
            {
                switch (dateType)
                {
                    case "W":
                        dt.Merge(matrixSqlOperater.GetOPWeekly(plant, strs[i-1]));
                        break;
                    case "M":
                        dt.Merge(matrixSqlOperater.GetOPMonthly(plant, strs[i-1]));
                        break;
                    case "Q":
                        dt.Merge(matrixSqlOperater.GetOPQuarterly(plant, strs[i - 1]));
                        break;
                    default:
                        break;
                }
            }
            return dt;
        }

        public ExecutionResult ExecuteMatrixRejectRate(string plant, string type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            Dictionary<string, string> dicTime;
            dicTime = GetTime("W");
            DataTable dtCrossPlant = matrixSqlOperater.GetRjtRateCrossPlant(type, dicTime["fromTime"], dicTime["endTime"]);
            DataTable dtWeekly = matrixSqlOperater.GetRjtRateWeekly(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            DataTable dtLine = matrixSqlOperater.GetRjtRateLine(type, plant, dicTime["fromTime"], dicTime["endTime"]);
            dicTime = GetTime("D");
            DataTable dtDaily = matrixSqlOperater.GetRjtRateDaily(type, plant, dicTime["fromTime"], dicTime["endTime"]);

            dicTime = GetTime("M");
            DataTable dtMonthly = matrixSqlOperater.GetRjtRateMonthly(type, plant, dicTime["fromTime"], dicTime["endTime"]);

            dicResult.Add(type + "CrossPlant", dtCrossPlant);
            dicResult.Add(type + "Daily", dtDaily);
            dicResult.Add(type + "Weekly", dtWeekly);
            dicResult.Add(type + "Monthly", dtMonthly);
            dicResult.Add(type + "Line", dtLine);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult MatrixRjtRateLine(string plant, string type,string typeDate)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            Dictionary<string, string> dicTime;
            var dtLine = new DataTable();

            switch (typeDate)
            {
                case "Daily":
                    dicTime = GetTime("D");
                    dtLine = matrixSqlOperater.GetRjtRateLineD(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                    break;
                case "Weekly":
                    dicTime = GetTime("W");
                    dtLine = matrixSqlOperater.GetRjtRateLine(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                    break;
                case "Monthly":
                    dicTime = GetTime("M");
                    dtLine = matrixSqlOperater.GetRjtRateLineM(type, plant, dicTime["fromTime"], dicTime["endTime"]);
                    break;
                default:
                    break;
            }

            dicResult.Add(type + "Line", dtLine);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
    }
}
