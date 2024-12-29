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
    class OverallQualityControl
    {
        private object[] mClientInfo;
        private MESLog mesLog;
        private string mDbName;
        private OverallQualitySqlOperater overallSqlOperater;

        public OverallQualityControl(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.mesLog = new MESLog("Dqms");
            overallSqlOperater = new OverallQualitySqlOperater(mClientInfo, dbName);
        }

        public ExecutionResult ExecuteOverallQuality(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            try
            {
                OverallQuality model = GetOverallQuality(plant);
                exeRes.Anything = model;
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = "Exception:" + ex.Message;
            }
            return exeRes;
        }

        public OverallQuality GetOverallQuality(string plant)
        {
            OverallQuality model = new OverallQuality();
            DataTable dt = new DataTable();
            switch (plant)
            {
                case "KSP2":
                    #region
                    model.Title = "<label>A32 KSP2 </label> <label style=\"color:#A0A00F\">Goal:90</label>";
                    string[] Indexs1 = { "QBR", "ARR", "EOLQ", "HSR" };
                    string[] DateList1 = GetDateList(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    model.categories = GetDateListEN(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    dt = overallSqlOperater.getOverallQualityData(plant, DateList1[0], DateList1[DateList1.Length - 1], Indexs1);
                    foreach (string index in Indexs1)
                    {
                        OverallQualitySerial serial = new Beans.OverallQualitySerial();
                        serial.type = "column";
                        if (index == "QBR")
                            serial.name = index + " (50%)";
                        if (index == "ARR")
                            serial.name = index + " (30%)";
                        if (index == "EOLQ")
                            serial.name = index + " (10%)";
                        if (index == "HSR")
                            serial.name = index + " (10%)";
                        foreach (string date in DateList1)
                        {
                            OverallQualityPoint point = new OverallQualityPoint();
                            double y = 0;
                            double? actual = null;
                            string strGoal = null;
                            DataRow[] drLi = dt.Select(" date='" + date + "' and value_str1='" + index + "' ");
                            if (drLi.Length > 0)
                            {
                                y = GetA32Score(index, Convert.ToDouble(drLi[0]["value"].ToString()));
                                actual = Convert.ToDouble(drLi[0]["value"].ToString());
                            }
                            if (index == "QBR")
                                strGoal = "Rank 1";
                            if (index == "ARR")
                                strGoal = "70";
                            if (index == "EOLQ")
                                strGoal = "1914";
                            if (index == "HSR")
                                strGoal = "0";
                            point.y = y;
                            point.actual = actual;
                            point.goal = strGoal;
                            serial.data.Add(point);
                        }
                        model.OverallQualitySeries.Add(serial);
                    }
                    #endregion
                    break;
                case "KSP3":
                case "CDP1":
                    #region
                    model.Title = "<label>A31 " + plant + " </label> <label style=\"color:#A0A00F\">Goal:90</label>";
                    string[] Indexs2 = { "QBR", "MWDC", "HSR", "FIR" };
                    string[] DateList2 = GetDateList(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    model.categories = GetDateListEN(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    dt = overallSqlOperater.getOverallQualityData(plant, DateList2[0], DateList2[DateList2.Length - 1], Indexs2);
                    foreach (string index in Indexs2)
                    {
                        OverallQualitySerial serial = new Beans.OverallQualitySerial();
                        serial.type = "column";
                        if (index == "QBR")
                            serial.name = index + " (50%)";
                        if (index == "MWDC")
                            serial.name = "MWD (15%)";
                        if (index == "HSR")
                            serial.name = index + " (15%)";
                        if (index == "FIR")
                            serial.name = index + " (20%)";
                        foreach (string date in DateList2)
                        {
                            OverallQualityPoint point = new OverallQualityPoint();
                            double y = 0;
                            double? actual = null;
                            string strGoal = null;
                            DataRow[] drLi = dt.Select(" date='" + date + "' and value_str1='" + index + "' ");
                            if (drLi.Length > 0)
                            {
                                y = GetA31Score(index, Convert.ToDouble(drLi[0]["value"].ToString()));
                                actual = Convert.ToDouble(drLi[0]["value"].ToString());
                            }
                            if (index == "QBR")
                                strGoal = "Rank 1";
                            if (index == "MWDC")
                                strGoal = "20";
                            if (index == "HSR")
                                strGoal = "0";
                            if (index == "FIR")
                                strGoal = "80";
                            point.y = y;
                            point.actual = actual;
                            point.goal = strGoal;
                            serial.data.Add(point);
                        }
                        model.OverallQualitySeries.Add(serial);
                    }
                    #endregion
                    break;
                case "KSP4":
                    #region
                    model.Title = "<label>C38 KSP4 </label> <label style=\"color:#A0A00F\">Goal:90</label>";
                    string[] Indexs3 = { "QBR", "Consumer IFIR", "HSR", "Customer OOB" };
                    string[] DateList3 = GetDateList(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    model.categories = GetDateListEN(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    dt = overallSqlOperater.getOverallQualityData(plant, DateList3[0], DateList3[DateList3.Length - 1], Indexs3);
                    foreach (string index in Indexs3)
                    {
                        OverallQualitySerial serial = new Beans.OverallQualitySerial();
                        serial.type = "column";
                        if (index == "QBR")
                            serial.name = index + " (50%)";
                        if (index == "Consumer IFIR")
                            serial.name = "IFIR (30%)";
                        if (index == "HSR")
                            serial.name = index + " (10%)";
                        if (index == "Customer OOB")
                            serial.name = "OOBA (10%)";
                        foreach (string date in DateList3)
                        {
                            OverallQualityPoint point = new OverallQualityPoint();
                            double y = 0;
                            double? actual = null;
                            string strGoal = null;
                            DataRow[] drLi = dt.Select(" date='" + date + "' and value_str1='" + index + "' ");
                            if (drLi.Length > 0)
                            {
                                y = GetC38Score(index, Convert.ToDouble(drLi[0]["value"].ToString()));
                                actual = Convert.ToDouble(drLi[0]["value"].ToString());
                            }
                            if (index == "QBR")
                                strGoal = "Rank 1";
                            if (index == "Consumer IFIR")
                                strGoal = "18500";
                            if (index == "HSR")
                                strGoal = "0";
                            if (index == "Customer OOB")
                                strGoal = "950";
                            point.y = y;
                            point.actual = actual;
                            point.goal = strGoal;
                            serial.data.Add(point);
                        }
                        model.OverallQualitySeries.Add(serial);
                    }
                    #endregion
                    break;
                case "CQP1":
                    #region
                    model.Title = "<label>ABO CQP1 </label> <label style=\"color:#A0A00F\">Goal:90</label>";
                    string[] Indexs4 = { "QBR", "AFR", "HSR", "OOBA" };
                    string[] DateList4 = GetDateList(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    model.categories = GetDateListEN(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    dt = overallSqlOperater.getOverallQualityData(plant, DateList4[0], DateList4[DateList4.Length - 1], Indexs4);
                    foreach (string index in Indexs4)
                    {
                        OverallQualitySerial serial = new Beans.OverallQualitySerial();
                        serial.type = "column";
                        if (index == "QBR")
                            serial.name = index + " (50%)";
                        if (index == "AFR")
                            serial.name = index + " (30%)";
                        if (index == "HSR")
                            serial.name = index + " (10%)";
                        if (index == "OOBA")
                            serial.name = index + " (10%)";
                        foreach (string date in DateList4)
                        {
                            OverallQualityPoint point = new OverallQualityPoint();
                            double y = 0;
                            double? actual = null;
                            string strGoal = null;
                            DataRow[] drLi = dt.Select(" date='" + date + "' and value_str1='" + index + "' ");
                            if (drLi.Length > 0)
                            {
                                y = GetABOScore(index, Convert.ToDouble(drLi[0]["value"].ToString()));
                                actual = Convert.ToDouble(drLi[0]["value"].ToString());
                            }
                            if (index == "QBR")
                                strGoal = "Rank 1";
                            if (index == "AFR")
                                strGoal = "4.5";
                            if (index == "HSR")
                                strGoal = "0";
                            if (index == "OOBA")
                                strGoal = "5000";
                            point.y = y;
                            point.actual = actual;
                            point.goal = strGoal;
                            serial.data.Add(point); 
                        }
                        model.OverallQualitySeries.Add(serial);
                    }
                    #endregion
                    break;
                case "TW01":
                    #region
                    model.Title = "<label>A31 TW01 </label> <label style=\"color:#A0A00F\">Goal:90</label>";
                    string[] Indexs5 = { "QBR", "MWDC", "HSR" };
                    string[] DateList5 = GetDateList(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    model.categories = GetDateListEN(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    dt = overallSqlOperater.getOverallQualityData(plant, DateList5[0], DateList5[DateList5.Length - 1], Indexs5);
                    foreach (string index in Indexs5)
                    {
                        OverallQualitySerial serial = new Beans.OverallQualitySerial();
                        serial.type = "column";
                        if (index == "QBR")
                            serial.name = index + " (50%)";
                        if (index == "MWDC")
                            serial.name = "MWD (25%)";
                        if (index == "HSR")
                            serial.name = index + " (25%)";
                        foreach (string date in DateList5)
                        {
                            OverallQualityPoint point = new OverallQualityPoint();
                            double y = 0;
                            double? actual = null;
                            string strGoal = null;
                            DataRow[] drLi = dt.Select(" date='" + date + "' and value_str1='" + index + "' ");
                            if (drLi.Length > 0)
                            {
                                y = GetCTYA31Score(index, Convert.ToDouble(drLi[0]["value"].ToString()));
                                actual = Convert.ToDouble(drLi[0]["value"].ToString());
                            }
                            if (index == "QBR")
                                strGoal = "Rank 1";
                            if (index == "MWDC")
                                strGoal = "20";
                            if (index == "HSR")
                                strGoal = "0";                           
                            point.y = y;
                            point.actual = actual;
                            point.goal = strGoal;
                            serial.data.Add(point);
                        }
                        model.OverallQualitySeries.Add(serial);
                    }
                    #endregion
                    break;
                case "VNP1":
                    #region
                    model.Title = "<label>A31 VNP1 </label> <label style=\"color:#A0A00F\">Goal:90</label>";
                    string[] Indexs6 = { "QBR", "FIR" };
                    string[] DateList6 = GetDateList(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    model.categories = GetDateListEN(DateTime.Now.AddMonths(-12), DateTime.Now.AddMonths(-1));
                    dt = overallSqlOperater.getOverallQualityData(plant, DateList6[0], DateList6[DateList6.Length - 1], Indexs6);
                    foreach (string index in Indexs6)
                    {
                        OverallQualitySerial serial = new Beans.OverallQualitySerial();
                        serial.type = "column";
                        if (index == "QBR")
                            serial.name = index + " (50%)";
                        if (index == "FIR")
                            serial.name = index + " (50%)";
                        foreach (string date in DateList6)
                        {
                            OverallQualityPoint point = new OverallQualityPoint();
                            double y = 0;
                            double? actual = null;
                            string strGoal = null;
                            DataRow[] drLi = dt.Select(" date='" + date + "' and value_str1='" + index + "' ");
                            if (drLi.Length > 0)
                            {
                                y = GetCVCA31Score(index, Convert.ToDouble(drLi[0]["value"].ToString()));
                                actual = Convert.ToDouble(drLi[0]["value"].ToString());
                            }
                            if (index == "QBR")
                                strGoal = "Rank 1";                            
                            if (index == "FIR")
                                strGoal = "80";
                            point.y = y;
                            point.actual = actual;
                            point.goal = strGoal;
                            serial.data.Add(point);
                        }
                        model.OverallQualitySeries.Add(serial);
                    }
                    #endregion
                    break;
            }
            OverallQualitySerial scoreSerial = new OverallQualitySerial();
            scoreSerial.type = "spline";
            scoreSerial.name = "score";
            //double score0 = 0, score1 = 0, score2 = 0, score3 = 0, score4 = 0, score5 = 0, score6 = 0,
            //    score7 = 0, score8 = 0, score9 = 0, score10 = 0, score11 = 0;
            List<OverallQualityPoint> scoreList = new List<OverallQualityPoint>(12);
            foreach (OverallQualitySerial serial in model.OverallQualitySeries)
            {
                for (int i = 0; i < serial.data.Count; i++)
                {
                    if (scoreList.Count == i)
                        scoreList.Add(new OverallQualityPoint());
                    OverallQualityPoint point = scoreList[i];
                    scoreList[i].y = point.y + serial.data[i].y;
                }
            }
            scoreSerial.data = scoreList;
            model.OverallQualitySeries.Add(scoreSerial);
            return model;
        }

        public double GetA32Score(string index, double value)
        {
            double score = 0;
            switch (index)
            {
                case "QBR":
                    score = 50 - (value - 1) * 10;
                    break;
                case "ARR":
                    if (value >= 70)
                        score = 30;
                    else if (value >= 50 && value < 70)
                        score = 20;
                    else if (value >= 30 && value < 50)
                        score = 10;
                    else
                        score = 0;
                    break;
                case "EOLQ":
                    if (value <= 1914)
                        score = 10;
                    else if (value > 1914 && value <= 2297)
                        score = 5;
                    else
                        score = 0;
                    break;
                case "HSR":
                    score = value == 0 ? 10 : 0;
                    break;
            }
            return score;
        }

        public double GetA31Score(string index, double value)
        {
            double score = 0;
            switch (index)
            {
                case "QBR":
                    score = 50 - (value - 1) * 10;
                    break;
                case "MWDC":
                    if (value <= 20)
                        score = 15;
                    else if (value > 20 && value <= 25)
                        score = 10;
                    else if (value > 25 && value <= 30)
                        score = 5;
                    else
                        score = 0;
                    break;
                case "HSR":
                    score = value == 0 ? 15 : 0;
                    break;
                case "FIR":
                    if (value >= 80)
                        score = 20;
                    else if (value >= 70 && value <= 79)
                        score = 15;
                    else if (value >= 60 && value <= 69)
                        score = 10;
                    else if (value >= 50 && value <= 59)
                        score = 5;
                    else
                        score = 0;
                    break;
            }
            return score;
        }

        public double GetC38Score(string index, double value)
        {
            double score = 0;
            switch (index)
            {
                case "QBR":
                    score = 50 - (value - 1) * 10;
                    break;
                case "Consumer IFIR":
                    if (value <= 18500)
                        score = 30;
                    else if (value > 18500 && value <= 21275)
                        score = 20;
                    else if (value > 21275 && value <= 22200)
                        score = 10;
                    else
                        score = 0;
                    break;
                case "HSR":
                    score = value == 0 ? 10 : 0;
                    break;
                case "Customer OOB":
                    if (value <= 950)
                        score = 10;
                    else if (value > 950 && value <= 1093)
                        score = 5;
                    else
                        score = 0;
                    break;
            }
            return score;
        }

        public double GetAIOScore(string index, double value)
        {
            double score = 0;
            switch (index)
            {
                case "QBR":
                    score = 50 - (value - 1) * 10;
                    break;
                case "IFIR":
                    if (value <= 6600)
                        score = 30;
                    else if (value > 6600 && value <= 7590)
                        score = 20;
                    else if (value > 7590 && value <= 7920)
                        score = 10;
                    else
                        score = 0;
                    break;
                case "HSR":
                    score = value == 0 ? 10 : 0;
                    break;
                case "OOBA":
                    if (value <= 850)
                        score = 10;
                    else if (value > 850 && value <= 978)
                        score = 5;
                    else
                        score = 0;
                    break;
            }
            return score;
        }

        public double GetABOScore(string index, double value)
        {
            double score = 0;
            switch (index)
            {
                case "QBR":
                    score = 50 - (value - 1) * 10;
                    break;
                case "AFR":
                    if (value <= 4.5)
                        score = 30;
                    else if (value > 4.5 && value <= 5)
                        score = 20;
                    else if (value > 5 && value <= 5.5)
                        score = 10;
                    else
                        score = 0;
                    break;
                case "HSR":
                    score = value == 0 ? 10 : 0;
                    break;
                case "OOBA":
                    if (value <= 3500)
                        score = 10;
                    else if (value > 3500 && value <= 5000)
                        score = 5;
                    else
                        score = 0;
                    break;
            }
            return score;
        }

        public double GetCTYA31Score(string index, double value)
        {
            double score = 0;
            switch (index)
            {
                case "QBR":
                    score = 50 - (value - 1) * 10;
                    break;
                case "MWDC":
                    if (value <= 25)
                        score = 25;
                    else if (value > 25 && value <= 30)
                        score = 20;
                    else if (value > 30 && value <= 35)
                        score = 10;
                    else
                        score = 0;
                    break;
                case "HSR":
                    score = value == 0 ? 25 : 0;
                    break;
            }
            return score;
        }

        public double GetCVCA31Score(string index, double value)
        {
            double score = 0;
            switch (index)
            {
                case "QBR":
                    score = 50 - (value - 1) * 10;
                    break;
                case "FIR":
                    if (value >= 80)
                        score = 50;
                    else if (value > 70 && value <= 79)
                        score = 35;
                    else if (value > 60 && value <= 69)
                        score = 25;
                    else if (value > 50 && value <= 59)
                        score = 15;
                    else
                        score = 0;
                    break;
            }
            return score;
        }

        public string[] GetDateList(DateTime dtFrom, DateTime dtEnd)
        {
            string Categories = "";
            string work_date = string.Empty;
            while (dtFrom.ToString("yyyyMM") != dtEnd.ToString("yyyyMM"))
            {
                work_date = dtFrom.ToString("yyyyMM");
                Categories += work_date.Substring(0, 4) + "M" + work_date.Substring(4) + ";";
                dtFrom = dtFrom.AddMonths(1);
            }
            Categories += dtEnd.ToString("yyyyMM").Substring(0, 4) + "M" + dtEnd.ToString("yyyyMM").Substring(4);
            return Categories.Split(';');
        }

        public string GetDateListEN(DateTime dtFrom, DateTime dtEnd)
        {
            string DateListEN = "";
            DateTime dtFrom0 = dtFrom;
            while (dtFrom.ToString("yyyyMM") != dtEnd.ToString("yyyyMM"))
            {
                if (dtFrom == dtFrom0)
                    DateListEN += "Y" + dtFrom.Year.ToString().Substring(2) + "/" + dtFrom.ToString("MMM", CultureInfo.CreateSpecificCulture("en-GB")) + ";";
                else
                    DateListEN += dtFrom.Month == 1 ? "Y" + dtFrom.Year.ToString().Substring(2) + "/" +
                        dtFrom.ToString("MMM", CultureInfo.CreateSpecificCulture("en-GB")) + ";" : dtFrom.ToString("MMM", CultureInfo.CreateSpecificCulture("en-GB")) + ";";
                dtFrom = dtFrom.AddMonths(1);
            }
            DateListEN += dtFrom.ToString("MMM", CultureInfo.CreateSpecificCulture("en-GB"));
            return DateListEN;
        }

    }
}
