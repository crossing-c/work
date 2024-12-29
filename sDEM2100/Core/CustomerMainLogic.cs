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
    class CustomerMainLogic
    {
        private object[] mClientInfo;
        private MESLog mesLog;
        private string mDbName;
        private CustomerSqlOperater customerSqlOperater;

        public CustomerMainLogic(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.mesLog = new MESLog("Dqms");
            customerSqlOperater = new CustomerSqlOperater(mClientInfo, dbName);
        }

        public ExecutionResult ExecuteCustomerIndex(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            //string year = DateTime.Now.Year.ToString();
            DataTable dtResult = new DataTable();
            dtResult.Columns.Add("plant_code");
            dtResult.Columns.Add("value_str1");
            dtResult.Columns.Add("date");
            dtResult.Columns.Add("value_real1");
            DataColumn dtTrend = new DataColumn("value_trend", typeof(double));
            dtResult.Columns.Add(dtTrend);
            DataColumn dtGoalTrend = new DataColumn("goal_trend", typeof(double));
            dtResult.Columns.Add(dtGoalTrend);
            DataTable dtTitle = customerSqlOperater.GetTitle(plant);
            if (dtTitle != null && dtTitle.Rows.Count > 0)
            {
                DataRow drResult;
                double valueReal1Now = 0,valueReal1Pre=0,trend=0;
                foreach(DataRow drTitle in dtTitle.Rows)
                {
                    string valueStr1 = drTitle["value_str1"].ToString();
                    string plant_code = drTitle["plant_code"].ToString();
                    if (valueStr1 != "QBR")
                    {
                        #region Week
                        //var dateWeekNow = string.Empty;
                        //var dateWeekPre = string.Empty;
                        //var dtWeek = customerSqlOperater.GetWeekValue(plant, valueStr1);
                        string dateWeekNow = WorkDateConvert.GetWorkDate("WEEKLY", -2);
                        string dateWeekPre = WorkDateConvert.GetWorkDate("WEEKLY", -3);
                        valueReal1Now = customerSqlOperater.GetValue(plant, valueStr1, dateWeekNow, "W");
                        valueReal1Pre = customerSqlOperater.GetValue(plant, valueStr1, dateWeekPre, "W");
                        //switch (dtWeek.Rows.Count)
                        //{ 
                        //    case 1:
                        //        dateWeekNow = dtWeek.Rows[0][1].ToString();
                        //        valueReal1Now = Convert.ToDouble(dtWeek.Rows[0][0]);
                        //        valueReal1Pre = 0;
                        //        break;
                        //    case 2:
                        //        dateWeekNow = dtWeek.Rows[0][1].ToString();
                        //        valueReal1Now = Convert.ToDouble(dtWeek.Rows[0][0]);
                        //        valueReal1Pre = Convert.ToDouble(dtWeek.Rows[1][0]);
                        //        break;
                        //    default:
                        //        dateWeekNow = WorkDateConvert.GetWorkDate("WEEKLY", 0);
                        //        valueReal1Now = 0;
                        //        valueReal1Pre = 0;
                        //        break;
                        //}

                        //资料里本年度最新的周和月的资料，比较是跟前一笔比较（前一笔日期不一定连贯的，有些周和月没有资料）
                        //if (!dateWeekNow.Contains(year))
                        //{
                        //    dateWeekNow = WorkDateConvert.GetWorkDate("WEEKLY", 0);
                        //    valueReal1Pre = valueReal1Now;
                        //    valueReal1Now = 0;
                        //}

                        if (valueReal1Now > valueReal1Pre)
                            trend = 1;
                        else if (valueReal1Now < valueReal1Pre)
                            trend = -1;
                        else
                            trend = 0;
                        drResult = dtResult.NewRow();
                        //if (!string.IsNullOrEmpty(dateWeekNow))
                            dateWeekNow = dateWeekNow.Substring(dateWeekNow.Length - 3, 3);
                        drResult["plant_code"] = plant_code;
                        drResult["value_str1"] = valueStr1;
                        drResult["date"] = dateWeekNow;
                        drResult["value_real1"] = valueReal1Now;
                        drResult["value_trend"] = trend;
                        drResult["goal_trend"] = GetGoalTrend(plant, valueStr1, trend);
                        dtResult.Rows.Add(drResult);
                        #endregion
                        //#region Month
                        //string dateMonthNow = WorkDateConvert.GetWorkDate("MONTHLY", -1);
                        //string dateMonthPre = WorkDateConvert.GetWorkDate("MONTHLY", -2);
                        //valueReal1Now = customerSqlOperater.GetValue(plant, valueStr1, dateMonthNow, "M");
                        //valueReal1Pre = customerSqlOperater.GetValue(plant, valueStr1, dateMonthPre, "M");
                        //if (valueReal1Now > valueReal1Pre)
                        //    trend = 1;
                        //else if (valueReal1Now < valueReal1Pre)
                        //    trend = -1;
                        //else
                        //    trend = 0;
                        //drResult = dtResult.NewRow();
                        //if (!string.IsNullOrEmpty(dateMonthNow))
                        //    dateMonthNow = dateMonthNow.Substring(dateMonthNow.Length - 3, 3);
                        //drResult["plant_code"] = plant_code;
                        //drResult["value_str1"] = valueStr1;
                        //drResult["date"] = dateMonthNow;
                        //drResult["value_real1"] = valueReal1Now;
                        //drResult["value_trend"] = trend;
                        //drResult["goal_trend"] = GetGoalTrend(plant, valueStr1, trend);
                        //dtResult.Rows.Add(drResult);
                        //#endregion 
                    }
                    //else
                    //{
                    #region Month
                    //var dateMonthNow = string.Empty;
                    //var dateMonthPre = string.Empty;
                    //var dtMonth = customerSqlOperater.GetMonthValue(plant, valueStr1);
                    string dateMonthNow = WorkDateConvert.GetWorkDate("MONTHLY", -1);
                    string dateMonthPre = WorkDateConvert.GetWorkDate("MONTHLY", -2);
                    valueReal1Now = customerSqlOperater.GetValue(plant, valueStr1, dateMonthNow, "M");
                    valueReal1Pre = customerSqlOperater.GetValue(plant, valueStr1, dateMonthPre, "M");
                    //switch (dtMonth.Rows.Count)
                    //{
                    //    case 1:
                    //        dateMonthNow = dtMonth.Rows[0][1].ToString();
                    //        valueReal1Now = Convert.ToDouble(dtMonth.Rows[0][0]);
                    //        valueReal1Pre = 0;
                    //        break;
                    //    case 2:
                    //        dateMonthNow = dtMonth.Rows[0][1].ToString();
                    //        valueReal1Now = Convert.ToDouble(dtMonth.Rows[0][0]);
                    //        valueReal1Pre = Convert.ToDouble(dtMonth.Rows[1][0]);
                    //        break;
                    //    default:
                    //        dateMonthNow = WorkDateConvert.GetWorkDate("MONTHLY", 0);
                    //        valueReal1Now = 0;
                    //        valueReal1Pre = 0;
                    //        break;
                    //}

                    //资料里本年度最新的周和月的资料，比较是跟前一笔比较（前一笔日期不一定连贯的，有些周和月没有资料）
                    //if (!dateMonthNow.Contains(year))
                    //{
                    //    dateMonthNow = WorkDateConvert.GetWorkDate("MONTHLY", 0);
                    //    valueReal1Pre = valueReal1Now;
                    //    valueReal1Now = 0;
                    //}

                    if (valueReal1Now > valueReal1Pre)
                            trend = 1;
                        else if (valueReal1Now < valueReal1Pre)
                            trend = -1;
                        else
                            trend = 0;
                        drResult = dtResult.NewRow();
                        //if (!string.IsNullOrEmpty(dateMonthNow))
                            dateMonthNow = dateMonthNow.Substring(dateMonthNow.Length - 3, 3);
                        drResult["plant_code"] = plant_code;
                        drResult["value_str1"] = valueStr1;
                        drResult["date"] = dateMonthNow;
                        drResult["value_real1"] = valueReal1Now;
                        drResult["value_trend"] = trend;
                        drResult["goal_trend"] = GetGoalTrend(plant, valueStr1,trend);
                        dtResult.Rows.Add(drResult);
                        #endregion
                    //}
                }
            }
            exeRes.Anything = dtResult;
            #endregion
            return exeRes;
        }

        public static double GetGoalTrend(string plant,string valueStr1,double trend)
        {
            double trend1 = 1;//默认越大越好
            if (valueStr1.Contains("QBR") || valueStr1.Contains("DEFOA") || valueStr1.Contains("EOLQ") || valueStr1.Contains("HSR") || valueStr1.Contains("MWDC") || (valueStr1.Contains("IFIR") && plant == "KSP4") || valueStr1.Contains("Customer OOB") || valueStr1.Contains("DOA") || valueStr1.Contains("AFR"))
            {
                trend1 = -1;//越小越好
            }
            //变好变坏 判断
            double temp = 0;
            temp = trend1 + trend;
            if (temp==2||temp==-2)
                trend = 1;
            if (temp==0)
                trend = -1;
            
            return trend;
        }
    }
}
