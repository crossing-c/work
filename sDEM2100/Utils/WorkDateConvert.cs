using System;
using System.Collections;
using System.Globalization;
using System.Data;
using Compal.MESComponent;

namespace sDEM2100.Utils
{
    class WorkDateConvert
    {
        public static string GetWorkDate(string frequency, int value)
        {
            string workDate = "";
            DateTime dtNow = DateTime.Now;
            switch (frequency)
            {
                case "DAILY":
                    dtNow = dtNow.AddDays(value);
                    workDate = dtNow.ToString("yyyyMMdd");
                    break;
                case "WEEKLY":
                    value = value - 1;
                    dtNow = dtNow.AddDays(7 * value);
                    GregorianCalendar gc = new GregorianCalendar();
                    int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    workDate = dtNow.ToString("yyyy") + "W" + w.ToString().PadLeft(2, '0');
                    break;
                case "MONTHLY":
                    dtNow = dtNow.AddMonths(1 * value);
                    workDate = dtNow.ToString("yyyyMM");
                    workDate = workDate.Insert(workDate.Length - 2, "M");
                    break;
                case "QUARTER":
                    dtNow = dtNow.AddMonths(3 * value);//上一个季度
                    int q = dtNow.Month % 3 == 0 ? dtNow.Month / 3 : (dtNow.Month / 3 + 1);
                    workDate = dtNow.ToString("yyyy") + "Q" + q;
                    break;
                case "YEARLY":
                    workDate = dtNow.ToString("yyyy");
                    break;
                default:
                    break;
            }
            return workDate;
        }

        public static bool GetDaysOfWeeks(int year, int weeks, CalendarWeekRule weekrule, out DateTime first, out DateTime last, object[] clientinfo)
        {
            ExecutionResult execRes = new ExecutionResult();
            InfoLightDBTools mDBTools = new InfoLightDBTools(clientinfo, "DC");
            first = DateTime.MinValue; last = DateTime.MinValue;
            string sqlText = @"select min(dateday) fromday,max(dateday) today
                              from(SELECT to_char(sysdate - ROWNUM + 7, 'yyyy/mm/dd') dateday,
                                           to_char(sysdate - ROWNUM + 7, 'iyyyiw') AS dateweek
                                      FROM dual
                                    CONNECT BY ROWNUM <= 360)
                             where dateweek = :dateweek";
            Hashtable dbParam = new Hashtable();
            dbParam.Clear();
            dbParam.Add("dateweek", year.ToString() + weeks.ToString().PadLeft(2, '0'));
            execRes = mDBTools.ExecuteQueryDSHt(sqlText, dbParam);
            if (execRes.Status)
            {
                DataSet dsRes = (DataSet)execRes.Anything;
                if (dsRes != null && dsRes.Tables.Count > 0 && dsRes.Tables[0].Rows.Count > 0)
                {
                    if (dsRes.Tables[0].Rows[0][0].ToString()!="")
                    {
                        first = Convert.ToDateTime(dsRes.Tables[0].Rows[0][0].ToString());
                        last = Convert.ToDateTime(dsRes.Tables[0].Rows[0][1].ToString());
                        execRes.Status = true;
                    }
                    else
                        execRes.Status = false;
                }
                else
                    execRes.Status = false;
            }
            return execRes.Status;
        }

        public static DataTable GetRangeWeeks(CalendarWeekRule weekrule, int iRange, object[] clientinfo)
        {
            ExecutionResult execRes = new ExecutionResult();
            InfoLightDBTools mDBTools = new InfoLightDBTools(clientinfo, "DC");
            DataTable dt = new DataTable();
            string sqlText = @"select min(dateday) fromday,max(dateday) today,dateweek weeks 
                              from(SELECT to_char(sysdate - ROWNUM + 7, 'yyyy/mm/dd') dateday,
                                           to_char(sysdate - ROWNUM + 7, 'iyyyiw') AS dateweek
                                      FROM dual
                                    CONNECT BY ROWNUM <= 120)
                             where dateweek between  to_char(sysdate-(7*:irange), 'iyyyiw') and to_char(sysdate , 'iyyyiw') 
                             group by dateweek order by dateweek";
            Hashtable dbParam = new Hashtable();
            dbParam.Clear();
            dbParam.Add("irange", iRange);
            execRes = mDBTools.ExecuteQueryDSHt(sqlText, dbParam);
            if (execRes.Status)
            {
                DataSet dsRes = (DataSet)execRes.Anything;
                if (dsRes != null && dsRes.Tables.Count > 0 && dsRes.Tables[0].Rows.Count > 0)
                    dt = dsRes.Tables[0];
            }

            return dt;
        }


        public static DataTable GetRangeWeeks(CalendarWeekRule weekrule, string vWeek1, string vWeek2, object[] clientinfo)
        {
            ExecutionResult execRes = new ExecutionResult();
            InfoLightDBTools mDBTools = new InfoLightDBTools(clientinfo, "DC");
            DataTable dt = new DataTable();
            string sqlText = @"select min(dateday) fromday,max(dateday) today 
                              from(SELECT to_char(sysdate - ROWNUM + 7, 'yyyymmdd') dateday,
                                           to_char(sysdate - ROWNUM + 7, 'iyyyiw') AS dateweek
                                      FROM dual
                                    CONNECT BY ROWNUM <= 120)
                             where dateweek between :w1 and :w2  ";
            Hashtable dbParam = new Hashtable();
            dbParam.Clear();
            dbParam.Add("w1", vWeek1);
            dbParam.Add("w2", vWeek2);
            execRes = mDBTools.ExecuteQueryDSHt(sqlText, dbParam);
            if (execRes.Status)
            {
                DataSet dsRes = (DataSet)execRes.Anything;
                if (dsRes != null && dsRes.Tables.Count > 0 && dsRes.Tables[0].Rows.Count > 0)
                    dt = dsRes.Tables[0];
            }

            return dt;
        }

        //       public static bool GetDaysOfWeeks(int year, int weeks, CalendarWeekRule weekrule, out DateTime first, out DateTime last)
        //       {
        //           ExecutionResult execRes = new ExecutionResult();
        //           object[] clientinfo;
        //           clientinfo = GetClientInfo("SMT");
        //           InfoLightDBTools mDBTools = new InfoLightDBTools(clientinfo, "DC");
        //           first = DateTime.MinValue; last = DateTime.MinValue;
        //           string sqlText = @"select min(dateday) fromday,max(dateday) today
        // from(SELECT to_char(sysdate - ROWNUM + 7, 'yyyy/mm/dd') dateday,
        //              to_char(sysdate - ROWNUM + 7, 'iyyyiw') AS dateweek
        //         FROM dual
        //       CONNECT BY ROWNUM <= 90)
        //where dateweek = :dateweek";
        //           Hashtable dbParam = new Hashtable();
        //           dbParam.Clear();
        //           dbParam.Add("dateweek", year.ToString() + weeks.ToString().PadLeft(2, '0'));
        //           execRes = mDBTools.ExecuteQueryDSHt(sqlText, dbParam);
        //           if (execRes.Status)
        //           {
        //               DataSet dsRes = (DataSet)execRes.Anything;
        //               if (dsRes != null && dsRes.Tables.Count > 0 && dsRes.Tables[0].Rows.Count > 0)
        //               {
        //                   first = Convert.ToDateTime(dsRes.Tables[0].Rows[0][0].ToString());
        //                   last = Convert.ToDateTime(dsRes.Tables[0].Rows[0][1].ToString());
        //                   execRes.Status = true;
        //               }
        //               else
        //                   execRes.Status = false;
        //           }
        //           return execRes.Status;
        //       } 

        public static bool GetDaysOfWeeks2(int year, int weeks, CalendarWeekRule weekrule, out DateTime first, out DateTime last)
        {
            //初始化 out 参数   
            first = DateTime.MinValue;
            last = DateTime.MinValue;

            if (year < 1 | year > 9999)
                return false;

            //一年最多53周地球人都知道...   
            if (weeks < 1 | weeks > 53)
                return false;

            //取当年首日为基准...为什么？容易得呗...   
            DateTime firstCurr = new DateTime(year, 1, 1);
            //取下一年首日用于计算...   
            DateTime firstNext = new DateTime(year + 1, 1, 1);

            //将当年首日星期几转换为数字...星期日特别处理...ISO 8601 标准...   
            int dayOfWeekFirst = (int)firstCurr.DayOfWeek;
            if (dayOfWeekFirst == 0) dayOfWeekFirst = 7;

            //得到未经验证的周首日...   
            first = firstCurr.AddDays((weeks - 1) * 7 - dayOfWeekFirst + 1);

            //周首日是上一年日期的情况...   
            if (first.Year < year)
            {
                switch (weekrule)
                {
                    case CalendarWeekRule.FirstDay:
                        first = firstCurr;
                        break;
                    case CalendarWeekRule.FirstFullWeek:
                        first = first.AddDays(7);
                        break;
                    case CalendarWeekRule.FirstFourDayWeek:
                        if (firstCurr.Subtract(first).Days > 3)
                        {
                            first = first.AddDays(7);
                        }
                        break;
                    default:
                        break;
                }
            }
            //得到未经验证的周末日...   
            last = first.AddDays(7).AddSeconds(-1);
            switch (weekrule)
            {
                case CalendarWeekRule.FirstDay:
                    //周末日是下一年日期的情况...    
                    if (last.Year > year)
                        last = firstNext.AddSeconds(-1);
                    else if (last.DayOfWeek != DayOfWeek.Monday)
                        last = first.AddDays(7 - (int)first.DayOfWeek).AddSeconds(-1);
                    break;
                case CalendarWeekRule.FirstFullWeek:
                    first = first.AddDays(7);
                    last = last.AddDays(7);
                    break;
                case CalendarWeekRule.FirstFourDayWeek:
                    //周末日距下一年首日不足4天则提前一周...    
                    if (last.Year > year && firstNext.Subtract(first).Days < 4)
                    {
                        first = first.AddDays(-7);
                        last = last.AddDays(-7);
                    }
                    break;
                default:
                    break;
            }
            return true;
        }
    }
}
