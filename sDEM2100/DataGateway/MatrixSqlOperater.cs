using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class MatrixSqlOperater
    {
        private object[] mClientInfo;
        private string mDBName;
        private InfoLightDBTools mDBTools;
        private InfoLightMSTools mDBmsTools;
        public MatrixSqlOperater(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDBName = dbName;
            this.mDBTools = new InfoLightDBTools(clientInfo, dbName);
            this.mDBmsTools = new InfoLightMSTools(clientInfo, "WARROOM");
        }
        //
        public DataTable GetMatrixIndexData(string plantCode)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select t.kpi_id,
                                   t.actual,
                                   case
                                     when t.actual - t.last_actual > 0 then
                                      1
                                     when t.actual - t.last_actual < 0 then
                                      -1
                                     else
                                      0
                                   end value_trend,
                                   t.trend goal_trend
                              from dqms.r_kpi_t t
                             where t.plant_code =:plantcode
                             order by t.kpi_id ";
            ht.Clear();
            ht.Add("plantcode", plantCode);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetSvlrrPlantAnalysis(string str1, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT o.plant_code,
                                   SUBSTRING(o.data_week,5,len(o.data_week)-4) date,
                                   case sum(o.value_int2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      Convert(decimal(18,1),Convert(float,sum(o.value_int1))/Convert(float,sum(o.value_int2))*1000000)
                                   end value
                              FROM V_wr_q_generic o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'SVLRR'
                               AND o.value_str1 = @str1
                               and m.target_plant_code in
                                   ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','KSP1-AEP','KSP1-SVR','VNP1')
                               and o.data_week between @fromTime and @endTime
                             group by o.plant_code, o.data_week
                             order by o.plant_code, o.data_week ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetFrrPlantAnalysis(string str1, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT o.plant_code,
                                   SUBSTRING(o.data_week,5,len(o.data_week)-4) date,
                                   case sum(o.value_int2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      Convert(decimal(18,1),Convert(float,sum(o.value_int1))/Convert(float,sum(o.value_int2))*1000000)
                                   end value
                              FROM V_wr_q_generic o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'FRR'
                               AND o.value_str1 = @str1
                               and m.target_plant_code in
                                   ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','KSP1-AEP','KSP1-SVR','VNP1')
                               and o.data_week between @fromTime and @endTime
                             group by o.plant_code, o.data_week
                             order by o.plant_code, o.data_week ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetSvlrrWeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT max(o.plant_code) plant_code,
                                   SUBSTRING(o.data_week,5,len(o.data_week)-4) date,
                                   case sum(o.value_int2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      Convert(decimal(18,1),Convert(float,sum(o.value_int1))/Convert(float,sum(o.value_int2))*1000000)
                                   end value
                              FROM V_wr_q_generic o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'SVLRR'
                               AND o.value_str1 =@str1
                               and m.target_plant_code =@plant
                               and o.data_week between @fromTime and @endTime
                             group by  o.data_week
                             order by  o.data_week ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetFrrWeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT max(o.plant_code) plant_code,
                                   SUBSTRING(o.data_week,5,len(o.data_week)-4) date,
                                   case sum(o.value_int2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      Convert(decimal(18,1),Convert(float,sum(o.value_int1))/Convert(float,sum(o.value_int2))*1000000)
                                   end value
                              FROM V_wr_q_generic o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'FRR'
                               AND o.value_str1 =@str1
                               and m.target_plant_code =@plant
                               and o.data_week between @fromTime and @endTime
                             group by  o.data_week
                             order by  o.data_week ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetSvlrrMonthlyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT max(o.plant_code) plant_code,
                                   SUBSTRING(o.data_month,5,len(o.data_month)-4) date,
                                   case sum(o.value_int2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      Convert(decimal(18,1),Convert(float,sum(o.value_int1))/Convert(float,sum(o.value_int2))*1000000)
                                   end value
                              FROM V_wr_q_generic o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'SVLRR'
                               AND o.value_str1 =@str1
                               and m.target_plant_code =@plant
                               and o.data_month between @fromTime and @endTime
                             group by  o.data_month
                             order by  o.data_month ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetFrrMonthlyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT max(o.plant_code) plant_code,
                                   SUBSTRING(o.data_month,5,len(o.data_month)-4) date,
                                   case sum(o.value_int2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      Convert(decimal(18,1),Convert(float,sum(o.value_int1))/Convert(float,sum(o.value_int2))*1000000)
                                   end value
                              FROM V_wr_q_generic o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'FRR'
                               AND o.value_str1 =@str1
                               and m.target_plant_code =@plant
                               and o.data_month between @fromTime and @endTime
                             group by  o.data_month
                             order by  o.data_month ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetOPWeekly(string plant, string data)
        {
            var exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            var ht = new Hashtable();
            var dtResult = new DataTable();
            var sql = @" select max(target_plant_code) plant_code,max(data_week) date,count (value_str1) value
from V_wr_q_generic t,V_wr_plant_mapping m WHERE t.plant_code = m.source_plant_code and 
t.kpi_category='Q0004' 
and t.date_type  ='w'
and m.target_plant_code = @plant 
and t.data_week = @data ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("data", data);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    var ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetOPMonthly(string plant, string data)
        {
            var exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            var ht = new Hashtable();
            var dtResult = new DataTable();
            var sql = @" select max(target_plant_code) plant_code,max(data_month) date,count (value_str1) value
from V_wr_q_generic t,V_wr_plant_mapping m WHERE t.plant_code = m.source_plant_code and 
t.kpi_category='Q0004' 
and t.date_type  ='m'
and m.target_plant_code =@plant 
and t.data_month =@data ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("data", data);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    var ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetOPQuarterly(string plant, string data)
        {
            var exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            var ht = new Hashtable();
            var dtResult = new DataTable();
            var sql = @" select max(target_plant_code) plant_code,
       max(data_quarter) date,
       count(value_str1) value
  from V_wr_q_generic t, V_wr_plant_mapping m
 WHERE t.plant_code = m.source_plant_code
   and t.kpi_category = 'Q0004'
   and t.date_type = 'q'
   and m.target_plant_code = @plant
   and t.data_quarter = @data ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("data", data);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    var ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        #region RejectRate
        public DataTable GetRejectRate(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select MAX(o.PLANT_CODE) PLANT_CODE,
       SUBSTRING(o.data_week, 5, len(o.data_week) - 4) date,
       case sum(o.value_int1)
         when 0 then
          0
         when null then
          0
         else
          Convert(decimal(18, 2),
                  SUM(value_int1) * 1.0 / SUM(value_int2) * 1000)
       end value
  from V_wr_q_generic2 o, V_wr_plant_mapping m
 WHERE o.plant_code = m.source_plant_code
   and o.kpi_category = 'Q0006'
   and o.value_str7 = @str1
   and m.target_plant_code = @plant
   and o.data_week between @fromTime and @endTime
 GROUP BY o.data_week
 order by o.data_week ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetRjtRateCrossPlant(string str1, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select o.PLANT_CODE,
       SUBSTRING(o.data_week, 5, len(o.data_week) - 4) date,
       case sum(o.value_int1)
         when 0 then
          0
         when null then
          0
         else
          Convert(decimal(18, 2),
                  SUM(value_int1) * 1.0 / SUM(value_int2) * 1000)
       end value
  from V_wr_q_generic2 o, V_wr_plant_mapping m
 WHERE o.plant_code = m.source_plant_code
   and o.kpi_category = 'Q0006'
   and o.value_str7 = @str1
   and o.data_week between @fromTime and @endTime
   and m.target_plant_code in   ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','KSP1-AEP','KSP1-SVR','VNP1')
 GROUP BY o.data_week,o.PLANT_CODE
 order by o.data_week,o.PLANT_CODE ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetRjtRateWeekly(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select MAX(o.PLANT_CODE) PLANT_CODE,
       SUBSTRING(o.data_week, 5, len(o.data_week) - 4) date,
       case sum(o.value_int1)
         when 0 then
          0
         when null then
          0
         else
          Convert(decimal(18, 2),
                  SUM(value_int1) * 1.0 / SUM(value_int2) * 1000)
       end value
  from V_wr_q_generic2 o, V_wr_plant_mapping m
 WHERE o.plant_code = m.source_plant_code
   and o.kpi_category = 'Q0006'
   and o.value_str7 = @str1
   and o.data_week between @fromTime and @endTime
   and m.target_plant_code = @plant
 GROUP BY o.data_week
 order by o.data_week ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);

            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetRjtRateDaily(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select MAX(o.PLANT_CODE) PLANT_CODE,
       o.data_day date,
       case sum(o.value_int1)
         when 0 then
          0
         when null then
          0
         else
          Convert(decimal(18, 2),
                  SUM(value_int1) * 1.0 / SUM(value_int2) * 1000)
       end value
  from V_wr_q_generic2 o, V_wr_plant_mapping m
 WHERE o.plant_code = m.source_plant_code
   and o.kpi_category = 'Q0006'
   and o.value_str7 = @str1
   and o.data_day between @fromTime and @endTime
   and m.target_plant_code = @plant
 GROUP BY o.data_day
 order by o.data_day ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);

            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetRjtRateMonthly(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select MAX(o.PLANT_CODE) PLANT_CODE,
       SUBSTRING(o.data_month, 5, len(o.data_month) - 4) date,
       case sum(o.value_int1)
         when 0 then
          0
         when null then
          0
         else
          Convert(decimal(18, 2),
                  SUM(value_int1) * 1.0 / SUM(value_int2) * 1000)
       end value
  from V_wr_q_generic2 o, V_wr_plant_mapping m
 WHERE o.plant_code = m.source_plant_code
   and o.kpi_category = 'Q0006'
   and o.value_str7 = @str1
   and o.data_month between @fromTime and @endTime
   and m.target_plant_code = @plant
 GROUP BY o.data_month
 order by o.data_month ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);

            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetRjtRateLine(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select value_str2 plant_code,
       SUBSTRING(o.data_week, 5, len(o.data_week) - 4) date,
       case sum(o.value_int1)
         when 0 then
          0
         when null then
          0
         else
          Convert(decimal(18, 2),
                  SUM(value_int1) * 1.0 / SUM(value_int2) * 1000)
       end value
  from V_wr_q_generic2 o, V_wr_plant_mapping m
 WHERE o.plant_code = m.source_plant_code
   and o.kpi_category = 'Q0006'
   and o.value_str7 = @str1
   and o.data_week between @fromTime and @endTime
   and m.target_plant_code = @plant
 GROUP BY o.data_week,value_str2
 order by o.data_week,value_str2 ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);

            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetRjtRateLineD(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select value_str2 plant_code,
       o.data_day date,
       case sum(o.value_int1)
         when 0 then
          0
         when null then
          0
         else
          Convert(decimal(18, 2),
                  SUM(value_int1) * 1.0 / SUM(value_int2) * 1000)
       end value
  from V_wr_q_generic2 o, V_wr_plant_mapping m
 WHERE o.plant_code = m.source_plant_code
   and o.kpi_category = 'Q0006'
   and o.value_str7 = @str1
   and o.data_day between @fromTime and @endTime
   and m.target_plant_code = @plant
 GROUP BY o.data_day,value_str2
 order by o.data_day,value_str2 ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);

            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetRjtRateLineM(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select value_str2 plant_code,
       SUBSTRING(o.data_month, 5, len(o.data_month) - 4) date,
       case sum(o.value_int1)
         when 0 then
          0
         when null then
          0
         else
          Convert(decimal(18, 2),
                  SUM(value_int1) * 1.0 / SUM(value_int2) * 1000)
       end value
  from V_wr_q_generic2 o, V_wr_plant_mapping m
 WHERE o.plant_code = m.source_plant_code
   and o.kpi_category = 'Q0006'
   and o.value_str7 = @str1
   and o.data_month between @fromTime and @endTime
   and m.target_plant_code = @plant
 GROUP BY o.data_month,value_str2
 order by o.data_month,value_str2 ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);

            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }
        #endregion




    }
}
