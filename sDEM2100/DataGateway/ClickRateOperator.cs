using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class ClickRateOperator
    {
        private InfoLightDBTools mDBTools;
        private InfoLightMSTools mDBmsTools;
        public ClickRateOperator(object[] clientInfo, string dbName)
        {
            mDBTools = new InfoLightDBTools(clientInfo, dbName);
            mDBmsTools = new InfoLightMSTools(clientInfo, "WARROOM");
        }
        public string GetGoal(string Index)
        {
            Hashtable ht = new Hashtable();
            string Goal = "";
            string sql = string.Empty;
            sql = "SELECT t.goal FROM DQMS.C_KPI_DEF_T t where kpi_name=:kpi_name";
            ht.Clear();
            ht.Add("kpi_name", Index);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if(result.Status)
            {
                DataTable dt = ((DataSet)result.Anything).Tables[0];
                if(dt.Rows.Count>0)
                {
                    Goal = dt.Rows[0][0].ToString();
                }
            }
            return Goal;
        }
        
        public DataTable GetClickRateByDeptWeekly(string func_id, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"  
select * from (
select *
  from (SELECT TOP 5 value_str2, sum(value_int1) qty
          FROM V_wr_q_generic2
         WHERE kpi_category = 'Q0011'
           and plant_code = @plant
           and value_str1 = N'" + func_id + @"'
           and data_week between @fromTime and @endTime
         group by value_str2
         order by qty desc) t1
union
select 'other' value_str1, SUM(value_int1) qty
  FROM V_wr_q_generic2
 WHERE kpi_category = 'Q0011'
   and plant_code = @plant
   and value_str1 = N'" + func_id + @"'
   and data_week between @fromTime and @endTime
   and value_str2 not in
       (select t.value_str2
          from (SELECT TOP 5 value_str2, sum(value_int1) qty
                  FROM V_wr_q_generic2
                 WHERE kpi_category = 'Q0011'
                   and plant_code = @plant
                   and value_str1 = N'" + func_id + @"'
                   and data_week between @fromTime and @endTime
                 group by value_str2
                 order by qty desc) t))t0 where t0.qty is not null";
            ht.Clear();
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

        public DataTable GetClickRateByDeptDaily(string func_id, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"
select * from (
select *
  from (SELECT TOP 5 value_str2, sum(value_int1) qty
          FROM V_wr_q_generic2
         WHERE kpi_category = 'Q0011'
           and plant_code = @plant
           and value_str1 =  N'" + func_id + @"'
           and data_day between @fromTime and @endTime
         group by value_str2
         order by qty desc) t1
union
select 'other' value_str1, SUM(value_int1) qty
  FROM V_wr_q_generic2
 WHERE kpi_category = 'Q0011'
   and plant_code = @plant
   and value_str1 =  N'" + func_id + @"'
   and data_day between @fromTime and @endTime
   and value_str2 not in
       (select t.value_str2
          from (SELECT TOP 5 value_str2, sum(value_int1) qty
                  FROM V_wr_q_generic2
                 WHERE kpi_category = 'Q0011'
                   and plant_code = @plant
                   and value_str1 =  N'" + func_id + @"'
                   and data_day between @fromTime and @endTime
                 group by value_str2
                 order by qty desc) t))t0 where t0.qty is not null";
            ht.Clear();
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

        public DataTable GetClickRateByDateWeekly(string func_id, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"  SELECT data_week work_date,value_str1,
       sum(value_int1) as qty
 FROM  V_wr_q_generic2
WHERE kpi_category = 'Q0011'
  and plant_code = @plant
  and value_str1 in (" + func_id+ @")
  and data_week between @fromTime and @endTime
group by data_week,value_str1
order by data_week  ";
            ht.Clear();
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

        public DataTable GetClickRateByDateDaily(string func_id, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"  SELECT data_day work_date,value_str1, 
       sum(value_int1) as qty
 FROM  V_wr_q_generic2
WHERE kpi_category = 'Q0011'
  and plant_code = @plant
  and value_str1 in (" + func_id + @")
  and data_day between @fromTime and @endTime
group by data_day,value_str1
order by data_day  ";
            ht.Clear();
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

        public DataTable GetClickRateByPlantWeekly(string func_id,string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"  SELECT data_week work_date, plant_code,value_str1,
       sum(value_int1) as qty
 FROM  V_wr_q_generic2
WHERE kpi_category = 'Q0011'
  and value_str1 in (" + func_id + @")
  and data_week between @fromTime and @endTime
group by data_week, plant_code,value_str1
order by data_week ";
            ht.Clear();
            ht.Add("func_id", func_id);
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

        public DataTable GetClickRateByPlantDaily(string func_id,string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"  SELECT data_day work_date, plant_code,value_str1,
       sum(value_int1) as qty
 FROM  V_wr_q_generic2
WHERE kpi_category = 'Q0011'
  and value_str1 in (" + func_id + @")
  and data_day between @fromTime and @endTime
group by data_day, plant_code,value_str1
order by data_day  ";
            ht.Clear();
            ht.Add("func_id", func_id);
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

    }
}
