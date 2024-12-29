using Compal.MESComponent;
using sDEM2100.Beans;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class DqmsSqlOperater
    {
        private object[] mClientInfo;
        private string mDBName;
        private InfoLightDBTools mDBTools;
        private InfoLightDBTools mDCDBTools;
        private InfoLightMSTools mDBmsTools;
        private MESLog mesLog;
        public DqmsSqlOperater(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDBName = dbName;
            this.mDBTools = new InfoLightDBTools(clientInfo, dbName);
            this.mDCDBTools = new InfoLightDBTools(clientInfo, "DC");
            this.mDBmsTools = new InfoLightMSTools(clientInfo, "WARROOM");
            mesLog = new MESLog(this.GetType().Name);
        }
        //
        public bool CheckDqmsKpi(int kpiId,string buCode,string plantCode)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            bool flag = false;
            string sql = @" select kpi_id
                                from dqms.r_kpi_t
                                where kpi_id = :kpiid
                                and bu_code = :bucode
                                and plant_code = :plantcode ";
            ht.Clear();
            ht.Add("kpiid", kpiId);
            ht.Add("bucode", buCode);
            ht.Add("plantcode", plantCode);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0)
                    {
                        DataTable dt = ds.Tables[0];
                        if (dt != null && dt.Rows.Count > 0)
                        {
                            flag = true;
                        }
                    }
                }
                else
                {
                    flag = true;
                }
            }
            catch(Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                flag = true;
            }
            return flag;
        }

        public DataTable GetDqmsKpi(int kpiId, string buCode, string plantCode)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select work_date,actual,score,last_actual,last_score,trend
                                from dqms.r_kpi_t
                                where kpi_id = :kpiid
                                and bu_code = :bucode
                                and plant_code = :plantcode ";
            ht.Clear();
            ht.Add("kpiid", kpiId);
            ht.Add("bucode", buCode);
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

        public ExecutionResult InsertDqmsKpi(DqmsBean dqms)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            string sql = @" insert into dqms.r_kpi_t
                                (kpi_id,
                                    bu_code,
                                    plant_code,
                                    work_date,
                                    actual,
                                    score,
                                    last_actual,
                                    last_score,
                                    trend,
                                    create_user,
                                    create_time)
                                values
                                (:kpiid,
                                    :bucode,
                                    :plantcode,
                                    :workdate,
                                    :actual,
                                    :score,
                                    :lastactual,
                                    :lastscore,
                                    :trend,
                                    'DEM2100',
                                    sysdate) ";
            ht.Clear();
            ht.Add("kpiid", dqms.KpiId);
            ht.Add("bucode", dqms.BuCode);
            ht.Add("plantcode", dqms.PlantCode);
            ht.Add("workdate", dqms.WorkDate);
            ht.Add("actual", dqms.Actual);
            ht.Add("score", dqms.Score);
            ht.Add("lastactual", dqms.LastActual);
            ht.Add("lastscore", dqms.LastScore);
            ht.Add("trend", dqms.Trend);
            exeRes = mDBTools.ExecuteUpdateHt(sql, ht);
            if (!exeRes.Status)
                mesLog.Error("DEM2100.InsertDqmsKpi error:"+"plant="+dqms.PlantCode+ ",kpiid="+dqms.KpiId+"," + exeRes.Message);
            return exeRes;
        }

        public ExecutionResult UpdateDqmsKpi(DqmsBean dqms)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            string sql = @" update dqms.r_kpi_t t
                              set t.work_date   = :workdate,
                                  t.actual      = :actual,
                                  t.score       = :score,
                                  t.last_actual = :lastactual,
                                  t.last_score  = :lastscore,
                                  t.trend       = :trend,
                                  t.update_user = 'DEM2100',
                                  t.update_time = sysdate
                            where t.kpi_id = :kpiid
                              and t.bu_code = :bucode
                              and t.plant_code = :plantcode ";
            ht.Clear();
            ht.Add("kpiid", dqms.KpiId);
            ht.Add("bucode", dqms.BuCode);
            ht.Add("plantcode", dqms.PlantCode);
            ht.Add("workdate", dqms.WorkDate);
            ht.Add("actual", dqms.Actual);
            ht.Add("score", dqms.Score);
            ht.Add("lastactual", dqms.LastActual);
            ht.Add("lastscore", dqms.LastScore);
            ht.Add("trend", dqms.Trend);
            exeRes = mDBTools.ExecuteUpdateHt(sql, ht);
            return exeRes;
        }

        //
        public DataTable GetKpiIdByFrequency(string datetype)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select t.kpi_id from dqms.c_kpi_def_t t where t.frequency =:datetype ";
            ht.Clear();
            ht.Add("datetype", datetype);
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

        public bool CheckDqmsKpiLog(int kpiId, string buCode, string plantCode,string workDate)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            bool flag = false;
            string sql = @" select t.kpi_id
                              from dqms.r_kpi_log_t t
                             where t.kpi_id = :kpiid
                               and t.bu_code = :bucode
                               and t.plant_code = :plantcode
                               and t.work_date = :workdate ";
            ht.Clear();
            ht.Add("kpiid", kpiId);
            ht.Add("bucode", buCode);
            ht.Add("plantcode", plantCode);
            ht.Add("workdate", workDate);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0)
                    {
                        DataTable dt = ds.Tables[0];
                        if (dt != null && dt.Rows.Count > 0)
                        {
                            flag = true;
                        }
                    }
                }
                else
                {
                    flag = true;
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                flag = true;
            }
            return flag;
        }

        public ExecutionResult InsertDqmsKpiLog(DqmsBean dqms)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            string sql = @" insert into dqms.r_kpi_log_t
                            (kpi_id,
                             bu_code,
                             plant_code,
                             work_date,
                             actual,
                             score,
                             trend,
                             create_user,
                             create_time)
                          values
                            (:kpiid,
                             :bucode,
                             :plantcode,
                             :workdate,
                             :actual,
                             :score,
                             :trend,
                             'DEM2100',
                             sysdate) ";
            ht.Clear();
            ht.Add("kpiid", dqms.KpiId);
            ht.Add("bucode", dqms.BuCode);
            ht.Add("plantcode", dqms.PlantCode);
            ht.Add("workdate", dqms.WorkDate);
            ht.Add("actual", dqms.LastActual);
            ht.Add("score", dqms.LastScore);
            ht.Add("trend", dqms.Trend);
            exeRes = mDBTools.ExecuteUpdateHt(sql, ht);
            return exeRes;
        }

        public DataTable GetDqmsParams(int kpiId,string plantCode)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select a.kpi_id,
                                   b.bu_code,
                                   b.plant_code,
                                   a.frequency,
                                   a.kpi_name,
                                   a.upper_limit,
                                   a.lower_limit,
                                   b.weight,
                                   a.trend
                              from dqms.c_kpi_def_t a, dqms.c_plant_kpi_def_t b
                             where a.kpi_id = b.kpi_id
                               and b.plant_code =:plantcode
                               and a.kpi_id =:kpiid
                             order by a.kpi_id asc ";
            ht.Clear();
            ht.Add("kpiid", kpiId);
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

        //SqlServer
        public DataTable GetFpyApSmt(string plant, string process)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            //string sql = @" select top 1 round(1 - Convert(float, sum(q.firstfail_qty + q.refail_qty)) /
            //                 sum(q.station_qty),
            //                 4) * 100,q.data_week
            //      from V_wr_q_plrr q, V_wr_plant_mapping m
            //     where q.plant_code = m.source_plant_code
            //       and m.target_plant_code =@plant
            //       and q.process =@process
            //       group by q.data_week
            //       order by q.data_week desc ";
            string sql = @" select top 1 round(1 - Convert(float, sum(q.firstfail_qty + q.refail_qty)) /
                            cast(sum(q.station_qty) as float) - sum(q.gap_qty)/ cast(sum(q.station_qty) as float),
                             4) * 100,q.data_week
                  from V_wr_q_plrr q, V_wr_plant_mapping m
                 where q.plant_code = m.source_plant_code
                   and m.target_plant_code =@plant
                   and q.process =@process
                   group by q.data_week
                   order by q.data_week desc ";
            ht.Clear();
            ht.Add("plant",plant);
            ht.Add("process",process);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0&& ds.Tables[0].Rows[0][0]!=null)
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

        public DataTable GetScrap(string plant, string str2,string str1)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select top 1 s.value_real2,s.data_month
                              from V_wr_m_generic s,V_wr_plant_mapping m
                             where s.plant_code = m.source_plant_code
                               and s.kpi_category='SCRAP'
                               and m.target_plant_code=@plant
                               and s.value_str2 =@str2
                               and s.value_str1 =@str1
                               order by s.data_month desc ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str2", str2);
            ht.Add("str1", str1);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0&&ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetOfflineRate(string plant, string str1)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select top 1 round(sum(o.value_real2) / sum(o.value_real1), 4) * 100,o.data_week
                                  from V_wr_q_generic o, V_wr_plant_mapping m
                                 where o.plant_code = m.source_plant_code
                                   and o.kpi_category='Q0001'
                                   and m.target_plant_code =@plant
                                   and o.value_str1 =@str1
                                   group by o.data_week
                                   order by o.data_week desc";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetReflowRate(string plant, string str1)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql1;
            if (str1 == "ASSY")
                sql1 = " in ('ASSY','CELL','REPAIR','TNB') ";
            else
                sql1 = "='PACK' ";
            string sql = @" select 
       case
         when b.output_qty = 0 then
          0
         when b.output_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.output_qty * 100, 2)
       end value,
       substr(a.work_date,1,4)||'W'||substr(a.work_date,5,2) work_date
  from (select t.plant_code,
               TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') work_date,
               sum(t.reflow_qty) qty
          from sfism4.r_kpi_reflow_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code = 'PCBG'
                                  and rownum = 1)
           and t.process = :str1
           and TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') =
               (SELECT MAX(TO_CHAR(TO_DATE(t.work_date || '000000',
                                           'YYYYMMDDHH24MISS'),
                                   'IYYYIW'))
                  FROM sfism4.r_kpi_reflow_t T)
         group by t.plant_code,
                  TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')
         order by t.plant_code,
                  TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')) a,
       (select t.plant_code,
               TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') work_date,
               sum(t.output_qty) output_qty
          from sfism4.r_kpi_fpy_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code = 'PCBG'
                                  and rownum = 1)
           and t.sub_process " + sql1 + @" 
           and t.process in ('FATP') 
           and TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') =
               (SELECT MAX(TO_CHAR(TO_DATE(t.work_date || '000000',
                                           'YYYYMMDDHH24MISS'),
                                   'IYYYIW'))
                  FROM sfism4.r_kpi_reflow_t T)
         group by t.plant_code,
                  TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')
         order by t.plant_code,
                  TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')) b
 where b.work_date = a.work_date(+)
   and b.WORK_DATE is not null
 ORDER BY b.WORK_DATE
";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1);
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetQBR(string plant, string str1)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select top 1 o.value_real1,o.data_month
                              from V_wr_q_generic o, V_wr_plant_mapping m
                             where m.source_plant_code=o.customer
                               and o.kpi_category='Q0003'
                               and m.target_plant_code =@plant
                               and o.value_str1=@str1
                               order by o.data_month desc ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetCTQ(string plant, string process)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select top 1 round(sum(c.qc_qty + c.mfg_qty) /
                                     sum(c.total_qc_qty + c.total_mfg_qty),
                                     4) * 100,c.data_week
                          from V_wr_q_ctq c, V_wr_plant_mapping m
                         where c.plant_code = m.source_plant_code
                           and m.target_plant_code =@plant
                           and c.process =@process
                           group by c.data_week 
                           order by c.data_week desc ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("process", process);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public string GetPreWfrFatpSmt(string plant, string str1, string dateTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            string result = "";
            string sql = @"SELECT SUM(t2.item_value) 
FROM (
SELECT  AVG(t1.item_value) AS item_value 
  FROM (
       SELECT data_day 
            , data_week 
            , data_month 
            , plant_code
            , value_str3 AS item_title
            , SUM(value_int1) AS item_value
         FROM V_wr_q_generic w WITH (NOLOCK) , V_wr_plant_mapping m
        WHERE w.plant_code = m.source_plant_code 
		  AND w.kpi_category='WFR'
          AND w.value_str2='DAILY'
          and m.target_plant_code =@plant
           and w.data_week = @datetime
		      /*(select distinct data_week from V_wr_q_generic d , V_wr_plant_mapping e
       			WHERE d.plant_code = e.source_plant_code 
				  AND d.kpi_category='WFR'
         		  AND d.value_str2='DAILY'
				  and e.target_plant_code =@plant
				  and d.data_day = @datetime
				  AND d.value_str1 =@str1
         		  AND d.value_str3 = 'TOTAL')*/
          AND w.value_str1 =@str1
          AND w.value_str3 = 'TOTAL' -- category != DISTRIBUTION
        GROUP BY w.data_day, w.data_week, w.data_month, w.plant_code, w.value_str3
     ) t1  
        GROUP BY t1.plant_code 
	) t2";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1);
            ht.Add("datetime", dateTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
                    {
                        result = ds.Tables[0].Rows[0][0].ToString();
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return result;
        }

        public double GetNowWfrFatpSmt(string plant, string str1, string dateTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            double res = 0;
            string sql = @"SELECT SUM(t2.item_value) 
FROM (
SELECT  AVG(t1.item_value) AS item_value 
  FROM (
       SELECT data_day 
            , data_week 
            , data_month 
            , plant_code
            , value_str3 AS item_title
            , SUM(value_int1) AS item_value
         FROM V_wr_q_generic w WITH (NOLOCK) , V_wr_plant_mapping m
        WHERE w.plant_code = m.source_plant_code 
		  AND w.kpi_category='WFR'
          AND w.value_str2='DAILY'
          and m.target_plant_code =@plant
          and w.data_week = @datetime
		      /*(select distinct data_week from V_wr_q_generic d , V_wr_plant_mapping e
       			WHERE d.plant_code = e.source_plant_code 
				  AND d.kpi_category='WFR'
         		  AND d.value_str2='DAILY'
				  and e.target_plant_code =@plant
				  and d.data_day = @datetime
				  AND d.value_str1 =@str1
         		  AND d.value_str3 = 'TOTAL')*/
          AND w.value_str1 =@str1
          AND w.value_str3 = 'TOTAL' -- category != DISTRIBUTION
        GROUP BY w.data_day, w.data_week, w.data_month, w.plant_code, w.value_str3
     ) t1 
	GROUP BY t1.plant_code 
	) t2 ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1);
            ht.Add("datetime", dateTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
                    {
                        string result = ds.Tables[0].Rows[0][0].ToString();
                        double.TryParse(result, out res);
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return res;
        }

        public DataTable GetPidLcd(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select top 1 sum(l.pid_qty)/sum(l.input_qty)*1000000,l.data_week
                                  from V_wr_q_lcd l, V_wr_plant_mapping m
                                 where l.plant_code = m.source_plant_code
                                   and m.target_plant_code =@plant
                                   group by l.data_week
                                   order by l.data_week desc ";
            ht.Clear();
            ht.Add("plant", plant);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetPidKps(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select top 1 sum(k.qty_1)/sum(k.qty_2)*1000000,k.data_week
                                  from V_wr_q_kps k, V_wr_plant_mapping m
                                 where k.plant_code = m.source_plant_code
                                   and m.target_plant_code =@plant
                                   group by k.data_week
                                   order by k.data_week desc ";
            ht.Clear();
            ht.Add("plant", plant);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetPidMe(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select top 1 sum(me.qty_1)/sum(me.qty_2)*1000000,me.data_week
                                  from V_wr_q_me me, V_wr_plant_mapping m
                                 where me.plant_code = m.source_plant_code
                                   and m.target_plant_code =@plant
                                   group by me.data_week
                                   order by me.data_week desc ";
            ht.Clear();
            ht.Add("plant", plant);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetLrrApSmt(string plant, string str1)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            //string sql = @" select case b.vinput
            //                     when 0 then
            //                      0
            //                     when null then
            //                      0
            //                     else
            //                      round(cast(a.v as float) / b.vinput * 100, 2)
            //                   end value_real1,
            //                   a.data_week
            //              from (SELECT sum(q.value_real1) v, q.data_week
            //                      FROM V_wr_q_generic q, V_wr_plant_mapping m
            //                     WHERE q.kpi_category = 'Q0009'
            //                       and m.source_plant_code = q.plant_code
            //                       and q.value_str3 != 'INPUT'
            //                       and m.target_plant_code = @plant
            //                       and q.value_str1 = @str1
            //                       and q.data_week = (select top 1 q.data_week
            //                                            FROM V_wr_q_generic q, V_wr_plant_mapping m
            //                                           WHERE q.kpi_category = 'Q0009'
            //                                             and m.source_plant_code = q.plant_code
            //                                             and q.value_str3 = 'INPUT'
            //                                             and m.target_plant_code = @plant
            //                                             and q.value_str1 = @str1
            //                                           order by q.data_week desc)
            //                     group by q.data_week) a,
            //                   (SELECT sum(q.value_real1) vinput
            //                      FROM V_wr_q_generic q, V_wr_plant_mapping m
            //                     WHERE q.kpi_category = 'Q0009'
            //                       and m.source_plant_code = q.plant_code
            //                       and q.value_str3 = 'INPUT'
            //                       and m.target_plant_code = @plant
            //                       and q.value_str1 = @str1
            //                       and q.data_week = (select top 1 q.data_week
            //                                            FROM V_wr_q_generic q, V_wr_plant_mapping m
            //                                           WHERE q.kpi_category = 'Q0009'
            //                                             and m.source_plant_code = q.plant_code
            //                                             and q.value_str3 = 'INPUT'
            //                                             and m.target_plant_code = @plant
            //                                             and q.value_str1 = @str1
            //                                           order by q.data_week desc)) b ";
            //1-FPY
            string sql = @"select top 1 round(Convert(float, sum(q.firstfail_qty + q.refail_qty)) /
                             sum(q.station_qty),
                             4) * 100,q.data_week
                  from V_wr_q_plrr q, V_wr_plant_mapping m
                 where q.plant_code = m.source_plant_code
                   and m.target_plant_code =@plant
                   and q.process =@str1
                   group by q.data_week
                   order by q.data_week desc ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetHsrApSmt(string plant, string str1)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"select top 1       
                               case 
                                 when value is null then
                                  0
                                 else
                                  a.value
                               end value,
                               a.date
                          from (SELECT q.plant_code,
                                       q.data_week date,
                                       cast(sum(q.value_int2) as float) value
                                  FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                 WHERE kpi_category = 'Q0010'
                                   and m.source_plant_code = q.plant_code
                                      and m.target_plant_code=@plant
                                   and q.process =@str1
                                 group by q.plant_code, q.data_week) a
                                 order by a.plant_code,a.date desc ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetSvlrrEe(string plant, string kpi_category = "SVLRR", string value_str1 = "EE")
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
           
            string sql = @" select top 1  
round((Convert(float,sum(t.value_int1))/Convert(float,sum(t.value_int2))*1000000),1),t.data_week
from V_wr_q_generic t,V_wr_plant_mapping m
where t.plant_code = m.source_plant_code 
and t.value_str1=@value_str1
and t.kpi_category =@kpi_category
and m.target_plant_code=@plant 
and t.data_week is not null
group by t.data_week order by t.data_week desc ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("kpi_category", kpi_category);
            ht.Add("value_str1", value_str1);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetOpWeek(string plant,string data_week)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            string sql = @" select count (value_str1),max(data_week) 
from V_wr_q_generic t,V_wr_plant_mapping m WHERE t.plant_code = m.source_plant_code and 
t.kpi_category='Q0004' 
and t.date_type  ='w'
and m.target_plant_code =@plant
and t.data_week =@data_week ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("data_week", data_week);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetOpMonth(string plant, string data_month)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select count (value_str1),max(data_month) 
from V_wr_q_generic t,V_wr_plant_mapping m WHERE t.plant_code = m.source_plant_code and 
t.kpi_category='Q0004' 
and t.date_type  ='m'
and m.target_plant_code =@plant
and t.data_month =@data_month ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("data_month", data_month);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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

        public DataTable GetOpQuarter(string plant, string data_quarter)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select count (value_str1),max(data_quarter) 
from V_wr_q_generic t,V_wr_plant_mapping m WHERE t.plant_code = m.source_plant_code and 
t.kpi_category='Q0004' 
and t.date_type  ='q'
and m.target_plant_code =@plant
and t.data_quarter =@data_quarter ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("data_quarter", data_quarter);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows[0][0] != null)
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
