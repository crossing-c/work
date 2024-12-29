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
    class MpSqlOperater
    {
        private object[] mClientInfo;
        private string mDBName;
        private InfoLightDBTools mDBTools;
        private InfoLightMSTools mDBmsTools;
        private InfoLightDBTools mDCDBTools;
        private MESLog meslog;
        public MpSqlOperater(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDBName = dbName;
            this.mDBTools = new InfoLightDBTools(clientInfo, dbName);
            this.mDBmsTools = new InfoLightMSTools(clientInfo, "WARROOM");
            this.mDCDBTools = new InfoLightDBTools(clientInfo, "DC");
            meslog = new MESLog("MpSqlOperater");
        }
        //
        public DataTable GetMpIndexData(string plantCode)
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

        public DataTable GetFpyIndexDailyData(string plantCode, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();

            string sql = @"   select b.plant_code,
                                       convert(varchar,convert(int,SUBSTRING(b.date, 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(b.date, 7, 2))) date,
                                       case
                                         when a.value is null then
                                          0
                                         else
                                          a.value
                                       end value
                                  from (select case f.process
                                                 when 'AP' then
                                                  f.plant_code + '-FATP'
                                                 else
                                                  f.plant_code + '-' + f.process
                                               end plant_code,
                                               f.data_day date,
                                               case 
                                                when sum(f.station_qty)<=0 then
                                                  0
                                                when sum(f.station_qty) is null then
												  0     
                                                when sum(f.gap_qty) is null then
                                                round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                                    cast(sum(f.station_qty) as float)) * 100,
                                                    2)                                                                                 
                                                else
                                                round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                                    cast(sum(f.station_qty) as float) - sum(f.gap_qty) / cast(sum(f.station_qty) as float)) * 100,
                                                    2)
                                            end value                             
                                          from V_wr_q_plrr f, V_wr_plant_mapping m
                                         where m.source_plant_code = f.plant_code
                                           and m.target_plant_code = @plantcode
                                           and f.process in ('AP', 'SMT')
                                           and f.data_day between @fromtime and @endtime
                                         group by case f.process
                                                    when 'AP' then
                                                     f.plant_code + '-FATP'
                                                    else
                                                     f.plant_code + '-' + f.process
                                                  end,
                                                  f.data_day) a
                                 right join (select t1.plant_code, t2.date
                                               from (SELECT distinct case f.process
                                                                       when 'AP' then
                                                                        f.plant_code + '-FATP'
                                                                       else
                                                                        f.plant_code + '-' + f.process
                                                                     end plant_code,
                                                                     'T' t
                                                       from V_wr_q_plrr f, V_wr_plant_mapping m
                                                      where m.source_plant_code = f.plant_code
                                                        and m.target_plant_code = @plantcode
                                                        and f.process in ('AP', 'SMT')
                                                        and f.data_day between @fromtime and @endtime) t1
                                               full join (SELECT distinct f.data_day date, 'T' t
                                                           from V_wr_q_plrr f, V_wr_plant_mapping m
                                                          where m.source_plant_code = f.plant_code
                                                            and m.target_plant_code = @plantcode
                                                            and f.process in ('AP', 'SMT')
                                                            and f.data_day between @fromtime and @endtime) t2
                                                 on t1.t = t2.t) b
                                    on (a.plant_code = b.plant_code and a.date = b.date)
                                 order by b.plant_code, b.date ";          

           //string sql = @"   select b.plant_code,
           //                            convert(varchar,convert(int,SUBSTRING(b.date, 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(b.date, 7, 2))) date,
           //                            case
           //                              when a.value is null then
           //                               0
           //                              else
           //                               a.value
           //                            end value
           //                       from (select case f.process
           //                                      when 'AP' then
           //                                       f.plant_code + '-FATP'
           //                                      else
           //                                       f.plant_code + '-' + f.process
           //                                    end plant_code,
           //                                    f.data_day date,
           //                                    case 
           //                                     when sum(f.station_qty)<=0 then
           //                                       0
           //                                     when sum(f.station_qty) is null then
											//	  0                                               
											//	when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
											//				cast(sum(f.station_qty) as float))<=0 then
											//	  0
           //                                      else
           //                                       round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
           //                             cast(sum(f.station_qty) as float)) * 100,
           //                             2)
           //                                    end value
           //                               from V_wr_q_plrr f, V_wr_plant_mapping m
           //                              where m.source_plant_code = f.plant_code
           //                                and m.target_plant_code = @plantcode
           //                                and f.process in ('AP', 'SMT')
           //                                and f.data_day between @fromtime and @endtime
           //                              group by case f.process
           //                                         when 'AP' then
           //                                          f.plant_code + '-FATP'
           //                                         else
           //                                          f.plant_code + '-' + f.process
           //                                       end,
           //                                       f.data_day) a
           //                      right join (select t1.plant_code, t2.date
           //                                    from (SELECT distinct case f.process
           //                                                            when 'AP' then
           //                                                             f.plant_code + '-FATP'
           //                                                            else
           //                                                             f.plant_code + '-' + f.process
           //                                                          end plant_code,
           //                                                          'T' t
           //                                            from V_wr_q_plrr f, V_wr_plant_mapping m
           //                                           where m.source_plant_code = f.plant_code
           //                                             and m.target_plant_code = @plantcode
           //                                             and f.process in ('AP', 'SMT')
           //                                             and f.data_day between @fromtime and @endtime) t1
           //                                    full join (SELECT distinct f.data_day date, 'T' t
           //                                                from V_wr_q_plrr f, V_wr_plant_mapping m
           //                                               where m.source_plant_code = f.plant_code
           //                                                 and m.target_plant_code = @plantcode
           //                                                 and f.process in ('AP', 'SMT')
           //                                                 and f.data_day between @fromtime and @endtime) t2
           //                                      on t1.t = t2.t) b
           //                         on (a.plant_code = b.plant_code and a.date = b.date)
           //                      order by b.plant_code, b.date ";
            ht.Clear();
            ht.Add("plantcode", plantCode);
            ht.Add("fromtime", fromTime);
            ht.Add("endtime", endTime);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
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
        public DataTable GetReflowIndexDailyData(string plantCode, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select b.plant_code,
       substr(b.work_date, 5, 2) || '/' || substr(b.work_date, 7, 2) as work_date,
       case
         when b.output_qty = 0 then
          0
         when b.output_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.output_qty * 100, 2)
       end value
  from (select t.plant_code || '-' || t.process AS plant_code,
               t.work_date,
               sum(t.reflow_qty) qty
          from sfism4.r_kpi_reflow_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code = 'PCBG'
                                  and rownum = 1)
           and t.process IN ('ASSY', 'PACK','DOCK')
           and t.work_date between :fromTime and  :endTime
         group by t.plant_code || '-' || t.process, t.work_date
         order by t.plant_code || '-' || t.process, t.work_date) a,
       (select plant_code, t.work_date,sum(output_qty)output_qty from (select 
       CASE t.sub_process 
         WHEN 'PACK' THEN t.plant_code || '-' || t.sub_process
         WHEN 'DOCK' THEN t.plant_code || '-' || t.sub_process 
         ELSE t.plant_code || '-ASSY' END plant_code,
               t.work_date,
               t.output_qty
          from sfism4.r_kpi_fpy_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code = 'PCBG'
                                  and rownum = 1)
           and t.process IN ('FATP')
           and t.sub_process IN ('ASSY','CELL','REPAIR', 'PACK','TNB','DOCK')
           and t.work_date between :fromTime and  :endTime )t
         group by plant_code, t.work_date
         order by plant_code, t.work_date) b
 where b.work_date = a.work_date(+)
   and b.plant_code = a.plant_code(+)
   and b.WORK_DATE is not null
 ORDER BY b.WORK_DATE
 ";
            ht.Clear();
            ht.Add("plant", plantCode);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
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
        #region Offline
        public DataTable GetOfflinePlantAnalysis(string str1, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case
                                     when a.value is null then
                                      0
                                     else
                                      a.value
                                   end value
                              from (        
                                    SELECT o.plant_code,
                                            o.data_week date,
                                            case sum(o.value_real1)
                                              when 0 then
                                               0
                                              when null then
                                               0
                                              else
                                               round(sum(o.value_real2) / sum(o.value_real1) * 100, 2)
                                            end value
                                      FROM V_wr_q_generic o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0001'
                                       AND o.value_str1 = @str1
                                       and m.target_plant_code in
                                           ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                       and o.data_week between @fromTime and @endTime
                                     group by o.plant_code, o.data_week) a
                             right join
                             (select t1.plant_code, t2.date
                                from (SELECT distinct o.plant_code, 'T' t
                                        FROM V_wr_q_generic o, V_wr_plant_mapping m
                                       WHERE m.source_plant_code = o.plant_code
                                         and o.kpi_category = 'Q0001'
                                         AND o.value_str1 = @str1
                                         and m.target_plant_code in
                                             ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                         and o.data_week between @fromTime and @endTime) t1
                                full join (SELECT distinct o.data_week date, 'T' t
                                            FROM V_wr_q_generic o, V_wr_plant_mapping m
                                           WHERE m.source_plant_code = o.plant_code
                                             and o.kpi_category = 'Q0001'
                                             AND o.value_str1 = @str1
                                             and m.target_plant_code in
                                                 ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                             and o.data_week between @fromTime and @endTime) t2
                                  on t1.t = t2.t) b
                                on (a.plant_code = b.plant_code and a.date = b.date)
                             order by b.plant_code, b.date ";
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

        public DataTable GetOfflineDailyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT (select top 1 o.plant_code
                                      FROM V_wr_q_generic o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0001'
                                       and o.value_str1 =@str1
                                       and m.target_plant_code =@plant
                                       and o.data_day between @fromTime and @endTime) plant_code,
                                   convert(varchar,convert(int,SUBSTRING(cast(o.data_day as varchar), 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(cast(o.data_day as varchar), 7, 2))) date,
                                   case sum(o.value_real1)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(o.value_real2) / sum(o.value_real1) * 100, 2)
                                   end value
                              FROM V_wr_q_generic o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'Q0001'
                               and o.value_str1 =@str1
                               and m.target_plant_code =@plant
                               and o.data_day between @fromTime and @endTime 
                             group by  o.data_day
                             order by  o.data_day ";
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

        public DataTable GetOfflineAnalysis(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            //string sql = @" SELECT o.plant_code,
            //                       o.data_day date,
            //                       case sum(o.value_real1)
            //                         when 0 then
            //                          0
            //                         when null then
            //                          0
            //                         else
            //                          round(sum(o.value_real2) / sum(o.value_real1) * 100, 2)
            //                       end value
            //                  FROM V_wr_q_generic o, V_wr_plant_mapping m
            //                 WHERE m.source_plant_code = o.plant_code
            //                   and o.kpi_category = 'Q0001'
            //                   AND o.value_str1 =@str1
            //                   and m.target_plant_code =@plant
            //                   and o.data_day between '20180924' and '20180930' -- -6
            //                 group by o.plant_code, o.data_day
            //                 order by o.plant_code, o.data_day ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            try
            {
                //exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                //if (exeRes.Status)
                //{
                //    DataSet ds = (DataSet)exeRes.Anything;
                //    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                //    {
                //        dtResult = ds.Tables[0];
                //    }
                //}
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetOfflineWeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT (select top 1 o.plant_code
                                      FROM V_wr_q_generic o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0001'
                                       and o.value_str1 =@str1
                                       and m.target_plant_code =@plant
                                       and o.data_week between @fromTime and @endTime) plant_code,
                                   SUBSTRING(o.data_week,5,len(o.data_week)-4) date,
                                   case sum(o.value_real1)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(o.value_real2) / sum(o.value_real1) * 100, 2)
                                   end value
                              FROM V_wr_q_generic o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'Q0001'
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

        public DataTable GetOfflineMonthlyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT (select top 1 o.plant_code
                                      FROM V_wr_q_generic o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0001'
                                       and o.value_str1 =@str1
                                       and m.target_plant_code =@plant
                                       and o.data_month between @fromTime and @endTime) plant_code,
                                   SUBSTRING(o.data_month,5,len(o.data_month)-4) date,
                                   case sum(o.value_real1)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(o.value_real2) / sum(o.value_real1) * 100, 2)
                                   end value
                              FROM V_wr_q_generic o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'Q0001'
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
        #endregion

        #region Reflow
        public DataTable GetReflowPlantAnalysis(string str1, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql1;
            if (str1 == "ASSY")
                sql1 = " in ('ASSY','CELL','REPAIR','TNB') ";
            else if (str1 == "DOCK")
                sql1 = "='DOCK' ";
            else
                sql1 = "='PACK' ";
            string sql = @" select b.plant_code,
       'W' || substr(b.work_date, 5, 6) as work_date,
       case
         when b.output_qty = 0 then
          0
         when b.output_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.output_qty * 100, 2)
       end value
  from (select t.plant_code,
               TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') work_date,
               sum(t.reflow_qty) qty
          from sfism4.r_kpi_reflow_t t
         where t.plant_code in (SELECT distinct d.SAP_PLANT
                                  FROM SFIS1.C_PLANT_DEF_T d
                                 WHERE d.RMS_PLANT in ('KSP3',
                                                       'KSP2',
                                                       'KSP4',
                                                       'CDP1',
                                                       'CQP1',
                                                       'TW01',
                                                       'CTY1',
                                                       'VNP1')
                                   and d.bu_code = 'PCBG')
           and t.process = :str1
           and t.work_date >= :fromTime
           and t.work_date < :endTime
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
         where t.plant_code IN (SELECT distinct d.SAP_PLANT
                                  FROM SFIS1.C_PLANT_DEF_T d
                                 WHERE d.RMS_PLANT in ('KSP3',
                                                       'KSP2',
                                                       'KSP4',
                                                       'CDP1',
                                                       'CQP1',
                                                       'TW01',
                                                       'CTY1',
                                                       'VNP1')
                                   and d.bu_code = 'PCBG')
           and t.sub_process " + sql1 + @"
           and t.process in ('FATP') 
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.plant_code,
                  TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')
         order by t.plant_code,
                  TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')) b
 where b.work_date = a.work_date(+)
   and b.plant_code = a.plant_code(+)
   and b.WORK_DATE is not null
 ORDER BY b.WORK_DATE, b.plant_code  ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetReflowDailyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql1;
            if (str1 == "ASSY")
                sql1 = " in ('ASSY','CELL','REPAIR','TNB') ";
            else if (str1 == "DOCK")
                sql1 = "='DOCK' ";
            else
                sql1 = "='PACK' ";
            string sql = @" select b.plant_code,
       substr(b.work_date, 5, 2)||'/'|| substr(b.work_date, 7, 2) as work_date,
       case
         when b.output_qty = 0 then
          0
         when b.output_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.output_qty * 100, 2)
       end value
  from (select t.plant_code,t.work_date, sum(t.reflow_qty) qty
          from sfism4.r_kpi_reflow_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code='PCBG'
                                  and rownum = 1)
           and t.process = :str1
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.plant_code,t.work_date
         order by t.plant_code,t.work_date) a,
       (select t.plant_code,t.work_date, sum(t.output_qty) output_qty
          from sfism4.r_kpi_fpy_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code='PCBG'
                                  and rownum = 1)
           and t.sub_process " + sql1 + @"
           and t.process in ('FATP') 
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.plant_code,t.work_date
         order by t.plant_code,t.work_date) b
 where b.work_date = a.work_date(+)
   and b.WORK_DATE is not null
 ORDER BY b.WORK_DATE
 ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetReflowWeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql1;
            if (str1 == "ASSY")
                sql1 = " in ('ASSY','CELL','REPAIR','TNB') ";
            else if (str1 == "DOCK")
                sql1 = "='DOCK' ";
            else
                sql1 = "='PACK' ";
            string sql = @" select  b.plant_code,'W' || substr(b.work_date, 5, 6) as work_date,
       case
         when b.output_qty = 0 then
          0
         when b.output_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.output_qty * 100, 2)
       end value
  from (select t.plant_code,TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') work_date,
               sum(t.reflow_qty) qty
          from sfism4.r_kpi_reflow_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code='PCBG'
                                  and rownum = 1)
           and t.process = :str1
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.plant_code,TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')
         order by t.plant_code,TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')) a,
       (select t.plant_code,TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') work_date,
               sum(t.output_qty) output_qty
          from sfism4.r_kpi_fpy_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code='PCBG'
                                  and rownum = 1)
           and t.sub_process " + sql1 + @"
           and t.process in ('FATP') 
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.plant_code,TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')
         order by t.plant_code,TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')) b
 where b.work_date = a.work_date(+)
order by b.work_date
 ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetReflowMonthlyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql1;
            if (str1 == "ASSY")
                sql1 = " in ('ASSY','CELL','REPAIR','TNB') ";
            else if (str1 == "DOCK")
                sql1 = "='DOCK' ";
            else
                sql1 = "='PACK' ";
            string sql = @" select b.plant_code,'M' || substr(b.WORK_DATE, 5, 6) as work_date,
       case
         when b.output_qty = 0 then
          0
         when b.output_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.output_qty * 100, 2)
       end value
  from (select t.plant_code,substr(T.WORK_DATE, 0, 6) WORK_DATE, sum(t.reflow_qty) qty
          from sfism4.r_kpi_reflow_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code='PCBG'
                                  and rownum = 1)
           and t.process = :str1
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.plant_code,substr(T.WORK_DATE, 0, 6)
         order by t.plant_code,substr(T.WORK_DATE, 0, 6)) a,
       (select t.plant_code,substr(T.WORK_DATE, 0, 6) WORK_DATE,
               sum(t.output_qty) output_qty
          from sfism4.r_kpi_fpy_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code='PCBG'
                                  and rownum = 1)
           and t.sub_process " + sql1 + @"
           and t.process in ('FATP') 
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.plant_code,substr(T.WORK_DATE, 0, 6)
         order by t.plant_code,substr(T.WORK_DATE, 0, 6)) b
 where b.work_date = a.work_date(+)
 ORDER BY b.WORK_DATE
 ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        #region Wfr
        public DataTable GetWfrDailyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT w.plant_code,cast(data_day as varchar) date
            , cast(SUM(value_int1) AS float) as value,data_day 
         FROM V_wr_q_generic w WITH (NOLOCK) , V_wr_plant_mapping m
        WHERE w.plant_code = m.source_plant_code 
		  AND w.kpi_category='WFR'
          AND w.value_str2='DAILY'
          and m.target_plant_code =@plant
          and w.value_str1 =@str1
          and w.data_day between @fromTime and @endTime
          AND w.value_str3 = 'TOTAL' -- category != DISTRIBUTION
        GROUP BY w.data_day,w.plant_code
		order by w.plant_code,w.data_day";
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

        public DataTable GetWfrWeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT t1.plant_code,data_week date, cast (AVG(t1.item_value)as float) AS value  
  FROM (
       SELECT w.plant_code,data_day 
            , data_week 
            , SUM(value_int1) AS item_value
         FROM V_wr_q_generic w WITH (NOLOCK) , V_wr_plant_mapping m
        WHERE w.plant_code = m.source_plant_code 
		  AND w.kpi_category='WFR'
          AND w.value_str2='DAILY'
           and m.target_plant_code =@plant
          and w.data_week between @fromTime and @endTime
          and w.value_str1 =@str1
          AND w.value_str3 = 'TOTAL' -- category != DISTRIBUTION
        GROUP BY w.plant_code,w.data_day, w.data_week
     ) t1
GROUP BY t1.plant_code,t1.data_week
 ORDER BY t1.plant_code,t1.data_week  ";
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

        public DataTable GetWfrMonthlyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"   SELECT t1.plant_code,data_month date, cast(AVG(t1.item_value)as float) AS value  
  FROM (
       SELECT w.plant_code,data_day 
            , data_month 
            , SUM(value_int1) AS item_value
         FROM V_wr_q_generic w WITH (NOLOCK) , V_wr_plant_mapping m
        WHERE w.plant_code = m.source_plant_code 
		  AND w.kpi_category='WFR'
          AND w.value_str2='DAILY'
          and m.target_plant_code =@plant
          and w.value_str1 =@str1
          and w.data_month between @fromTime and @endTime
          AND w.value_str3 = 'TOTAL' -- category != DISTRIBUTION
        GROUP BY w.plant_code,w.data_day, w.data_month
     ) t1
GROUP BY t1.plant_code,t1.data_month
 ORDER BY t1.plant_code,t1.data_month    ";
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

        #region Ctq
        public DataTable GetCtqPlantAnalysis(string process, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case
                                     when a.value is null then
                                      0
                                     else
                                      a.value
                                   end value
                              from (select c.plant_code,
                                           c.data_week date,
                                           case sum(c.total_qc_qty + c.total_mfg_qty)
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round(sum(c.qc_qty + c.mfg_qty) /
                                                    sum(c.total_qc_qty + c.total_mfg_qty) * 100,
                                                    2)
                                           end value
                                      from V_wr_q_ctq c, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = c.plant_code
                                       and c.process = @process
                                       and m.target_plant_code in
                                           ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                       and c.data_week between @fromTime and @endTime
                                     group by c.plant_code, c.data_week) a
                             right join (select t1.plant_code, t2.date
                                           from (SELECT distinct c.plant_code, 'T' t
                                                   from V_wr_q_ctq c, V_wr_plant_mapping m
                                                  WHERE m.source_plant_code = c.plant_code
                                                    and c.process = @process
                                                    and m.target_plant_code in
                                                        ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                                    and c.data_week between @fromTime and @endTime) t1
                                           full join (SELECT distinct c.data_week date, 'T' t
                                                       from V_wr_q_ctq c, V_wr_plant_mapping m
                                                      WHERE m.source_plant_code = c.plant_code
                                                        and c.process = @process
                                                        and m.target_plant_code in
                                                            ('KSP3',
                                                             'KSP2',
                                                             'KSP4',
                                                             'CDP1',
                                                             'CQP1',
                                                             'TW01','VNP1')
                                                        and c.data_week between @fromTime and @endTime) t2
                                             on t1.t = t2.t) b
                                on (a.plant_code = b.plant_code and a.date = b.date)
                             order by b.plant_code, b.date ";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetCtqDailyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select (select top 1 c.plant_code
                                  from V_wr_q_ctq c, V_wr_plant_mapping m
                                 WHERE m.source_plant_code = c.plant_code
                                   and c.process =@process
                                   and m.target_plant_code=@plant
                                   and c.data_day between @fromTime and @endTime) plant_code,
                               convert(varchar,convert(int,SUBSTRING(cast(c.data_day as varchar), 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(cast(c.data_day as varchar), 7, 2))) date,
                               case sum(c.total_qc_qty + c.total_mfg_qty)
                                 when 0 then
                                  0
                                 when null then
                                  0
                                 else
                                  round(sum(c.qc_qty + c.mfg_qty) /
                                        sum(c.total_qc_qty + c.total_mfg_qty) * 100,
                                        2)
                               end value
                          from V_wr_q_ctq c, V_wr_plant_mapping m
                         WHERE m.source_plant_code = c.plant_code
                           and c.process =@process
                           and m.target_plant_code=@plant
                           and c.data_day between @fromTime and @endTime
                         group by  c.data_day
                         order by  c.data_day ";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetCtqAnalysis(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            //string sql = @" SELECT o.plant_code,
            //                       o.data_day date,
            //                       case sum(o.value_real1)
            //                         when 0 then
            //                          0
            //                         when null then
            //                          0
            //                         else
            //                          round(sum(o.value_real2) / sum(o.value_real1) * 100, 2)
            //                       end value
            //                  FROM V_wr_q_generic o, V_wr_plant_mapping m
            //                 WHERE m.source_plant_code = o.plant_code
            //                   and o.kpi_category = 'Q0001'
            //                   AND o.value_str1 =@str1
            //                   and m.target_plant_code =@plant
            //                   and o.data_day between '20180924' and '20180930' -- -6
            //                 group by o.plant_code, o.data_day
            //                 order by o.plant_code, o.data_day ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            try
            {
                //exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                //if (exeRes.Status)
                //{
                //    DataSet ds = (DataSet)exeRes.Anything;
                //    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                //    {
                //        dtResult = ds.Tables[0];
                //    }
                //}
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetCtqWeeklyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select (select top 1 c.plant_code
                                  from V_wr_q_ctq c, V_wr_plant_mapping m
                                 WHERE m.source_plant_code = c.plant_code
                                   and c.process =@process
                                   and m.target_plant_code=@plant
                                   and c.data_week between @fromTime and @endTime) plant_code,
                               SUBSTRING(c.data_week,5,len(c.data_week)-4) date,
                               case sum(c.total_qc_qty + c.total_mfg_qty)
                                 when 0 then
                                  0
                                 when null then
                                  0
                                 else
                                  round(sum(c.qc_qty + c.mfg_qty) /
                                        sum(c.total_qc_qty + c.total_mfg_qty) * 100,
                                        2)
                               end value
                          from V_wr_q_ctq c, V_wr_plant_mapping m
                         WHERE m.source_plant_code = c.plant_code
                           and c.process =@process
                           and m.target_plant_code=@plant
                           and c.data_week between @fromTime and @endTime
                         group by  c.data_week
                         order by  c.data_week ";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetCtqMonthlyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select (select top 1 c.plant_code
                                  from V_wr_q_ctq c, V_wr_plant_mapping m
                                 WHERE m.source_plant_code = c.plant_code
                                   and c.process = @process
                                   and m.target_plant_code = @plant
                                   and c.data_month between @fromTime and @endTime) plant_code,
                               SUBSTRING(c.data_month,5,len(c.data_month)-4) date,
                               case sum(c.total_qc_qty + c.total_mfg_qty)
                                 when 0 then
                                  0
                                 when null then
                                  0
                                 else
                                  round(sum(c.qc_qty + c.mfg_qty) /
                                        sum(c.total_qc_qty + c.total_mfg_qty) * 100,
                                        2)
                               end value
                          from V_wr_q_ctq c, V_wr_plant_mapping m
                         WHERE m.source_plant_code = c.plant_code
                           and c.process =@process
                           and m.target_plant_code =@plant
                           and c.data_month between @fromTime and @endTime
                         group by c.data_month
                         order by c.data_month ";
            ht.Clear();
            ht.Add("process", process);
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

        #region Pid
        public DataTable GetPidLcdDailyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" Select (select top 1 l.plant_code
                                  FROM V_wr_q_lcd l, V_wr_plant_mapping m
                                 where m.source_plant_code = l.plant_code
                                   and m.target_plant_code =@plant
                                   and l.data_day between @fromTime and @endTime) plant_code,
                               convert(varchar,convert(int,SUBSTRING(cast(l.data_day as varchar), 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(cast(l.data_day as varchar), 7, 2))) date,
                               case sum(l.input_qty)
                                 when 0 then
                                  0
                                 when null then
                                  0
                                 else
                                  round(sum(l.pid_qty) / sum(l.input_qty) * 1000000,2)
                               end value
                          FROM V_wr_q_lcd l, V_wr_plant_mapping m
                         where m.source_plant_code = l.plant_code
                           and m.target_plant_code =@plant
                           and l.data_day between @fromTime and @endTime
                         group by  l.data_day
                         order by  l.data_day ";
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

        public DataTable GetPidLcdWeeklyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" Select (select top 1 l.plant_code
                                      FROM V_wr_q_lcd l, V_wr_plant_mapping m
                                     where m.source_plant_code = l.plant_code
                                       and m.target_plant_code =@plant
                                       and l.data_week between @fromTime and @endTime) plant_code,
                                   SUBSTRING(l.data_week,5,len(l.data_week)-4) date,
                                   case sum(l.input_qty)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(l.pid_qty) / sum(l.input_qty) * 1000000,2)
                                   end value
                              FROM V_wr_q_lcd l, V_wr_plant_mapping m
                             where m.source_plant_code = l.plant_code
                               and m.target_plant_code =@plant
                               and l.data_week between @fromTime and @endTime
                             group by  l.data_week
                             order by  l.data_week ";
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

        public DataTable GetPidLcdMonthlyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" Select (select top 1 l.plant_code
                                      FROM V_wr_q_lcd l, V_wr_plant_mapping m
                                     where m.source_plant_code = l.plant_code
                                       and m.target_plant_code =@plant
                                       and l.data_month between @fromTime and @endTime) plant_code,
                                   SUBSTRING(l.data_month,5,len(l.data_month)-4) date,
                                   case sum(l.input_qty)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(l.pid_qty) / sum(l.input_qty) * 1000000, 2)
                                   end value
                              FROM V_wr_q_lcd l, V_wr_plant_mapping m
                             where m.source_plant_code = l.plant_code
                               and m.target_plant_code =@plant
                               and l.data_month between @fromTime and @endTime
                             group by l.data_month
                             order by l.data_month ";
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

        public DataTable GetPidMeDailyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select (select top 1 me.plant_code
                                      from V_wr_q_me me, V_wr_plant_mapping m
                                     where m.source_plant_code = me.plant_code
                                       and m.target_plant_code =@plant
                                       and me.data_day between @fromTime and @endTime) plant_code,
                                   convert(varchar,convert(int,SUBSTRING(cast(me.data_day as varchar), 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(cast(me.data_day as varchar), 7, 2))) date,
                                   case sum(me.qty_2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(me.qty_1) / sum(me.qty_2) * 1000000, 2)
                                   end value
                              from V_wr_q_me me, V_wr_plant_mapping m
                             where m.source_plant_code = me.plant_code
                               and m.target_plant_code =@plant
                               and me.data_day between @fromTime and @endTime
                             group by  me.data_day
                             order by  me.data_day ";
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

        public DataTable GetPidMeWeeklyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select (select top 1 me.plant_code
                                      from V_wr_q_me me, V_wr_plant_mapping m
                                     where m.source_plant_code = me.plant_code
                                       and m.target_plant_code =@plant
                                       and me.data_week between @fromTime and @endTime) plant_code,
                                   SUBSTRING(me.data_week,5,len(me.data_week)-4) date,
                                   case sum(me.qty_2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(me.qty_1) / sum(me.qty_2) * 1000000, 2)
                                   end value
                              from V_wr_q_me me, V_wr_plant_mapping m
                             where m.source_plant_code = me.plant_code
                               and m.target_plant_code =@plant
                               and me.data_week between @fromTime and @endTime
                             group by me.data_week
                             order by me.data_week ";
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

        public DataTable GetPidMeMonthlyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select (select top 1 me.plant_code
                                      from V_wr_q_me me, V_wr_plant_mapping m
                                     where m.source_plant_code = me.plant_code
                                       and m.target_plant_code =@plant
                                       and me.data_month between @fromTime and @endTime) plant_code,
                                   SUBSTRING(me.data_month,5,len(me.data_month)-4) date,
                                   case sum(me.qty_2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(me.qty_1) / sum(me.qty_2) * 1000000, 2)
                                   end value
                              from V_wr_q_me me, V_wr_plant_mapping m
                             where m.source_plant_code = me.plant_code
                               and m.target_plant_code =@plant
                               and me.data_month between @fromTime and @endTime
                             group by me.data_month
                             order by me.data_month ";
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

        public DataTable GetPidKpsDailyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select (select top 1 k.plant_code
                                     from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code
                                       and m.target_plant_code =@plant
                                       and k.data_day between @fromTime and @endTime) plant_code,
                                           convert(varchar,convert(int,SUBSTRING(cast(k.data_day as varchar), 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(cast(k.data_day as varchar), 7, 2))) date,
                                           case sum(k.qty_2)
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round(sum(k.qty_1) / sum(k.qty_2) * 1000000,2)
                                           end value
                                      from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code
                                       and m.target_plant_code =@plant
                                       and k.data_day between @fromTime and @endTime
                                     group by  k.data_day
                                     order by  k.data_day ";
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

        public DataTable GetPidKpsWeeklyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select (select top 1 k.plant_code
                                     from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code
                                       and m.target_plant_code =@plant
                                       and k.data_week between @fromTime and @endTime) plant_code,
                                           SUBSTRING(k.data_week,5,len(k.data_week)-4) date,
                                           case sum(k.qty_2)
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round(sum(k.qty_1) / sum(k.qty_2) * 1000000,2)
                                           end value
                                      from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code
                                       and m.target_plant_code =@plant
                                       and k.data_week between @fromTime and @endTime
                                     group by  k.data_week
                                     order by  k.data_week ";
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

        public DataTable GetPidKpsMonthlyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select ( select top 1 k.plant_code
                                     from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code
                                        and m.target_plant_code =@plant
                                        and k.data_month between @fromTime and @endTime) plant_code,
                                           SUBSTRING(k.data_month,5,len(k.data_month)-4) date,
                                           case sum(k.qty_2)
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round(sum(k.qty_1) / sum(k.qty_2) * 1000000,2)
                                           end value
                                      from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code
                                       and m.target_plant_code =@plant
                                       and k.data_month between @fromTime and @endTime
                                     group by  k.data_month
                                     order by  k.data_month ";
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

        public DataTable GetPidReRateDailyTrend(string plant, string fromTime, string endTime, string pType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = "";
            if (plant == "VNP1")
            {
                sql = @" select :plant as plant_code,  
                                   to_char(to_date(a.work_date, 'yyyymmdd'), 'mm/dd') as ""date"",
                                   ROUND(nvl(sum(a.qaok_qty),0)*100/nvl(sum(a.tot_qty),0),2) as ""VALUE"" ,
                                   nvl(sum(a.qaok_qty),0) as OK_QTY 
                             from sfism4.r_reborn_summary_t a 
                             where a.plant_code='VNNB1' ";
                if (pType != "")
                    sql += @"         and a.type=:ptype ";
                sql += @"             and a.work_date between :fromTime and :endTime group by plant_code,a.work_date order by a.work_date ";
            }
            else
            {
                sql = @" select b.rms_plant as plant_code,  
                                   to_char(to_date(a.work_date, 'yyyymmdd'), 'mm/dd') as ""date"",
                                   ROUND(nvl(sum(a.qaok_qty),0)*100/nvl(sum(a.tot_qty),0),2) as ""VALUE"" ,
                                   nvl(sum(a.qaok_qty),0) as OK_QTY
                            from sfism4.r_reborn_summary_t a,sfis1.c_plant_def_t b 
                            where a.plant_code = b.mes_plant and b.rms_plant =:plant and b.bu_code = 'PCBG' ";
                if (pType != "")
                    sql += @"         and a.type=:ptype ";
                sql += @"             and a.work_date between :fromTime and :endTime group by b.rms_plant,a.work_date order by a.work_date ";
            }
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (pType != "")
                ht.Add("ptype", pType);
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetPidReRateWeeklyTrend(string plant, string fromTime, string endTime, string pType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = "";
            if (plant == "VNP1")
                sql = @" select a.plant_code,
                                   a.sdate as ""date"",
                                   ROUND(nvl(sum(a.qaok_qty),0)*100/nvl(sum(a.tot_qty),0),2) as ""VALUE"" ,
                                   nvl(sum(a.qaok_qty),0) as OK_QTY
                            from 
                            (select :plant as plant_code, to_char(to_date(a.work_date, 'yyyymmdd'), 'iyyyiw') as week ,'W'||to_char(to_date(a.work_date, 'yyyymmdd'), 'iw') as sdate, 
                                    a.qaok_qty, a.tot_qty
                             from sfism4.r_reborn_summary_t a 
                             where a.plant_code='VNNB1' ";
            else
                sql = @" select a.plant_code,
                                   a.sdate as ""date"",
                                   ROUND(nvl(sum(a.qaok_qty),0)*100/nvl(sum(a.tot_qty),0),2) as ""VALUE"",
                                   nvl(sum(a.qaok_qty),0) as OK_QTY
                            from 
                            (select b.rms_plant as plant_code, to_char(to_date(a.work_date, 'yyyymmdd'), 'iyyyiw') as week ,'W'||to_char(to_date(a.work_date, 'yyyymmdd'), 'iw') as sdate, 
                                    a.qaok_qty, a.tot_qty
                             from sfism4.r_reborn_summary_t a, sfis1.c_plant_def_t b
                             where a.plant_code = b.mes_plant and b.rms_plant = :plant and b.bu_code = 'PCBG'";
            if (pType != "")
                sql += @"         and a.type=:ptype ";
            sql += @"             and to_char(to_date(a.work_date, 'yyyymmdd'), 'iyyyiw') between :fromTime and :endTime)a
                            group by a.plant_code, a.week,a.sdate order by a.week";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime.Replace("W", ""));
            ht.Add("endTime", endTime.Replace("W", ""));
            if (pType != "")
                ht.Add("ptype", pType);
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetPidReRateMonthlyTrend(string plant, string fromTime, string endTime, string pType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = "";
            if (plant == "VNP1")
                sql = @" select a.plant_code,
                                   a.sdate as ""date"",
                                   ROUND(nvl(sum(a.qaok_qty),0)*100/nvl(sum(a.tot_qty),0),2) as ""VALUE"" ,
                                   nvl(sum(a.qaok_qty),0) as OK_QTY
                            from 
                            (select :plant as plant_code, to_char(to_date(a.work_date, 'yyyymmdd'), 'yyyymm') as mon,'M'||to_char(to_date(a.work_date,'yyyymmdd'), 'mm') as sdate
                                    , a.qaok_qty, a.tot_qty
                             from sfism4.r_reborn_summary_t a 
                             where a.plant_code='VNNB1' ";
            else
                sql = @" select a.plant_code,
                                   a.sdate as ""date"",
                                   ROUND(nvl(sum(a.qaok_qty),0)*100/nvl(sum(a.tot_qty),0),2) as ""VALUE"" ,
                                   nvl(sum(a.qaok_qty),0) as OK_QTY
                            from 
                            (select b.rms_plant as plant_code, to_char(to_date(a.work_date, 'yyyymmdd'), 'yyyymm') as mon,'M'||to_char(to_date(a.work_date,'yyyymmdd'), 'mm') as sdate
                                    , a.qaok_qty, a.tot_qty
                             from sfism4.r_reborn_summary_t a, sfis1.c_plant_def_t b
                             where a.plant_code = b.mes_plant and b.rms_plant = :plant and b.bu_code = 'PCBG'";
            if (pType != "")
                sql += @"         and a.type=:ptype ";
            sql += @"             and to_char(to_date(a.work_date, 'yyyymmdd'), 'yyyymm') between :fromTime and :endTime)a
                            group by a.plant_code, a.mon, a.sdate order by a.mon";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime.Replace("M", ""));
            ht.Add("endTime", endTime.Replace("M", ""));
            if (pType != "")
                ht.Add("ptype", pType);
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetReRateType(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"select distinct a.type as ptype  
                             from sfism4.r_reborn_summary_t a, sfis1.c_plant_def_t b
                             where a.plant_code = b.mes_plant and b.rms_plant = :plant and b.bu_code = 'PCBG'
                                    and to_char(to_date(a.work_date, 'yyyymmdd'), 'yyyymm') >= :checkdate";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("checkdate", DateTime.Now.AddMonths(-3).ToString("yyyyMM"));
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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

        #endregion

        #region Scrap
        public DataTable GetScrap(string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case
                                     when a.value is null then
                                      0
                                     else
                                      cast(a.value as float)
                                   end value
                              from (SELECT g.plant_code,
                                           g.data_month date,
                                           CONVERT(FLOAT, round(g.value_real2, 3, 1)) value
                                      FROM V_wr_m_generic g
                                     WHERE g.kpi_category = 'SCRAP'
                                       and g.plant_code = 'All Plants'
                                       AND g.value_str1 = 'CPB'
                                       and g.value_str2 = 'Operation'
                                       and g.data_month between @fromTime and @endTime
                                    union
                                    SELECT g.plant_code,
                                           g.data_month date,
                                           CONVERT(FLOAT, round(g.value_real2, 3, 1)) value
                                      FROM V_wr_m_generic g, V_wr_plant_mapping m
                                     WHERE g.plant_code = m.source_plant_code 
                                       and g.kpi_category = 'SCRAP'
                                       AND g.value_str1 = 'CPB'
                                       and g.value_str2 = 'Operation'
                                       and m.target_plant_code in
                                           ('KSP2', 'KSP3', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                       and g.data_month between @fromTime and @endTime
                                     group by g.plant_code, g.data_month, g.value_real2) a
                             right join (select t1.plant_code, t2.date
                                           from (SELECT distinct g.plant_code, 'T' t
                                                   from (SELECT g.plant_code
                                                           FROM V_wr_m_generic g
                                                          WHERE g.kpi_category = 'SCRAP'
                                                            and g.plant_code = 'All Plants'
                                                            AND g.value_str1 = 'CPB'
                                                            and g.value_str2 = 'Operation'
                                                            and g.data_month between @fromTime and
                                                                @endTime
                                                         union
                                                         SELECT g.plant_code
                                                           FROM V_wr_m_generic g, V_wr_plant_mapping m
                                                          WHERE g.plant_code = m.source_plant_code 
                                                            and g.kpi_category = 'SCRAP'
                                                            AND g.value_str1 = 'CPB'
                                                            and g.value_str2 = 'Operation'
                                                            and m.target_plant_code in
                                                                ('KSP2',
                                                                 'KSP3',
                                                                 'KSP4',
                                                                 'CDP1',
                                                                 'CQP1',
                                                                 'TW01','VNP1')
                                                            and g.data_month between @fromTime and
                                                                @endTime) g) t1
                                           full join (SELECT distinct g.data_month date, 'T' t
                                                       from (SELECT g.data_month
                                                               FROM V_wr_m_generic g
                                                              WHERE g.kpi_category = 'SCRAP'
                                                                and g.plant_code = 'All Plants'
                                                                AND g.value_str1 = 'CPB'
                                                                and g.value_str2 = 'Operation'
                                                                and g.data_month between @fromTime and
                                                                    @endTime
                                                             union
                                                             SELECT g.data_month
                                                               FROM V_wr_m_generic     g,
                                                                    V_wr_plant_mapping m
                                                              WHERE g.plant_code = m.source_plant_code 
                                                                and g.kpi_category = 'SCRAP'
                                                                AND g.value_str1 = 'CPB'
                                                                and g.value_str2 = 'Operation'
                                                                and m.target_plant_code in
                                                                    ('KSP2',
                                                                     'KSP3',
                                                                     'KSP4',
                                                                     'CDP1',
                                                                     'CQP1',
                                                                     'TW01','VNP1')
                                                                and g.data_month between @fromTime and
                                                                    @endTime) g) t2
                                             on t1.t = t2.t) b
                                on (a.plant_code = b.plant_code and a.date = b.date)
                             order by b.plant_code, b.date ";
            ht.Clear();
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

        #region FPY
        public DataTable GetFpyPlantAnalysis(string process, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"  select b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case
                                     when a.value is null then
                                      0
                                     else
                                      a.value
                                   end value
                              from (select f.plant_code,
                                           f.data_week date,
                                           case
                                             when sum(f.station_qty) <= 0 then
                                              0
                                             when sum(f.station_qty) is null then
                                              0
                                             when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                                  cast(sum(f.station_qty) as float)) <= 0 then
                                              0
                                             else
                                              round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                                    cast(sum(f.station_qty) as float)) * 100,
                                                    2)
                                           end value
                                      from V_wr_q_plrr f, V_wr_plant_mapping m
                                     where m.source_plant_code = f.plant_code
                                       and m.target_plant_code in
                                           ('KSP3','KSP1-AEP','KSP1-SVR', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','TW01DOCK','CDDOCK','VNP1')
                                       and f.process = @process
                                       and f.data_week between @fromTime and @endTime
                                     group by f.plant_code, f.data_week) a
                             right join (select t1.plant_code, t2.date
                                           from (SELECT distinct f.plant_code, 'T' t
                                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                                  where m.source_plant_code = f.plant_code
                                                    and m.target_plant_code in
                                                        ('KSP3','KSP1-AEP','KSP1-SVR',
                                                         'KSP2',
                                                         'KSP4',
                                                         'CDP1',
                                                         'CQP1',
                                                         'TW01',
                                                         'TW01DOCK',
                                                         'CDDOCK',
                                                         'VNP1')
                                                    and f.process = @process
                                                    and f.data_week between @fromTime and @endTime) t1
                                           full join (SELECT distinct f.data_week date, 'T' t
                                                       from V_wr_q_plrr f, V_wr_plant_mapping m
                                                      where m.source_plant_code = f.plant_code
                                                        and m.target_plant_code in
                                                            ('KSP3','KSP1-AEP','KSP1-SVR',
                                                             'KSP2',
                                                             'KSP4',
                                                             'CDP1',
                                                             'CQP1',
                                                             'TW01',
                                                         'TW01DOCK',
                                                         'CDDOCK',
                                                             'VNP1')
                                                        and f.process = @process
                                                        and f.data_week between @fromTime and @endTime) t2
                                             on t1.t = t2.t) b
                                on (a.date = b.date and a.plant_code = b.plant_code)
                             order by b.plant_code, b.date ";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetFpyAnalysis(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            //string sql = @" SELECT o.plant_code,
            //                       o.data_day date,
            //                       case sum(o.value_real1)
            //                         when 0 then
            //                          0
            //                         when null then
            //                          0
            //                         else
            //                          round(sum(o.value_real2) / sum(o.value_real1) * 100, 2)
            //                       end value
            //                  FROM V_wr_q_generic o, V_wr_plant_mapping m
            //                 WHERE m.source_plant_code = o.plant_code
            //                   and o.kpi_category = 'Q0001'
            //                   AND o.value_str1 =@process
            //                   and m.target_plant_code =@plant
            //                   and o.data_day between '20180924' and '20180930' -- -6
            //                 group by o.plant_code, o.data_day
            //                 order by o.plant_code, o.data_day ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            try
            {
                //exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                //if (exeRes.Status)
                //{
                //    DataSet ds = (DataSet)exeRes.Anything;
                //    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                //    {
                //        dtResult = ds.Tables[0];
                //    }
                //}
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetFpyDutyCategory(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            //string sql = @" SELECT o.plant_code,
            //                       o.data_day date,
            //                       case sum(o.value_real1)
            //                         when 0 then
            //                          0
            //                         when null then
            //                          0
            //                         else
            //                          round(sum(o.value_real2) / sum(o.value_real1) * 100, 2)
            //                       end value
            //                  FROM V_wr_q_generic o, V_wr_plant_mapping m
            //                 WHERE m.source_plant_code = o.plant_code
            //                   and o.kpi_category = 'Q0001'
            //                   AND o.value_str1 =@process
            //                   and m.target_plant_code =@plant
            //                   and o.data_day between '20180924' and '20180930' -- -6
            //                 group by o.plant_code, o.data_day
            //                 order by o.plant_code, o.data_day ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            try
            {
                //exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                //if (exeRes.Status)
                //{
                //    DataSet ds = (DataSet)exeRes.Anything;
                //    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                //    {
                //        dtResult = ds.Tables[0];
                //    }
                //}
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetFpyDailyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select f.plant_code,
 convert(varchar,convert(int,SUBSTRING(cast(f.data_day as varchar), 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(cast(f.data_day as varchar), 7, 2))) date,
                               case 
                                 when sum(f.station_qty)<=0 then
                                  0
                                 when sum(f.station_qty) is null then
                                  0
                                when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                        cast(sum(f.station_qty) as float))<=0 then
                                  0
                                 else
                                  round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                        cast(sum(f.station_qty) as float)) * 100,
                                        2)
                               end value,f.data_day 
                          from V_wr_q_plrr f, V_wr_plant_mapping m
                         where m.source_plant_code = f.plant_code
                           and m.target_plant_code=@plant ";
            //if (plant == "VNP1")
            //    sql += @"   AND M.source_plant_code='VNNB1' ";
            sql += @"        and f.process =@process
                           and f.data_day between @fromTime and @endTime
                         group by f.plant_code,f.data_day
                         order by f.data_day,f.plant_code ";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetFpyWeeklyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select f.plant_code,
                                SUBSTRING(f.data_week,5,len(f.data_week)-4) date,
                                case 
                                  when sum(f.station_qty)<=0 then
                                   0
                                  when sum(f.station_qty) is null then
                                   0
                                  when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float))<=0 then
                                   0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                end value,f.data_week
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code ";
            //if (plant == "VNP1")
            //    sql += @"   AND M.source_plant_code='VNNB1' ";
            sql += @"        
                            and m.target_plant_code =@plant
                            and f.process =@process 
                            and f.data_week between @fromTime and @endTime
                          group by f.plant_code,f.data_week
                          order by f.data_week ,f.plant_code";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetFpyMonthlyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select f.plant_code,
                                SUBSTRING(f.data_month,5,len(f.data_month)-4) date,
                                case 
                                  when sum(f.station_qty)<=0 then
                                   0
                                  when sum(f.station_qty) is null then
                                   0
                                  when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float))<=0 then
                                   0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                end value,f.data_month 
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code
                            and m.target_plant_code =@plant ";
            //if (plant == "VNP1")
            //    sql += @"   AND M.source_plant_code='VNNB1' ";
            sql += @"        
                            and f.process =@process 
                            and f.data_month between @fromTime and @endTime
                          group by f.plant_code,f.data_month
                          order by f.data_month,f.plant_code";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetFpyByLine(string str1, string plant, string time)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT top 15 (select top 1 l.plant_code
                                         FROM V_wr_q_generic l, V_wr_plant_mapping m
                                        WHERE m.source_plant_code = l.plant_code
                                          and kpi_category = 'TYPE1'
                                          AND data_name = 'S004'
                                          and m.target_plant_code = @plant
                                          and l.value_str1 = @str1
                                          and l.data_week = @time) plant_code,
                               l.value_str2 line_name,
                               case sum(l.value_int1)
                                 when 0 then
                                  0
                                 when null then
                                  0
                                 else
                                  round(sum(l.value_int2 + l.value_int3) /
                                        cast(sum(l.value_int1) as float) * 100,
                                        2)
                               end TOTAL_FAIL,
                               cast(sum(l.value_int2) as float) FAIL_QTY
                          FROM V_wr_q_generic l, V_wr_plant_mapping m
                         WHERE m.source_plant_code = l.plant_code
                           and kpi_category = 'TYPE1'
                           AND data_name = 'S004'
                           and m.target_plant_code = @plant ";
            //if (plant == "VNP1")
            //    sql += @"   AND M.source_plant_code='VNNB1' ";
            sql += @"        
                           and l.value_str1 = @str1
                           and l.data_week = @time
                         group by l.value_str2
                         order by total_fail desc ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("time", time);
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

        public DataTable GetFpyByModel(string str1, string plant, string time)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT top 15 (select top 1 l.plant_code
                                     FROM V_wr_q_generic l, V_wr_plant_mapping m
                                    WHERE m.source_plant_code = l.plant_code
                                      and kpi_category = 'TYPE1'
                                      AND data_name = 'S005'
                                      and m.target_plant_code = @plant
                                      and l.value_str1 = @str1
                                      and l.data_week = @time) plant_code,
                           l.value_str2 line_name,
                           case sum(l.value_int1)
                             when 0 then
                              0
                             when null then
                              0
                             else
                              round(sum(l.value_int2 + l.value_int3) /
                                    cast(sum(l.value_int1) as float) * 100,
                                    2)
                           end TOTAL_FAIL,
                           case sum(l.value_int1)
                             when 0 then
                              0
                             when null then
                              0
                             else
                              round(sum(l.value_int2) / cast(sum(l.value_int1) as float) * 100,
                                    2)
                           end FIRST_FAIL,
                           case sum(l.value_int1)
                             when 0 then
                              0
                             when null then
                              0
                             else
                              round(sum(l.value_int3) / cast(sum(l.value_int1) as float) * 100,
                                    2)
                           end REFAIL,
                           cast(sum(l.value_int1) as float) INPUT_QTY
                      FROM V_wr_q_generic l, V_wr_plant_mapping m
                     WHERE m.source_plant_code = l.plant_code
                       and kpi_category = 'TYPE1'
                       AND data_name = 'S005'
                       and m.target_plant_code = @plant ";
            //if (plant == "VNP1")
            //    sql += @"   AND M.source_plant_code='VNNB1' ";
            sql += @"        
                       and l.value_str1 = @str1
                       and l.data_week = @time
                     group by l.value_str2
                     order by total_fail desc ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("time", time);
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

        #region Lrr
        public string GetLrrPlant(string str1, string plant)
        {
            string res = "";
            Hashtable ht = new Hashtable();
            string sql = @" SELECT distinct q.plant_code
                                     FROM V_wr_q_generic q, V_wr_plant_mapping m
                                    WHERE q.kpi_category = 'Q0009'
                                      and m.source_plant_code = q.plant_code
                                      and m.target_plant_code =@plant
                                      and q.value_str3 = 'INPUT'
                                      and q.value_str1 =@str1 ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            try
            {
                res = mDBmsTools.ExecuteQueryStrHt(sql, ht);
            }
            catch 
            {
            }
            return res;
        }

        public DataTable GetLrrPlantAnalysis(string str1, string fromTime, string endTime, string str3Type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string str3 = "";
            //增加cpa 单个查询条件
            if (str1 != "AP")
            {
                if (str3Type == "ALL")
                {
                    str3 = " in ('置件', '印刷', '材料', '制程', 'NTF') ";
                }
                else
                {
                    str3 = " = '" + str3Type + "' ";
                }
            }
            else
            {
                if (str3Type == "ALL")
                {
                    str3 = " in ('作業', '材料', 'PCBA LRR', 'NTF') ";
                }
                else
                {
                    str3 = " = '" + str3Type + "' ";
                }
            }

            string sql = @" SELECT b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case b.value
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round((cast(a.value as float) / b.value) * 100, 2)
                                   end value
                              FROM (select b.plant_code,
                                           b.date,
                                           case
                                             when a.value is null then
                                              0
                                             else
                                              a.value
                                           end value
                                      from (SELECT sum(q.value_real1) value,
                                                   q.data_week date,
                                                   q.plant_code
                                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                                             WHERE q.kpi_category = 'Q0009'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code in
                                                   ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                               and q.value_str3 " + str3 + @"
                                               and q.value_str1 =@str1
                                               and q.data_week between @fromtime and @endtime
                                             group by q.plant_code, q.data_week) a
                                     right join (select t1.plant_code, t2.date
                                                  from (SELECT distinct q.plant_code, 'T' t
                                                                   FROM V_wr_q_plrr     q,
                                                                        V_wr_plant_mapping m
                                                                  WHERE m.source_plant_code =q.plant_code
                                                           and m.target_plant_code in
                                                               ('KSP3',
                                                                'KSP2',
                                                                'KSP4',
                                                                'CDP1',
                                                                'CQP1',
                                                                'TW01','VNP1')
                                                           and q.process =@str1
                                                           and q.data_week between @fromtime and @endtime ) t1
                                                  full join (SELECT distinct q.data_week date, 'T' t
                                                                   FROM V_wr_q_plrr     q,
                                                                        V_wr_plant_mapping m
                                                                  WHERE m.source_plant_code = q.plant_code
                                                               and m.target_plant_code in
                                                                   ('KSP3',
                                                                    'KSP2',
                                                                    'KSP4',
                                                                    'CDP1',
                                                                    'CQP1',
                                                                    'TW01','VNP1')
                                                               and q.process =@str1
                                                               and q.data_week between @fromtime and @endtime ) t2
                                                    on t1.t = t2.t) b
                                        on (a.date = b.date and a.plant_code = b.plant_code)) a
                              full join (select b.plant_code,
                                                b.date,
                                                case
                                                  when a.value is null then
                                                   0
                                                  else
                                                   a.value
                                                end value
                                           from (SELECT sum(q.station_qty) value,
                                                        q.data_week date,
                                                        q.plant_code
                                                   FROM V_wr_q_plrr q, V_wr_plant_mapping m
                                                  WHERE m.source_plant_code = q.plant_code
                                                    and m.target_plant_code in
                                                        ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                                    and q.process =@str1
                                                    and q.data_week between @fromtime and @endtime
                                                  group by q.plant_code, q.data_week) a
                                          right join (select t1.plant_code, t2.date
                                                       from (SELECT distinct q.plant_code, 'T' t
                                                               FROM V_wr_q_plrr     q,
                                                                    V_wr_plant_mapping m
                                                              WHERE m.source_plant_code = q.plant_code
                                                                and m.target_plant_code in
                                                                    ('KSP3',
                                                                     'KSP2',
                                                                     'KSP4',
                                                                     'CDP1',
                                                                     'CQP1',
                                                                     'TW01','VNP1')
                                                                and q.process =@str1
                                                                and q.data_week between @fromtime and @endtime ) t1
                                                       full join (SELECT distinct q.data_week date, 'T' t
                                                                   FROM V_wr_q_plrr     q,
                                                                        V_wr_plant_mapping m
                                                                  WHERE m.source_plant_code =
                                                                        q.plant_code
                                                                    and m.target_plant_code in
                                                                        ('KSP3',
                                                                         'KSP2',
                                                                         'KSP4',
                                                                         'CDP1',
                                                                         'CQP1',
                                                                         'TW01','VNP1')
                                                                    and q.process =@str1
                                                                    and q.data_week between @fromtime and @endtime ) t2
                                                         on t1.t = t2.t) b
                                             on (a.date = b.date and a.plant_code = b.plant_code)) b
                                on a.date = b.date
                               and a.plant_code = b.plant_code
                             order by b.plant_code, b.date ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("fromtime", fromTime);
            ht.Add("endtime", endTime);
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

        public DataTable GetLrrRMSPlantAnalysisWeek(string str1, string fromTime, string endTime, string str3Type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string str3 = "";
            //增加cpa 单个查询条件
            if (str1 != "AP")
            {
                if (str3Type == "ALL")
                {
                    str3 = " in ('置件', '印刷', '材料', '制程', 'NTF') ";
                }
                else
                {
                    str3 = " = '" + str3Type + "' ";
                }
            }
            else
            {
                if (str3Type == "ALL")
                {
                    str3 = " in ('作業', '材料', 'PCBA LRR', 'NTF') ";
                }
                else
                {
                    str3 = " = '" + str3Type + "' ";
                }
            }

            string sql = @"SELECT b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case b.value
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round((cast(a.value as float) / b.value) * 100, 2)
                                   end value
                              FROM (select b.plant_code,
                                           b.date,
                                           case
                                             when a.value is null then
                                              0
                                             else
                                              a.value
                                           end value
                                      from (SELECT sum(q.value_real1) value,
                                                   q.data_week date,
                                                   m.target_plant_code plant_code
                                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                                             WHERE q.kpi_category = 'Q0009'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code in
                                                   ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                               and q.value_str3 " + str3 + @"
                                               and q.value_str1 =@str1
                                               and q.data_week between @fromtime and @endtime
                                             group by m.target_plant_code, q.data_week) a
                                     right join (select t1.plant_code, t2.date
                                                  from (SELECT distinct m.target_plant_code plant_code, 'T' t
                                                                   FROM V_wr_q_plrr     q,
                                                                        V_wr_plant_mapping m
                                                                  WHERE m.source_plant_code =q.plant_code
                                                           and m.target_plant_code in
                                                               ('KSP3',
                                                                'KSP2',
                                                                'KSP4',
                                                                'CDP1',
                                                                'CQP1',
                                                                'TW01','VNP1')
                                                           and q.process =@str1
														   and q.data_week between @fromtime and @endtime ) t1
                                                  full join (SELECT distinct q.data_week date, 'T' t
                                                                   FROM V_wr_q_plrr     q,
                                                                        V_wr_plant_mapping m
                                                                  WHERE m.source_plant_code = q.plant_code
                                                               and m.target_plant_code in
                                                                   ('KSP3',
                                                                    'KSP2',
                                                                    'KSP4',
                                                                    'CDP1',
                                                                    'CQP1',
                                                                    'TW01','VNP1')
                                                              and q.process =@str1
														   and q.data_week between @fromtime and @endtime ) t2
                                                    on t1.t = t2.t) b
                                        on (a.date = b.date and a.plant_code = b.plant_code)) a
                              full join (select b.plant_code,
                                                b.date,
                                                case
                                                  when a.value is null then
                                                   0
                                                  else
                                                   a.value
                                                end value
                                           from (SELECT sum(q.station_qty) value,
                                                        q.data_week date,
                                                        m.target_plant_code plant_code
                                                   FROM V_wr_q_plrr q, V_wr_plant_mapping m
                                                  WHERE m.source_plant_code = q.plant_code
                                                    and m.target_plant_code in
                                                        ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                                    and q.process =@str1
												    and q.data_week between @fromtime and @endtime
                                                  group by m.target_plant_code, q.data_week) a
                                          right join (select t1.plant_code, t2.date
                                                       from (SELECT distinct m.target_plant_code plant_code, 'T' t
                                                               FROM V_wr_q_plrr     q,
                                                                    V_wr_plant_mapping m
                                                              WHERE m.source_plant_code = q.plant_code
                                                                and m.target_plant_code in
                                                                    ('KSP3',
                                                                     'KSP2',
                                                                     'KSP4',
                                                                     'CDP1',
                                                                     'CQP1',
                                                                     'TW01','VNP1')
                                                                and q.process =@str1
												    and q.data_week between @fromtime and @endtime ) t1
                                                       full join (SELECT distinct q.data_week date, 'T' t
                                                                   FROM V_wr_q_plrr     q,
                                                                        V_wr_plant_mapping m
                                                                  WHERE m.source_plant_code =
                                                                        q.plant_code
                                                                    and m.target_plant_code in
                                                                        ('KSP3',
                                                                         'KSP2',
                                                                         'KSP4',
                                                                         'CDP1',
                                                                         'CQP1',
                                                                         'TW01','VNP1')
                                                                   and q.process =@str1
												    and q.data_week between @fromtime and @endtime ) t2
                                                         on t1.t = t2.t) b
                                             on (a.date = b.date and a.plant_code = b.plant_code)) b
                                on a.date = b.date
                               and a.plant_code = b.plant_code
                             order by b.plant_code, b.date ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("fromtime", fromTime);
            ht.Add("endtime", endTime);
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

        public DataTable GetLrrRMSPlantAnalysisMonth(string str1, string fromTime, string endTime, string str3Type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string str3 = "";
            //增加cpa 单个查询条件
            if (str1 != "AP")
            {
                if (str3Type == "ALL")
                {
                    str3 = " in ('置件', '印刷', '材料', '制程', 'NTF') ";
                }
                else
                {
                    str3 = " = '" + str3Type + "' ";
                }
            }
            else
            {
                if (str3Type == "ALL")
                {
                    str3 = " in ('作業', '材料', 'PCBA LRR', 'NTF') ";
                }
                else
                {
                    str3 = " = '" + str3Type + "' ";
                }
            }

            string sql = @"SELECT b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case b.value
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round((cast(a.value as float) / b.value) * 100, 2)
                                   end value
                              FROM (select b.plant_code,
                                           b.date,
                                           case
                                             when a.value is null then
                                              0
                                             else
                                              a.value
                                           end value
                                      from (SELECT sum(q.value_real1) value,
                                                   q.data_month date,
                                                   m.target_plant_code plant_code
                                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                                             WHERE q.kpi_category = 'Q0009'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code in
                                                   ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                               and q.value_str3 " + str3 + @"
                                               and q.value_str1 =@str1
                                               and q.data_month between @fromtime and @endtime
                                             group by m.target_plant_code, q.data_month) a
                                     right join (select t1.plant_code, t2.date
                                                  from (SELECT distinct m.target_plant_code plant_code, 'T' t
                                                                   FROM V_wr_q_plrr     q,
                                                                        V_wr_plant_mapping m
                                                                  WHERE m.source_plant_code =q.plant_code
                                                           and m.target_plant_code in
                                                               ('KSP3',
                                                                'KSP2',
                                                                'KSP4',
                                                                'CDP1',
                                                                'CQP1',
                                                                'TW01','VNP1')
                                                           and q.process =@str1
														   and q.data_month between @fromtime and @endtime ) t1
                                                  full join (SELECT distinct q.data_month date, 'T' t
                                                                   FROM V_wr_q_plrr     q,
                                                                        V_wr_plant_mapping m
                                                                  WHERE m.source_plant_code = q.plant_code
                                                               and m.target_plant_code in
                                                                   ('KSP3',
                                                                    'KSP2',
                                                                    'KSP4',
                                                                    'CDP1',
                                                                    'CQP1',
                                                                    'TW01','VNP1')
                                                              and q.process =@str1
														   and q.data_month between @fromtime and @endtime ) t2
                                                    on t1.t = t2.t) b
                                        on (a.date = b.date and a.plant_code = b.plant_code)) a
                              full join (select b.plant_code,
                                                b.date,
                                                case
                                                  when a.value is null then
                                                   0
                                                  else
                                                   a.value
                                                end value
                                           from (SELECT sum(q.station_qty) value,
                                                        q.data_month date,
                                                        m.target_plant_code plant_code
                                                   FROM V_wr_q_plrr q, V_wr_plant_mapping m
                                                  WHERE m.source_plant_code = q.plant_code
                                                    and m.target_plant_code in
                                                        ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CQP1', 'TW01','VNP1')
                                                    and q.process =@str1
												    and q.data_month between @fromtime and @endtime
                                                  group by m.target_plant_code, q.data_month) a
                                          right join (select t1.plant_code, t2.date
                                                       from (SELECT distinct m.target_plant_code plant_code, 'T' t
                                                               FROM V_wr_q_plrr     q,
                                                                    V_wr_plant_mapping m
                                                              WHERE m.source_plant_code = q.plant_code
                                                                and m.target_plant_code in
                                                                    ('KSP3',
                                                                     'KSP2',
                                                                     'KSP4',
                                                                     'CDP1',
                                                                     'CQP1',
                                                                     'TW01','VNP1')
                                                                and q.process =@str1
												    and q.data_month between @fromtime and @endtime ) t1
                                                       full join (SELECT distinct q.data_month date, 'T' t
                                                                   FROM V_wr_q_plrr     q,
                                                                        V_wr_plant_mapping m
                                                                  WHERE m.source_plant_code =
                                                                        q.plant_code
                                                                    and m.target_plant_code in
                                                                        ('KSP3',
                                                                         'KSP2',
                                                                         'KSP4',
                                                                         'CDP1',
                                                                         'CQP1',
                                                                         'TW01','VNP1')
                                                                   and q.process =@str1
												    and q.data_month between @fromtime and @endtime ) t2
                                                         on t1.t = t2.t) b
                                             on (a.date = b.date and a.plant_code = b.plant_code)) b
                                on a.date = b.date
                               and a.plant_code = b.plant_code
                             order by b.plant_code, b.date ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("fromtime", fromTime);
            ht.Add("endtime", endTime);
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

        public DataTable GetLrrMonthlyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string str3 = "('作業', '材料', 'PCBA LRR', 'NTF')";
            if (str1 != "AP")
                str3 = "('置件', '印刷', '材料', '制程', 'NTF')";
            string sql = @"select plant_code,SUBSTRING(data_month,5,len(data_month)-4) date,value from (
                          SELECT a.plant_code,
                           b.data_month,--SUBSTRING(b.data_month,5,len(b.data_month)-4) date,
                           case b.vinput
                             when 0 then
                              0
                             when null then
                              0
                             else
                              round((cast(a.v as float) / b.vinput) * 100, 2)
                           end value
                      FROM (SELECT sum(q.value_real1) v,
                                   q.data_month,
                                   q.plant_code + '-' + q.value_str3 plant_code
                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                             WHERE q.kpi_category = 'Q0009'
                               and m.source_plant_code = q.plant_code
                               and m.target_plant_code = @plant
                               and q.value_str3 in " + str3 + @"
                               and q.value_str1 = @str1
                               and q.data_month between @fromTime and @endTime
                             group by q.plant_code + '-' + q.value_str3, q.data_month) a
                      full join (SELECT sum(q.station_qty) vinput,
                                        q.data_month,
                                        q.plant_code + '-INPUT' plant_code
                                   FROM V_wr_q_plrr q, V_wr_plant_mapping m
                                  WHERE m.source_plant_code = q.plant_code
                                    and m.target_plant_code = @plant
                                    and q.process = @str1
                                    and q.data_month between @fromTime and @endTime
                                  group by q.plant_code + '-INPUT', q.data_month) b
                        on a.data_month = b.data_month ) r
                        where r.plant_code is not null
                     order by r.data_month ";
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

        public DataTable GetLrrWeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string str3 = "('作業', '材料', 'PCBA LRR', 'NTF')";
            if (str1 != "AP")
                str3 = "('置件', '印刷', '材料', '制程', 'NTF')";
            string sql = @"select * from (
                        SELECT a.plant_code,
                           SUBSTRING(b.data_week,5,len(b.data_week)-4) date,
                           case b.vinput
                             when 0 then
                              0
                             when null then
                              0
                             else
                              round((cast(a.v as float) / b.vinput) * 100, 2)
                           end value ,b.data_week 
                      FROM (SELECT sum(q.value_real1) v,
                                   q.data_week,
                                   q.plant_code + '-' + q.value_str3 plant_code
                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                             WHERE q.kpi_category = 'Q0009'
                               and m.source_plant_code = q.plant_code
                               and m.target_plant_code = @plant
                               and q.value_str3 in " + str3 + @"
                               and q.value_str1 = @str1
                               and q.data_week between @fromTime and @endTime
                             group by q.plant_code + '-' + q.value_str3, q.data_week) a
                      full join (SELECT sum(q.station_qty) vinput,
                                        q.data_week,
                                        q.plant_code + '-INPUT'  plant_code
                                   FROM V_wr_q_plrr q, V_wr_plant_mapping m
                                  WHERE m.source_plant_code = q.plant_code
                                    and m.target_plant_code = @plant
                                    and q.process = @str1
                                    and q.data_week between @fromTime and @endTime
                                  group by q.plant_code + '-INPUT', q.data_week) b
                        on a.data_week = b.data_week ) r
                        where r.plant_code is not null 
                     order by r.plant_code desc ,r.data_week  ";
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

        public DataTable GetDutyTypeCategory(string str1, string plant, string time)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string str3 = "('作業', '材料', 'PCBA LRR', 'NTF')";
            if (str1 != "AP")
                str3 = "('置件', '印刷', '材料', '制程', 'NTF')";
            string sql = @" SELECT q.value_str3 category, sum(q.value_real1) value
                                    FROM V_wr_q_generic q, V_wr_plant_mapping m
                                   WHERE q.kpi_category = 'Q0009'
                                     and m.source_plant_code = q.plant_code
                                     and m.target_plant_code = @plant
                                     and q.value_str3 in " + str3 + @"
                                     and q.value_str1 = @str1
                                     and q.data_week = @time
                                     and q.value_real1 is not null
                                     group by q.value_str3 ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("time", time);
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

        #region PCBA LRR
        //TTL
        public DataTable GetPcbaLrrPlantAnalysisWFRWeek(string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            string sql = @"
                        SELECT p.RMS_PLANT as PLANT_CODE,
                               'W' || substr(pl.week_code, 5, 6) as work_date,
                               NVL(sum(qty), 0) value
                          FROM SFISM4.R_DAILY_PCBALRR_T pl, sfis1.c_plant_def_t p
                         where pl.plant_code = p.mes_plant
                           AND pl.WORK_DATE >= :fromTime
                           AND pl.WORK_DATE < :endtime
                           and pl.duty_party = 'LRR WFR'
                           and (p.RMS_PLANT in ('KSP3',
                                                'KSP2',
                                                'KSP4',
                                                --'CDP1',
                                                'CQP1',
                                                'CTY1',
                                                'VNP1') or
                               (p.RMS_PLANT = 'CDP1' AND pl.cust_no != 'EDOCKING'))
                           and p.bu_code = 'PCBG'
                         group by p.RMS_PLANT, pl.week_code
                         order by p.RMS_PLANT, pl.week_code
                        ";
            ht.Clear();
            ht.Add("fromtime", fromTime);
            ht.Add("endtime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
                else
                    meslog.Error("GetPcbaLrrPlantAnalysis" + exeRes.Message);
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                meslog.Error(ex.Message);
            }
            return dtResult;
        }
        //TTL
        public DataTable GetPcbaLrrPlantAnalysisWFRMonth(string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            string sql = @"
                        SELECT PLANT_CODE, 'M' || substr(work_date, 5, 6) WORK_DATE, value
                          FROM (SELECT p.RMS_PLANT as PLANT_CODE,
                                       substr(pl.work_date, 1, 6) as work_date,
                                       NVL(sum(qty), 0) value
                          FROM SFISM4.R_DAILY_PCBALRR_T pl, sfis1.c_plant_def_t p
                         where pl.plant_code = p.mes_plant
                           AND pl.WORK_DATE >= :fromTime
                           AND pl.WORK_DATE < :endtime
                           and pl.duty_party = 'LRR WFR'
                           and (p.RMS_PLANT in ('KSP3',
                                                'KSP2',
                                                'KSP4',
                                                --'CDP1',
                                                'CQP1',
                                                'CTY1',
                                                'VNP1') or
                               (p.RMS_PLANT = 'CDP1' AND pl.cust_no != 'EDOCKING'))
                           and p.bu_code = 'PCBG'
                         group by p.RMS_PLANT, substr(pl.work_date, 1, 6)
                         order by p.RMS_PLANT, substr(pl.work_date, 1, 6))";
            ht.Clear();
            ht.Add("fromtime", fromTime);
            ht.Add("endtime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
                else
                    meslog.Error("GetPcbaLrrPlantAnalysis" + exeRes.Message);
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                meslog.Error(ex.Message);
            }
            return dtResult;
        }
        public string GetStationSummaryWhere(string plant)
        {
            return GetStationSummaryWhere(plant, "");
        }
        public string GetStationSummaryWhere(string plant, string vEx)
        {
            string sqlWhere = string.Empty;
            sqlWhere += " AND T.DATE_TYPE = 'D' ";
            sqlWhere += " and t.mo_type = 'ZP01' ";
            sqlWhere += " AND T.FROM_DB NOT LIKE '%SMT%' ";
            if (plant == "KSP4")
                sqlWhere += " AND T.CUST_NO IN ('A39','A58','C38','H38','N88') ";
            if (plant == "VNP1" || plant == "CDP1")
            {
                sqlWhere += " AND t.prod_type IN ('PAD', 'NB', 'AIO','DOCKING')  ";
                sqlWhere += " AND ((T.GROUP_NAME || '' = 'STRU'AND t.prod_type !='DOCKING') OR(T.GROUP_NAME || '' = 'AFT ' AND t.prod_type ='DOCKING'))  ";
                if (vEx != "")
                    sqlWhere += " AND ((T.PLANT_CODE='VN12' and t.from_db = :ex2) or (T.PLANT_CODE !='VN12')) ";
            }
            else
            {
                sqlWhere += " AND t.prod_type IN ('PAD', 'NB', 'AIO')  ";
                sqlWhere += " AND T.GROUP_NAME || '' = 'STRU'  ";
            }
            return sqlWhere;
        }

        public DataTable GetPcbaLrrPlantAnalysisWeek(string fromTime, string endTime, string str3Type, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string str3 = "",  strEx2 = "", strEx3 = "";
            if (strExType != "")
            {
                strEx2 = " and ((p.rms_plant='VNP1' and pl.cust_no=:ex1) or (p.rms_plant!='VNP1'))  ";
                strEx3 = " and ((p.rms_plant='VNP1' and t.from_db = :ex2) or (p.rms_plant!='VNP1'))   ";
            }
            //增加cpa 单个查询条件
            if (str3Type == "ALL")
            {
                str3 = " ='SMT' ";
            }
            else
            {
                str3 = " = '" + str3Type + "' ";
            }
            string sql = @"select case plant_code when 'VNP1' then plant_code||:explant else plant_code end as plant_code,
       'W' || substr(work_date, 5, 6) as work_date,
       nvl(value, 0) value
  from (select a.PLANT_CODE,
               a.work_date,
       case
         when b.ouput_qty = 0 then
          0
         when b.ouput_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.ouput_qty * 100, 2)
       end value 
          from (SELECT p.RMS_PLANT as PLANT_CODE,
                       pl.week_code work_date,
                       NVL(sum(qty), 0) qty
                  FROM SFISM4.R_DAILY_PCBALRR_T pl, sfis1.c_plant_def_t p
                 where pl.plant_code = p.mes_plant
                   AND pl.WORK_DATE >=  :fromTime
                   AND pl.WORK_DATE <:endTime
                   and pl.duty_party " + str3 + @"
                   and (p.RMS_PLANT in ('KSP3',
                                       'KSP2',
                                       'KSP4',
                                       --'CDP1',
                                       'CQP1',
                                       'CTY1',
                                       'VNP1') or (p.RMS_PLANT='CDP1' AND pl.cust_no !='EDOCKING' ))
                   and p.bu_code = 'PCBG'" + strEx2 + @"
                 group by p.RMS_PLANT, pl.week_code
                 order by p.RMS_PLANT, pl.week_code) a,
               (SELECT p.RMS_PLANT as PLANT_CODE,
                       TO_CHAR(TO_DATE(t.work_date || '000000',
                                       'YYYYMMDDHH24MISS'), 'IYYYIW') work_date,
                       NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                  FROM SFISM4.r_station_summary_t T, sfis1.c_plant_def_t p
                 WHERE T.PLANT_CODE = p.sap_plant
                   AND T.DATE_TYPE = 'D'
                   and t.mo_type = 'ZP01'
                   and (t.prod_type IN ('PAD', 'NB', 'AIO') OR (t.prod_type IN ('PAD', 'NB', 'AIO','DOCKING')and P.RMS_PLANT = 'VNP1' ))
                   AND T.FROM_DB NOT LIKE '%SMT%'
                   AND (1=1 OR(T.PLANT_CODE='CN55' AND T.CUST_NO IN ('A39','A58','C38','H38','N88')) )
                   AND ((T.GROUP_NAME || '' = 'STRU' AND t.prod_type != 'DOCKING') OR
               (T.GROUP_NAME || '' = 'AFT ' AND t.prod_type = 'DOCKING' and P.RMS_PLANT = 'VNP1'))
                   AND T.WORK_DATE >=  :fromTime
                   AND T.WORK_DATE <:endTime
                   and p.RMS_PLANT in ('KSP3',
                                       'KSP2',
                                       'KSP4',
                                       'CDP1',
                                       'CQP1',
                                       'CTY1',
                                       'VNP1')
                   and p.bu_code = 'PCBG'" + strEx3 + @"
                 group by p.RMS_PLANT,
                          TO_CHAR(TO_DATE(t.work_date || '000000',
                                          'YYYYMMDDHH24MISS') ,'IYYYIW')) b
         where b.PLANT_CODE = a.PLANT_CODE(+)
           and b.work_date = a.work_date(+)
           AND A.PLANT_CODE IS NOT NULL
         order by plant_code, work_date)
";
            ht.Clear();
            ht.Add("fromtime", fromTime);
            ht.Add("endtime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                    ht.Add("ex2", "CHV_EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                    ht.Add("ex2", "CHV_A31");
                }
                ht.Add("explant", strExType);
            }
            else
                ht.Add("explant", "");
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
                else
                    meslog.Error("GetPcbaLrrPlantAnalysis" + exeRes.Message);
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                meslog.Error(ex.Message);
            }
            return dtResult;
        }

        public DataTable GetPcbaLrrPlantAnalysisMonth(string fromTime, string endTime, string str3Type, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string str3 = "",   strEx2 = "", strEx3 = "";
            if (strExType != "")
            {
                strEx2 = " and ((p.rms_plant='VNP1' and pl.cust_no=:ex1) or (p.rms_plant!='VNP1'))  ";
                strEx3 = " and ((p.rms_plant='VNP1' and t.from_db = :ex2) or (p.rms_plant!='VNP1'))   ";
            }
            //增加cpa 单个查询条件
            if (str3Type == "ALL")
            {
                str3 = " ='SMT' ";
            }
            else
            {
                str3 = " = '" + str3Type + "' ";
            }
            string sql = @"select case plant_code when 'VNP1' then plant_code||:explant else plant_code end as plant_code,
       'M' || substr(work_date, 5, 6) as work_date,
       nvl(value, 0) value
  from (select a.PLANT_CODE,
               a.work_date,
       case
         when b.ouput_qty = 0 then
          0
         when b.ouput_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.ouput_qty * 100, 2)
       end value 
          from (SELECT p.RMS_PLANT as PLANT_CODE,
                       SUBSTR(pl.work_date,1,6) work_date,
                       NVL(sum(qty), 0) qty
                  FROM SFISM4.R_DAILY_PCBALRR_T pl, sfis1.c_plant_def_t p
                 where pl.plant_code = p.mes_plant
                   AND pl.WORK_DATE >=  :fromTime
                   AND pl.WORK_DATE <:endTime
                   and pl.duty_party " + str3 + @"
                   and (p.RMS_PLANT in ('KSP3',
                                       'KSP2',
                                       'KSP4',
                                       --'CDP1',
                                       'CQP1',
                                       'CTY1',
                                       'VNP1') or (p.RMS_PLANT='CDP1' AND pl.cust_no !='EDOCKING' ))
                   and p.bu_code = 'PCBG'" + strEx2 + @"
                 group by p.RMS_PLANT, SUBSTR(pl.work_date,1,6)
                 order by p.RMS_PLANT, SUBSTR(pl.work_date,1,6)) a,
               (SELECT p.RMS_PLANT as PLANT_CODE,
                       SUBSTR(T.WORK_DATE,1,6) work_date,
                       NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                  FROM SFISM4.r_station_summary_t T, sfis1.c_plant_def_t p
                 WHERE T.PLANT_CODE = p.sap_plant
                   AND T.DATE_TYPE = 'D'
                   and t.mo_type = 'ZP01'
                   and (t.prod_type IN ('PAD', 'NB', 'AIO') OR (t.prod_type IN ('PAD', 'NB', 'AIO','DOCKING')and P.RMS_PLANT = 'VNP1' ))
                   AND T.FROM_DB NOT LIKE '%SMT%'
                   AND (1=1 OR(T.PLANT_CODE='CN55' AND T.CUST_NO IN ('A39','A58','C38','H38','N88')) )
                   AND ((T.GROUP_NAME || '' = 'STRU' AND t.prod_type != 'DOCKING') OR
               (T.GROUP_NAME || '' = 'AFT ' AND t.prod_type = 'DOCKING' and P.RMS_PLANT = 'VNP1'))
                   AND T.WORK_DATE >=  :fromTime
                   AND T.WORK_DATE <:endTime
                   and p.RMS_PLANT in ('KSP3',
                                       'KSP2',
                                       'KSP4',
                                       'CDP1',
                                       'CQP1',
                                       'CTY1',
                                       'VNP1')
                   and p.bu_code = 'PCBG'" + strEx3 + @"
                 group by p.RMS_PLANT,
                         SUBSTR(T.WORK_DATE,1,6)) b
         where b.PLANT_CODE = a.PLANT_CODE(+)
           and b.work_date = a.work_date(+)
           AND A.PLANT_CODE IS NOT NULL
         order by plant_code, work_date)
";
            ht.Clear();
            ht.Add("fromtime", fromTime);
            ht.Add("endtime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                    ht.Add("ex2", "CHV_EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                    ht.Add("ex2", "CHV_A31");
                }
                ht.Add("explant", strExType);
            }
            else
                ht.Add("explant", "");
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
                else
                    meslog.Error("GetPcbaLrrPlantAnalysis" + exeRes.Message);
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                meslog.Error(ex.Message);
            }
            return dtResult;
        }

        public DataTable GetPcbaLrrMonthlyTrend(string plant, string fromTime, string endTime, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlCust = "";
            if (strExType != "")
                sqlCust += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlCust += " and pl.cust_no !='EDOCKING' ";
            string str3 = "('SMT','FATP','PE','NTF','Small/B','LRR WFR') ";
            string sql = @"select plant_code,
       'M' || substr(WORK_DATE, 5, 6) as work_date,
       nvl(value, 0) value
  from (select a.plant_code plant_code,
               a.work_date,
       case
         when b.ouput_qty = 0 then
          0
         when b.ouput_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.ouput_qty * 100, 2)
       end value 
          from (SELECT pl.plant_code ||:explant || '-' || pl.duty_party as plant_code,
                substr(pl.WORK_DATE, 0, 6) work_date,
                nvl(sum(qty), 0) qty
           FROM SFISM4.R_DAILY_PCBALRR_T pl
          where pl.plant_code = (select p.mes_plant
                                   from sfis1.c_plant_def_t p
                                  where p.bu_code = 'PCBG'
                                    and P.RMS_PLANT = :plant
                                    and rownum = 1) " + sqlCust + @"
            AND pl.WORK_DATE >=  :fromTime
            AND pl.WORK_DATE < :endTime 
            and pl.duty_party in " + str3 + @"
          group by pl.plant_code ||:explant || '-' || pl.duty_party,
                   substr(pl.WORK_DATE, 0, 6)
          order by pl.plant_code ||:explant || '-' || pl.duty_party,
                   substr(pl.WORK_DATE, 0, 6)) a,
               (SELECT T.PLANT_CODE,
                       substr(T.WORK_DATE, 0, 6) WORK_DATE,
                       NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                  FROM SFISM4.r_station_summary_t T
                 WHERE T.PLANT_CODE =   (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                   " + GetStationSummaryWhere(plant, strExType) + @"
                   AND T.WORK_DATE >= :fromTime
                   AND T.WORK_DATE < :endTime 
                 group by T.PLANT_CODE, substr(T.WORK_DATE, 0, 6)
                 order by T.PLANT_CODE, substr(T.WORK_DATE, 0, 6)) b
         where b.WORK_DATE = a.WORK_DATE(+)
             and a.WORK_DATE is not null
         order by WORK_DATE)  ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                    ht.Add("ex2", "CHV_EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                    ht.Add("ex2", "CHV_A31");
                }
                ht.Add("explant", strExType);
            }
            else
                ht.Add("explant", "");
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
                else
                    meslog.Error("GetPcbaLrrMonthlyTrend" + exeRes.Message);
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                meslog.Error(ex.Message);
            }
            return dtResult;
        }

        public DataTable GetPcbaLrrWeeklyTrend(string plant, string fromTime, string endTime, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlCust = "";
            if (strExType != "")
                sqlCust += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlCust += " and pl.cust_no !='EDOCKING' ";
            string str3 = "('SMT','FATP','PE','NTF','Small/B','LRR WFR') ";
            string sql = @"select a.plant_code,
       'W' || substr(a.work_date, 5, 6) as work_date,
       case
         when b.ouput_qty = 0 then
          0
         when b.ouput_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.ouput_qty * 100, 2)
       end value
  from (SELECT pl.plant_code ||:explant || '-' || pl.duty_party as plant_code,
               pl.week_code WORK_DATE,
               NVL(sum(qty) ,0) qty
          FROM SFISM4.R_DAILY_PCBALRR_T pl
         where pl.plant_code = (select p.mes_plant
                                  from sfis1.c_plant_def_t p
                                 where p.bu_code = 'PCBG'
                                   and P.RMS_PLANT = :plant
                                   and rownum = 1) " + sqlCust + @"
           AND pl.WORK_DATE >= :fromTime
           AND pl.WORK_DATE < :endTime 
           and pl.duty_party in " + str3 + @"
         group by pl.plant_code ||:explant || '-' || pl.duty_party, pl.week_code
         order by pl.plant_code ||:explant || '-' || pl.duty_party, pl.week_code) a,
       (SELECT T.PLANT_CODE,
               TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') work_date,
               NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T
         WHERE T.PLANT_CODE =  (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant, strExType) + @"
           AND T.WORK_DATE >= :fromTime
           AND T.WORK_DATE < :endTime 
         group by T.PLANT_CODE,
                  TO_CHAR(TO_DATE(t.work_date || '000000',
                                  'YYYYMMDDHH24MISS'),
                          'IYYYIW')) b
 where b.work_date = a.work_date(+)
   and a.WORK_DATE is not null
 order by a.WORK_DATE  ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                    ht.Add("ex2", "CHV_EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                    ht.Add("ex2", "CHV_A31");
                }
                ht.Add("explant", strExType);
            }
            else
                ht.Add("explant", "");
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
                else
                    meslog.Error("GetPcbaLrrWeeklyTrend" + exeRes.Message);
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                meslog.Error(ex.Message);
            }
            return dtResult;
        }

        public DataTable GetPcbaLrrDailyTrend(string plant, string fromTime, string endTime, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlCust = "";
            if (strExType != "")
                sqlCust += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlCust += " and pl.cust_no !='EDOCKING' ";
            string str3 = "('SMT','FATP','PE','NTF','Small/B','LRR WFR') ";
            string sql = @"select a.plant_code,
       substr(a.work_date, 5, 2)||'/'|| substr(a.work_date, 7, 2) as work_date,
       case
         when b.ouput_qty = 0 then
          0
         when b.ouput_qty is null then
          0
         when a.qty is null then
          0
         else
          round(a.qty / b.ouput_qty * 100, 2)
       end value
  from (SELECT pl.plant_code ||:explant || '-' || pl.duty_party as plant_code,
               pl.WORK_DATE,
               NVL(sum(qty) ,0) qty
          FROM SFISM4.R_DAILY_PCBALRR_T pl
         where pl.plant_code = (select p.mes_plant
                                  from sfis1.c_plant_def_t p
                                 where p.bu_code = 'PCBG'
                                   and P.RMS_PLANT = :plant
                                   and rownum = 1) " + sqlCust + @"
           AND pl.WORK_DATE >= :fromTime
           AND pl.WORK_DATE < :endTime 
           and pl.duty_party in " + str3 + @"
         group by pl.plant_code ||:explant || '-' || pl.duty_party, pl.WORK_DATE
         order by pl.plant_code ||:explant || '-' || pl.duty_party, pl.WORK_DATE) a,
       (SELECT T.PLANT_CODE,
               T.WORK_DATE,
               NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T
         WHERE T.PLANT_CODE =  (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant, strExType) + @"
           AND T.WORK_DATE >= :fromTime
           AND T.WORK_DATE < :endTime 
         group by T.PLANT_CODE,
                  T.WORK_DATE) b
 where b.work_date = a.work_date(+)
   and a.WORK_DATE is not null
 order by WORK_DATE  ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                    ht.Add("ex2", "CHV_EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                    ht.Add("ex2", "CHV_A31");
                }
                ht.Add("explant", strExType);
            }
            else
                ht.Add("explant", "");
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
                else
                    meslog.Error("GetPcbaLrrWeeklyTrend" + exeRes.Message);
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                meslog.Error(ex.Message);
            }
            return dtResult;
        }

        public DataTable GetPcbaLrrDutyTypeCategory(string plant, string fromTime, string endTime, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlCust = "";
            if (strExType != "")
                sqlCust += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlCust += " and pl.cust_no !='EDOCKING' ";
            string sql = @" SELECT pl.duty_party as category, NVL(sum(qty) ,0) value
             FROM SFISM4.R_DAILY_PCBALRR_T pl
            where pl.plant_code = (select p.mes_plant
                                              from sfis1.c_plant_def_t p
                                             where p.bu_code = 'PCBG'
                                               and P.RMS_PLANT = :plant
                                               and rownum = 1) " + sqlCust + @"
              AND pl.WORK_DATE >= :fromTime
              AND pl.WORK_DATE < :endTime
              and pl.duty_party in ('SMT','FATP','PE','NTF','Small/B')
            group by pl.duty_party ";

            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                    ht.Add("ex1", "EDOCKING");
                else if (strExType == "_NB")
                    ht.Add("ex1", "A31");
            }
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
                else
                    meslog.Error("GetPcbaLrrDutyTypeCategory" + exeRes.Message);
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
                meslog.Error(ex.Message);
            }
            return dtResult;
        }

        public DataTable GetPcbaLrrDutyTypeCategory2(string plant, string fromTime, string endTime, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlCust = "";
            if (strExType != "")
                sqlCust += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlCust += " and pl.cust_no !='EDOCKING' ";
            string sql = @" SELECT a.category,
       case
         when ouput_qty = 0 then
          0
         when ouput_qty is null then
          0
         when qty is null then
          0
         else
          round(qty / ouput_qty * 100, 2)
       end value
  FROM (SELECT pl.duty_party as category, NVL(sum(qty) ,0) qty
          FROM SFISM4.R_DAILY_PCBALRR_T pl
         where pl.plant_code = (select p.mes_plant
                                  from sfis1.c_plant_def_t p
                                 where p.bu_code = 'PCBG'
                                   and P.RMS_PLANT = :plant
                                   and rownum = 1) " + sqlCust + @"
           AND pl.WORK_DATE >= :fromTime
           AND pl.WORK_DATE < :endTime
           and pl.duty_party in ('SMT', 'FATP', 'PE', 'NTF', 'Small/B')
         group by pl.duty_party) A,
       (SELECT NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T
         WHERE T.PLANT_CODE =   (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant, strExType) + @"
           AND T.WORK_DATE >= :fromTime
           AND T.WORK_DATE < :endTime ) b
           order by value desc";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                    ht.Add("ex2", "CHV_EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                    ht.Add("ex2", "CHV_A31");
                }
            }
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
                else
                    meslog.Error("GetPcbaLrrDutyTypeCategory2" + exeRes.Message);
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetPcbaLrrL2Data(string str3Type, string plant, string fromTime, string endTime, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlwhere = string.Empty;
            string sqlcolumn = string.Empty;
            string sqlgroup = string.Empty;
            if (strExType != "")
                sqlwhere += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlwhere += " and pl.cust_no !='EDOCKING' ";
            if (str3Type != "PE")
            {
                sqlcolumn = "pl.reason_code,";
                sqlgroup = "pl.reason_code";
            }
            else
            {
                sqlcolumn = "pl.location_type as reason_code,";
                sqlgroup = "pl.location_type";
            }
            #region
            string sql = @" select REASON_CODE,a.qty, case when a.qty is null then 0 when b.ouput_qty is null then 0 when b.ouput_qty =0 then 0  else round(a.qty / b.ouput_qty * 100, 3)  end  rate,
       case
         when a.qty is null then
          0
         when c.qty is null then
          0
         when c.qty = 0 then
          0
         else
          round(a.qty / c.qty * 100, 3)
       end rate2
  from (select * from (SELECT " + sqlcolumn + @" sum(pl.qty) qty
          from sfism4.r_daily_pcbalrr_t pl
         where pl.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)    " + sqlwhere + @"
             and pl.work_date >= :fromTime
            and pl.WORK_DATE < :endTime
            and pl.duty_party = :str3Type
         group by " + sqlgroup + @"
         order by qty desc) where rownum<6) a,
       (SELECT NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T
         WHERE T.PLANT_CODE =  (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant, strExType) + @"
           AND T.WORK_DATE >=  :fromTime
           AND T.WORK_DATE < :endTime ) b ,
       (SELECT NVL(sum(pl.qty), 0) qty
          from sfism4.r_daily_pcbalrr_t pl
         where pl.plant_code = (select p.mes_plant
                                  from sfis1.c_plant_def_t p
                                 where p.bu_code = 'PCBG'
                                   and p.rms_plant =:plant
                                   and rownum <= 1)
           and pl.work_date >= :fromTime
           and pl.WORK_DATE < :endTime
           and pl.duty_party = :str3Type) c
  order by  a.qty desc  ";
            #endregion
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str3Type", str3Type);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                    ht.Add("ex2", "CHV_EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                    ht.Add("ex2", "CHV_A31");
                }
            }
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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
        # region L2ByReasonDaily
        public DataTable GetL2ByReasonDailyTrend(string duty_party, string plant, string fromTime, string endTime, string reason_code, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlwhere = string.Empty;
            if (strExType != "")
                sqlwhere += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlwhere += " and pl.cust_no !='EDOCKING' ";
            if (duty_party == "PE")
                sqlwhere += "  and ((pl.duty_party = :duty_party and pl.location_type = :reason_code ) OR pl.duty_party='LRR WFR') ";
            else
                sqlwhere += "  and ((pl.duty_party = :duty_party and pl.reason_code = :reason_code ) OR pl.duty_party='LRR WFR') ";
            string sql = "";
            #region
            sql = @"
select substr(a.work_date, 5, 2) || '/' || substr(a.work_date, 7, 2) work_date, 
case when a.duty_party='LRR WFR' then a.duty_party else 'DPPM' end plant_code,
case when a.qty is null then 0 when b.ouput_qty is null then 0 when b.ouput_qty =0 then 0  when a.duty_party = 'LRR WFR' then
          round(a.qty / b.ouput_qty * 100, 2)
         else
          round(a.qty / b.ouput_qty * 1000000, 0)  end  value
  from (select pl.work_date, pl.duty_party, sum(qty) qty
          from sfism4.r_daily_pcbalrr_t   pl,
               sfis1.c_plant_def_t        p
         where pl.plant_code = p.mes_plant
           and p.bu_code = 'PCBG'
           and pl.WORK_DATE >= :fromTime
           and pl.WORK_DATE < :endTime
           and p.rms_plant = :plant
           " + sqlwhere + @"
         group by pl.work_date, pl.duty_party) a,
       (SELECT t.work_date,
               NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T
         WHERE T.PLANT_CODE =  (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant, strExType) + @"
           AND T.WORK_DATE >= :fromTime
           AND T.WORK_DATE < :endTime
         group by t.work_date) b
 where a.work_date = b.work_date(+)
   and a.work_date is not null
 order by a.work_date   ";
            #endregion
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("reason_code", reason_code);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                    ht.Add("ex2", "CHV_EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                    ht.Add("ex2", "CHV_A31");
                }
            }
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetL2ByReasonMonthlyTrend(string duty_party, string plant, string fromTime, string endTime, string reason_code, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlwhere = string.Empty;
            if (strExType != "")
                sqlwhere += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlwhere += " and pl.cust_no !='EDOCKING' ";
            DataTable dt0 = new DataTable();
            string sql = "";
            if (duty_party == "PE")
                sqlwhere += "  and ((pl.duty_party = :duty_party and pl.location_type = :reason_code ) OR pl.duty_party='LRR WFR') ";
            else
                sqlwhere += "  and ((pl.duty_party = :duty_party and pl.reason_code = :reason_code ) OR pl.duty_party='LRR WFR') ";
            #region 分子
            sql = @"select 'M' || substr(a.work_date, 5, 2) work_date,case when a.duty_party='LRR WFR' then a.duty_party else 'DPPM' end plant_code, a.qty,0.0 value
  from (select substr(pl.work_date, 0, 6) work_date,pl.duty_party, nvl(sum(qty),0)  qty
          from sfism4.r_daily_pcbalrr_t   pl,
               sfis1.c_plant_def_t        p
         where pl.plant_code = p.mes_plant
           and p.bu_code = 'PCBG'
           and pl.WORK_DATE >= :fromTime
           and pl.WORK_DATE < :endTime
           and p.rms_plant = :plant 
           " + sqlwhere + @"
         group by substr(pl.work_date, 0, 6),pl.duty_party) a
 order by a.work_date    ";
            #endregion
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("reason_code", reason_code);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                }
            }
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                        if (dtResult.Rows.Count > 0)
                        {
                            #region 分母
                            sql = @"select 'M' || substr(work_date, 5, 2) work_date, ouput_qty from (SELECT substr(t.work_date, 0, 6) work_date,
               NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T,
               sfis1.c_plant_def_t        p
         WHERE T.PLANT_CODE = P.SAP_PLANT
           " + GetStationSummaryWhere(plant, strExType) + @"
           AND T.WORK_DATE >= :fromTime
           AND T.WORK_DATE < :endTime
           and p.rms_plant = :plant
           and p.bu_code = 'PCBG'
         group by substr(t.work_date, 0, 6)
          order by substr(t.work_date, 0, 6))";
                            ht.Clear();
                            ht.Add("plant", plant);
                            ht.Add("fromTime", fromTime);
                            ht.Add("endTime", endTime);
                            if (strExType != "")
                            {
                                if (strExType == "_DOCK")
                                {
                                    ht.Add("ex2", "CHV_EDOCKING");
                                }
                                else if (strExType == "_NB")
                                {
                                    ht.Add("ex2", "CHV_A31");
                                }
                            }
                            exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
                            #endregion
                            if (exeRes.Status)
                            {
                                ds = (DataSet)exeRes.Anything;
                                if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                                {
                                    dt0 = ds.Tables[0];
                                    if (dt0.Rows.Count > 0)
                                    {
                                        foreach (DataRow dr in dtResult.Rows)
                                        {
                                            DataRow[] drs = dt0.Select(" work_date='" + dr["work_date"].ToString() + "'");
                                            if (drs.Length > 0)
                                            {
                                                if (dr["plant_code"].ToString() == "DPPM")
                                                    dr["value"] = Math.Round(Convert.ToDouble(dr["qty"].ToString()) / Convert.ToDouble(drs[0]["ouput_qty"].ToString()) * 1000000, 0);
                                                else
                                                    dr["value"] = Math.Round(Convert.ToDouble(dr["qty"].ToString()) / Convert.ToDouble(drs[0]["ouput_qty"].ToString()) * 100, 2);
                                            }
                                            else
                                                dr["value"] = 0.0;
                                        }
                                    }
                                }
                            }
                        }
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

        public DataTable GetL2ByReasonWeeklyTrend(string duty_party, string plant, string fromTime, string endTime, string reason_code, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlwhere = string.Empty;
            if (strExType != "")
                sqlwhere += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlwhere += " and pl.cust_no !='EDOCKING' ";
            string sql = " ";
            if (duty_party == "PE")
                sqlwhere += "  and ((pl.duty_party = :duty_party and pl.location_type = :reason_code ) OR pl.duty_party='LRR WFR') ";
            else
                sqlwhere += "  and ((pl.duty_party = :duty_party and pl.reason_code = :reason_code ) OR pl.duty_party='LRR WFR') ";
            sql = @"
select 'W' || Substr(a.work_date, 5, 2) work_date,
      case when a.duty_party='LRR WFR' then a.duty_party else 'DPPM' end plant_code,
case when a.qty is null then 0 when b.ouput_qty is null then 0 when b.ouput_qty =0 then 0  when a.duty_party = 'LRR WFR' then
          round(a.qty / b.ouput_qty * 100, 2)
         else
          round(a.qty / b.ouput_qty * 1000000, 0)  end  value
  from (select pl.week_code work_date,pl.duty_party, sum(qty) qty
                  from sfism4.r_daily_pcbalrr_t   pl,
                       sfis1.c_plant_def_t        p
                 where pl.plant_code = p.mes_plant
                   and p.bu_code = 'PCBG'
                   and pl.WORK_DATE >= :fromTime
                   and pl.WORK_DATE < :endTime
                   and p.rms_plant = :plant 
                   " + sqlwhere + @"
                 group by pl.week_code,pl.duty_party) a,
               (SELECT TO_CHAR(TO_DATE(t.work_date || '010000',
                                       'YYYYMMDDHH24MISS'),
                               'IYYYIW') work_date,
                       NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                  FROM SFISM4.r_station_summary_t T
                 WHERE T.PLANT_CODE =  (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant, strExType) + @"
                   AND T.WORK_DATE >= :fromTime
                   AND T.WORK_DATE < :endTime
                 group by TO_CHAR(TO_DATE(t.work_date || '010000',
                                       'YYYYMMDDHH24MISS'),
                               'IYYYIW')) b
         where a.work_date = b.work_date(+)
           and a.work_date is not null
 order by a.work_date  ";
            #endregion
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("reason_code", reason_code);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                    ht.Add("ex2", "CHV_EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                    ht.Add("ex2", "CHV_A31");
                }
            }
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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
        public DataTable GetPcbaLrrTop5Model_serials(string duty_party, string plant, string fromTime, string endTime, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlCust = "";
            if (strExType != "")
                sqlCust += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            if (plant.Contains("CD"))
                sqlCust += " and pl.cust_no !='EDOCKING' ";
            string sql = @"select model_serial
  from (select model_serial, sum(qty) qty
          from (SELECT a.*, nvl(b.model_serial, 'NA') model_serial
                  from (SELECT pl.model_name, pl.qty
                          from sfism4.r_daily_pcbalrr_t pl
                         where pl.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                            " + sqlCust + @"
                           AND pl.WORK_DATE >= :fromTime
                           AND pl.WORK_DATE < :endTime
                           and pl.duty_party = :duty_party) a,
                       (SELECT m.model_name, m.model_serial
                          FROM sfis1.c_model_serial_def_t m
                         where m.plant_code = :plant
                           and m.bu_code = 'PCBG') b
                 WHERE a.model_name = b.model_name(+)
                   and a.model_name is not null)
         group by model_serial
         order by qty desc)
 where rownum < 6 ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                }
            }
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetPcbaLrrNumerator(string duty_party, string plant, string fromTime, string endTime, string strExType, string reason_code = "", string model_serial = "", string floor = "", string location_code = "", bool L4PEButton = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlcolumn = string.Empty;
            string sqlcolumn1 = string.Empty;
            string sqlwhere = string.Empty;
            string sqlgroupby = string.Empty;
            if (strExType != "")
                sqlwhere += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
            sqlwhere += @" AND pl.WORK_DATE >= :fromTime
                                           AND pl.WORK_DATE < :endTime
                                           and pl.duty_party =:duty_party ";

            if (plant.Contains("CD"))
                sqlwhere += " and pl.cust_no !='EDOCKING' ";
            if (!string.IsNullOrEmpty(location_code))
                sqlwhere += " and pl.location_code in " + location_code;
            if (floor == "L4")
            {
                if (L4PEButton)//SMT or FATP byPE
                    sqlcolumn += " pl.location_type location_code,";
                else
                    sqlcolumn += " pl.location_code,";
                sqlcolumn1 += " pl.location_code,";
                sqlgroupby += ",pl.location_code";
            }
            if (duty_party != "PE")
            {
                sqlcolumn += " pl.reason_code,";
                sqlcolumn1 += " pl.reason_code,";
                sqlwhere += " and pl.reason_code in " + reason_code;
                sqlgroupby += ",pl.reason_code";
            }
            else
            {
                sqlcolumn += " pl.location_type as reason_code,";
                sqlcolumn1 += " pl.reason_code,";
                sqlwhere += " and pl.location_type in " + reason_code;
                sqlgroupby += ",pl.reason_code";
            }
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                {
                    ht.Add("ex1", "EDOCKING");
                }
                else if (strExType == "_NB")
                {
                    ht.Add("ex1", "A31");
                }
            }
            string sql = "";
            #region 分子
            sql = @"  SELECT " + sqlcolumn1 + @"pl.model_serial, sum(pl.qty) qty
    FROM (SELECT a.*, nvl(b.model_serial, 'NA') model_serial
            from (SELECT " + sqlcolumn + @"pl.model_name, pl.qty
                    from sfism4.r_daily_pcbalrr_t pl
                   where pl.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                     " + sqlwhere + @" ) a,
                 (SELECT m.model_name, m.model_serial
                    FROM sfis1.c_model_serial_def_t m
                   where m.plant_code = :plant
                     and m.bu_code = 'PCBG' ) b
           WHERE a.model_name = b.model_name(+)) pl
   where pl.model_serial in " + model_serial + @"
   group by pl.model_serial" + sqlgroupby;
            #endregion
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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
        public DataTable GetPcbaLrrDenominator(string duty_party, string plant, string fromTime, string endTime, string strExType, string reason_code = "", string model_serial = "", string floor = "", string location_code = "")
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = "";
            #region 分母 Model_serial NOT NA
            if (model_serial != "NA")
            {
                sql = @"  SELECT /*+ index(PK_STATION_SUMMARY)*/ m.model_serial,NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                             FROM SFISM4.r_station_summary_t T,
                                  sfis1.c_model_serial_def_t m,
                                  sfis1.c_plant_def_t        p
                            WHERE T.PLANT_CODE = P.SAP_PLANT
                              AND t.model_name = m.model_name
                              and m.plant_code = p.rms_plant 
                              and m.bu_code = p.bu_code
                              AND m.MODEL_SERIAL IN " + model_serial + @"
           " + GetStationSummaryWhere(plant, strExType) + @"
                              AND T.WORK_DATE >= :fromTime
                              AND T.WORK_DATE < :endTime
                              and p.rms_plant = :plant
                              and p.bu_code = 'PCBG'
                              group by m.model_serial ";
                ht.Clear();
                ht.Add("plant", plant);
                ht.Add("fromTime", fromTime);
                ht.Add("endTime", endTime);
                if (strExType != "")
                {
                    if (strExType == "_DOCK")
                    {
                        ht.Add("ex2", "CHV_EDOCKING");
                    }
                    else if (strExType == "_NB")
                    {
                        ht.Add("ex2", "CHV_A31");
                    }
                }
            }
            #endregion
            else
            {
                #region Model_Serial NA
                string sqlcolumn = string.Empty;
                string sqlwhere = string.Empty;
                if (strExType != "")
                    sqlwhere += " and ((pl.plant_code='VN1' and pl.cust_no=:ex1) or (pl.plant_code !='VN1'))  ";
                sqlwhere += @" AND pl.WORK_DATE >= :fromTime
                                           AND pl.WORK_DATE < :endTime
                                           and pl.duty_party =:duty_party ";

                if (plant.Contains("CD"))
                    sqlwhere += " and pl.cust_no !='EDOCKING' ";

                sql = @"SELECT pl.model_serial, sum(pl.ouput_qty) ouput_qty
  FROM (SELECT a.*, nvl(b.model_serial, 'NA') model_serial, c.ouput_qty
          from (SELECT distinct pl.model_name
                  from sfism4.r_daily_pcbalrr_t pl
                 where pl.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                   " + sqlwhere + @") a,
               (SELECT m.model_name, m.model_serial
                  FROM sfis1.c_model_serial_def_t m
                 where m.plant_code = :plant
                   and m.bu_code = 'PCBG') b,
               (SELECT /*+ index(PK_STATION_SUMMARY)*/
                 t.model_name,
                 NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                  FROM SFISM4.r_station_summary_t T
                 WHERE T.PLANT_CODE = (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant, strExType) + @"
                   AND T.WORK_DATE >= :fromTime
                   AND T.WORK_DATE < :endTime
                 group by model_name) c
         WHERE c.model_name = a.model_name(+)
           and c.model_name = b.model_name(+)
           and a.model_name is not null) pl
 where pl.model_serial = 'NA'
 group by pl.model_serial
";
                ht.Clear();
                ht.Add("plant", plant);
                ht.Add("duty_party", duty_party);
                ht.Add("fromTime", fromTime);
                ht.Add("endTime", endTime);
                if (strExType != "")
                {
                    if (strExType == "_DOCK")
                    {
                        ht.Add("ex1", "EDOCKING");
                        ht.Add("ex2", "CHV_EDOCKING");
                    }
                    else if (strExType == "_NB")
                    {
                        ht.Add("ex1", "A31");
                        ht.Add("ex2", "CHV_A31");
                    }
                }
                #endregion
            }
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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
        public DataTable GetPcbaLrrL3orL4Data(string str3Type, string plant, string fromTime, string endTime, string reason_codes, string model_serials, string strExType, string floor = "", string location_code = "", bool L4PEButton = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            try
            {
                DataTable dtNumerator = GetPcbaLrrNumerator(str3Type, plant, fromTime, endTime, strExType, reason_codes, model_serials, floor, location_code, L4PEButton);
                DataTable dtDenominator = GetPcbaLrrDenominator(str3Type, plant, fromTime, endTime, strExType, reason_codes, model_serials, floor, location_code);
                DataTable dtNADenominator = dtDenominator.Clone();
                if (model_serials.Contains("NA"))
                    dtNADenominator = GetPcbaLrrDenominator(str3Type, plant, fromTime, endTime, strExType, reason_codes, "NA", floor, location_code);
                if (dtNumerator.Rows.Count > 0)
                {
                    dtResult = dtNumerator;
                    dtResult.Columns.Add("rate", Type.GetType("System.Double"));
                    foreach (DataRow dr in dtResult.Rows)
                    {
                        var qty = Convert.ToDouble(string.IsNullOrEmpty(dr["qty"].ToString()) ? "0" : dr["qty"].ToString());
                        DataRow[] ouput;
                        if (dr["model_serial"].ToString() == "NA")
                            ouput = dtNADenominator.AsEnumerable().Where(t => t.Field<string>("model_serial") == dr["model_serial"].ToString()).ToArray();
                        else
                            ouput = dtDenominator.AsEnumerable().Where(t => t.Field<string>("model_serial") == dr["model_serial"].ToString()).ToArray();
                        if (ouput.Count() > 0)
                        {
                            double ouput_qty = Convert.ToDouble(string.IsNullOrEmpty(ouput.First()["ouput_qty"].ToString()) ? "0" : ouput.First()["ouput_qty"].ToString());
                            if (ouput_qty != 0)
                                dr["rate"] = Convert.ToInt32(qty / ouput_qty * 1000000);
                            else
                                dr["rate"] = 0;
                        }
                        else
                            dr["rate"] = 0;
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

        public DataTable GetPcbaLrrTop5Model(string duty_party, string plant, string fromTime, string endTime, string reason_code = "", string model_serial = "", string floor = "", string location_code = "")
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlcolumn = string.Empty;
            string sqlwhere = string.Empty;
            string sqlgroupby = string.Empty;
            sqlwhere += @" AND pl.WORK_DATE >= :fromTime
                                           AND pl.WORK_DATE < :endTime
                                           and pl.duty_party =:duty_party ";

            if (plant.Contains("CD"))
                sqlwhere += " and pl.cust_no !='EDOCKING' ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("model_serial", model_serial);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            string sql = "";
            #region 分子
            sql = @"select model_name
  from (select model_name, sum(qty) qty
          from (SELECT a.*, nvl(b.model_serial, 'NA') model_serial
                  from (SELECT pl.model_name, pl.qty
                          from sfism4.r_daily_pcbalrr_t pl
                         where pl.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                           " + sqlwhere + @" ) a,
                       (SELECT m.model_name, m.model_serial
                          FROM sfis1.c_model_serial_def_t m
                         where m.plant_code = :plant
                           and m.bu_code = 'PCBG') b
                 WHERE a.model_name = b.model_name(+)
                   and a.model_name is not null)
         where model_serial = :model_serial
         group by model_name
         order by qty desc)
 where rownum < 6
";
            #endregion
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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
        public DataTable GetPcbaLrrL3L4ModelData(string duty_party, string plant, string fromTime, string endTime, string reason_codes, string model_serial, string floor, string location_code = "")
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();

            string model_names = "('";
            DataTable dtTop5Model = GetPcbaLrrTop5Model(duty_party, plant, fromTime, endTime, reason_codes, model_serial, floor, location_code);
            if (dtTop5Model.Rows.Count > 0)
            {
                foreach (DataRow dr in dtTop5Model.Rows)
                {
                    model_names += dr[0].ToString() + "','";
                }
            }
            model_names += "')";

            string sqlcolumn = string.Empty;
            string sqlwhere = string.Empty;
            string sqlgroupby = string.Empty;
            sqlwhere += @" AND pl.WORK_DATE >= :fromTime
                                           AND pl.WORK_DATE < :endTime
                                           and pl.duty_party =:duty_party ";

            if (plant.Contains("CD"))
                sqlwhere += " and pl.cust_no !='EDOCKING' ";
            if (!string.IsNullOrEmpty(location_code))
                sqlwhere += " and pl.location_code in " + location_code;
            if (floor == "L4")
            {
                sqlcolumn += " pl.location_code,";
                sqlgroupby += ",pl.location_code";
            }
            if (duty_party != "PE")
            {
                sqlcolumn += " pl.reason_code,";
                sqlwhere += " and pl.reason_code in " + reason_codes;
                sqlgroupby += ",pl.reason_code";
            }
            else
            {
                sqlcolumn += " pl.location_type as reason_code,";
                sqlwhere += " and pl.location_type in " + reason_codes;
                sqlgroupby += ",pl.location_type";
            }
            #region
            string sql = @" select a.*,
              case
                when a.qty is null then
                 0
                when b.ouput_qty is null then
                 0
                when b.ouput_qty = 0 then
                 0
                else
                 round(a.qty / b.ouput_qty * 1000000, 0)
              end rate
         from (SELECT " + sqlcolumn + @" pl.model_name, sum(pl.qty) qty
                 from sfism4.r_daily_pcbalrr_t pl
                where pl.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                  " + sqlwhere + @"
                  and pl.model_name in " + model_names + @"
                group by pl.model_name" + sqlgroupby + @") a,
              (SELECT /*+ index(PK_STATION_SUMMARY)*/
                t.model_name,
                NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                 FROM SFISM4.r_station_summary_t T, sfis1.c_plant_def_t p
                WHERE T.PLANT_CODE = P.SAP_PLANT
                  and t.model_name in " + model_names + @"
                  " + GetStationSummaryWhere(plant) + @"
                  AND T.WORK_DATE >= :fromTime
                  AND T.WORK_DATE < :endTime
                  and p.rms_plant = :plant
                  and p.bu_code = 'PCBG'
                group by t.model_name) b
        where b.model_name = a.model_name(+)
          and a.reason_code is not null
          and a.model_name is not null";
            #endregion
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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


        public DataTable GetL4ByReasonDailyTrend(string duty_party, string plant, string fromTime, string endTime, string reason_code, string location_code, string model_serial)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlcolumn = string.Empty;
            string sqlwhere = string.Empty;
            string sqlgroupby = string.Empty;
            sqlwhere += @" AND pl.WORK_DATE >= :fromTime
                                           AND pl.WORK_DATE < :endTime 
and pl.duty_party = :duty_party
and pl.location_code = :location_code";
            if (plant.Contains("CD"))
                sqlwhere += " and pl.cust_no !='EDOCKING' ";
            if (duty_party == "PE")
                sqlwhere += " and pl.location_type = :reason_code ";
            else
                sqlwhere += " and pl.reason_code = :reason_code ";
            string sql = string.Empty;
            if (model_serial != "NA")
                #region model_Serial not NA
                sql = @"select substr(a.work_date, 5, 2) || '/' || substr(a.work_date, 7, 2)work_date,plant_code,value  from (
select a.work_date,
      case when a.duty_party='LRR WFR' then a.duty_party else 'DPPM' end plant_code,
case when a.qty is null then 0 when b.ouput_qty is null then 0 when b.ouput_qty =0 then 0  when a.duty_party = 'LRR WFR' then
          round(a.qty / b.ouput_qty * 100, 2)
         else
          round(a.qty / b.ouput_qty * 1000000, 0)  end  value
  from (select work_date,duty_party, sum(qty) qty
          from (SELECT a.*, nvl(b.model_serial, 'NA') model_serial
                  FROM (select pl.model_name, pl.work_date,pl.duty_party, pl.qty
                          from sfism4.r_daily_pcbalrr_t pl
                         where pl.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                           " + sqlwhere + @") a,
                       (SELECT m.model_name, m.model_serial
                          FROM sfis1.c_model_serial_def_t m
                         where m.plant_code = :plant
                           and m.bu_code = 'PCBG') b
                 WHERE a.model_name = b.model_name(+)
                   and a.model_name is not null)
         where model_serial = :model_serial
         group by work_date,duty_party) a,
       (SELECT t.work_date,
               NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T,
               sfis1.c_model_serial_def_t m
         WHERE T.PLANT_CODE = (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           and t.model_name = m.model_name
           and m.plant_code =:plant
           and m.bu_code = 'PCBG'
           " + GetStationSummaryWhere(plant) + @"
           AND T.WORK_DATE >= :fromTime
           AND T.WORK_DATE < :endTime
           and m.model_serial = :model_serial
         group by t.work_date) b
 where a.work_date = b.work_date(+)
   and a.work_date is not null
union
 select a.work_date,
       case
         when a.duty_party = 'LRR WFR' then
          a.duty_party
         else
          'DPPM'
       end plant_code,
       case
         when a.qty is null then
          0
         when b.ouput_qty is null then
          0
         when b.ouput_qty = 0 then
          0
         when a.duty_party = 'LRR WFR' then
          round(a.qty / b.ouput_qty * 100, 2)
         else
          round(a.qty / b.ouput_qty * 1000000, 0)
       end value
  from (SELECT t.work_date, t.duty_party, sum(t.qty) qty
          from sfism4.r_daily_pcbalrr_t t
         where t.plant_code = (select p.mes_plant
                                 from sfis1.c_plant_def_t p
                                where p.bu_code = 'PCBG'
                                  and p.rms_plant = :plant
                                  and rownum <= 1)
           and t.duty_party = 'LRR WFR'
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.work_date, t.duty_party) a,
       (SELECT t.work_date,
               NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T
         WHERE T.PLANT_CODE = (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant) + @"
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.work_date) b
 where a.work_date = b.work_date(+)
   and a.work_date is not null)a
   order by a.work_date ";
            #endregion
            else
                #region model_Serial is NA
                sql = @"select substr(work_date, 5, 2) || '/' || substr(work_date, 7, 2) work_date,
       case
         when qty is null then
          0
         when ouput_qty is null then
          0
         when ouput_qty = 0 then
          0
         else
          round(qty / ouput_qty * 1000000, 0)
       end value
  from (select a.work_date, sum(a.qty) qty, sum(b.ouput_qty) ouput_qty
          from (select work_date, model_name, sum(qty) qty
                  from (SELECT a.*, nvl(b.model_serial, 'NA') model_serial
                          FROM (select pl.model_name, pl.work_date, pl.qty
                                  from sfism4.r_daily_pcbalrr_t pl
                                 where pl.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                                   " + sqlwhere + @") a,
                               (SELECT m.model_name, m.model_serial
                                  FROM sfis1.c_model_serial_def_t m
                                 where m.plant_code = :plant
                                   and m.bu_code = 'PCBG') b
                         WHERE a.model_name = b.model_name(+)
                           and a.model_name is not null)
                 where model_serial = :model_serial
                 group by work_date, model_name) a,
               (SELECT t.work_date,
                       t.model_name,
                       NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                  FROM SFISM4.r_station_summary_t T
                 WHERE T.PLANT_CODE = (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                   " + GetStationSummaryWhere(plant) + @"
                   AND T.WORK_DATE >= :fromTime
                   AND T.WORK_DATE < :endTime
                 group by t.work_date, t.model_name) b
         where b.work_date = a.work_date(+)
           and b.model_name = a.model_name(+)
           and a.work_date is not null
           and a.model_name is not null
         group by a.work_date)
 order by work_date ";
            #endregion
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("reason_code", reason_code);
            ht.Add("location_code", location_code);
            ht.Add("model_serial", model_serial);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetL4ByReasonMonthlyTrend(string duty_party, string plant, string fromTime, string endTime, string reason_code, string location_code, string model_serial)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlwhere = @"  and t.WORK_DATE >= :fromTime
                                                    and t.WORK_DATE < :endTime
                                                    and t.duty_party = :duty_party 
                                                    and t.location_code = :location_code";
            if (plant.Contains("CD"))
                sqlwhere += " and t.cust_no !='EDOCKING' ";
            if (duty_party == "PE")
                sqlwhere += "  and t.location_type = :reason_code ";
            else
                sqlwhere += "  and t.reason_code = :reason_code ";
            DataTable dt0 = new DataTable();
            #region 
            string sql = @"select 'M' || substr(a.work_date, 5, 2) work_date,plant_code,value  from (
            select a.work_date,
       case when a.duty_party='LRR WFR' then a.duty_party else 'DPPM' end plant_code,
case when a.qty is null then 0 when b.ouput_qty is null then 0 when b.ouput_qty =0 then 0  when a.duty_party = 'LRR WFR' then
          round(a.qty / b.ouput_qty * 100, 2)
         else
          round(a.qty / b.ouput_qty * 1000000, 0)  end  value
  from (SELECT /*+ index(PK_STATION_SUMMARY)*/
         substr(t.work_date, 0, 6) work_date,
         NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T, SFIS1.C_MODEL_SERIAL_DEF_T M
         WHERE T.PLANT_CODE = (select P.SAP_PLANT
                                 from sfis1.c_plant_def_t p
                                where p.bu_code = 'PCBG'
                                  and p.rms_plant =  :plant
                                  and rownum <= 1)
           AND T.MODEL_NAME = M.MODEL_NAME
           AND M.BU_CODE = 'PCBG'
           AND M.PLANT_CODE = :plant
           AND M.MODEL_SERIAL = :model_serial
           " + GetStationSummaryWhere(plant) + @"
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by substr(t.work_date, 0, 6)) b,
       (SELECT substr(t.work_date, 1, 6) work_date,t.duty_party, sum(t.qty) qty
          from sfism4.r_daily_pcbalrr_t t, SFIS1.C_MODEL_SERIAL_DEF_T M
         where t.plant_code = (select p.mes_plant
                                 from sfis1.c_plant_def_t p
                                where p.bu_code = 'PCBG'
                                  and p.rms_plant = :plant
                                  and rownum <= 1)
           AND T.MODEL_NAME = M.MODEL_NAME
           AND M.BU_CODE = 'PCBG'
           AND M.PLANT_CODE = :plant
           AND M.MODEL_SERIAL = :model_serial
           " + sqlwhere + @"
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by substr(t.work_date, 1, 6),t.duty_party) a
 where a.work_date = b.work_date
and a.work_date is not null
 union
 select a.work_date,
       case
         when a.duty_party = 'LRR WFR' then
          a.duty_party
         else
          'DPPM'
       end plant_code,
       case
         when a.qty is null then
          0
         when b.ouput_qty is null then
          0
         when b.ouput_qty = 0 then
          0
         when a.duty_party = 'LRR WFR' then
          round(a.qty / b.ouput_qty * 100, 2)
         else
          round(a.qty / b.ouput_qty * 1000000, 0)
       end value
  from (SELECT substr(t.work_date, 1, 6) work_date, t.duty_party, sum(t.qty) qty
          from sfism4.r_daily_pcbalrr_t t
         where t.plant_code = (select p.mes_plant
                                 from sfis1.c_plant_def_t p
                                where p.bu_code = 'PCBG'
                                  and p.rms_plant = :plant
                                  and rownum <= 1)
           and t.duty_party = 'LRR WFR'
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by substr(t.work_date, 1, 6) , t.duty_party) a,
       (SELECT substr(t.work_date, 1, 6) work_date,
               NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T
         WHERE T.PLANT_CODE = (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant) + @"
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by substr(t.work_date, 1, 6)) b
 where a.work_date = b.work_date(+)
   and a.work_date is not null)a
   order by a.work_date ";
            #endregion
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("reason_code", reason_code);
            ht.Add("model_serial", model_serial);
            ht.Add("location_code", location_code);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetL4TrendChart(string DateType, string duty_party, string plant, string fromTime, string endTime, string reason_code, string location_code, string model_serial)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = string.Empty;
            string sqlwhere = @"  and t.WORK_DATE >= :fromTime
                   and t.WORK_DATE < :endTime
                   and t.duty_party = :duty_party 
                   and t.location_code=:location_code";
            if (plant.Contains("CD"))
                sqlwhere += " and t.cust_no !='EDOCKING' ";
            if (duty_party == "PE")
                sqlwhere += @" and t.location_type = :reason_code ";
            else
                sqlwhere += @" and t.reason_code = :reason_code ";
            switch (DateType)
            {
                case "D":
                    #region
                    sql = @"select substr(t.work_date, 5, 2) || '/' || substr(t.work_date, 7, 2) as work_date,
        case when t.duty_party='LRR WFR' then t.duty_party else 'DPPM' end plant_code,
case when t.qty is null then 0 when t.ouput_qty is null then 0 when t.ouput_qty =0 then 0  when t.duty_party = 'LRR WFR' then
          round(t.qty / t.ouput_qty * 100, 2)
         else
          round(t.qty / t.ouput_qty * 1000000, 0)  end  value
  from (select a.work_date,
               nvl((select c.model_serial
                     from sfis1.c_model_serial_def_t c
                    where c.model_name = a.model_name
                      and rownum = 1),
                   'NA') model_serial,
               a.qty,
               a.duty_party,
               b.ouput_qty
          from (SELECT /*+ index(PK_STATION_SUMMARY)*/
                 t.work_date,
                 t.model_name,
                 NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                  FROM SFISM4.r_station_summary_t T
                 WHERE T.PLANT_CODE = (select P.SAP_PLANT
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                   " + GetStationSummaryWhere(plant) + @"
                   AND T.WORK_DATE >= :fromTime
                   AND T.WORK_DATE < :endTime
                 group by t.work_date, t.model_name) b,
               (SELECT t.work_date,
                       t.model_name,
                       t.duty_party,
                       sum(t.qty) qty
                  from sfism4.r_daily_pcbalrr_t t
                 where t.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                   " + sqlwhere + @"
                 group by t.work_date, t.model_name,duty_party) a
         where a.work_date = b.work_date
           and a.model_name = b.model_name) t
 where t.model_serial = :model_serial
 group by t.work_date
 order by t.work_date";
                    #endregion
                    break;
                case "W":
                    #region
                    sql = @"select 'W' || Substr(t.work_date, 5, 2) as work_date,
       case
         when sum(t.qty) is null then
          0
         when sum(t.ouput_qty)  is null then
          0
         when sum(t.ouput_qty)  = 0 then
          0
         else
          round(sum(t.qty) / sum(t.ouput_qty)  * 1000000, 0)
       end value
  from (select a.work_date,
               nvl((select c.model_serial
                     from sfis1.c_model_serial_def_t c
                    where c.model_name = a.model_name
                      and rownum = 1),
                   'NA') model_serial,
               a.qty,
               b.ouput_qty
          from (SELECT /*+ index(PK_STATION_SUMMARY)*/
                 TO_CHAR(TO_DATE(t.work_date || '010000',
                                          'YYYYMMDDHH24MISS'),
                                  'IYYYIW') work_date,
                 t.model_name,
                 NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                  FROM SFISM4.r_station_summary_t T
                 WHERE T.PLANT_CODE = (select P.SAP_PLANT
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                   " + GetStationSummaryWhere(plant) + @"
                   AND T.WORK_DATE >= :fromTime
                   AND T.WORK_DATE < :endTime
                 group by TO_CHAR(TO_DATE(t.work_date || '010000',
                                          'YYYYMMDDHH24MISS'),
                                  'IYYYIW'), t.model_name) b,
               (SELECT t.week_code work_date,
                       t.model_name,
                       sum(t.qty) qty
                  from sfism4.r_daily_pcbalrr_t t
                 where t.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                   " + sqlwhere + @"
                 group by t.week_code, t.model_name) a
         where a.work_date = b.work_date
           and a.model_name = b.model_name) t
 where t.model_serial = :model_serial
 group by t.work_date
 order by t.work_date";
                    #endregion
                    break;
                case "M":
                    #region  Month
                    sql = @"select 'M' || substr(t.work_date, 5, 2) as work_date,
       case
         when sum(t.qty) is null then
          0
         when sum(t.ouput_qty)  is null then
          0
         when sum(t.ouput_qty)  = 0 then
          0
         else
          round(sum(t.qty) / sum(t.ouput_qty)  * 1000000, 0)
       end value
  from (select a.work_date,
               nvl((select c.model_serial
                     from sfis1.c_model_serial_def_t c
                    where c.model_name = a.model_name
                      and rownum = 1),
                   'NA') model_serial,
               a.qty,
               b.ouput_qty
          from (SELECT /*+ index(PK_STATION_SUMMARY)*/
                 substr(t.work_date, 0, 6) work_date,
                 t.model_name,
                 NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
                  FROM SFISM4.r_station_summary_t T
                 WHERE T.PLANT_CODE = (select P.SAP_PLANT
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                   " + GetStationSummaryWhere(plant) + @"
                   AND T.WORK_DATE >= :fromTime
                   AND T.WORK_DATE < :endTime
                 group by substr(t.work_date, 0, 6), t.model_name) b,
               (SELECT substr(t.work_date, 1, 6) work_date,
                       t.model_name,
                       sum(t.qty) qty
                  from sfism4.r_daily_pcbalrr_t t
                 where t.plant_code = (select p.mes_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
                   " + sqlwhere + @"
                 group by substr(t.work_date, 1, 6), t.model_name) a
         where a.work_date = b.work_date
           and a.model_name = b.model_name) t
 where t.model_serial = :model_serial
 group by t.work_date
 order by t.work_date ";
                    #endregion
                    break;
            }
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", duty_party);
            ht.Add("reason_code", reason_code);
            ht.Add("model_serial", model_serial);
            ht.Add("location_code", location_code);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetL4ByReasonWeeklyTrend(string str3Type, string plant, string fromTime, string endTime, string reason_code, string location_code, string model_serial)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlwhere = "";
            if (plant.Contains("CD"))
                sqlwhere += " and t.cust_no !='EDOCKING' ";
            if (str3Type != "PE")
                sqlwhere += " and t.reason_code=:reason_code ";
            else
                sqlwhere += " and t.location_type=:reason_code ";
            string sql = "";
            #region  model_serial NOT NA
            sql = @"select 'W' || Substr(a.work_date, 5, 2) work_date,plant_code,value  from (
select  a.work_date,
        case when a.duty_party='LRR WFR' then a.duty_party else 'DPPM' end plant_code,
case when a.qty is null then 0 when b.ouput_qty is null then 0 when b.ouput_qty =0 then 0  when a.duty_party = 'LRR WFR' then
          round(a.qty / b.ouput_qty * 100, 2)
         else
          round(a.qty / b.ouput_qty * 1000000, 0)  end  value
   from (SELECT /*+ index(PK_STATION_SUMMARY)*/
          TO_CHAR(TO_DATE(t.work_date || '010000', 'YYYYMMDDHH24MISS'),
                  'IYYYIW') work_date,
          NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
           FROM SFISM4.r_station_summary_t T, SFIS1.C_MODEL_SERIAL_DEF_T M
          WHERE T.PLANT_CODE = (select P.SAP_PLANT
                                  from sfis1.c_plant_def_t p
                                 where p.bu_code = 'PCBG'
                                   and p.rms_plant = :plant
                                   and rownum <= 1)
            AND T.MODEL_NAME = M.MODEL_NAME
            AND M.BU_CODE = 'PCBG'
            AND M.PLANT_CODE = :plant
            AND M.MODEL_SERIAL = :model_serial
            " + GetStationSummaryWhere(plant) + @"
            and t.work_date >=  :fromTime
            and t.work_date < :endTime
          group by TO_CHAR(TO_DATE(t.work_date || '010000',
                                   'YYYYMMDDHH24MISS'),
                           'IYYYIW')) b,
        (SELECT t.week_code work_date,t.duty_party, sum(t.qty) qty
           from sfism4.r_daily_pcbalrr_t t, SFIS1.C_MODEL_SERIAL_DEF_T M
          where t.plant_code = (select p.mes_plant
                                  from sfis1.c_plant_def_t p
                                 where p.bu_code = 'PCBG'
                                   and p.rms_plant = :plant
                                   and rownum <= 1)
            AND T.MODEL_NAME = M.MODEL_NAME
            AND M.BU_CODE = 'PCBG'
            AND M.PLANT_CODE = :plant
            AND M.MODEL_SERIAL = :model_serial
            " + sqlwhere + @"
            and t.location_code = :location_code
             and (t.duty_party =:duty_party or t.duty_party ='LRR WFR') 
            and t.work_date >= :fromTime
            and t.work_date < :endTime
          group by t.week_code,t.duty_party) a
  where a.work_date = b.work_date
 union
 select a.work_date,
       case
         when a.duty_party = 'LRR WFR' then
          a.duty_party
         else
          'DPPM'
       end plant_code,
       case
         when a.qty is null then
          0
         when b.ouput_qty is null then
          0
         when b.ouput_qty = 0 then
          0
         when a.duty_party = 'LRR WFR' then
          round(a.qty / b.ouput_qty * 100, 2)
         else
          round(a.qty / b.ouput_qty * 1000000, 0)
       end value
  from (SELECT t.week_code work_date, t.duty_party, sum(t.qty) qty
          from sfism4.r_daily_pcbalrr_t t
         where t.plant_code = (select p.mes_plant
                                 from sfis1.c_plant_def_t p
                                where p.bu_code = 'PCBG'
                                  and p.rms_plant = :plant
                                  and rownum <= 1)
           and t.duty_party = 'LRR WFR'
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by t.week_code, t.duty_party) a,
       (SELECT TO_CHAR(TO_DATE(t.work_date || '010000',
                                       'YYYYMMDDHH24MISS'),
                               'IYYYIW') work_date,
               NVL(SUM(T.PASS_QTY + T.FAIL_QTY), 999999) AS ouput_qty
          FROM SFISM4.r_station_summary_t T
         WHERE T.PLANT_CODE = (select p.sap_plant
                                         from sfis1.c_plant_def_t p
                                        where p.bu_code = 'PCBG'
                                          and p.rms_plant = :plant
                                          and rownum <= 1)
           " + GetStationSummaryWhere(plant) + @"
           and t.work_date >= :fromTime
           and t.work_date < :endTime
         group by TO_CHAR(TO_DATE(t.work_date || '010000',
                                       'YYYYMMDDHH24MISS'),
                               'IYYYIW')) b
 where a.work_date = b.work_date(+)
   and a.work_date is not null)a
   order by a.work_date ";
            #endregion
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("duty_party", str3Type);
            ht.Add("reason_code", reason_code);
            ht.Add("location_code", location_code);
            ht.Add("model_serial", model_serial);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetPcbaLrrExcelData(string str3Type, string plant, string fromTime, string endTime, string strExType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlwhere = "", strEx2 = "";
            if (plant.Contains("CD"))
                sqlwhere += " and t.cust_no !='EDOCKING' ";
            if (strExType != "")
            {
                if (plant == "VNP1")
                    strEx2 = " and t.cust_no=:ex1 ";
            }
            string sql = "";
            #region  model_serial NOT NA
            sql = @"SELECT work_date,
                         cust_no,
                         model_name,  nvl((select c.model_serial
                             from sfis1.c_model_serial_def_t c
                            where c.model_name = t.model_name and c.plant_code= :plant 
                              and rownum = 1),
                           'NA') model_serial,
                         duty_party,
                         reason_code,
                         location_code,
                         location_type,
                         qty 
                FROM (  select work_date,
                         cust_no,
                         model_name, 
                         duty_party,
                         reason_code,
                         location_code,
                         location_type,
                         sum(qty) AS qty 
                    from sfism4.r_daily_pcbalrr_t t
                   where t.work_date >= :fromTime
                     and t.work_date < :endTime
                     " + sqlwhere + strEx2 + @"                   
                     and t.plant_code = (select p.mes_plant
                                           from sfis1.c_plant_def_t p
                                          where p.bu_code = 'PCBG'
                                            and p.rms_plant = :plant
                                            and rownum<=1)
                    GROUP BY work_date,
                         cust_no,
                         model_name, 
                         duty_party,
                         reason_code,
                         location_code,
                         location_type    
                    ) t  
                   order by work_date ";
            #endregion
            ht.Clear();
            ht.Add("plant", plant);
            //ht.Add("duty_party", str3Type);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strExType != "")
            {
                if (strExType == "_DOCK")
                    ht.Add("ex1", "EDOCKING");
                else if (strExType == "_NB")
                    ht.Add("ex1", "A31");
            }
            try
            {
                exeRes = mDBTools.ExecuteQueryDSHt(sql, ht);
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

        #region HSR
        public DataTable GetHsrPlantAnalysis(string str1, string plant, string fromTime, string endTime, string str3Type)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string res = "";
            switch (str3Type)
            {
                case "HOLD QTY":
                    res = " cast(sum(q.value_int2) as float) value ";
                    break;
                case "HOLD DPPM":
                    res = " cast(sum(q.value_int2) as float)/cast(sum(q.value_int1) as float)*1000000 value ";
                    break;
                case "SORTING QTY":
                    res = " cast(sum(q.value_int3) as float) value ";
                    break;
                case "SORTING DPPM":
                    res = " cast(sum(q.value_int3) as float)/cast(sum(q.value_int1) as float)*1000000 value ";
                    break;
                case "REWORK QTY":
                    res = " cast(sum(q.value_int4) as float) value ";
                    break;
                case "REWORK DPPM":
                    res = " cast(sum(q.value_int4) as float)/cast(sum(q.value_int1) as float)*1000000 value ";
                    break;
                default:
                    res = " cast(sum(q.value_int2) as float) value ";
                    break;
            }
            string sql = @" select b.plant_code,
                                  SUBSTRING(b.date,5,len(b.date)-4) date,
                                  case
                                    when a.value is null then
                                     0
                                    else
                                     round(value,0)
                                  end value
                             from (SELECT q.plant_code,
                                          q.data_week date,
                                          " + res + @"
                                     FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                    WHERE kpi_category = 'Q0010'
                                      and m.source_plant_code = q.plant_code
                                      and q.data_week between @fromTime and @endTime
                                      and q.process = @str1
                                    group by q.plant_code, q.data_week) a
                            right join (select t1.plant_code, t2.date
                                          from (SELECT distinct q.plant_code, 'T' t
                                                  FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                 WHERE kpi_category = 'Q0010'
                                                   and m.source_plant_code = q.plant_code
                                                   and q.data_week between @fromTime and @endTime
                                                   and q.process = @str1) t1
                                          full join (SELECT distinct q.data_week date, 'T' t
                                                      FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                     WHERE kpi_category = 'Q0010'
                                                       and m.source_plant_code = q.plant_code
                                                       and q.data_week between @fromTime and
                                                           @endTime
                                                       and q.process = @str1) t2
                                            on t1.t = t2.t) b
                               on (a.date = b.date and a.plant_code = b.plant_code)
                            order by plant_code, b.date ";


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

        public DataTable GetHsrDailyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select b.plant_code,
                                   convert(varchar,convert(int,SUBSTRING(b.date, 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(b.date, 7, 2))) date,
                                   case
                                     when z.value is null then
                                      0
                                     else
                                      z.value
                                   end value
                              from (select a.plant_code + '-Hold' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_day date,
                                                   cast(sum(q.value_int2) as float) value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_day between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_day) a
                                    union
                                    select a.plant_code + '-Hold DPPM' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_day date,
                                                   cast(sum(q.value_int2) as float) /
                                                   cast(sum(q.value_int1) as float) * 1000000 value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_day between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_day) a
                                    union
                                    select a.plant_code + '-Sorting' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_day date,
                                                   cast(sum(q.value_int3) as float) value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_day between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_day) a
                                    union
                                    select a.plant_code + '-Sorting DPPM' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_day date,
                                                   cast(sum(q.value_int3) as float) /
                                                   cast(sum(q.value_int1) as float) * 1000000 value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_day between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_day) a
                                    union
                                    select a.plant_code + '-Rework' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_day date,
                                                   cast(sum(q.value_int4) as float) value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_day between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_day) a
                                    union
                                    select a.plant_code + '-Rework DPPM' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_day date,
                                                   cast(sum(q.value_int4) as float) /
                                                   cast(sum(q.value_int1) as float) * 1000000 value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_day between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_day) a) z
                             right join (select t1.plant_code, t2.date
                                           from (SELECT plant_code, 'T' t
                                                   FROM (SELECT top 1 cast(plant_code + '-Hold' as varchar(50)) p1,
                                                                cast(plant_code + '-Hold DPPM' as
                                                                     varchar(50)) p2,
                                                                cast(plant_code + '-Sorting' as
                                                                     varchar(50)) p3,
                                                                cast(plant_code + '-Sorting DPPM' as
                                                                     varchar(50)) p4,
                                                                cast(plant_code + '-Rework' as varchar(50)) p5,
                                                                cast(plant_code + '-Rework DPPM' as
                                                                     varchar(50)) p6
                                                           FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                          WHERE kpi_category = 'Q0010'
                                                            and m.source_plant_code = q.plant_code
                                                            and m.target_plant_code = @plant
                                                            and q.data_day between @fromTime and @endTime
                                                            and q.process = @str1) p UNPIVOT(plant_code FOR value_type IN(p1,
                                                                                                                          p2,
                                                                                                                          p3,
                                                                                                                          p4,
                                                                                                                          p5,
                                                                                                                          p6)) vot) t1
                                           full join (SELECT distinct data_day date, 'T' t
                                                       FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                      WHERE kpi_category = 'Q0010'
                                                        and m.source_plant_code = q.plant_code
                                                        and m.target_plant_code = @plant
                                                        and q.data_day between @fromTime and @endTime
                                                        and q.process = @str1) t2
                                             on t1.t = t2.t) b
                                on (z.plant_code = b.plant_code and z.date = b.date)
                              order by b.plant_code, b.date ";
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

        public DataTable GetHsrWeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case
                                     when z.value is null then
                                      0
                                     else
                                      z.value
                                   end value
                              from (select a.plant_code + '-Hold' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_week date,
                                                   cast(sum(q.value_int2) as float) value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_week between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_week) a
                                    union
                                    select a.plant_code + '-Hold DPPM' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_week date,
                                                   cast(sum(q.value_int2) as float) /
                                                   cast(sum(q.value_int1) as float) * 1000000 value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_week between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_week) a
                                    union
                                    select a.plant_code + '-Sorting' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_week date,
                                                   cast(sum(q.value_int3) as float) value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_week between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_week) a
                                    union
                                    select a.plant_code + '-Sorting DPPM' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_week date,
                                                   cast(sum(q.value_int3) as float) /
                                                   cast(sum(q.value_int1) as float) * 1000000 value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_week between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_week) a
                                    union
                                    select a.plant_code + '-Rework' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_week date,
                                                   cast(sum(q.value_int4) as float) value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_week between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_week) a
                                    union
                                    select a.plant_code + '-Rework DPPM' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_week date,
                                                   cast(sum(q.value_int4) as float) /
                                                   cast(sum(q.value_int1) as float) * 1000000 value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_week between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_week) a) z
                             right join (select t1.plant_code, t2.date
                                           from (SELECT plant_code, 'T' t
                                                   FROM (SELECT top 1 cast(plant_code + '-Hold' as varchar(50)) p1,
                                                                cast(plant_code + '-Hold DPPM' as
                                                                     varchar(50)) p2,
                                                                cast(plant_code + '-Sorting' as
                                                                     varchar(50)) p3,
                                                                cast(plant_code + '-Sorting DPPM' as
                                                                     varchar(50)) p4,
                                                                cast(plant_code + '-Rework' as varchar(50)) p5,
                                                                cast(plant_code + '-Rework DPPM' as
                                                                     varchar(50)) p6
                                                           FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                          WHERE kpi_category = 'Q0010'
                                                            and m.source_plant_code = q.plant_code
                                                            and m.target_plant_code = @plant
                                                            and q.data_week between @fromTime and @endTime
                                                            and q.process = @str1) p UNPIVOT(plant_code FOR value_type IN(p1,
                                                                                                                          p2,
                                                                                                                          p3,
                                                                                                                          p4,
                                                                                                                          p5,
                                                                                                                          p6)) vot) t1
                                           full join (SELECT distinct data_week date, 'T' t
                                                       FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                      WHERE kpi_category = 'Q0010'
                                                        and m.source_plant_code = q.plant_code
                                                        and m.target_plant_code = @plant
                                                        and q.data_week between @fromTime and @endTime
                                                        and q.process = @str1) t2
                                             on t1.t = t2.t) b
                                on (z.plant_code = b.plant_code and z.date = b.date)
                              order by b.plant_code, b.date ";
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

        public DataTable GetHsrMonthlyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case
                                     when z.value is null then
                                      0
                                     else
                                      z.value
                                   end value
                              from (select a.plant_code + '-Hold' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_month date,
                                                   cast(sum(q.value_int2) as float) value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_month between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_month) a
                                    union
                                    select a.plant_code + '-Hold DPPM' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_month date,
                                                   cast(sum(q.value_int2) as float) /
                                                   cast(sum(q.value_int1) as float) * 1000000 value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_month between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_month) a
                                    union
                                    select a.plant_code + '-Sorting' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_month date,
                                                   cast(sum(q.value_int3) as float) value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_month between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_month) a
                                    union
                                    select a.plant_code + '-Sorting DPPM' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_month date,
                                                   cast(sum(q.value_int3) as float) /
                                                   cast(sum(q.value_int1) as float) * 1000000 value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_month between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_month) a
                                    union
                                    select a.plant_code + '-Rework' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_month date,
                                                   cast(sum(q.value_int4) as float) value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_month between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_month) a
                                    union
                                    select a.plant_code + '-Rework DPPM' plant_code,
                                           a.date,
                                           case
                                             when value is null then
                                              0
                                             else
                                              round(value, 0)
                                           end value
                                      from (SELECT q.plant_code,
                                                   q.data_month date,
                                                   cast(sum(q.value_int4) as float) /
                                                   cast(sum(q.value_int1) as float) * 1000000 value
                                              FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                             WHERE kpi_category = 'Q0010'
                                               and m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_month between @fromTime and @endTime
                                               and q.process = @str1
                                             group by q.plant_code, q.data_month) a) z
                             right join (select t1.plant_code, t2.date
                                           from (SELECT plant_code, 'T' t
                                                   FROM (SELECT top 1 cast(plant_code + '-Hold' as varchar(50)) p1,
                                                                cast(plant_code + '-Hold DPPM' as
                                                                     varchar(50)) p2,
                                                                cast(plant_code + '-Sorting' as
                                                                     varchar(50)) p3,
                                                                cast(plant_code + '-Sorting DPPM' as
                                                                     varchar(50)) p4,
                                                                cast(plant_code + '-Rework' as varchar(50)) p5,
                                                                cast(plant_code + '-Rework DPPM' as
                                                                     varchar(50)) p6
                                                           FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                          WHERE kpi_category = 'Q0010'
                                                            and m.source_plant_code = q.plant_code
                                                            and m.target_plant_code = @plant
                                                            and q.data_month between @fromTime and @endTime
                                                            and q.process = @str1) p UNPIVOT(plant_code FOR value_type IN(p1,
                                                                                                                          p2,
                                                                                                                          p3,
                                                                                                                          p4,
                                                                                                                          p5,
                                                                                                                          p6)) vot) t1
                                           full join (SELECT distinct data_month date, 'T' t
                                                       FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                      WHERE kpi_category = 'Q0010'
                                                        and m.source_plant_code = q.plant_code
                                                        and m.target_plant_code = @plant
                                                        and q.data_month between @fromTime and @endTime
                                                        and q.process = @str1) t2
                                             on t1.t = t2.t) b
                                on (z.plant_code = b.plant_code and z.date = b.date)
                              order by b.plant_code, b.date ";
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

        #region Insight
        public DataTable GetInsightPlantAnalysis(string process, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @"  select b.plant_code,
                                   SUBSTRING(b.date,5,len(b.date)-4) date,
                                   case
                                     when a.value is null then
                                      0
                                     else
                                      a.value
                                   end value
                              from (select f.plant_code,
                                           f.data_week date,
                                           case 
                                 when sum(f.station_qty)=0 then
                                  0
                                 when sum(f.station_qty) is null then
                                  0
                                when sum(f.gap_qty) is null then
								   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                  --when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                  --       cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float))<=0 then
                                  -- 0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float)) * 100,
                                         2)
                               end value
                                      from V_wr_q_plrr f, V_wr_plant_mapping m
                                     where m.source_plant_code = f.plant_code
                                       and m.target_plant_code in
                                           ('KSP3', 'KSP2', 'KSP4', 'CDP1', 'CDDOCK', 'CQP1', 'TW01', 'TW01DOCK', 'VNP1')
                                       and f.process = @process
                                       and f.data_week between @fromTime and @endTime
                                     group by f.plant_code, f.data_week) a
                             right join (select t1.plant_code, t2.date
                                           from (SELECT distinct f.plant_code, 'T' t
                                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                                  where m.source_plant_code = f.plant_code
                                                    and m.target_plant_code in
                                                        ('KSP3',
                                                         'KSP2',
                                                         'KSP4',
                                                         'CDP1',
                                                         'CDDOCK',
                                                         'CQP1',
                                                         'TW01',
                                                         'TW01DOCK',
                                                         'VNP1')
                                                    and f.process = @process
                                                    and f.data_week between @fromTime and @endTime) t1
                                           full join (SELECT distinct f.data_week date, 'T' t
                                                       from V_wr_q_plrr f, V_wr_plant_mapping m
                                                      where m.source_plant_code = f.plant_code
                                                        and m.target_plant_code in
                                                            ('KSP3',
                                                             'KSP2',
                                                             'KSP4',
                                                             'CDP1',
                                                             'CDDOCK',
                                                             'CQP1',
                                                             'TW01',
                                                             'TW01DOCK',
                                                             'VNP1')
                                                        and f.process = @process
                                                        and f.data_week between @fromTime and @endTime) t2
                                             on t1.t = t2.t) b
                                on (a.date = b.date and a.plant_code = b.plant_code)
                             order by b.plant_code, b.date ";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetInsightDailyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select f.plant_code,
                               convert(varchar,convert(int,SUBSTRING(cast(f.data_day as varchar), 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(cast(f.data_day as varchar), 7, 2))) date,
                               case 
                                 when sum(f.station_qty)=0 then
                                 0
                                 when sum(f.station_qty) is null then
                                  0
                                when sum(f.gap_qty) is null then
								   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                  --when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                  --       cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float))<=0 then
                                  -- 0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float)) * 100,
                                         2)
                               end value,f.data_day 
                          from V_wr_q_plrr f, V_wr_plant_mapping m
                         where m.source_plant_code = f.plant_code
                           and m.target_plant_code=@plant ";
            //if (plant == "VNP1")
            //    sql += @"   AND M.source_plant_code='VNNB1' ";
            sql += @"        
                           and f.process =@process
                           and f.data_day between @fromTime and @endTime
                         group by f.plant_code,f.data_day
                         order by f.data_day,f.plant_code ";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetInsightWeeklyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select f.plant_code,
                                SUBSTRING(f.data_week,5,len(f.data_week)-4) date,
                                case 
                                 when sum(f.station_qty)=0 then
                                 0
                                 when sum(f.station_qty) is null then
                                  0
                                when sum(f.gap_qty) is null then
								   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                  --when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                  --       cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float))<=0 then
                                  -- 0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float)) * 100,
                                         2)
                               end value,f.data_week
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code
                            and m.target_plant_code =@plant ";
            //if (plant == "VNP1")
            //    sql += @"   AND M.source_plant_code='VNNB1' ";
            sql += @"        
                            and f.process =@process 
                            and f.data_week between @fromTime and @endTime
                          group by f.plant_code,f.data_week
                          order by f.data_week,f.plant_code";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetInsightMonthlyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select f.plant_code,
                                SUBSTRING(f.data_month,5,len(f.data_month)-4) date,
                                case 
                                 when sum(f.station_qty)=0 then
                                  0
                                 when sum(f.station_qty) is null then
                                  0
                                when sum(f.gap_qty) is null then
								   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                  --when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                  --       cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float))<=0 then
                                  -- 0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float)) * 100,
                                         2)
                               end value,f.data_month
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code
                            and m.target_plant_code =@plant ";
            //if (plant == "VNP1")
            //    sql += @"   AND M.source_plant_code='VNNB1' ";
            sql += @"        
                            and f.process =@process 
									and f.data_month!='2020M01'
									and f.data_month!='2020M02'
                            and f.data_month between @fromTime and @endTime
                          group by f.plant_code,f.data_month
                          order by f.data_month,f.plant_code ";
            ht.Clear();
            ht.Add("process", process);
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

        public DataTable GetGapDailyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select * from 
                            (select 'FPY' as plant_code,
                               convert(varchar,convert(int,SUBSTRING(cast(f.data_day as varchar), 5, 2)))+'/'+RIGHT('0'+convert(varchar,convert(int,SUBSTRING(cast(f.data_day as varchar), 7, 2))),2) date,
                               case 
                                 when sum(f.station_qty)<=0 then
                                  0
                                 when sum(f.station_qty) is null then
                                  0
                                when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                        cast(sum(f.station_qty) as float))<=0 then
                                  0
                                 else
                                  round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                        cast(sum(f.station_qty) as float)) * 100,
                                        2)
                               end value,f.data_day 
                          from V_wr_q_plrr f, V_wr_plant_mapping m
                         where m.source_plant_code = f.plant_code
                           and m.target_plant_code=@plant ";
            if (plant.IndexOf("VNP1") > -1)
                sql += @"   AND M.source_plant_code=@s_plant ";
            sql += @"        
                           and f.process =@process
                           and f.data_day between @fromTime and @endTime
                         group by f.data_day
                         union
                         select 'INSIGHT' as plant_code,
                               convert(varchar,convert(int,SUBSTRING(cast(f.data_day as varchar), 5, 2)))+'/'+RIGHT('0'+convert(varchar,convert(int,SUBSTRING(cast(f.data_day as varchar), 7, 2))),2) date,
                               case 
                                 when sum(f.station_qty)=0 then
                                 0
                                 when sum(f.station_qty) is null then
                                  0
                                when sum(f.gap_qty) is null then
								   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                  --when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                  --       cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float))<=0 then
                                  -- 0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float)) * 100,
                                         2)
                               end value,f.data_day
                          from V_wr_q_plrr f, V_wr_plant_mapping m
                         where m.source_plant_code = f.plant_code
                           and m.target_plant_code=@plant ";
            if (plant.IndexOf("VNP1") > -1)
                sql += @"   AND M.source_plant_code=@s_plant ";
            sql += @"        
                           and f.process =@process
                           and f.data_day between @fromTime and @endTime
                         group by f.data_day
                        ) a order by a.plant_code,a.data_day ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (plant.IndexOf("VNP1") > -1)
            {
                ht.Add("plant", "VNP1");
                if (plant.IndexOf("NB1") > -1)
                    ht.Add("s_plant", "VNNB1");
                else if (plant.IndexOf("DOCK") > -1)
                    ht.Add("s_plant", "VN1");
                else
                    ht.Add("s_plant", "VN1");
            }
            else
                ht.Add("plant", plant);
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

        public DataTable GetGapWeeklyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select * from 
                            ( select 'FPY' as plant_code,
                                SUBSTRING(f.data_week,5,len(f.data_week)-4) date,
                                case 
                                  when sum(f.station_qty)<=0 then
                                   0
                                  when sum(f.station_qty) is null then
                                   0
                                  when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float))<=0 then
                                   0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                end value,f.data_week
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code
                            and m.target_plant_code =@plant ";
            if (plant.IndexOf("VNP1") > -1)
                sql += @"   AND M.source_plant_code=@s_plant ";
            sql += @"        
                            and f.process =@process 
                            and f.data_week between @fromTime and @endTime
                          group by f.data_week
                          union
                           select 'INSIGHT' as plant_code,
                                SUBSTRING(f.data_week,5,len(f.data_week)-4) date,
                                case 
                                 when sum(f.station_qty)=0 then
                                 0
                                 when sum(f.station_qty) is null then
                                  0
                                when sum(f.gap_qty) is null then
								   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                  --when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                  --       cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float))<=0 then
                                  -- 0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float)) * 100,
                                         2)
                               end value,f.data_week
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code
                            and m.target_plant_code =@plant ";
            if (plant.IndexOf("VNP1") > -1)
                sql += @"   AND M.source_plant_code=@s_plant ";
            sql += @"        
                            and f.process =@process 
                            and f.data_week between @fromTime and @endTime
                          group by f.data_week 
                        ) a order by a.plant_code,a.data_week ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (plant.IndexOf("VNP1") > -1)
            {
                ht.Add("plant", "VNP1");
                if (plant.IndexOf("NB1") > -1)
                    ht.Add("s_plant", "VNNB1");
                else if (plant.IndexOf("DOCK") > -1)
                    ht.Add("s_plant", "VN1");
                else
                    ht.Add("s_plant", "VN1");
            }
            else
                ht.Add("plant", plant);
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

        public DataTable GetGapMonthlyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select * from 
                            ( select 'FPY' as plant_code,
                                SUBSTRING(f.data_month,5,len(f.data_month)-4) date,
                                case 
                                  when sum(f.station_qty)<=0 then
                                   0
                                  when sum(f.station_qty) is null then
                                   0
                                  when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float))<=0 then
                                   0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                end value,f.data_month
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code
                            and m.target_plant_code =@plant ";
            if (plant.IndexOf("VNP1") > -1)
                sql += @"   AND M.source_plant_code=@s_plant ";
            sql += @"        
                            and f.process =@process 
                            and f.data_month between @fromTime and @endTime
                          group by f.data_month
                          union
                            select 'INSIGHT' as plant_code,
                                SUBSTRING(f.data_month,5,len(f.data_month)-4) date,
                                case 
                                 when sum(f.station_qty)=0 then
                                  0
                                 when sum(f.station_qty) is null then
                                  0
                                when sum(f.gap_qty) is null then
								   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)) * 100,
                                         2)
                                  --when (1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                  --       cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float))<=0 then
                                  -- 0
                                  else
                                   round((1 - (sum(f.firstfail_qty) + sum(f.refail_qty)) /
                                         cast(sum(f.station_qty) as float)-sum(f.gap_qty)/cast(sum(f.station_qty) as float)) * 100,
                                         2)
                               end value,f.data_month
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code
                            and m.target_plant_code =@plant ";
            if (plant.IndexOf("VNP1") > -1)
                sql += @"   AND M.source_plant_code=@s_plant ";
            sql += @"        
                            and f.process =@process 
									and f.data_month!='2020M01'
									and f.data_month!='2020M02'
                            and f.data_month between @fromTime and @endTime
                          group by f.data_month
                        ) a order by a.plant_code,a.data_month ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (plant.IndexOf("VNP1") > -1)
            {
                ht.Add("plant", "VNP1");
                if (plant.IndexOf("NB1") > -1)
                    ht.Add("s_plant", "VNNB1");
                else if (plant.IndexOf("DOCK") > -1)
                    ht.Add("s_plant", "VN1");
                else
                    ht.Add("s_plant", "VN1");
            }
            else
                ht.Add("plant", plant);
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
