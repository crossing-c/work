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
    class CrossCheckOperator
    {
        private InfoLightDBTools mDBTools;
        private InfoLightDBTools mDCDBTools;
        private InfoLightMSTools mDBmsTools;
        private InfoLightDBTools mHGPGTools;
        public CrossCheckOperator(object[] clientInfo, string dbName)
        {
            mDBTools = new InfoLightDBTools(clientInfo, dbName);
            mDCDBTools = new InfoLightDBTools(clientInfo, "DC");
            mDBmsTools = new InfoLightMSTools(clientInfo, "WARROOM");
            mHGPGTools = new InfoLightDBTools(clientInfo, "HGPG");
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
            if (result.Status)
            {
                DataTable dt = ((DataSet)result.Anything).Tables[0];
                if (dt.Rows.Count > 0)
                {
                    Goal = dt.Rows[0][0].ToString();
                }
            }
            return Goal;
        }

        public DataTable GetFpyDailyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 f.plant_code
                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                  where m.source_plant_code = f.plant_code
                                    and m.target_plant_code=@plant
                                    and f.process =@process
                                    and f.data_day between @fromTime and @endTime) plant_code,
                               f.data_day date,
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
                               end value
                          from V_wr_q_plrr f, V_wr_plant_mapping m
                         where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"      and m.target_plant_code=@plant
                           and f.process =@process
                           and f.data_day between @fromTime and @endTime
                         group by f.data_day
                         order by f.data_day ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 f.plant_code
                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                  where m.source_plant_code = f.plant_code
                                    and m.target_plant_code =@plant
                                    and f.process =@process 
                                    and f.data_week between @fromTime and @endTime) plant_code,
                                f.data_week date,
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
                                end value
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"       and m.target_plant_code =@plant
                            and f.process =@process 
                            and f.data_week between @fromTime and @endTime
                          group by f.data_week
                          order by f.data_week ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 f.plant_code
                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                  where m.source_plant_code = f.plant_code
                                    and m.target_plant_code =@plant
                                    and f.process =@process 
                                    and f.data_month between @fromTime and @endTime) plant_code,
                                f.data_month date,
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
                                end value
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"      and m.target_plant_code =@plant
                            and f.process =@process 
                            and f.data_month between @fromTime and @endTime
                          group by f.data_month
                          order by f.data_month ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetQ119DailyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT (select top 1 o.plant_code
                                      FROM V_wr_q_generic2 o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0008'
                                       and o.data_name= @str1
                                       and m.target_plant_code =@plant
                                       and o.data_day between  @fromTime and @endTime ) plant_code,
                                   o.data_day date,
                                   sum(isnull(o.value_int1,0)) value
                              FROM V_wr_q_generic2 o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'Q0008'
                               and o.data_name= @str1
                               and m.target_plant_code =@plant
                               and o.data_day between  @fromTime and @endTime    
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

        public DataTable GetQ119WeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT (select top 1 o.plant_code
                                      FROM V_wr_q_generic2 o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0008'
                                       and o.data_name= @str1
                                       and m.target_plant_code =@plant
                                       and o.data_week between  @fromTime and @endTime ) plant_code,
                                   o.data_week date,
                                   sum(isnull(o.value_int1,0)) value
                              FROM V_wr_q_generic2 o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'Q0008'
                               and o.data_name= @str1
                               and m.target_plant_code =@plant
                               and o.data_week between  @fromTime and @endTime    
                             group by  o.data_week
                             order by  o.data_week  ";
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

        public DataTable GetQ119MonthlyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT (select top 1 o.plant_code
                                      FROM V_wr_q_generic2 o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0008'
                                       and o.data_name= @str1
                                       and m.target_plant_code =@plant
                                       and o.data_month between  @fromTime and @endTime ) plant_code,
                                   o.data_month date,
                                   sum(isnull(o.value_int1,0)) value
                              FROM V_wr_q_generic2 o, V_wr_plant_mapping m
                             WHERE m.source_plant_code = o.plant_code
                               and o.kpi_category = 'Q0008'
                               and o.data_name= @str1
                               and m.target_plant_code =@plant
                               and o.data_month between  @fromTime and @endTime    
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

        public DataTable GetOfflineDailyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" SELECT (select top 1 o.plant_code
                                      FROM V_wr_q_generic o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0001'
                                       and o.value_str1 =@str1
                                       and m.target_plant_code =@plant ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @" and o.data_day between @fromTime and @endTime) plant_code,
                                   o.data_day date,
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
                               and m.target_plant_code =@plant ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"        and o.data_day between @fromTime and @endTime 
                             group by  o.data_day
                             order by  o.data_day ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetOfflineWeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" SELECT (select top 1 o.plant_code
                                      FROM V_wr_q_generic o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0001'
                                       and o.value_str1 =@str1
                                       and m.target_plant_code =@plant ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"      and o.data_week between @fromTime and @endTime) plant_code,
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
                               AND o.value_str1 =@str1
                               and m.target_plant_code =@plant ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"        and o.data_week between @fromTime and @endTime
                             group by  o.data_week
                             order by  o.data_week ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" SELECT (select top 1 o.plant_code
                                      FROM V_wr_q_generic o, V_wr_plant_mapping m
                                     WHERE m.source_plant_code = o.plant_code
                                       and o.kpi_category = 'Q0001'
                                       and o.value_str1 =@str1
                                       and m.target_plant_code =@plant ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"  and o.data_month between @fromTime and @endTime) plant_code,
                                   o.data_month date,
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
                               and m.target_plant_code =@plant ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"     and o.data_month between @fromTime and @endTime
                             group by  o.data_month
                             order by  o.data_month ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetReflowDailyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "", strType2 = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                {
                    strType = "VN_EDOCKING";
                    strType2 = "CHV_EDOCKING";
                }
                else if (aa[1] == "NB")
                {
                    strType = "VN_A31";
                    strType2 = "CHV_A31";
                }
            }
            else if (plant.IndexOf("CTY1DOCK") > -1)
            {
                plant = "CTY1";
                strType = "DOCK";
            }

            else if (plant.IndexOf("CDDOCK") > -1)
            {
                plant = "CDP1";
                strType = "DOCK";
            }
            string sql1;
            if (str1 == "ASSY")
            {
                if (strType.IndexOf("DOCK") > -1)
                    sql1 = @"='' ";
                else
                    sql1 = " in ('ASSY','CELL','REPAIR','TNB') ";
            }
            else
            {
                if (strType.IndexOf("DOCK") > -1)
                    sql1 = @"='DOCK' ";
                else
                    sql1 = "='PACK' ";
            }
            string sql = @" select b.plant_code,
       b.work_date,
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
           and t.work_date between :fromTime and :endTime ";
            if (strType != "" && strType2 != "")
                sql += @"  and t.from_db = :splant ";
            sql += @" group by t.plant_code,t.work_date
         order by t.plant_code,t.work_date) a,
       (select t.plant_code,t.work_date, sum(t.output_qty) output_qty
          from sfism4.r_kpi_fpy_t t
         where t.plant_code = (SELECT d.SAP_PLANT
                                 FROM SFIS1.C_PLANT_DEF_T d
                                WHERE d.RMS_PLANT = :plant
                                  and d.bu_code='PCBG'
                                  and rownum = 1)
           and t.sub_process " + sql1 + @"
          and t.process in ('FATP')  ";
            if (strType != "" && strType2 != "")
                sql += @"  and t.from_db = :splant2 ";
            sql += @" 
           and t.work_date between :fromTime and :endTime
         group by t.plant_code,t.work_date
         order by t.plant_code,t.work_date) b
 where b.work_date = a.work_date(+)
   and b.WORK_DATE is not null
 ORDER BY b.WORK_DATE
 ";
            ht.Clear();
            if (str1 == "PACK" && strType.IndexOf("DOCK") > -1)
                ht.Add("str1", "DOCK");
            else
                ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "" && strType2 != "")
            {
                ht.Add("splant", strType);
                ht.Add("splant2", strType2);
            }
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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
            string strType = "", strType2 = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                {
                    strType = "VN_EDOCKING";
                    strType2 = "CHV_EDOCKING";
                }
                else if (aa[1] == "NB")
                {
                    strType = "VN_A31";
                    strType2 = "CHV_A31";
                }
            }
            else if (plant.IndexOf("CTY1DOCK") > -1)
            {
                plant = "CTY1";
                strType = "DOCK";
            }

            else if (plant.IndexOf("CDDOCK") > -1)
            {
                plant = "CDP1";
                strType = "DOCK";
            }
            string sql1;
            if (str1 == "ASSY")
            {
                if (strType.IndexOf("DOCK") > -1)
                    sql1 = @"='' ";
                else
                    sql1 = " in ('ASSY','CELL','REPAIR','TNB') ";
            }
            else
            {
                if (strType.IndexOf("DOCK") > -1)
                    sql1 = @"='DOCK' ";
                else
                    sql1 = @"='PACK' ";
            }

            string sql = @" select  b.plant_code,substr(b.WORK_DATE, 1, 4) || 'W' || substr(b.work_date, 5, 6) as work_date,
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
           and t.process = :str1 ";
            if (strType != "" && strType2 != "")
                sql += @"  and t.from_db = :splant ";
            sql += @" 
           and TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') BETWEEN :fromTime AND :endTime
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
           and t.process in ('FATP')  ";
            if (strType != "" && strType2 != "")
                sql += @"  and t.from_db = :splant2 ";
            sql += @" 
           and TO_CHAR(TO_DATE(t.work_date || '000000', 'YYYYMMDDHH24MISS'),
                       'IYYYIW') BETWEEN :fromTime AND :endTime
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
            if (str1 == "PACK" && strType.IndexOf("DOCK") > -1)
                ht.Add("str1", "DOCK");
            else
                ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime.Replace("W", ""));
            ht.Add("endTime", endTime.Replace("W", ""));
            if (strType != "" && strType2 != "")
            {
                ht.Add("splant", strType);
                ht.Add("splant2", strType2);
            }
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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
            string strType = "", strType2 = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                {
                    strType = "VN_EDOCKING";
                    strType2 = "CHV_EDOCKING";
                }
                else if (aa[1] == "NB")
                {
                    strType = "VN_A31";
                    strType2 = "CHV_A31";
                }
            }
            else if (plant.IndexOf("CTY1DOCK") > -1)
            {
                plant = "CTY1";
                strType = "DOCK";
            }

            else if (plant.IndexOf("CDDOCK") > -1)
            {
                plant = "CDP1";
                strType = "DOCK";
            }
            string sql1;
            if (str1 == "ASSY")
            {
                if (strType.IndexOf("DOCK") > -1)
                    sql1 = @"='' ";
                else
                    sql1 = " in ('ASSY','CELL','REPAIR','TNB') ";
            }
            else
            {
                if (strType.IndexOf("DOCK") > -1)
                    sql1 = @"='DOCK' ";
                else
                    sql1 = "='PACK' ";
            }
            string sql = @" select b.plant_code,substr(b.WORK_DATE, 1, 4) || 'M' || substr(b.WORK_DATE, 5, 6) as work_date,
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
           and t.process = :str1 ";
            if (strType != "" && strType2 != "")
                sql += @"  and t.from_db = :splant ";
            sql += @" 
           and substr(T.WORK_DATE, 0, 6) BETWEEN :fromTime and :endTime
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
           and t.process in ('FATP')  ";
            if (strType != "" && strType2 != "")
                sql += @"  and t.from_db = :splant2 ";
            sql += @" 
           and substr(T.WORK_DATE, 0, 6) BETWEEN :fromTime and :endTime
         group by t.plant_code,substr(T.WORK_DATE, 0, 6)
         order by t.plant_code,substr(T.WORK_DATE, 0, 6)) b
 where b.work_date = a.work_date(+)
 ORDER BY b.WORK_DATE
 ";
            ht.Clear();
            if (str1 == "PACK" && strType.IndexOf("DOCK") > -1)
                ht.Add("str1", "DOCK");
            else
                ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime.Replace("M", ""));
            ht.Add("endTime", endTime.Replace("M", ""));
            if (strType != "" && strType2 != "")
            {
                ht.Add("splant", strType);
                ht.Add("splant2", strType2);
            }
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetBaseAoiDailyTrend( string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();          
            
            string sql = @" select x.work_date, case when x.fail = 0 then 0 else round((1-x.fail/y.pass)*100,2) end value  from 
                                 (select a.work_date,sum(a.fail_qty) as fail  from sfism4.r_station_summary_t a where a.group_name ='BASE_AOI_BR' and a.plant_code like 'CQ%' 
                                 and a.wo_master='YES' and a.work_date between :fromTime and :endTime
                                 group by a.work_date)x,
                                 ( select a.work_date,sum(a.pass_qty) as pass from sfism4.r_station_summary_t a where a.group_name ='BASE_AOI' and a.plant_code like 'CQ%' 
                                 and a.wo_master='YES' and a.work_date between :fromTime and :endTime
                                 group by a.work_date)y
                                 where x.work_date=y.work_date order by x.work_date ";
            ht.Clear();             
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);           
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetBaseAoiWeeklyTrend(string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select substr(x.WORK_DATE, 1, 4) || 'W' || substr(x.work_date, 5, 6) as work_date, case when x.fail = 0 then 0 else round((1-x.fail/y.pass)*100,2) end value from 
                                 (select TO_CHAR(TO_DATE(a.work_date || '000000', 'YYYYMMDDHH24MISS'),'IYYYIW') work_date,sum(a.fail_qty) as fail 
                                    from sfism4.r_station_summary_t a where a.group_name ='BASE_AOI_BR' and a.plant_code like 'CQ%' 
                                         and a.wo_master='YES' and TO_CHAR(TO_DATE(a.work_date || '000000', 'YYYYMMDDHH24MISS'),'IYYYIW') BETWEEN :fromTime AND :endTime
                                 group by TO_CHAR(TO_DATE(a.work_date || '000000', 'YYYYMMDDHH24MISS'),'IYYYIW'))x,
                                 (select TO_CHAR(TO_DATE(a.work_date || '000000', 'YYYYMMDDHH24MISS'),'IYYYIW') work_date,sum(a.pass_qty) as pass 
                                   from sfism4.r_station_summary_t a where a.group_name ='BASE_AOI' and a.plant_code like 'CQ%' 
                                         and a.wo_master='YES' and TO_CHAR(TO_DATE(a.work_date || '000000', 'YYYYMMDDHH24MISS'),'IYYYIW') BETWEEN :fromTime AND :endTime
                                 group by TO_CHAR(TO_DATE(a.work_date || '000000', 'YYYYMMDDHH24MISS'),'IYYYIW'))y
                                 where x.work_date=y.work_date order by x.work_date ";            
            ht.Clear();           
            ht.Add("fromTime", fromTime.Replace("W", ""));
            ht.Add("endTime", endTime.Replace("W", ""));
         
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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

        public DataTable GetBaseAoiMonthlyTrend( string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select substr(x.WORK_DATE, 1, 4) || 'M' || substr(x.WORK_DATE, 5, 6) as work_date, case when x.fail = 0 then 0 else round((1-x.fail/y.pass)*100,2) end value from 
                                 (select substr(a.WORK_DATE, 0, 6) work_date,sum(a.fail_qty) as fail 
                                    from sfism4.r_station_summary_t a where a.group_name ='BASE_AOI_BR' and a.plant_code like 'CQ%' 
                                         and a.wo_master='YES' and substr(a.WORK_DATE, 0, 6) BETWEEN :fromTime AND :endTime
                                 group by substr(a.WORK_DATE, 0, 6))x,
                                 (select substr(a.WORK_DATE, 0, 6) work_date,sum(a.pass_qty) as pass 
                                   from sfism4.r_station_summary_t a where a.group_name ='BASE_AOI' and a.plant_code like 'CQ%' 
                                         and a.wo_master='YES' and substr(a.WORK_DATE, 0, 6) BETWEEN :fromTime AND :endTime
                                 group by substr(a.WORK_DATE, 0, 6))y
                                 where x.work_date=y.work_date order by x.work_date ";
            
            ht.Clear();            
            ht.Add("fromTime", fromTime.Replace("M", ""));
            ht.Add("endTime", endTime.Replace("M", ""));
             
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 c.plant_code
                                  from V_wr_q_ctq c, V_wr_plant_mapping m
                                 WHERE m.source_plant_code = c.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"    
                                   and c.process =@process
                                   and m.target_plant_code=@plant
                                   and c.data_day between @fromTime and @endTime) plant_code,
                               c.data_day date,
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
                         WHERE m.source_plant_code = c.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"    
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
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetCtqWeeklyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 c.plant_code
                                  from V_wr_q_ctq c, V_wr_plant_mapping m
                                 WHERE m.source_plant_code = c.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"    
                                   and c.process =@process
                                   and m.target_plant_code=@plant
                                   and c.data_week between @fromTime and @endTime) plant_code,
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
                         WHERE m.source_plant_code = c.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"    
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
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 c.plant_code
                                  from V_wr_q_ctq c, V_wr_plant_mapping m
                                 WHERE m.source_plant_code = c.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"    
                                   and c.process = @process
                                   and m.target_plant_code = @plant
                                   and c.data_month between @fromTime and @endTime) plant_code,
                               c.data_month date,
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
                         WHERE m.source_plant_code = c.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"    
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
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetMBScrapMonthly(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select *
                              from (SELECT a.plant_code,
                                           a.data_month date,
                                           case a.ate_input_qty
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round((cast(a.mb_qty as float) / ate_input_qty) * 1000000,
                                                    0)
                                           end value
                                      FROM (SELECT sum(q.mb_qty) mb_qty, 
                                                sum(q.ate_input_qty) ate_input_qty, 
                                                q.data_month, 
                                                q.plant_code
                                              FROM V_wr_q_mb q, V_wr_plant_mapping m
                                             WHERE m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_month between @fromTime and @endTime
                                            group by q.data_month, 
                                            q.plant_code ) a) r 
                                        where r.plant_code is not null
                                        order by r.plant_code, r.date ";
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

        public DataTable GetMBScrapWeekly(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select *
                              from (SELECT a.plant_code,
                                           a.data_week date,
                                           case a.ate_input_qty
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round((cast(a.mb_qty as float) / ate_input_qty) * 1000000,
                                                    0)
                                           end value
                                      FROM (SELECT  sum(q.mb_qty) mb_qty, 
                                                    sum(q.ate_input_qty) ate_input_qty, 
                                                    q.data_week, 
                                                    q.plant_code
                                              FROM V_wr_q_mb q, V_wr_plant_mapping m
                                             WHERE m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_week between @fromTime and @endTime
                                            group by q.data_week, 
                                            q.plant_code ) a) r 
                                        where r.plant_code is not null
                                        order by r.plant_code, r.date ";
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

        public DataTable GetMBScrapDaily(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select *
                              from (SELECT a.plant_code,
                                           a.data_day date,
                                           case a.ate_input_qty
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round((cast(a.mb_qty as float) / ate_input_qty) * 1000000,
                                                    0)
                                           end value
                                      FROM (SELECT q.mb_qty, q.ate_input_qty, q.data_day, q.plant_code
                                              FROM V_wr_q_mb q, V_wr_plant_mapping m
                                             WHERE m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_day between @fromTime and @endTime
                                            ) a) r 
                                        where r.plant_code is not null
                                        order by r.plant_code, r.date ";
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

        public DataTable GetCPUReplaceRateDaily(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select *
                              from (SELECT a.plant_code,
                                           a.data_day date,
                                           case a.ate_input_qty
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round((cast(a.cpu_qty as float) / ate_input_qty) * 1000000,
                                                    0)
                                           end value
                                      FROM (SELECT sum(q.cpu_qty) cpu_qty, 
                                                   sum(q.ate_input_qty) ate_input_qty, 
                                                    q.data_day, 
                                                    q.plant_code
                                              FROM V_wr_q_cpu q, V_wr_plant_mapping m
                                             WHERE m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_day between @fromTime and @endTime
                                            group by q.data_day,
                                            q.plant_code ) a) r 
                                        where r.plant_code is not null
                                        order by r.plant_code, r.date ";
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

        public DataTable GetCPUReplaceRateWeekly(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select *
                              from (SELECT a.plant_code,
                                           a.data_week date,
                                           case a.ate_input_qty
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round((cast(a.cpu_qty as float) / ate_input_qty) * 1000000,
                                                    0)
                                           end value
                                      FROM (SELECT sum(q.cpu_qty) cpu_qty, 
                                                   sum(q.ate_input_qty) ate_input_qty, 
                                                    q.data_week, 
                                                    q.plant_code
                                              FROM V_wr_q_cpu q, V_wr_plant_mapping m
                                             WHERE m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_week between @fromTime and @endTime
                                            group by q.data_week,
                                            q.plant_code ) a) r 
                                        where r.plant_code is not null
                                        order by r.plant_code, r.date ";
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

        public DataTable GetCPUReplaceRateMonthly(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select *
                              from (SELECT a.plant_code,
                                           a.data_month date,
                                           case a.ate_input_qty
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round((cast(a.cpu_qty as float) / ate_input_qty) * 1000000,
                                                    0)
                                           end value
                                      FROM (SELECT sum(q.cpu_qty) cpu_qty, 
                                                   sum(q.ate_input_qty) ate_input_qty, 
                                                    q.data_month, 
                                                    q.plant_code
                                              FROM V_wr_q_cpu q, V_wr_plant_mapping m
                                             WHERE m.source_plant_code = q.plant_code
                                               and m.target_plant_code = @plant
                                               and q.data_month between @fromTime and @endTime
                                            group by q.data_month,
                                            q.plant_code ) a) r 
                                        where r.plant_code is not null
                                        order by r.plant_code, r.date ";
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

        public DataTable GetLrrDailyTrend(string str1, string str3, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select * from (
                        SELECT a.plant_code,
                           b.data_day date,
                           case b.vinput
                             when 0 then
                              0
                             when null then
                              0
                             else
                              round((cast(a.v as float) / b.vinput) * 100, 2)
                           end value
                      FROM (SELECT sum(q.value_real1) v,
                                   q.data_day,
                                   q.plant_code + '-' + q.value_str3 plant_code
                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                             WHERE q.kpi_category = 'Q0009'
                               and m.source_plant_code = q.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"   
                               and m.target_plant_code = @plant
                               and q.value_str3 =@str3
                               and q.value_str1 = @str1
                               and q.data_day between @fromTime and @endTime
                             group by q.plant_code + '-' + q.value_str3, q.data_day) a
                      full join (SELECT sum(q.station_qty) vinput,
                                        q.data_day,
                                        q.plant_code + '-INPUT' plant_code
                                   FROM V_wr_q_plrr q, V_wr_plant_mapping m
                                  WHERE m.source_plant_code = q.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"   
                                    and m.target_plant_code = @plant
                                    and q.process = @str1
                                    and q.data_day between @fromTime and @endTime
                                  group by q.plant_code + '-INPUT', q.data_day) b
                        on a.data_day = b.data_day ) r
                        where r.plant_code is not null
                     order by r.plant_code, r.date  ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("str3", str3);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetLrrMonthlyTrend(string str1, string str3, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            //string str3 = "('作業', '材料', 'PCBA LRR', 'NTF')";
            //if (str1 != "AP")
            //    str3 = "('置件', '印刷', '材料', '制程', 'NTF')";
            string sql = @"select * from (
                          SELECT a.plant_code,
                           b.data_month date,
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
                               and m.source_plant_code = q.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"   
                               and m.target_plant_code = @plant
                               and q.value_str3 =@str3
                               and q.value_str1 = @str1
                               and q.data_month between @fromTime and @endTime
                             group by q.plant_code + '-' + q.value_str3, q.data_month) a
                      full join (SELECT sum(q.station_qty) vinput,
                                        q.data_month,
                                        q.plant_code + '-INPUT' plant_code
                                   FROM V_wr_q_plrr q, V_wr_plant_mapping m
                                  WHERE m.source_plant_code = q.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"   
                                    and m.target_plant_code = @plant
                                    and q.process = @str1
                                    and q.data_month between @fromTime and @endTime
                                  group by q.plant_code + '-INPUT', q.data_month) b
                        on a.data_month = b.data_month ) r
                        where r.plant_code is not null
                     order by r.plant_code, r.date ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("str3", str3);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetLrrWeeklyTrend(string str1, string str3, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            //string str3 = "('作業', '材料', 'PCBA LRR', 'NTF')";
            //if (str1 != "AP")
            //    str3 = "('置件', '印刷', '材料', '制程', 'NTF')";
            string sql = @"select * from (
                        SELECT a.plant_code,
                           b.data_week date,
                           case b.vinput
                             when 0 then
                              0
                             when null then
                              0
                             else
                              round((cast(a.v as float) / b.vinput) * 100, 2)
                           end value
                      FROM (SELECT sum(q.value_real1) v,
                                   q.data_week,
                                   q.plant_code + '-' + q.value_str3 plant_code
                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                             WHERE q.kpi_category = 'Q0009'
                               and m.source_plant_code = q.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"   
                               and m.target_plant_code = @plant
                               and q.value_str3 =@str3
                               and q.value_str1 = @str1
                               and q.data_week between @fromTime and @endTime
                             group by q.plant_code + '-' + q.value_str3, q.data_week) a
                      full join (SELECT sum(q.station_qty) vinput,
                                        q.data_week,
                                        q.plant_code + '-INPUT' plant_code
                                   FROM V_wr_q_plrr q, V_wr_plant_mapping m
                                  WHERE m.source_plant_code = q.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"   
                                    and m.target_plant_code = @plant
                                    and q.process = @str1
                                    and q.data_week between @fromTime and @endTime
                                  group by q.plant_code + '-INPUT', q.data_week) b
                        on a.data_week = b.data_week ) r
                        where r.plant_code is not null
                     order by r.plant_code, r.date ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("str3", str3);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetSVLRRFRRDaily(string category, string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();  
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @"select * from (SELECT a.plant_code,
                                               a.data_day date,
                                               case a.input_qty
                                                 when 0 then
                                                  0
                                                 when null then
                                                  0
                                                 else
                                                  round((cast(a.vid_qty as float) / input_qty) * 1000000,0)
                                               end value
                                          FROM (SELECT sum(q.value_int1) vid_qty,
                                                       sum(q.value_int2) input_qty,
                                                       q.data_day,
                                                       q.plant_code + '-' + q.kpi_category + '-' +
                                                       q.value_str1 plant_code
                                                  FROM V_wr_q_generic q, V_wr_plant_mapping m
                                                 WHERE q.kpi_category = @category
                                                   and m.source_plant_code = q.plant_code
                                                   and m.target_plant_code = @plant
                                                   and q.value_str1 =@str1
                                                and q.data_day between @fromTime and @endTime
                                                 group by q.plant_code + '-' + q.kpi_category + '-' +
                                                          q.value_str1,
                                                          q.data_day) a) r
                                 where r.plant_code is not null
                                 order by r.plant_code, r.date ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("category", category);
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

        public DataTable GetSVLRRFRRWeekly(string category, string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @"select * from (SELECT a.plant_code,
                                               a.data_week date,
                                               case a.input_qty
                                                 when 0 then
                                                  0
                                                 when null then
                                                  0
                                                 else
                                                  round((cast(a.vid_qty as float) / input_qty) * 1000000, 0)
                                               end value
                                          FROM (SELECT sum(q.value_int1) vid_qty,
                                                       sum(q.value_int2) input_qty,
                                                       q.data_week,
                                                       q.plant_code + '-' + q.kpi_category + '-' +
                                                       q.value_str1 plant_code
                                                  FROM V_wr_q_generic q, V_wr_plant_mapping m
                                                 WHERE q.kpi_category = @category
                                                   and m.source_plant_code = q.plant_code
                                                   and m.target_plant_code = @plant
                                                   and q.value_str1 = @str1
                                                and q.data_week between @fromTime and @endTime
                                                 group by q.plant_code + '-' + q.kpi_category + '-' +
                                                          q.value_str1,
                                                          q.data_week) a) r
                                 where r.plant_code is not null
                                 order by r.plant_code, r.date ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("category", category);
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

        public DataTable GetSVLRRFRRMonthly(string category, string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0]; 
            }
            string sql = @"select * from (SELECT a.plant_code,
                                               a.data_month date,
                                               case a.input_qty
                                                 when 0 then
                                                  0
                                                 when null then
                                                  0
                                                 else
                                                  round((cast(a.vid_qty as float) / input_qty) * 1000000, 0)
                                               end value
                                          FROM (SELECT sum(q.value_int1) vid_qty,
                                                       sum(q.value_int2) input_qty,
                                                       q.data_month,
                                                       q.plant_code + '-' + q.kpi_category + '-' +
                                                       q.value_str1 plant_code
                                                  FROM V_wr_q_generic q, V_wr_plant_mapping m
                                                 WHERE q.kpi_category = @category
                                                   and m.source_plant_code = q.plant_code
                                                   and m.target_plant_code = @plant
                                                   and q.value_str1 = @str1
                                                and q.data_month between @fromTime and @endTime
                                                 group by q.plant_code + '-' + q.kpi_category + '-' +
                                                          q.value_str1,
                                                          q.data_month) a) r
                                 where r.plant_code is not null
                                 order by r.plant_code, r.date ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("category", category);
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

        public DataTable GetRepairBackPotRateWeekly(string str1, string plant, string fromTime, string endTime)
        {
            {
                ExecutionResult exeRes = new ExecutionResult();
                exeRes.Status = true;
                exeRes.Message = string.Empty;
                Hashtable ht = new Hashtable();
                DataTable dtResult = new DataTable();
                string strType = "";
                if (plant.IndexOf("VNP1-") > -1)
                {
                    plant = "VNP1";
                    //string[] aa = plant.Split('-');
                    //plant = aa[0];
                    //if (aa[1] == "DOCK")
                    //    strType = "CHV_EDOCKING";
                    //else if (aa[1] == "NB")
                    //    strType = "CHV_A31";
                }
                string sql = @"select * from (SELECT a.plant_code,
                                           a.data_week date,
                                           case a.qty2
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round((cast(a.qty1 as float) / qty2) * 100, 2)
                                           end value
                                      FROM (SELECT sum(q.value_real4) qty1,
                                                   sum(q.value_real1) qty2,
                                                   q.data_week,
                                                   q.plant_code + '-' +
                                                   q.value_str1 plant_code
                                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                                             WHERE q.kpi_category = 'Q0007'
                                               and m.source_plant_code = q.plant_code";
                if (strType != "")
                    sql += @"  and m.source_plant_code = @splant ";
                sql += @"
                                               and m.target_plant_code = @plant
                                               and q.value_str1 = @str1
                                               and q.data_week between @fromTime and @endTime
                                             group by q.plant_code  + '-' +
                                                      q.value_str1,
                                                      q.data_week) a) r
                             where r.plant_code is not null
                             order by r.plant_code, r.date ";
                ht.Clear();
                ht.Add("str1", str1);
                ht.Add("plant", plant);
                ht.Add("fromTime", fromTime);
                ht.Add("endTime", endTime);
                if (strType != "")
                    ht.Add("splant", strType);
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

        public DataTable GetRepairBackPotRateDaily(string str1, string plant, string fromTime, string endTime)
        {
            {
                ExecutionResult exeRes = new ExecutionResult();
                exeRes.Status = true;
                exeRes.Message = string.Empty;
                Hashtable ht = new Hashtable();
                DataTable dtResult = new DataTable();
                string strType = "";
                if (plant.IndexOf("VNP1-") > -1)
                {
                    string[] aa = plant.Split('-');
                    plant = aa[0];
                    if (aa[1] == "DOCK")
                        strType = "VN1";
                    else if (aa[1] == "NB")
                        strType = "VNNB1";
                }
                string sql = @"select * from (SELECT a.plant_code,
                                           a.data_day date,
                                           case a.qty2
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round((cast(a.qty1 as float) / qty2) * 100, 2)
                                           end value
                                      FROM (SELECT sum(q.value_real4) qty1,
                                                   sum(q.value_real1) qty2,
                                                   q.data_day,
                                                   q.plant_code + '-' +
                                                   q.value_str1 plant_code
                                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                                             WHERE q.kpi_category = 'Q0007'
                                               and m.source_plant_code = q.plant_code";
                if (strType != "")
                    sql += @"  and m.source_plant_code = @splant ";
                sql += @"
                                               and m.target_plant_code = @plant
                                               and q.value_str1 = @str1
                                               and q.data_day between @fromTime and @endTime
                                             group by q.plant_code  + '-' +
                                                      q.value_str1,
                                                      q.data_day) a) r
                             where r.plant_code is not null
                             order by r.plant_code, r.date ";
                ht.Clear();
                ht.Add("str1", str1);
                ht.Add("plant", plant);
                ht.Add("fromTime", fromTime);
                ht.Add("endTime", endTime);
                if (strType != "")
                    ht.Add("splant", strType);
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

        public DataTable GetRepairBackPotRateMonthly(string str1, string plant, string fromTime, string endTime)
        {
            {
                ExecutionResult exeRes = new ExecutionResult();
                exeRes.Status = true;
                exeRes.Message = string.Empty;
                Hashtable ht = new Hashtable();
                DataTable dtResult = new DataTable();
                string strType = "";
                if (plant.IndexOf("VNP1-") > -1)
                {
                    string[] aa = plant.Split('-');
                    plant = aa[0];
                    if (aa[1] == "DOCK")
                        strType = "VN1";
                    else if (aa[1] == "NB")
                        strType = "VNNB1";
                }
                string sql = @"select * from (SELECT a.plant_code,
                                           a.data_month date,
                                           case a.qty2
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round((cast(a.qty1 as float) / qty2) * 100, 2)
                                           end value
                                      FROM (SELECT sum(q.value_real4) qty1,
                                                   sum(q.value_real1) qty2,
                                                   q.data_month,
                                                   q.plant_code + '-' +
                                                   q.value_str1 plant_code
                                              FROM V_wr_q_generic q, V_wr_plant_mapping m
                                             WHERE q.kpi_category = 'Q0007'
                                               and m.source_plant_code = q.plant_code";
                if (strType != "")
                    sql += @"  and m.source_plant_code = @splant ";
                sql += @"
                                               and m.target_plant_code = @plant
                                               and q.value_str1 = @str1
                                               and q.data_month between @fromTime and @endTime
                                             group by q.plant_code  + '-' +
                                                      q.value_str1,
                                                      q.data_month) a) r
                             where r.plant_code is not null
                             order by r.plant_code, r.date ";
                ht.Clear();
                ht.Add("str1", str1);
                ht.Add("plant", plant);
                ht.Add("fromTime", fromTime);
                ht.Add("endTime", endTime);
                if (strType != "")
                    ht.Add("splant", strType);
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

        #region Pid
        public DataTable GetPidLcdDailyTrend(string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" Select (select top 1 l.plant_code
                                  FROM V_wr_q_lcd l, V_wr_plant_mapping m
                                 where m.source_plant_code = l.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                   and m.target_plant_code =@plant
                                   and l.data_day between @fromTime and @endTime) plant_code,
                               cast(l.data_day as varchar) date,
                               case sum(l.input_qty)
                                 when 0 then
                                  0
                                 when null then
                                  0
                                 else
                                  round(sum(l.pid_qty) / sum(l.input_qty) * 1000000,0)
                               end value
                          FROM V_wr_q_lcd l, V_wr_plant_mapping m
                         where m.source_plant_code = l.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                           and m.target_plant_code =@plant
                           and l.data_day between @fromTime and @endTime
                         group by  l.data_day
                         order by  l.data_day ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" Select (select top 1 l.plant_code
                                      FROM V_wr_q_lcd l, V_wr_plant_mapping m
                                     where m.source_plant_code = l.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and l.data_week between @fromTime and @endTime) plant_code,
                                   l.data_week date,
                                   case sum(l.input_qty)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(l.pid_qty) / sum(l.input_qty) * 1000000,0)
                                   end value
                              FROM V_wr_q_lcd l, V_wr_plant_mapping m
                             where m.source_plant_code = l.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                               and m.target_plant_code =@plant
                               and l.data_week between @fromTime and @endTime
                             group by  l.data_week
                             order by  l.data_week ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" Select (select top 1 l.plant_code
                                      FROM V_wr_q_lcd l, V_wr_plant_mapping m
                                     where m.source_plant_code = l.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and l.data_month between @fromTime and @endTime) plant_code,
                                   l.data_month date,
                                   case sum(l.input_qty)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(l.pid_qty) / sum(l.input_qty) * 1000000, 0)
                                   end value
                              FROM V_wr_q_lcd l, V_wr_plant_mapping m
                             where m.source_plant_code = l.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                               and m.target_plant_code =@plant
                               and l.data_month between @fromTime and @endTime
                             group by l.data_month
                             order by l.data_month ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 me.plant_code
                                      from V_wr_q_me me, V_wr_plant_mapping m
                                     where m.source_plant_code = me.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and me.data_day between @fromTime and @endTime) plant_code,
                                   cast(me.data_day as varchar) date,
                                   case sum(me.qty_2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(me.qty_1) / sum(me.qty_2) * 1000000, 0)
                                   end value
                              from V_wr_q_me me, V_wr_plant_mapping m
                             where m.source_plant_code = me.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                               and m.target_plant_code =@plant
                               and me.data_day between @fromTime and @endTime
                             group by  me.data_day
                             order by  me.data_day ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 me.plant_code
                                      from V_wr_q_me me, V_wr_plant_mapping m
                                     where m.source_plant_code = me.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and me.data_week between @fromTime and @endTime) plant_code,
                                   me.data_week date,
                                   case sum(me.qty_2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(me.qty_1) / sum(me.qty_2) * 1000000, 0)
                                   end value
                              from V_wr_q_me me, V_wr_plant_mapping m
                             where m.source_plant_code = me.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                               and m.target_plant_code =@plant
                               and me.data_week between @fromTime and @endTime
                             group by me.data_week
                             order by me.data_week ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 me.plant_code
                                      from V_wr_q_me me, V_wr_plant_mapping m
                                     where m.source_plant_code = me.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and me.data_month between @fromTime and @endTime) plant_code,
                                   me.data_month date,
                                   case sum(me.qty_2)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(sum(me.qty_1) / sum(me.qty_2) * 1000000, 0)
                                   end value
                              from V_wr_q_me me, V_wr_plant_mapping m
                             where m.source_plant_code = me.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                               and m.target_plant_code =@plant
                               and me.data_month between @fromTime and @endTime
                             group by me.data_month
                             order by me.data_month ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 k.plant_code
                                     from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and k.data_day between @fromTime and @endTime) plant_code,
                                           cast(k.data_day as varchar) date,
                                           case sum(k.qty_2)
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round(sum(k.qty_1) / sum(k.qty_2) * 1000000,0)
                                           end value
                                      from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and k.data_day between @fromTime and @endTime
                                     group by  k.data_day
                                     order by  k.data_day ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 k.plant_code
                                     from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and k.data_week between @fromTime and @endTime) plant_code,
                                           k.data_week date,
                                           case sum(k.qty_2)
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round(sum(k.qty_1) / sum(k.qty_2) * 1000000,0)
                                           end value
                                      from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and k.data_week between @fromTime and @endTime
                                     group by  k.data_week
                                     order by  k.data_week ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select ( select top 1 k.plant_code
                                     from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                        and m.target_plant_code =@plant
                                        and k.data_month between @fromTime and @endTime) plant_code,
                                           k.data_month date,
                                           case sum(k.qty_2)
                                             when 0 then
                                              0
                                             when null then
                                              0
                                             else
                                              round(sum(k.qty_1) / sum(k.qty_2) * 1000000,0)
                                           end value
                                      from V_wr_q_kps k, V_wr_plant_mapping m
                                     where m.source_plant_code = k.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                       and m.target_plant_code =@plant
                                       and k.data_month between @fromTime and @endTime
                                     group by  k.data_month
                                     order by  k.data_month ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        #region Wfr
        public DataTable GetWfrDailyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" SELECT data_day  date
            , SUM(value_int1) AS value
         FROM V_wr_q_generic w WITH (NOLOCK) , V_wr_plant_mapping m
        WHERE w.plant_code = m.source_plant_code  ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
		  AND w.kpi_category='WFR'
          AND w.value_str2='DAILY'
          and m.target_plant_code =@plant
          and w.value_str1 =@str1
          and w.data_day between @fromTime and @endTime
          AND w.value_str3 = 'TOTAL' -- category != DISTRIBUTION
        GROUP BY w.data_day
        order by  w.data_day ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" SELECT data_week date, AVG(t1.item_value) AS value  
  FROM (
       SELECT data_day 
            , data_week 
            , SUM(value_int1) AS item_value
         FROM V_wr_q_generic w WITH (NOLOCK) , V_wr_plant_mapping m
        WHERE w.plant_code = m.source_plant_code  ";
            // if (strType != "" && str1 != "SMT")
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
		  AND w.kpi_category='WFR'
          AND w.value_str2='DAILY'
           and m.target_plant_code =@plant
          and w.data_week between @fromTime and @endTime
          and w.value_str1 =@str1
          AND w.value_str3 = 'TOTAL' -- category != DISTRIBUTION
        GROUP BY w.data_day, w.data_week
     ) t1
GROUP BY t1.data_week
 ORDER BY t1.data_week  ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" SELECT data_month date, AVG(t1.item_value) AS value  
  FROM (
       SELECT data_day 
            , data_month 
            , SUM(value_int1) AS item_value
         FROM V_wr_q_generic w WITH (NOLOCK) , V_wr_plant_mapping m
        WHERE w.plant_code = m.source_plant_code  ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
		  AND w.kpi_category='WFR'
          AND w.value_str2='DAILY'
          and m.target_plant_code =@plant
          and w.value_str1 =@str1
          and w.data_month between @fromTime and @endTime
          AND w.value_str3 = 'TOTAL' -- category != DISTRIBUTION
        GROUP BY w.data_day, w.data_month
     ) t1
GROUP BY t1.data_month
 ORDER BY t1.data_month  ";
            ht.Clear();
            ht.Add("str1", str1);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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
        public DataTable GetUPPHDaily(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select * from (SELECT a.plant_code,
                                                   a.work_date date,
                                                   case a.qty2
                                                     when 0 then
                                                      0
                                                     when null then
                                                      0
                                                     else
                                                      round((cast(a.qty1 as float) / qty2), 2)
                                                   end value
                                              FROM (SELECT sum(u.upph_qty) qty1,
                                                           sum(u.man_hour) qty2,
                                                           u.work_date,
                                                           u.plant_code + '-' + u.process plant_code
                                                      FROM V_wr_kpi_upx u, V_wr_plant_mapping m
                                                     WHERE u.process = @process
                                                       and m.source_plant_code = u.plant_code
                                                       and m.target_plant_code = @plant
													   and u.cust_no!='CDSMTDOCK'
                                                       and u.work_date between @fromTime and @endTime
                                                     group by u.plant_code + '-' + process, u.work_date) a) r
                                     where r.plant_code is not null
                                     order by r.plant_code, r.date ";
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

        public DataTable GetUPPHWeekly(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select * from (SELECT a.plant_code,
                                                   a.data_week date,
                                                   case a.qty2
                                                     when 0 then
                                                      0
                                                     when null then
                                                      0
                                                     else
                                                      round((cast(a.qty1 as float) / qty2), 2)
                                                   end value
                                              FROM (SELECT sum(u.upph_qty) qty1,
                                                           sum(u.man_hour) qty2,
                                                           u.data_week,
                                                           u.plant_code + '-' + u.process plant_code
                                                      FROM V_wr_kpi_upx u, V_wr_plant_mapping m
                                                     WHERE u.process = @process
                                                       and m.source_plant_code = u.plant_code
                                                       and m.target_plant_code = @plant
													   and u.cust_no!='CDSMTDOCK'
                                                       and u.data_week between @fromTime and @endTime
                                                     group by u.plant_code + '-' + process, u.data_week) a) r
                                     where r.plant_code is not null
                                     order by r.plant_code, r.date ";
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

        public DataTable GetUPPHMonthly(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select * from (SELECT a.plant_code,
                                                   a.data_month date,
                                                   case a.qty2
                                                     when 0 then
                                                      0
                                                     when null then
                                                      0
                                                     else
                                                      round((cast(a.qty1 as float) / qty2), 2)
                                                   end value
                                              FROM (SELECT sum(u.upph_qty) qty1,
                                                           sum(u.man_hour) qty2,
                                                           u.data_month,
                                                           u.plant_code + '-' + u.process plant_code
                                                      FROM V_wr_kpi_upx u, V_wr_plant_mapping m
                                                     WHERE u.process = @process
                                                       and m.source_plant_code = u.plant_code
                                                       and m.target_plant_code = @plant
													   and u.cust_no!='CDSMTDOCK'
                                                       and u.data_month between @fromTime and @endTime
                                                     group by u.plant_code + '-' + process, u.data_month) a) r
                                     where r.plant_code is not null
                                     order by r.plant_code, r.date ";
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

        public DataTable GetNewStaffRateDaily(string type, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();            
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select a.plant,
                                   a.data_day date,
                                   case a2.sumqty
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round((cast(a.qty as float) / a2.sumqty) * 100, 1)
                                   end value,
                                   a.qty,
                                   a2.sumqty
                              from V_wr_a_arm a,
                                   (select a.data_day, sum(qty) sumqty
                                      from V_wr_a_arm a
                                     WHERE a.plant = @plant
                                       and a.data_day between @fromTime and @endTime
                                     group by data_day) a2
                             WHERE a.data_day = a2.data_day
                               and a.mapping = @type
                               and a.plant = @plant
                               and a.data_day between @fromTime and @endTime
                             order by a.plant, date  ";
            ht.Clear();
            ht.Add("type", type);
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

        public DataTable GetNewStaffRateWeekly(string type, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select a.plant,
                                   a.data_week date,
                                   case a2.sumqty
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round((cast(a.qty as float) / a2.sumqty) * 100, 1)
                                   end value,
                                   a.qty,
                                   a2.sumqty
                              from V_wr_a_arm a,
                                   (select a.data_week, sum(qty) sumqty
                                      from V_wr_a_arm a
                                     WHERE a.plant = @plant
                                       and a.data_week between @fromTime and @endTime
                                     group by data_week) a2
                             WHERE a.data_week = a2.data_week
                               and a.mapping = @type
                               and a.plant = @plant
                               and a.data_week between @fromTime and @endTime
                             order by a.plant, date  ";
            ht.Clear();
            ht.Add("type", type);
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

        public DataTable GetNewStaffRateMonthly(string type, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select a.plant,
                                   a.data_month date,
                                   case a2.sumqty
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round((cast(a.qty as float) / a2.sumqty) * 100, 1)
                                   end value,
                                   a.qty,
                                   a2.sumqty
                              from V_wr_a_arm a,
                                   (select a.data_month, sum(qty) sumqty
                                      from V_wr_a_arm a
                                     WHERE a.plant = @plant
                                       and a.data_month between @fromTime and @endTime
                                     group by data_month) a2
                             WHERE a.data_month = a2.data_month
                               and a.mapping = @type
                               and a.plant = @plant
                               and a.data_month between @fromTime and @endTime
                             order by a.plant, date  ";
            ht.Clear();
            ht.Add("type", type);
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

        public DataTable GetInsightYRDailyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 f.plant_code
                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                  where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                    and m.target_plant_code=@plant
                                    and f.process =@process
                                    and f.data_day between @fromTime and @endTime) plant_code,
                               f.data_day date,
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
                         where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                           and m.target_plant_code=@plant
                           and f.process =@process
                           and f.data_day between @fromTime and @endTime
                         group by f.data_day
                         order by f.data_day ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetInsightYRWeeklyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 f.plant_code
                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                  where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                    and m.target_plant_code =@plant
                                    and f.process =@process 
                                    and f.data_week between @fromTime and @endTime) plant_code,
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
                          where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                            and m.target_plant_code =@plant
                            and f.process =@process 
                            and f.data_week between @fromTime and @endTime
                          group by f.data_week
                          order by f.data_week ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetInsightYRMonthlyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 f.plant_code
                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                  where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                    and m.target_plant_code =@plant
                                    and f.process =@process 
                                    and f.data_month between @fromTime and @endTime) plant_code,
                                f.data_month date,
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
                          where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                            and m.target_plant_code =@plant
                            and f.process =@process 
									and f.data_month!='2020M01'
									and f.data_month!='2020M02'
                            and f.data_month between @fromTime and @endTime
                          group by f.data_month
                          order by f.data_month ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetRjtRateDaily(string type, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @"  select MAX(o.PLANT_CODE) PLANT_CODE,
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
   and o.value_str7 =@type
   and o.data_day between @fromTime and @endTime
   and m.target_plant_code = @plant
 GROUP BY o.data_day
 order by o.data_day  ";
            ht.Clear();
            ht.Add("type", type);
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

        public DataTable GetRjtRateWeekly(string type, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @"  select MAX(o.PLANT_CODE) PLANT_CODE,
       o.data_week date,
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
   and o.value_str7 =@type
   and o.data_week between @fromTime and @endTime
   and m.target_plant_code = @plant
 GROUP BY o.data_week
 order by o.data_week  ";
            ht.Clear();
            ht.Add("type", type);
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

        public DataTable GetRjtRateMonthly(string type, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @"  select MAX(o.PLANT_CODE) PLANT_CODE,
       o.data_month date,
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
   and o.value_str7 =@type
   and o.data_month between @fromTime and @endTime
   and m.target_plant_code = @plant
 GROUP BY o.data_month
 order by o.data_month  ";
            ht.Clear();
            ht.Add("type", type);
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

        public DataTable GetHsrDailyTrend(string str1, string plant, string fromTime, string endTime, string str2)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select b.plant_code,
                                   b.date,--convert(varchar,convert(int,SUBSTRING(b.date, 5, 2)))+'/'+convert(varchar,convert(int,SUBSTRING(b.date, 7, 2))) date,
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
                                                   FROM (SELECT top 1 cast(plant_code + '-" + str2 + @"' as varchar(50)) p1
                                                           FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                          WHERE kpi_category = 'Q0010'
                                                            and m.source_plant_code = q.plant_code
                                                            and m.target_plant_code = @plant
                                                            and q.data_day between @fromTime and @endTime
                                                            and q.process = @str1) p UNPIVOT(plant_code FOR value_type IN(p1)) vot) t1
                                           full join (SELECT distinct data_day date, 'T' t
                                                       FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                      WHERE kpi_category = 'Q0010'
                                                        and m.source_plant_code = q.plant_code
                                                        and m.target_plant_code = @plant
                                                        and q.data_day between @fromTime and @endTime
                                                        and q.process = @str1) t2
                                             on t1.t = t2.t) b
                                on (z.plant_code = b.plant_code and z.date = b.date)
                             --order by b.plant_code, b.date ";
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

        public DataTable GetHsrWeeklyTrend(string str1, string plant, string fromTime, string endTime, string str2)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select b.plant_code,
                                   b.date,--SUBSTRING(b.date,5,len(b.date)-4) date,
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
                                                   FROM (SELECT top 1 cast(plant_code + '-" + str2 + @"' as varchar(50)) p1
                                                           FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                          WHERE kpi_category = 'Q0010'
                                                            and m.source_plant_code = q.plant_code
                                                            and m.target_plant_code = @plant
                                                            and q.data_week between @fromTime and @endTime
                                                            and q.process = @str1) p UNPIVOT(plant_code FOR value_type IN(p1)) vot) t1
                                           full join (SELECT distinct data_week date, 'T' t
                                                       FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                      WHERE kpi_category = 'Q0010'
                                                        and m.source_plant_code = q.plant_code
                                                        and m.target_plant_code = @plant
                                                        and q.data_week between @fromTime and @endTime
                                                        and q.process = @str1) t2
                                             on t1.t = t2.t) b
                                on (z.plant_code = b.plant_code and z.date = b.date)
                             --order by b.plant_code, b.date ";
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

        public DataTable GetHsrMonthlyTrend(string str1, string plant, string fromTime, string endTime, string str2)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            if (plant.IndexOf("VNP1-") > -1)
            {
                plant = "VNP1";
                //string[] aa = plant.Split('-');
                //plant = aa[0];
                //if (aa[1] == "DOCK")
                //    strType = "CHV_EDOCKING";
                //else if (aa[1] == "NB")
                //    strType = "CHV_A31";
            }
            string sql = @" select b.plant_code,
                                   b.date,--SUBSTRING(b.date,5,len(b.date)-4) date,
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
                                                   FROM (SELECT top 1 cast(plant_code + '-" + str2 + @"' as varchar(50)) p1
                                                           FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                          WHERE kpi_category = 'Q0010'
                                                            and m.source_plant_code = q.plant_code
                                                            and m.target_plant_code = @plant
                                                            and q.data_month between @fromTime and @endTime
                                                            and q.process = @str1) p UNPIVOT(plant_code FOR value_type IN(p1)) vot) t1
                                           full join (SELECT distinct data_month date, 'T' t
                                                       FROM V_wr_q_generic2 q, V_wr_plant_mapping m
                                                      WHERE kpi_category = 'Q0010'
                                                        and m.source_plant_code = q.plant_code
                                                        and m.target_plant_code = @plant
                                                        and q.data_month between @fromTime and @endTime
                                                        and q.process = @str1) t2
                                             on t1.t = t2.t) b
                                on (z.plant_code = b.plant_code and z.date = b.date)
                             --order by b.plant_code, b.date ";
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

        public DataTable GetOutputDailyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 f.plant_code
                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                  where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                    and m.target_plant_code=@plant
                                    and f.process =@process
                                    and f.data_day between @fromTime and @endTime) plant_code,
                               f.data_day date,
                               case 
                                  when sum(f.station_qty)<=0 then
                                   0
                                  when sum(f.station_qty) is null then
                                   0                                  
                                  else
                                   sum(f.station_qty)
                                end value
                          from V_wr_q_plrr f, V_wr_plant_mapping m
                         where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                           and m.target_plant_code=@plant
                           and f.process =@process
                           and f.data_day between @fromTime and @endTime
                         group by f.data_day
                         order by f.data_day ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetOutputWeeklyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 f.plant_code
                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                  where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                    and m.target_plant_code =@plant
                                    and f.process =@process 
                                    and f.data_week between @fromTime and @endTime) plant_code,
                                f.data_week date,
                                case 
                                  when sum(f.station_qty)<=0 then
                                   0
                                  when sum(f.station_qty) is null then
                                   0                                  
                                  else
                                   sum(f.station_qty)
                                end value
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                            and m.target_plant_code =@plant
                            and f.process =@process 
                            and f.data_week between @fromTime and @endTime
                          group by f.data_week
                          order by f.data_week ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public DataTable GetOutputMonthlyTrend(string process, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strType = "";
            if (plant.IndexOf("VNP1-") > -1)
            {
                string[] aa = plant.Split('-');
                plant = aa[0];
                if (aa[1] == "DOCK")
                    strType = "VN1";
                else if (aa[1] == "NB")
                    strType = "VNNB1";
            }
            string sql = @" select (select top 1 f.plant_code
                                   from V_wr_q_plrr f, V_wr_plant_mapping m
                                  where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                                    and m.target_plant_code =@plant
                                    and f.process =@process 
                                    and f.data_month between @fromTime and @endTime) plant_code,
                                f.data_month date,
                                case 
                                  when sum(f.station_qty)<=0 then
                                   0
                                  when sum(f.station_qty) is null then
                                   0                                  
                                  else
                                   sum(f.station_qty)
                                end value
                           from V_wr_q_plrr f, V_wr_plant_mapping m
                          where m.source_plant_code = f.plant_code ";
            if (strType != "")
                sql += @"  and m.source_plant_code = @splant ";
            sql += @"
                            and m.target_plant_code =@plant
                            and f.process =@process 
                            and f.data_month between @fromTime and @endTime
                          group by f.data_month
                          order by f.data_month ";
            ht.Clear();
            ht.Add("process", process);
            ht.Add("plant", plant);
            ht.Add("fromTime", fromTime);
            ht.Add("endTime", endTime);
            if (strType != "")
                ht.Add("splant", strType);
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

        public string GetDataDictionary(string vDataType, string vDataCode)
        {
            string result = "";
            Hashtable ht = new Hashtable();

            string sql = @" SELECT t.DATA_VALUE  FROM sfis1.C_DATA_DICTIONARY_T t WHERE t.DATA_TYPE =:data_type and t.DATA_CODE = :data_code";
            ht.Clear();
            ht.Add("data_type", vDataType);
            ht.Add("data_code", vDataCode);

            result = mDCDBTools.ExecuteQueryStrHt(sql, ht);

            return result;
        }

        public DataTable GetUsageDailyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strWorkPlace = "";
            if (plant.IndexOf("VNP1") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "VNP1");
            else if (plant.IndexOf("CTY1") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "TW01");
            else if (plant.IndexOf("CDDOCK") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "CDP1");
            else if (plant.IndexOf("KSP1") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "KSP1");
            else
                strWorkPlace = GetDataDictionary("USER_PLANT", plant);
            if (strWorkPlace != "")
            {
                try
                {
                    string sql = @"select substr(:fromdate,0,8) as date1,nvl(sum(t.value),0) as value from
                                    (select l.user_id,nvl(count(*),0) as value from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id 
                                        and l.controller='FPYDeepDive' and l.action=:action 
                                        and u.user_id not in ('yuhua_lu',
                                                             'haiyan_qiao',
                                                             'lin_yuan',
                                                             'miracle_chen',
                                                             'william_jin',
                                                             'shiuan_huang',
                                                             'harry_zhu',
                                                             'yatao_zhu',
                                                             'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller and l.login_time between to_date(:fromdate,'yyyyMMddhh24miss') and to_date(:enddate,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t@dc_link e
                                         where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name !='製造系統處'
                                   and e.dept_name not like '%智能%' 
                                   and e.working_place like :workplace";
                    ht.Clear();
                    ht.Add("workplace", strWorkPlace);
                    ht.Add("action", str1);
                    ht.Add("fromdate", fromTime);
                    ht.Add("enddate", endTime);

                    exeRes = mHGPGTools.ExecuteQueryDSHt(sql, ht);
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
            }

            return dtResult;
        }

        public DataTable GetUsageWeeklyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strWorkPlace = "";
            if (plant.IndexOf("VNP1") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "VNP1");
            else if (plant.IndexOf("CTY1") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "TW01");
            else if (plant.IndexOf("CDDOCK") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "CDP1");
            else if (plant.IndexOf("KSP1") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "KSP1");
            else
                strWorkPlace = GetDataDictionary("USER_PLANT", plant);
            if (strWorkPlace != "")
            {
                try
                {
                    string sql = @"select TO_CHAR(TO_DATE(:fromdate, 'YYYYMMDDHH24MISS'),'IYYY')||'W'||TO_CHAR(TO_DATE(:fromdate,'YYYYMMDDHH24MISS'),'IW') as date1,nvl(sum(t.value),0) as value from
                                    (select l.user_id,nvl(count(*),0) as value from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id 
                                        and l.controller='FPYDeepDive' and l.action=:action 
                                        and u.user_id not in ('yuhua_lu',
                                                             'haiyan_qiao',
                                                             'lin_yuan',
                                                             'miracle_chen',
                                                             'william_jin',
                                                             'shiuan_huang',
                                                             'harry_zhu',
                                                             'yatao_zhu',
                                                             'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller and l.login_time between to_date(:fromdate,'yyyyMMddhh24miss') and to_date(:enddate,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t@dc_link e
                                         where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name !='製造系統處'
                                   and e.dept_name not like '%智能%' 
                                   and e.working_place like :workplace ";
                    ht.Clear();
                    ht.Add("workplace", strWorkPlace);
                    ht.Add("action", str1);
                    ht.Add("fromdate", fromTime);
                    ht.Add("enddate", endTime);

                    exeRes = mHGPGTools.ExecuteQueryDSHt(sql, ht);
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
            }

            return dtResult;
        }

        public DataTable GetUsageMonthlyTrend(string str1, string plant, string fromTime, string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string strWorkPlace = "";
            if (plant.IndexOf("VNP1") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "VNP1");
            else if (plant.IndexOf("CTY1") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "TW01");
            else if (plant.IndexOf("CDDOCK") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "CDP1");
            else if (plant.IndexOf("KSP1") > -1)
                strWorkPlace = GetDataDictionary("USER_PLANT", "KSP1");
            else
                strWorkPlace = GetDataDictionary("USER_PLANT", plant);
            if (strWorkPlace != "")
            {                
                try
                {
                    string sql = @"select substr(:fromdate, 1, 4) || 'M' || substr(:fromdate, 5, 2) as date1,nvl(sum(t.value),0) as value from
                                    (select l.user_id,nvl(count(*),0) as value from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id 
                                        and l.controller='FPYDeepDive' and l.action=:action
                                        and u.user_id not in ('yuhua_lu',
                                                             'haiyan_qiao',
                                                             'lin_yuan',
                                                             'miracle_chen',
                                                             'william_jin',
                                                             'shiuan_huang',
                                                             'harry_zhu',
                                                             'yatao_zhu',
                                                             'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller and l.login_time between to_date(:fromdate,'yyyyMMddhh24miss') and to_date(:enddate,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t@dc_link e
                                         where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name !='製造系統處'
                                   and e.dept_name not like '%智能%' 
                                   and e.working_place like :workplace";
                    ht.Clear();
                    ht.Add("workplace", strWorkPlace);
                    ht.Add("action", str1);
                    ht.Add("fromdate", fromTime);
                    ht.Add("enddate", endTime);

                    exeRes = mHGPGTools.ExecuteQueryDSHt(sql, ht);
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
            }

            return dtResult;
        }
    }
}
