using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class NpiSqlOperater
    {
        private object[] mClientInfo;
        private string mDBName;
        private InfoLightMSTools mDBmsTools;
        public NpiSqlOperater(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDBName = dbName;
            this.mDBmsTools = new InfoLightMSTools(clientInfo, "WARROOM");
        }

        public DataTable GetModelData(string plant,string fromTime,string endTime)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select customer,
                               value_str1 model_name,
                               '0' total_fail,
                               'NA' trend,
                               'NA' color
                          from V_wr_q_generic
                         where kpi_category = 'OTBMCL'
                           and customer = @plant
                           and data_week between @fromtime and @endtime ";
            ht.Clear();
            ht.Add("plant", plant);
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

        public DataTable GetWeekValue(string plant, string model)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" SELECT top 2 l.value_str2 line_name,
                                   l.data_week,
                                   case sum(l.value_int1)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(100 - sum(l.value_int2 + l.value_int3) /
                                            cast(sum(l.value_int1) as float) * 100,
                                            2)
                                   end TOTAL_FAIL
                              FROM V_wr_q_generic l, V_wr_plant_mapping m
                             WHERE m.source_plant_code = l.plant_code
                               and kpi_category = 'TYPE1'
                               AND data_name = 'S005'
                               and m.target_plant_code =@plant
                               and l.value_str1 = 'AP'
                               and l.value_str2 = @model
                             group by l.value_str2, l.data_week
                             order by l.value_str2, l.data_week desc ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("model", model);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    dtResult = ((DataSet)exeRes.Anything).Tables[0];
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public double GetWeekTotalValue(string plant, string model)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            double res = 0;
            string sql = @" SELECT top 1 l.value_str2 line_name,
                                   l.data_week,
                                   case sum(l.value_int1)
                                     when 0 then
                                      0
                                     when null then
                                      0
                                     else
                                      round(100 - sum(l.value_int2 + l.value_int3) /
                                            cast(sum(l.value_int1) as float) * 100,
                                            2)
                                   end TOTAL_FAIL
                              FROM V_wr_q_generic l, V_wr_plant_mapping m
                             WHERE m.source_plant_code = l.plant_code
                               and kpi_category = 'TYPE1'
                               AND data_name = 'S005'
                               and m.target_plant_code =@plant
                               and l.value_str1 = 'AP'
                               and l.value_str2 = @model
                             group by l.value_str2, l.data_week
                             order by l.value_str2, l.data_week desc ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("model", model);
            try
            {
                exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = exeRes.Anything as DataSet;
                    if (ds != null && ds.Tables.Count > 0)
                    {
                        DataTable dt = ds.Tables[0];
                        if (dt != null && dt.Rows.Count > 0)
                        {
                            double.TryParse(dt.Rows[0]["TOTAL_FAIL"].ToString(),out res);
                        }
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
    }
}
