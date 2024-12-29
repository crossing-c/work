using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class CustomerSqlOperater
    {
        private object[] mClientInfo;
        private string mDBName;
        private InfoLightMSTools mDBmsTools;
        public CustomerSqlOperater(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDBName = dbName;
            this.mDBmsTools = new InfoLightMSTools(clientInfo, "WARROOM");
        }

        public DataTable GetTitle(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sql = @" select distinct c.customer plant_code,c.value_str1
                                  FROM V_wr_q_generic c, V_wr_plant_mapping m
                                 WHERE m.source_plant_code = c.customer
                                   and c.kpi_category = 'Q0003'
                                   and m.target_plant_code =@plant
                                   order by c.value_str1 desc ";
            ht.Clear();
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

        public double GetValue(string plant,string str1,string time,string dateType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            double res = 0;
            string sql = @" select c.value_real1
                                  FROM V_wr_q_generic c, V_wr_plant_mapping m
                                 WHERE m.source_plant_code = c.customer
                                   and c.kpi_category = 'Q0003'
                                   and m.target_plant_code =@plant
                                   and c.value_str1=@str1 ";
            if (dateType.ToUpper() == "W")
            {
                sql += @" and c.data_week=@time ";
            }else if (dateType.ToUpper() == "M")
            {
                sql += @" and c.data_month=@time ";
            }
            else if (dateType.ToUpper() == "Q")
            {
                sql += @" and c.data_quarter=@time ";
            }
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1);
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
                        if (dtResult != null && dtResult.Rows.Count > 0)
                        {
                            string result = dtResult.Rows[0][0].ToString();
                            double.TryParse(result, out res);
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

        public DataTable GetWeekValue(string plant, string str1)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            string sql = @" select top 2 c.value_real1,c.data_week
      FROM V_wr_q_generic c, V_wr_plant_mapping m
     WHERE m.source_plant_code = c.customer
       and c.kpi_category = 'Q0003'
       and m.target_plant_code =@plant
       and c.value_str1=@str1
       and c.value_real1 !=0
       and c.data_week is not null
       order by c.data_week desc "; 
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1); 
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

        public DataTable GetMonthValue(string plant, string str1)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            string sql = @" select top 2 c.value_real1,c.data_month
      FROM V_wr_q_generic c, V_wr_plant_mapping m
     WHERE m.source_plant_code = c.customer
       and c.kpi_category = 'Q0003'
       and m.target_plant_code =@plant
       and c.value_str1=@str1
       and c.value_real1 !=0
       and c.data_month is not null
       order by c.data_month desc ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("str1", str1);
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

    }
}
