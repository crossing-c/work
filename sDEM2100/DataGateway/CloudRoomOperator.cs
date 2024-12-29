using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Data.SqlClient;

namespace sDEM2100.DataGateway
{
    class CloudRoomOperator
    {
        private InfoLightMSTools mDBMSTools;
        public CloudRoomOperator(object[] clientInfo, string dbName)
        {
            mDBMSTools = new InfoLightMSTools(clientInfo, dbName);
        }
        public DataTable GetKpi_Name(string plant)
        {
            DataTable dt = new DataTable();
            List<SqlParameter> listParameter = new List<SqlParameter>();
            string sql = @" select distinct top 6 c.value_str1
                                           FROM V_wr_q_generic c, V_wr_plant_mapping m
                                          WHERE m.source_plant_code = c.customer
                                            and c.kpi_category = 'Q0003'
                                            and m.target_plant_code = @plant
                                          order by c.value_str1 desc  ";
            listParameter.Clear();
            SqlParameter parameter = new SqlParameter();
            parameter.ParameterName = "@plant";
            parameter.DbType = DbType.String;
            parameter.Value = plant;
            listParameter.Add(parameter);
            ExecutionResult result = mDBMSTools.ExecuteQueryDS(sql, listParameter);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable GetData(string plant,string kpi_name,string datetype)
        {
            DataTable dt = new DataTable();
            List<SqlParameter> listParameter = new List<SqlParameter>();
            string strcolumn = "";
            if (datetype == "M")
                strcolumn = " c.data_month ";
            if (datetype == "W")
                strcolumn = " c.data_week ";
            string sql = @"select *from (select top 7 c.value_str1,"+strcolumn+ @",
                                                            sum(c.value_real1) value_real1
                                                       FROM V_wr_q_generic c, V_wr_plant_mapping m
                                                      WHERE m.source_plant_code = c.customer
                                                        and c.kpi_category = 'Q0003'
                                                        and m.target_plant_code =@plant
                                                        and c.value_str1 =@kpi_name
                                                        and (" + strcolumn + @" is not null or " + strcolumn + @" = '')
                                                      group by c.value_str1, " + strcolumn + @"
                                                      order by " + strcolumn + @" desc) d
                                              order by " + strcolumn.Substring(3) ;
            listParameter.Clear();
            SqlParameter parameter = new SqlParameter();
            parameter.ParameterName = "@plant";
            parameter.DbType = DbType.String;
            parameter.Value = plant;
            listParameter.Add(parameter);
           SqlParameter  parameter2 = new SqlParameter();
            parameter2.ParameterName = "@kpi_name";
            parameter2.DbType = DbType.String;
            parameter2.Value = kpi_name;
            listParameter.Add(parameter2);
            ExecutionResult result = mDBMSTools.ExecuteQueryDS(sql, listParameter);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
    }
}
