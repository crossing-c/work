using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class OverallQualitySqlOperater
    {
        private object[] mClientInfo;
        private string mDBName;
        private InfoLightMSTools mDBmsTools;
        public OverallQualitySqlOperater(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDBName = dbName;
            this.mDBmsTools = new InfoLightMSTools(clientInfo, "WARROOM");
        }

        public DataTable getOverallQualityData(string plant, string fDate, string eDate, string[] value_str)
        {
            ExecutionResult exeRes = new ExecutionResult();
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlValue_str = "''"; ;
            foreach (string str in value_str)
            {
                sqlValue_str += ",'" + str + "'";
            }
            string sql = @" select c.value_str1,data_month date,value_real1 value
      FROM V_wr_q_generic c, V_wr_plant_mapping m
     WHERE m.source_plant_code = c.customer
       and c.kpi_category = 'Q0003'
       and m.target_plant_code =@plant
       and c.value_str1 in (" + sqlValue_str + @")
       and c.data_month between @fDate and @eDate ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fDate", fDate);
            ht.Add("eDate", eDate);
            exeRes = mDBmsTools.ExecuteQueryDSHt(sql, ht);
            if (exeRes.Status)
            {
                dtResult = ((DataSet)exeRes.Anything).Tables[0];
            }
            return dtResult;
        }

    }
}
