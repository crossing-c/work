using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class ScoreCardOperator
    {
        private InfoLightDBTools mDBTools;
        private Hashtable ht;
        public ScoreCardOperator(object[] clientInfo, string dbName)
        {
            mDBTools = new InfoLightDBTools(clientInfo, dbName);
            ht = new Hashtable();
        }
        public DataTable GetScoreCardData()
        {
            DataTable dt = new DataTable();
            string sql = @" SELECT T.PLANT_CODE, SUM(T.SCORE) AS SUMSCORE
                                              FROM dqms.r_kpi_t t,dqms.c_kpi_def_t D
                                              WHERE T.KPI_ID=D.KPI_ID
                                              AND D.IS_SCORE='Y'
                                              and t.plant_code!='CKY1'
                                              and t.plant_code!='KSP1-AEP'
                                              and t.plant_code!='KSP1-SVR'
                                             GROUP BY PLANT_CODE
                                             ORDER BY SUMSCORE DESC ";
            ExecutionResult result = mDBTools.ExecuteQueryDS(sql);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable GetKpi_Name()
        {
            DataTable dt = new DataTable();
            string sql = @"SELECT KPI_NAME as " + "\"Index\"" + " from(SELECT DISTINCT KPI_NAME, KPI_ID  FROM dqms.c_kpi_def_t where IS_SCORE='Y' ORDER BY KPI_ID)";
            ExecutionResult result = mDBTools.ExecuteQueryDS(sql);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public double  GetACTUAL(string kpi_name,string plant_code)
        {
            DataTable dt = new DataTable();
            double Actual = 0;
            string sql = @"select KPI_NAME,PLANT_CODE,ACTUAL from dqms.r_kpi_t a,dqms.c_kpi_def_t b 
                                            where a.kpi_id=b.kpi_id 
                                            AND a.plant_code=:plant_code
                                            AND b.kpi_name=:kpi_name
                                            ORDER BY KPI_NAME";
            ht.Clear();
            ht.Add("kpi_name", kpi_name);
            ht.Add("plant_code", plant_code);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql,ht);
            if (result.Status)
            {
                dt = ((DataSet)result.Anything).Tables[0];
                if (dt.Rows.Count > 0)
                    Actual = Convert.ToDouble(dt.Rows[0]["ACTUAL"].ToString());
            }
            return Actual;
        }
        public DataTable GetGOAL(string kpi_name)
        {
            DataTable dt = new DataTable();
            string sql = @"select KPI_NAME,goal,TREND
                              from dqms.c_kpi_def_t b
                             where kpi_name = :kpi_name ";
            ht.Clear();
            ht.Add("kpi_name", kpi_name);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
            {
                dt = ((DataSet)result.Anything).Tables[0];
            }
            return dt;
        }
        public bool CheckWeight(string kpi_name, string plant_code)
        {
            DataTable dt = new DataTable();
            bool check = false;
            string sql = @" SELECT P.WEIGHT
                            FROM dqms.c_plant_kpi_def_t P, dqms.c_kpi_def_t D
                            WHERE P.KPI_ID = D.KPI_ID
                            AND P.PLANT_CODE = :plant_code
                            AND D.KPI_NAME = :kpi_name ";
            ht.Clear();
            ht.Add("kpi_name", kpi_name);
            ht.Add("plant_code", plant_code);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
            {
                dt = ((DataSet)result.Anything).Tables[0];
                if (dt.Rows.Count == 0&&plant_code!="GOAL" && plant_code != "TREND")
                {
                    check = true;
                } 
            }
            return check;
        }
        public double GetScore(string kpi_name, string plant_code)
        {
            DataTable dt = new DataTable();
            double SCORE = 0;
            string sql = @"select KPI_NAME,PLANT_CODE,SCORE from dqms.r_kpi_t a,dqms.c_kpi_def_t b 
                                            where a.kpi_id=b.kpi_id 
                                            AND a.plant_code=:plant_code
                                            AND b.kpi_name=:kpi_name
                                            ORDER BY KPI_NAME";
            ht.Clear();
            ht.Add("kpi_name", kpi_name);
            ht.Add("plant_code", plant_code);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
            {
                dt = ((DataSet)result.Anything).Tables[0];
                if (dt.Rows.Count > 0)
                    SCORE = Convert.ToDouble(dt.Rows[0]["SCORE"].ToString());
            }
            return SCORE;
        }
        public double GetWeight(string kpi_name,string plant_code)
        {
            DataTable dt = new DataTable();
            double weight = 0;
            string sql = @"select k.KPI_NAME, t.PLANT_CODE, t.Weight
                                          from dqms.c_plant_kpi_def_t t, dqms.c_kpi_def_t k
                                          where t.kpi_id=k.kpi_id
                                          and plant_code=:plant_code
                                          and kpi_name=:kpi_name";
            ht.Clear();
            ht.Add("kpi_name",kpi_name);
            ht.Add("plant_code", plant_code);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql,ht);
            if (result.Status)
            { 
                dt = ((DataSet)result.Anything).Tables[0];
                if (dt.Rows.Count > 0)
                    weight = Convert.ToDouble(dt.Rows[0]["WEIGHT"])*100;
            }
            return weight;
        }
        
    }
}
