using Compal.MESComponent;
using sDEM2100.DataGateway;
using sDEM2100.Utils;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace sDEM2100.Beans
{
    class WfrSmt
    {
        private string plant;
        private object[] mClientInfo;
        private string mDbName;
        private DqmsSqlOperater dqmsSqlOperater;

        public WfrSmt(object[] clientInfo, string dbName,string plant)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.plant = plant;
            dqmsSqlOperater = new DqmsSqlOperater(mClientInfo, dbName);
        }

        public static double ComputationScore(double actual,double weight)
        {
            double score = actual / 100 * weight * 100;
            return double.Parse(Math.Round((decimal)score, 2,MidpointRounding.AwayFromZero).ToString());
        }

        public static int CheckTrend(double score, double lastScore, int iniTrend)
        {
            int trend = 0;
            if (score > lastScore)
                trend = 1;
            if (score == lastScore)
                trend = 0;
            if (score < lastScore)
                trend = -1;
            //判断好坏
            int temp = 0;
            temp = iniTrend + trend;
            if (temp == 2 || temp == -2)
                trend = 1;
            if (temp == 0)
                trend = -1;
            return trend;
        }

        public ExecutionResult DoOpreater(double actual,int id)
        {
            DqmsBean dqmsBean = new DqmsBean();
            DataTable dtWfrSmt = dqmsSqlOperater.GetDqmsParams(id, plant);
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            int iniTrend = 0;
            if (dtWfrSmt != null && dtWfrSmt.Rows.Count > 0)
            {
                string frequency = dtWfrSmt.Rows[0]["frequency"].ToString();
                dqmsBean.KpiId = id;
                dqmsBean.BuCode = dtWfrSmt.Rows[0]["bu_code"].ToString();
                dqmsBean.PlantCode = plant;
                dqmsBean.WorkDate = WorkDateConvert.GetWorkDate(frequency,1);
                dqmsBean.Actual = actual;
                double weight = double.Parse(dtWfrSmt.Rows[0]["weight"].ToString());
                double lowerlimit = double.Parse(dtWfrSmt.Rows[0]["lower_limit"].ToString());
                iniTrend = int.Parse(dtWfrSmt.Rows[0]["trend"].ToString());
                dqmsBean.Score = WfrSmt.ComputationScore(dqmsBean.Actual, weight);
                bool flag = dqmsSqlOperater.CheckDqmsKpi(dqmsBean.KpiId, dqmsBean.BuCode, dqmsBean.PlantCode);
                string oldWorkDate;
                double lastActual, lastScore;
                int oldTrend;
                if (flag)
                {
                    //存在
                    DataTable dtTmp = dqmsSqlOperater.GetDqmsKpi(dqmsBean.KpiId, dqmsBean.BuCode, dqmsBean.PlantCode);
                    oldWorkDate = dtTmp.Rows[0]["work_date"].ToString();
                    //lastActual = double.Parse(dtTmp.Rows[0]["actual"].ToString());
                    //lastScore = double.Parse(dtTmp.Rows[0]["score"].ToString());
                    oldTrend = int.Parse(dtTmp.Rows[0]["trend"].ToString());
                    //if (oldWorkDate != dqmsBean.WorkDate)
                    //{
                        //再查一次 上周
                        double.TryParse(dqmsSqlOperater.GetPreWfrFatpSmt(plant, "SMT", WorkDateConvert.GetWorkDate(frequency,0)),out lastActual);
                        lastScore = WfrSmt.ComputationScore(lastActual, weight);
                        dqmsBean.LastActual = lastActual;
                        dqmsBean.LastScore = lastScore;
                    //}
                    //else
                    //{
                    //    dqmsBean.LastActual = double.Parse(dtTmp.Rows[0]["last_actual"].ToString());
                    //    dqmsBean.LastScore = double.Parse(dtTmp.Rows[0]["last_score"].ToString());
                    //}
                    dqmsBean.Trend = WfrSmt.CheckTrend(dqmsBean.Actual, dqmsBean.LastActual, iniTrend);
                    exeRes = dqmsSqlOperater.UpdateDqmsKpi(dqmsBean);
                    if (oldWorkDate != dqmsBean.WorkDate)
                    {
                        //记录log
                        flag = dqmsSqlOperater.CheckDqmsKpiLog(dqmsBean.KpiId, dqmsBean.BuCode, dqmsBean.PlantCode, dqmsBean.WorkDate);
                        if (!flag)
                        {
                            dqmsBean.Trend = oldTrend;
                            dqmsBean.WorkDate = oldWorkDate;
                            exeRes = dqmsSqlOperater.InsertDqmsKpiLog(dqmsBean);
                        }
                    }
                }
                else
                {
                    dqmsBean.LastActual = 0;
                    dqmsBean.LastScore = 0;
                    dqmsBean.Trend = 0;
                    exeRes = dqmsSqlOperater.InsertDqmsKpi(dqmsBean);
                    //dqmsSqlOperater.InsertDqmsKpiLog(dqmsBean);
                }
            }
            return exeRes;
        }
    }
}
