﻿using Compal.MESComponent;
using sDEM2100.DataGateway;
using sDEM2100.Utils;
using System;
using System.Data;


namespace sDEM2100.Beans
{
    class FpySmt
    {
        private string plant;
        private object[] mClientInfo;
        private string mDbName;
        private DqmsSqlOperater dqmsSqlOperater;

        public FpySmt(object[] clientInfo, string dbName, string plant)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.plant = plant;
            dqmsSqlOperater = new DqmsSqlOperater(mClientInfo, dbName);
        }

        public static double ComputationScore(double actual, double weight)
        {
            double score = actual / 100 * weight * 100;
            return double.Parse(Math.Round((decimal)score, 2, MidpointRounding.AwayFromZero).ToString());
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

        public ExecutionResult DoOpreater(int id)
        {
            DqmsBean dqmsBean = new DqmsBean();
            DataTable dtFpySmt = dqmsSqlOperater.GetDqmsParams(id, plant);
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            int iniTrend = 0;
            if (dtFpySmt != null && dtFpySmt.Rows.Count > 0)
            {
                string frequency = dtFpySmt.Rows[0]["frequency"].ToString();
                dqmsBean.KpiId = id;
                dqmsBean.BuCode = dtFpySmt.Rows[0]["bu_code"].ToString();
                dqmsBean.PlantCode = plant;
                DataTable dtSource = dqmsSqlOperater.GetFpyApSmt(plant, "SMT");
                string sWorkDate = "", sValue = "0";
                double sdValue = 0;
                if (dtSource != null && dtSource.Rows.Count > 0)
                {
                    sValue = dtSource.Rows[0][0].ToString();
                    sWorkDate = dtSource.Rows[0][1].ToString();

                    dqmsBean.WorkDate = sWorkDate;
                    double.TryParse(sValue, out sdValue);
                    dqmsBean.Actual = sdValue;
                    double weight = double.Parse(dtFpySmt.Rows[0]["weight"].ToString());
                    double lowerlimit = double.Parse(dtFpySmt.Rows[0]["lower_limit"].ToString());
                    iniTrend = int.Parse(dtFpySmt.Rows[0]["trend"].ToString());
                    dqmsBean.Score = FpySmt.ComputationScore(dqmsBean.Actual, weight);

                    string oldWorkDate;
                    double lastActual, lastScore;
                    int oldTrend;

                    DataTable dtTmp = dqmsSqlOperater.GetDqmsKpi(dqmsBean.KpiId, dqmsBean.BuCode, dqmsBean.PlantCode);
                    if (dtTmp != null && dtTmp.Rows.Count > 0)
                    {
                        //hgpg存在
                        oldWorkDate = dtTmp.Rows[0]["work_date"].ToString();
                        lastActual = double.Parse(dtTmp.Rows[0]["actual"].ToString());
                        lastScore = double.Parse(dtTmp.Rows[0]["score"].ToString());
                        oldTrend = int.Parse(dtTmp.Rows[0]["trend"].ToString());

                        if (oldWorkDate != dqmsBean.WorkDate)
                        {
                            dqmsBean.LastActual = lastActual;
                            dqmsBean.LastScore = lastScore;
                            dqmsBean.Trend = FpySmt.CheckTrend(dqmsBean.Actual, dqmsBean.LastActual, iniTrend);
                            exeRes = dqmsSqlOperater.UpdateDqmsKpi(dqmsBean);
                            #region log
                            bool flag = dqmsSqlOperater.CheckDqmsKpiLog(dqmsBean.KpiId, dqmsBean.BuCode, dqmsBean.PlantCode, dqmsBean.WorkDate);
                            if (!flag)
                            {
                                dqmsBean.Trend = oldTrend;
                                dqmsBean.WorkDate = oldWorkDate;
                                exeRes = dqmsSqlOperater.InsertDqmsKpiLog(dqmsBean);
                            }
                            #endregion
                        }
                        else
                        {
                            dqmsBean.LastActual = double.Parse(dtTmp.Rows[0]["last_actual"].ToString());
                            dqmsBean.LastScore = double.Parse(dtTmp.Rows[0]["last_score"].ToString());
                            dqmsBean.Trend = FpySmt.CheckTrend(dqmsBean.Actual, dqmsBean.LastActual, iniTrend);
                            exeRes = dqmsSqlOperater.UpdateDqmsKpi(dqmsBean);
                        }
                    }
                    else
                    {
                        //第一次 不存在
                        dqmsBean.LastActual = 0;
                        dqmsBean.LastScore = 0;
                        dqmsBean.Trend = 0;
                        exeRes = dqmsSqlOperater.InsertDqmsKpi(dqmsBean);
                    }
                }
            }
            return exeRes;
        }
    }
}
