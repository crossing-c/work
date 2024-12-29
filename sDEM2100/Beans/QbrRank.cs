using Compal.MESComponent;
using sDEM2100.DataGateway;
using sDEM2100.Utils;
using System;
using System.Data;

namespace sDEM2100.Beans
{
    class QbrRank
    {
        private string plant;
        private object[] mClientInfo;
        private string mDbName;
        private DqmsSqlOperater dqmsSqlOperater;

        public QbrRank(object[] clientInfo, string dbName, string plant)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.plant = plant;
            dqmsSqlOperater = new DqmsSqlOperater(mClientInfo, dbName);
        }

        public static double ComputationScore(double actual, double weight, double lowerLimit)
        {
            //double result= weight * 100 * (1 - (actual - 1) / lowerLimit);
            double result = weight * 100 -(actual - 1)*3;
            return double.Parse(Math.Round((decimal)result, 2, MidpointRounding.AwayFromZero).ToString());
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
            DataTable dtQbrRank = dqmsSqlOperater.GetDqmsParams(id, plant);
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            int iniTrend = 0;
            if (dtQbrRank != null && dtQbrRank.Rows.Count > 0)
            {
                string frequency = dtQbrRank.Rows[0]["frequency"].ToString();
                dqmsBean.KpiId = id;
                dqmsBean.BuCode = dtQbrRank.Rows[0]["bu_code"].ToString();
                dqmsBean.PlantCode = plant;
                DataTable dtSource = dqmsSqlOperater.GetQBR(plant, "QBR");
                string sWorkDate = "", sValue = "0";
                double sdValue = 0;
                if (dtSource != null && dtSource.Rows.Count > 0)
                {
                    sValue = dtSource.Rows[0][0].ToString();
                    sWorkDate = dtSource.Rows[0][1].ToString();

                    dqmsBean.WorkDate = sWorkDate;
                    double.TryParse(sValue, out sdValue);
                    dqmsBean.Actual = sdValue;
                    double weight = double.Parse(dtQbrRank.Rows[0]["weight"].ToString());
                    double lowerlimit = double.Parse(dtQbrRank.Rows[0]["lower_limit"].ToString());
                    iniTrend = int.Parse(dtQbrRank.Rows[0]["trend"].ToString());
                    dqmsBean.Score = QbrRank.ComputationScore(dqmsBean.Actual, weight, lowerlimit);

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
                            dqmsBean.Trend = QbrRank.CheckTrend(dqmsBean.Actual, dqmsBean.LastActual, iniTrend);
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
                            dqmsBean.Trend = QbrRank.CheckTrend(dqmsBean.Actual, dqmsBean.LastActual, iniTrend);
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
