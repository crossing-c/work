using Compal.MESComponent;
using sDEM2100.DataGateway;


namespace sDEM2100.Beans
{
    class SvlrrMe
    {
        private string plant;
        private object[] mClientInfo;
        private string mDbName;
        private DqmsSqlOperater dqmsSqlOperater;

        public SvlrrMe(object[] clientInfo, string dbName, string plant)
        {
            mClientInfo = clientInfo;
            mDbName = dbName;
            this.plant = plant;
            dqmsSqlOperater = new DqmsSqlOperater(mClientInfo, dbName);
        }

        public ExecutionResult DoOpreater(int id)
        {
            var dqmsBean = new DqmsBean();
            var dt = dqmsSqlOperater.GetDqmsParams(id, plant);
            var exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            //var iniTrend = 0;
            if (dt != null && dt.Rows.Count > 0)
            {
                dqmsBean.KpiId = id;
                dqmsBean.PlantCode = plant;
                dqmsBean.BuCode = dt.Rows[0]["bu_code"].ToString();
                var dtSource = dqmsSqlOperater.GetSvlrrEe(plant, "SVLRR","ME");
                string sWorkDate = "", sValue = "0";
                double sdValue = 0;
                if (dtSource != null && dtSource.Rows.Count > 0)
                {
                    sValue = dtSource.Rows[0][0].ToString();
                    sWorkDate = dtSource.Rows[0][1].ToString();

                    dqmsBean.WorkDate = sWorkDate;
                    double.TryParse(sValue, out sdValue);
                    dqmsBean.Actual = sdValue;

                    var dtTmp = dqmsSqlOperater.GetDqmsKpi(dqmsBean.KpiId, dqmsBean.BuCode, dqmsBean.PlantCode);
                    if (dtTmp != null && dtTmp.Rows.Count > 0)
                    {
                        //hgpg存在
                        var oldWorkDate = dtTmp.Rows[0]["work_date"].ToString();
                        var lastActual = double.Parse(dtTmp.Rows[0]["actual"].ToString());

                        if (oldWorkDate != dqmsBean.WorkDate)
                        {
                            dqmsBean.LastActual = lastActual;
                            exeRes = dqmsSqlOperater.UpdateDqmsKpi(dqmsBean);
                            #region log
                            var flag = dqmsSqlOperater.CheckDqmsKpiLog(dqmsBean.KpiId, dqmsBean.BuCode, dqmsBean.PlantCode, dqmsBean.WorkDate);
                            if (!flag)
                            {
                                dqmsBean.WorkDate = oldWorkDate;
                                exeRes = dqmsSqlOperater.InsertDqmsKpiLog(dqmsBean);
                            }
                            #endregion
                        }
                        else
                        {
                            dqmsBean.LastActual = double.Parse(dtTmp.Rows[0]["last_actual"].ToString());
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
