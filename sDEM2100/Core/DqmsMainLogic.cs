using Compal.MESComponent;
using sDEM2100.Beans;
using sDEM2100.DataGateway;
using sDEM2100.Utils;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;

namespace sDEM2100.Core
{
    class DqmsMainLogic
    {
        private object[] mClientInfo;
        DateTime dtNow;
        private MESLog mesLog;
        private string mDbName;
        private DqmsSqlOperater dqmsSqlOperater;

        public DqmsMainLogic(object[] clientInfo, string dbName)
        {
            this.mClientInfo = clientInfo;
            this.mDbName = dbName;
            this.mesLog = new MESLog("Dqms");
            dtNow = DateTime.Now;
            dqmsSqlOperater = new DqmsSqlOperater(mClientInfo, dbName);
        }

        public ExecutionResult ExecuteWeekly(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            int id = -1;
            #region
            DataTable dtResult=dqmsSqlOperater.GetKpiIdByFrequency("WEEKLY");
            if (dtResult != null && dtResult.Rows.Count > 0)
            {
                foreach(DataRow dr in dtResult.Rows)
                {
                    id = -1;
                    int.TryParse(dr["kpi_id"].ToString(),out id);
                    if(id!=-1)
                        exeRes = WeeklyOperater(id,plant, "WEEKLY");
                }
            }
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteMonthly(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            int id = -1;

            #region
            DataTable dtResult = dqmsSqlOperater.GetKpiIdByFrequency("MONTHLY");
            if (dtResult != null && dtResult.Rows.Count > 0)
            {
                foreach (DataRow dr in dtResult.Rows)
                {
                    id = -1;
                    int.TryParse(dr["kpi_id"].ToString(), out id);
                    if (id != -1)
                        exeRes = MonthlyOperater(id, plant, "MONTHLY");
                }
            }
            #endregion

            return exeRes;
        }

        public ExecutionResult ExecuteQuarter(string plant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            int id = -1;

            #region
            DataTable dtResult = dqmsSqlOperater.GetKpiIdByFrequency("QUARTER");
            if (dtResult != null && dtResult.Rows.Count > 0)
            {
                foreach (DataRow dr in dtResult.Rows)
                {
                    id = -1;
                    int.TryParse(dr["kpi_id"].ToString(), out id);
                    if (id != -1)
                        exeRes = QuarterOperater(id, plant, "QUARTER");
                }
            }
            #endregion

            return exeRes;
        }

        public ExecutionResult WeeklyOperater(int id,string plant,string frequency)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            switch (id)
            {
                case 100:
                    FpyAp fpyAp = new FpyAp(this.mClientInfo, this.mDbName, plant);
                    exeRes = fpyAp.DoOpreater(id);
                    break;
                case 101:
                    FpySmt fpySmt = new FpySmt(this.mClientInfo, this.mDbName, plant);
                    exeRes = fpySmt.DoOpreater(id);
                    break;
                case 103:
                    //OfflineRateProduct offlineRateProduct = new OfflineRateProduct(this.mClientInfo, this.mDbName, plant);
                    //exeRes = offlineRateProduct.DoOpreater(id);
                    ReflowRate reflowRateASSY = new ReflowRate(this.mClientInfo, this.mDbName, plant);
                    exeRes = reflowRateASSY.DoOpreater(id, "ASSY");
                    break;
                case 104:
                    //OfflineRateSemiProduct offlineRateSemiProduct = new OfflineRateSemiProduct(this.mClientInfo, this.mDbName, plant);
                    //exeRes = offlineRateSemiProduct.DoOpreater(id);
                    ReflowRate reflowRatePACK = new ReflowRate(this.mClientInfo, this.mDbName, plant);
                    exeRes = reflowRatePACK.DoOpreater(id, "PACK");
                    break;
                case 105:
                    PidLcd pidLcd = new PidLcd(this.mClientInfo, this.mDbName, plant);
                    exeRes = pidLcd.DoOpreater(id);
                    break;
                case 106:
                    PidKeyParts pidKps = new PidKeyParts(this.mClientInfo, this.mDbName, plant);
                    exeRes = pidKps.DoOpreater(id);
                    break;
                case 107:
                    PidMe pidMe = new PidMe(this.mClientInfo, this.mDbName, plant);
                    exeRes = pidMe.DoOpreater(id);
                    break;
                case 109:
                    WfrAp wfrAp = new WfrAp(this.mClientInfo, this.mDbName, plant);
                    double fatp = dqmsSqlOperater.GetNowWfrFatpSmt(plant, "FATP", WorkDateConvert.GetWorkDate("WEEKLY", 1));
                    exeRes = wfrAp.DoOpreater(fatp, id);
                    break;
                case 110:
                    WfrSmt wfrSmt = new WfrSmt(this.mClientInfo, this.mDbName, plant);
                    double wsmt = dqmsSqlOperater.GetNowWfrFatpSmt(plant, "SMT", WorkDateConvert.GetWorkDate("WEEKLY", 1));
                    exeRes = wfrSmt.DoOpreater(wsmt, id);
                    break;
                case 111:
                    Ctq ctq = new Ctq(this.mClientInfo, this.mDbName, plant);
                    exeRes = ctq.DoOpreater(id);
                    break;
                case 112:
                    LrrAp lrrAP = new LrrAp(this.mClientInfo, this.mDbName, plant);
                    exeRes = lrrAP.DoOpreater(id);
                    break;
                case 113:
                    LrrSmt lrrSmt = new LrrSmt(this.mClientInfo, this.mDbName, plant);
                    exeRes = lrrSmt.DoOpreater(id);
                    break;
                case 114:
                    SvlrrEe svlrrEe = new SvlrrEe(mClientInfo, mDbName, plant);
                    exeRes = svlrrEe.DoOpreater(id);
                    break;
                case 115:
                    FrrEe frrEe = new FrrEe(mClientInfo, mDbName, plant);
                    exeRes = frrEe.DoOpreater(id);
                    break;
                case 116:
                    SvlrrMe SvlrrMe = new SvlrrMe(mClientInfo, mDbName, plant);
                    exeRes = SvlrrMe.DoOpreater(id);
                    break; 
                case 117:
                    FrrMe frrMe = new FrrMe(mClientInfo, mDbName, plant);
                    exeRes = frrMe.DoOpreater(id);
                    break;
                case 118:
                    OpWeek opWeek = new OpWeek(mClientInfo, mDbName, plant);
                    exeRes = opWeek.DoOpreater(id);
                    break;
                case 121:
                    HsrAp hsrAp = new HsrAp(mClientInfo, mDbName, plant);
                    exeRes = hsrAp.DoOpreater(id);
                    break;
                case 122:
                    HsrSmt hsrSmt = new HsrSmt(mClientInfo, mDbName, plant);
                    exeRes = hsrSmt.DoOpreater(id);
                    break;                 
            }
            return exeRes;
        }

        public ExecutionResult MonthlyOperater(int id, string plant, string frequency)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            switch (id)
            {
                case 102:
                    Scrap scrap = new Scrap(this.mClientInfo, this.mDbName, plant);
                    exeRes = scrap.DoOpreater(id);
                    break;
                case 108:
                    QbrRank qbrRank = new QbrRank(this.mClientInfo, this.mDbName, plant);
                    exeRes = qbrRank.DoOpreater(id);
                    break;
                case 119:
                    OpMonth opMonth = new OpMonth(mClientInfo, mDbName, plant);
                    exeRes = opMonth.DoOpreater(id);
                    break;
      
            }
            return exeRes;
        }

        public ExecutionResult QuarterOperater(int id, string plant, string frequency)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            switch (id)
            {
                case 120:
                    OpQuarter opQuarter = new OpQuarter(mClientInfo, mDbName, plant);
                    exeRes = opQuarter.DoOpreater(id);
                    break;
            }
            return exeRes;
        }
    }
}
