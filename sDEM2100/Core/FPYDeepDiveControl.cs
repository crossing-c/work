using sDEM2100.Beans;
using Compal.MESComponent;
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
    class FPYDeepDiveControl
    {
        private DateTime dtNow;
        private static FPYDeepDiveOperator fpydeepdiveOperator;
        private static MpSqlOperater mpsqlOperator;
        private object[] mClientInfo;
        public FPYDeepDiveControl(object[] clientInfo, string DBName)
        {
            dtNow = DateTime.Now;
            fpydeepdiveOperator = new FPYDeepDiveOperator(clientInfo, DBName);
            mpsqlOperator = new MpSqlOperater(clientInfo, DBName);
            mClientInfo = clientInfo;
        }

        public ExecutionResult GetFpyByLob(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = fpydeepdiveOperator.GetFpyDataByLob(plant, sapplant, bydateType, qCond);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0);
            // DataTable dtGroup = dtLobFpy.Copy();
            DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroup(plant, sapplant, bydateType, qCond);
            if (dtGroup.Rows.Count == 0)
                dtGroup.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);
            dicResult.Add(bydateType + "FpyGroup", dtGroup);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobSMT(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobSMT(plant, sapplant, bydateType, qCond);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0);
            // DataTable dtGroup = dtLobFpy.Copy();
            DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupSMT(plant, sapplant, bydateType, qCond, 10);
            if (dtGroup.Rows.Count == 0)
                dtGroup.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);
            dicResult.Add(bydateType + "FpyGroup", dtGroup);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetFpyByLobInsightSMT(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobInsightSMT(plant, sapplant, bydateType, qCond);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);
            // DataTable dtGroup = dtLobFpy.Copy();
            //    execRes = Dd.GetFpyByGapInsightSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
            DataTable dtGap = fpydeepdiveOperator.GetFpyDataGapInsightSMT(plant, sapplant, bydateType, qCond, "", "", "", 10);
            if (dtGap.Rows.Count == 0)
                dtGap.Rows.Add("N/A", "N/A", 0, 0);
            dicResult.Add(bydateType + "FpyGap", dtGap);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobInsight(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobInsight(plant, sapplant, bydateType, qCond);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0, 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);
            // DataTable dtGroup = dtLobFpy.Copy();
            //    execRes = Dd.GetFpyByGapInsightSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
            DataTable dtGap = fpydeepdiveOperator.GetFpyDataGapInsight(plant, sapplant, bydateType, qCond, "", "", "", 10);
            if (dtGap.Rows.Count == 0)
                dtGap.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyGap", dtGap);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobPcbLrr(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobPcbLrr(plant, sapplant, bydateType, qCond);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);
            DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupPcbLrr(plant, sapplant, bydateType, qCond);
            if (dtGroup.Rows.Count == 0)
                dtGroup.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyGroup", dtGroup);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyLobData(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = new DataTable();
            if (bMinus)
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobMinus(plant, sapplant, bydateType, qCond, vOrderBy, vOrderAsc, vRows);
            else
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLob(plant, sapplant, bydateType, qCond, vOrderBy, vOrderAsc, vRows);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyLobDataSMT(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = new DataTable();
            if (bMinus)
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobSMTMinus(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            else
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobSMT(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyLobDataInsightSMT(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = new DataTable();
            if (bMinus)
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobInsightSMTMinus(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            else
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobInsightSMT(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetFpyLobDataInsight(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = new DataTable();
            if (bMinus)
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobInsightMinus(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            else
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobInsight(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0, 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyLobDataPcbLrr(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false, bool bFloor = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = new DataTable();
            if (bMinus)
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobPcbLrrMinus(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows, bFloor);
            else
                dtLobFpy = fpydeepdiveOperator.GetFpyDataByLobPcbLrr(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows, bFloor);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyLOB", dtLobFpy);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyDataPcbLrrIOP(string plant, string sapplant, string bydateType, string qCond, string vType, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLobFpy = new DataTable();
            dtLobFpy = fpydeepdiveOperator.GetFpyDataPcbLrrIOP(plant, sapplant, bydateType, qCond, vType, vOrderBy, vOrderAsc, vRows, bMinus);
            if (dtLobFpy.Rows.Count == 0)
                dtLobFpy.Rows.Add("N/A", 0, 0, 0);
            if (vType.IndexOf("LOB") > -1)
                dicResult.Add(bydateType + "FpyLOB", dtLobFpy);
            else
                dicResult.Add(bydateType + "FpyLine", dtLobFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLast(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLast = fpydeepdiveOperator.GetFpyDataLast(plant, sapplant, bydateType, qCond);
            DataTable dtThis = fpydeepdiveOperator.GetFpyDataThis(plant, sapplant, bydateType, qCond);
            DataTable dtResult = new DataTable();
            dtResult.Columns.Add("LDATE");
            dtResult.Columns.Add("LINPUT");
            dtResult.Columns.Add("LFPY");
            dtResult.Columns.Add("TDATE");
            dtResult.Columns.Add("TINPUT");
            dtResult.Columns.Add("TFPY");
            DataRow dr = dtResult.NewRow();
            if (dtLast.Rows.Count > 0)
            {
                dr["LDATE"] = dtLast.Rows[0]["FPYDATE"];
                dr["LINPUT"] = dtLast.Rows[0]["INPUT"];
                dr["LFPY"] = dtLast.Rows[0]["FPY"];
            }
            else
            {
                dr["LDATE"] = "";
                dr["LINPUT"] = 0;
                dr["LFPY"] = 0;
            }
            if (dtThis.Rows.Count > 0)
            {
                dr["TDATE"] = dtThis.Rows[0]["FPYDATE"];
                dr["TINPUT"] = dtThis.Rows[0]["INPUT"];
                dr["TFPY"] = dtThis.Rows[0]["FPY"];
            }
            else
            {
                dr["TDATE"] = "";
                dr["TINPUT"] = 0;
                dr["TFPY"] = 0;
            }
            dtResult.Rows.Add(dr);
            dicResult.Add(bydateType + "FpyLast", dtResult);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetFpyByGroup(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroup(plant, sapplant, bydateType, qCond, vOrderBy, vOrderAsc);
            dicResult.Add(bydateType + "FpyGroup", dtGroup);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLastSMT(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLast = fpydeepdiveOperator.GetFpyDataLastSMT(plant, sapplant, bydateType, qCond);
            DataTable dtThis = fpydeepdiveOperator.GetFpyDataThisSMT(plant, sapplant, bydateType, qCond);
            DataTable dtResult = new DataTable();
            dtResult.Columns.Add("LDATE");
            dtResult.Columns.Add("LINPUT");
            dtResult.Columns.Add("LFPY");
            dtResult.Columns.Add("TDATE");
            dtResult.Columns.Add("TINPUT");
            dtResult.Columns.Add("TFPY");
            DataRow dr = dtResult.NewRow();
            if (dtLast.Rows.Count > 0)
            {
                dr["LDATE"] = dtLast.Rows[0]["FPYDATE"];
                dr["LINPUT"] = dtLast.Rows[0]["INPUT"];
                dr["LFPY"] = dtLast.Rows[0]["FPY"];
            }
            else
            {
                dr["LDATE"] = "";
                dr["LINPUT"] = 0;
                dr["LFPY"] = 0;
            }
            if (dtThis.Rows.Count > 0)
            {
                dr["TDATE"] = dtThis.Rows[0]["FPYDATE"];
                dr["TINPUT"] = dtThis.Rows[0]["INPUT"];
                dr["TFPY"] = dtThis.Rows[0]["FPY"];
            }
            else
            {
                dr["TDATE"] = "";
                dr["TINPUT"] = 0;
                dr["TFPY"] = 0;
            }
            dtResult.Rows.Add(dr);
            dicResult.Add(bydateType + "FpyLast", dtResult);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLastInsightSMT(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLast = fpydeepdiveOperator.GetFpyDataLastInsightSMT(plant, sapplant, bydateType, qCond);
            DataTable dtThis = fpydeepdiveOperator.GetFpyDataThisInsightSMT(plant, sapplant, bydateType, qCond);
            DataTable dtResult = new DataTable();
            dtResult.Columns.Add("LDATE");
            dtResult.Columns.Add("LINPUT");
            dtResult.Columns.Add("LFPY");
            dtResult.Columns.Add("TDATE");
            dtResult.Columns.Add("TINPUT");
            dtResult.Columns.Add("TFPY");
            DataRow dr = dtResult.NewRow();
            if (dtLast.Rows.Count > 0)
            {
                dr["LDATE"] = dtLast.Rows[0]["FPYDATE"];
                dr["LINPUT"] = dtLast.Rows[0]["INPUT"];
                dr["LFPY"] = dtLast.Rows[0]["FPY"];
            }
            else
            {
                dr["LDATE"] = "";
                dr["LINPUT"] = 0;
                dr["LFPY"] = 0;
            }
            if (dtThis.Rows.Count > 0)
            {
                dr["TDATE"] = dtThis.Rows[0]["FPYDATE"];
                dr["TINPUT"] = dtThis.Rows[0]["INPUT"];
                dr["TFPY"] = dtThis.Rows[0]["FPY"];
            }
            else
            {
                dr["TDATE"] = "";
                dr["TINPUT"] = 0;
                dr["TFPY"] = 0;
            }
            dtResult.Rows.Add(dr);
            dicResult.Add(bydateType + "FpyLast", dtResult);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLastInsight(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLast = fpydeepdiveOperator.GetFpyDataLastInsight(plant, sapplant, bydateType, qCond);
            DataTable dtThis = fpydeepdiveOperator.GetFpyDataThisInsight(plant, sapplant, bydateType, qCond);
            DataTable dtResult = new DataTable();
            dtResult.Columns.Add("LDATE");
            dtResult.Columns.Add("LINPUT");
            dtResult.Columns.Add("LFPY");
            dtResult.Columns.Add("TDATE");
            dtResult.Columns.Add("TINPUT");
            dtResult.Columns.Add("TFPY");
            DataRow dr = dtResult.NewRow();
            if (dtLast.Rows.Count > 0)
            {
                dr["LDATE"] = dtLast.Rows[0]["FPYDATE"];
                dr["LINPUT"] = dtLast.Rows[0]["INPUT"];
                dr["LFPY"] = dtLast.Rows[0]["FPY"];
            }
            else
            {
                dr["LDATE"] = "";
                dr["LINPUT"] = 0;
                dr["LFPY"] = 0;
            }
            if (dtThis.Rows.Count > 0)
            {
                dr["TDATE"] = dtThis.Rows[0]["FPYDATE"];
                dr["TINPUT"] = dtThis.Rows[0]["INPUT"];
                dr["TFPY"] = dtThis.Rows[0]["FPY"];
            }
            else
            {
                dr["TDATE"] = "";
                dr["TINPUT"] = 0;
                dr["TFPY"] = 0;
            }
            dtResult.Rows.Add(dr);
            dicResult.Add(bydateType + "FpyLast", dtResult);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLastPcbLrr(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLast = fpydeepdiveOperator.GetFpyDataLastPcbLrr(plant, sapplant, bydateType, qCond);
            DataTable dtThis = fpydeepdiveOperator.GetFpyDataThisPcbLrr(plant, sapplant, bydateType, qCond);
            DataTable dtResult = new DataTable();
            dtResult.Columns.Add("LDATE");
            dtResult.Columns.Add("LINPUT");
            dtResult.Columns.Add("LFPY");
            dtResult.Columns.Add("TDATE");
            dtResult.Columns.Add("TINPUT");
            dtResult.Columns.Add("TFPY");
            DataRow dr = dtResult.NewRow();
            if (dtLast.Rows.Count > 0)
            {
                dr["LDATE"] = dtLast.Rows[0]["FPYDATE"];
                dr["LINPUT"] = dtLast.Rows[0]["INPUT"];
                dr["LFPY"] = dtLast.Rows[0]["FPY"];
            }
            else
            {
                dr["LDATE"] = "";
                dr["LINPUT"] = 0;
                dr["LFPY"] = 0;
            }
            if (dtThis.Rows.Count > 0)
            {
                dr["TDATE"] = dtThis.Rows[0]["FPYDATE"];
                dr["TINPUT"] = dtThis.Rows[0]["INPUT"];
                dr["TFPY"] = dtThis.Rows[0]["FPY"];
            }
            else
            {
                dr["TDATE"] = "";
                dr["TINPUT"] = 0;
                dr["TFPY"] = 0;
            }
            dtResult.Rows.Add(dr);
            dicResult.Add(bydateType + "FpyLast", dtResult);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByGroupSMT(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtGroup = new DataTable();
            if (bMinus)
                dtGroup = fpydeepdiveOperator.GetFpyDataByGroupSMTMinus(plant, sapplant, bydateType, qCond, "", "", "", vOrderBy, vOrderAsc, vRows);
            else
                dtGroup = fpydeepdiveOperator.GetFpyDataByGroupSMT(plant, sapplant, bydateType, qCond, vOrderBy, vOrderAsc, vRows);
            if (dtGroup.Rows.Count == 0)
                dtGroup.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyGroup", dtGroup);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByGroupPcbLrr(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtGroup = new DataTable();
            if (bMinus)
                dtGroup = fpydeepdiveOperator.GetFpyDataByGroupPcbLrrMinus(plant, sapplant, bydateType, qCond, "", "", "", vOrderBy, vOrderAsc);
            else
                dtGroup = fpydeepdiveOperator.GetFpyDataByGroupPcbLrr(plant, sapplant, bydateType, qCond, vOrderBy, vOrderAsc);
            if (dtGroup.Rows.Count == 0)
                dtGroup.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyGroup", dtGroup);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByTrend(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];

            DataTable dtTrend = new DataTable();
            dBaseDate = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
            dtMonth = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-5).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "", "", "", "", "", "");
            if (dtMonth.Rows.Count > 0)
            {
                for (int i = dtMonth.Rows.Count - 1; i >= 0; i--)
                    if (dtMonth.Rows[i]["fpy"].ToString() == "0")
                        dtMonth.Rows.Remove(dtMonth.Rows[i]);
                dtTrend.Merge(dtMonth);
            }
            dBaseDate = DateTime.Now;
            GregorianCalendar gc = new GregorianCalendar();
            int w1 = gc.GetWeekOfYear(DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture), CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            int w2 = gc.GetWeekOfYear(dBaseDate, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            string strWeek1 = "", strWeek2 = "";
            DateTime sTime, eTime;
            if (w1 > w2)
            {
                strWeek1 = dBaseDate.AddYears(-1).ToString("yyyy") + w1.ToString();
                strWeek2 = dBaseDate.ToString("yyyy") + w2.ToString();
            }
            else
            {
                strWeek1 = dBaseDate.ToString("yyyy") + w1.ToString();
                strWeek2 = dBaseDate.ToString("yyyy") + w2.ToString();
            }
            DataTable dtRange = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, strWeek1, strWeek2, mClientInfo);
            if (dtRange.Rows.Count > 0)
            {
                sTime = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                eTime = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtWeek = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dtRange.Rows[0]["fromday"].ToString(), dtRange.Rows[0]["today"].ToString(), "", "", "", "", "", "");
                if (dtWeek.Rows.Count > 0)
                {
                    for (int i = dtWeek.Rows.Count - 1; i >= 0; i--)
                        if (dtWeek.Rows[i]["fpy"].ToString() == "0")
                            dtWeek.Rows.Remove(dtWeek.Rows[i]);
                    dtTrend.Merge(dtWeek);
                }
            }
            dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-3).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "", "", "", "", "", "");
            if (dtDay.Rows.Count > 0)
            {
                for (int i = dtDay.Rows.Count - 1; i >= 0; i--)
                    if (dtDay.Rows[i]["fpy"].ToString() == "0")
                        dtDay.Rows.Remove(dtDay.Rows[i]);
                dtTrend.Merge(dtDay);
            }
            if (dtTrend.Rows.Count == 0)
                dtTrend.Rows.Add("N/A", 0, 0, "");
            dicResult.Add(bydateType + "FpyTrend", dtTrend);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByTrendInsight(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];

            DataTable dtTrend = new DataTable();
            dBaseDate = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
            dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-5).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "", "", "", "", "", "");
            if (dtMonth.Rows.Count > 0)
            {
                for (int i = dtMonth.Rows.Count - 1; i >= 0; i--)
                    if (dtMonth.Rows[i]["Insightfpy"].ToString() == "0")
                        dtMonth.Rows.Remove(dtMonth.Rows[i]);
                dtTrend.Merge(dtMonth);
            }
            dBaseDate = DateTime.Now;
            GregorianCalendar gc = new GregorianCalendar();
            int w1 = gc.GetWeekOfYear(DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture), CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            int w2 = gc.GetWeekOfYear(dBaseDate, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            string strWeek1 = "", strWeek2 = "";
            DateTime sTime, eTime;
            if (w1 > w2)
            {
                strWeek1 = dBaseDate.AddYears(-1).ToString("yyyy") + w1.ToString();
                strWeek2 = dBaseDate.ToString("yyyy") + w2.ToString();
            }
            else
            {
                strWeek1 = dBaseDate.ToString("yyyy") + w1.ToString();
                strWeek2 = dBaseDate.ToString("yyyy") + w2.ToString();
            }
            DataTable dtRange = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, strWeek1, strWeek2, mClientInfo);
            if (dtRange.Rows.Count > 0)
            {
                sTime = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                eTime = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Weekly_" + strShift, dtRange.Rows[0]["fromday"].ToString(), dtRange.Rows[0]["today"].ToString(), "", "", "", "", "", "");
                if (dtWeek.Rows.Count > 0)
                {
                    for (int i = dtWeek.Rows.Count - 1; i >= 0; i--)
                        if (dtWeek.Rows[i]["Insightfpy"].ToString() == "0")
                            dtWeek.Rows.Remove(dtWeek.Rows[i]);
                    dtTrend.Merge(dtWeek);
                }
            }
            dtDay = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-3).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "", "", "", "", "", "");
            if (dtDay.Rows.Count > 0)
            {
                for (int i = dtDay.Rows.Count - 1; i >= 0; i--)
                    if (dtDay.Rows[i]["Insightfpy"].ToString() == "0")
                        dtDay.Rows.Remove(dtDay.Rows[i]);
                dtTrend.Merge(dtDay);
            }
            if (dtTrend.Rows.Count == 0)
                dtTrend.Rows.Add("N/A", 0, 0, "");
            dicResult.Add(bydateType + "FpyTrend", dtTrend);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetFpyByTrendSMT(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];

            DataTable dtTrend = new DataTable();
            dBaseDate = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
            dtMonth = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-5).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "", "", "", "", "", "");
            if (dtMonth.Rows.Count > 0)
            {
                for (int i = dtMonth.Rows.Count - 1; i >= 0; i--)
                    if (dtMonth.Rows[i]["fpy"].ToString() == "0")
                        dtMonth.Rows.Remove(dtMonth.Rows[i]);
                dtTrend.Merge(dtMonth);
            }
            dBaseDate = DateTime.Now;
            GregorianCalendar gc = new GregorianCalendar();
            int w1 = gc.GetWeekOfYear(DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture), CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            int w2 = gc.GetWeekOfYear(dBaseDate, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            string strWeek1 = "", strWeek2 = "";
            DateTime sTime, eTime;
            if (w1 > w2)
            {
                strWeek1 = dBaseDate.AddYears(-1).ToString("yyyy") + w1.ToString();
                strWeek2 = dBaseDate.ToString("yyyy") + w2.ToString();
            }
            else
            {
                strWeek1 = dBaseDate.ToString("yyyy") + w1.ToString();
                strWeek2 = dBaseDate.ToString("yyyy") + w2.ToString();
            }
            DataTable dtRange = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, strWeek1, strWeek2, mClientInfo);
            if (dtRange.Rows.Count > 0)
            {
                sTime = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                eTime = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Weekly_" + strShift, dtRange.Rows[0]["fromday"].ToString(), dtRange.Rows[0]["today"].ToString(), "", "", "", "", "", "");
                if (dtWeek.Rows.Count > 0)
                {
                    for (int i = dtWeek.Rows.Count - 1; i >= 0; i--)
                        if (dtWeek.Rows[i]["fpy"].ToString() == "0")
                            dtWeek.Rows.Remove(dtWeek.Rows[i]);
                    dtTrend.Merge(dtWeek);
                }
            }
            dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-3).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "", "", "", "", "", "");
            if (dtDay.Rows.Count > 0)
            {
                for (int i = dtDay.Rows.Count - 1; i >= 0; i--)
                    if (dtDay.Rows[i]["fpy"].ToString() == "0")
                        dtDay.Rows.Remove(dtDay.Rows[i]);
                dtTrend.Merge(dtDay);
            }
            if (dtTrend.Rows.Count == 0)
                dtTrend.Rows.Add("N/A", 0, 0, "");
            dicResult.Add(bydateType + "FpyTrend", dtTrend);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByTrendSMTInsight(string plant, string sapplant, string bydateType, string qCond)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];

            DataTable dtTrend = new DataTable();
            dBaseDate = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
            dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-5).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "", "", "", "", "", "");
            if (dtMonth.Rows.Count > 0)
            {
                for (int i = dtMonth.Rows.Count - 1; i >= 0; i--)
                    if (dtMonth.Rows[i]["Insightfpy"].ToString() == "0")
                        dtMonth.Rows.Remove(dtMonth.Rows[i]);
                dtTrend.Merge(dtMonth);
            }
            dBaseDate = DateTime.Now;
            GregorianCalendar gc = new GregorianCalendar();
            int w1 = gc.GetWeekOfYear(DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture), CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            int w2 = gc.GetWeekOfYear(dBaseDate, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            string strWeek1 = "", strWeek2 = "";
            DateTime sTime, eTime;
            if (w1 > w2)
            {
                strWeek1 = dBaseDate.AddYears(-1).ToString("yyyy") + w1.ToString();
                strWeek2 = dBaseDate.ToString("yyyy") + w2.ToString();
            }
            else
            {
                strWeek1 = dBaseDate.ToString("yyyy") + w1.ToString();
                strWeek2 = dBaseDate.ToString("yyyy") + w2.ToString();
            }
            DataTable dtRange = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, strWeek1, strWeek2, mClientInfo);
            if (dtRange.Rows.Count > 0)
            {
                sTime = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                eTime = DateTime.ParseExact(DateTime.Now.ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Weekly_" + strShift, dtRange.Rows[0]["fromday"].ToString(), dtRange.Rows[0]["today"].ToString(), "", "", "", "", "", "");
                if (dtWeek.Rows.Count > 0)
                {
                    for (int i = dtWeek.Rows.Count - 1; i >= 0; i--)
                        if (dtWeek.Rows[i]["Insightfpy"].ToString() == "0")
                            dtWeek.Rows.Remove(dtWeek.Rows[i]);
                    dtTrend.Merge(dtWeek);
                }
            }
            dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-3).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "", "", "", "", "", "");
            if (dtDay.Rows.Count > 0)
            {
                for (int i = dtDay.Rows.Count - 1; i >= 0; i--)
                    if (dtDay.Rows[i]["Insightfpy"].ToString() == "0")
                        dtDay.Rows.Remove(dtDay.Rows[i]);
                dtTrend.Merge(dtDay);
            }
            if (dtTrend.Rows.Count == 0)
                dtTrend.Rows.Add("N/A", 0, 0, "");
            dicResult.Add(bydateType + "FpyTrend", dtTrend);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByGapInsightSMT(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtGap = new DataTable();
            if (bMinus)
                dtGap = fpydeepdiveOperator.GetFpyDataGapInsightSMTMinus(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            else
                dtGap = fpydeepdiveOperator.GetFpyDataGapInsightSMT(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            if (dtGap.Rows.Count == 0)
                dtGap.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyGap", dtGap);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByGapInsight(string plant, string sapplant, string bydateType, string qCond, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtGap = new DataTable();
            if (bMinus)
                dtGap = fpydeepdiveOperator.GetFpyDataGapInsightMinus(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            else
                dtGap = fpydeepdiveOperator.GetFpyDataGapInsight(plant, sapplant, bydateType, qCond, "", vOrderBy, vOrderAsc, vRows);
            if (dtGap.Rows.Count == 0)
                dtGap.Rows.Add("N/A", "N/A", 0, 0);
            dicResult.Add(bydateType + "FpyGap", dtGap);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetFpyByLobByModel(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            if (qLOB.IndexOf("_") > -1)
            {
                string[] aa = qLOB.Split('_');
                string vDataType = aa[1];
                string strLob = aa[0];
                string[] bb = qModel.Split('_');
                string strModel = bb[0];
                string strProcess = bb[1];
                string strOrderBy = bb[2];
                string strOrderAsc = bb[3];
                if (vDataType == "Lin")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLine(plant, sapplant, bydateType, qCond, strProcess, strLob, strOrderBy, strOrderAsc, 10, strModel);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                    exeRes.Anything = dicResult;
                }
                else if (vDataType == "Gro")
                {
                    DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroup(plant, sapplant, bydateType, qCond, strLob, strModel, "", strOrderBy, strOrderAsc);
                    dicResult.Add(bydateType + "FpyGroup", dtGroup);
                    exeRes.Anything = dicResult;
                }
            }
            else
            {
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "MODEL";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qModel, "LOB", qLOB, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
                #region Duty Table
                DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDuty(plant, sapplant, bydateType, qCond, qLOB, qModel, "MODEL");
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("catalogy", typeof(string));
                dtDuty.Columns.Add("error_rate", typeof(double));
                dtDuty.Columns.Add("percentage", typeof(double));

                if (dtDutyInfo.Rows.Count > 0)
                {
                    DataTable dtDutyItem = dtDutyInfo.DefaultView.ToTable(true, "duty_type");
                    for (int i = 0; i < dtDutyItem.Rows.Count; i++)
                    {
                        dtDuty.Rows.Add(dtDutyItem.Rows[i]["duty_type"].ToString(), 0, 0);
                    }
                    if (dtDuty.Rows.Count < 6)
                    {
                        if (dtDuty.Select("catalogy='作業'").Length == 0)
                            dtDuty.Rows.Add("作業", 0, 0);
                        if (dtDuty.Select("catalogy='PCBA LRR'").Length == 0)
                            dtDuty.Rows.Add("PCBA LRR", 0, 0);
                        if (dtDuty.Select("catalogy='材料'").Length == 0)
                            dtDuty.Rows.Add("材料", 0, 0);
                        if (dtDuty.Select("catalogy='設計'").Length == 0)
                            dtDuty.Rows.Add("設計", 0, 0);
                        if (dtDuty.Select("catalogy='NTF'").Length == 0)
                            dtDuty.Rows.Add("NTF", 0, 0);
                        if (dtDuty.Select("catalogy='其他'").Length == 0)
                            dtDuty.Rows.Add("其他", 0, 0);
                    }
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDuty.Rows.Count; i++)
                    {
                        string strType = dtDuty.Rows[i]["catalogy"].ToString();
                        double dPercent = 0;
                        if (strType == "作業" || strType == "材料" || strType == "設計")
                            dtDuty.Rows[i]["catalogy"] = strType + "不良";
                        DataRow[] dr = dtDutyInfo.Select("DUTY_TYPE='" + strType + "'");
                        if (dr.Length > 0)
                        {
                            dPercent = double.Parse(dr[0]["percents"].ToString());
                            dtDuty.Rows[i]["error_rate"] = double.Parse(dr[0]["fpy"].ToString());
                        }
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtDuty.Rows[i]["percentage"] = dAllPercent;
                    }
                }
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
                #endregion
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroup(plant, sapplant, bydateType, qCond, qLOB, qModel, "", "FPY", "DESC");
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
                DataTable dtLine = fpydeepdiveOperator.GetFpyDataByLine(plant, sapplant, bydateType, qCond, "ASSY", qLOB, "FPY", "DESC", 10, qModel);
                dicResult.Add(bydateType + "FpyLine", dtLine);

                exeRes.Anything = dicResult;
                #endregion
            }

            return exeRes;
        }

        public ExecutionResult GetFpyByLobByLine(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qLine)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region Duty Table
            if (!(qLOB == "" && qLine != ""))
            {
                DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDuty(plant, sapplant, bydateType, qCond, qLOB, qLine, "LINE");
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("catalogy", typeof(string));
                dtDuty.Columns.Add("error_rate", typeof(double));
                dtDuty.Columns.Add("percentage", typeof(double));

                if (dtDutyInfo.Rows.Count > 0)
                {
                    DataTable dtDutyItem = dtDutyInfo.DefaultView.ToTable(true, "duty_type");
                    for (int i = 0; i < dtDutyItem.Rows.Count; i++)
                    {
                        dtDuty.Rows.Add(dtDutyItem.Rows[i]["duty_type"].ToString(), 0, 0);
                    }
                    if (dtDuty.Rows.Count < 6)
                    {
                        if (dtDuty.Select("catalogy='作業'").Length == 0)
                            dtDuty.Rows.Add("作業", 0, 0);
                        if (dtDuty.Select("catalogy='PCBA LRR'").Length == 0)
                            dtDuty.Rows.Add("PCBA LRR", 0, 0);
                        if (dtDuty.Select("catalogy='材料'").Length == 0)
                            dtDuty.Rows.Add("材料", 0, 0);
                        if (dtDuty.Select("catalogy='設計'").Length == 0)
                            dtDuty.Rows.Add("設計", 0, 0);
                        if (dtDuty.Select("catalogy='NTF'").Length == 0)
                            dtDuty.Rows.Add("NTF", 0, 0);
                        if (dtDuty.Select("catalogy='其他'").Length == 0)
                            dtDuty.Rows.Add("其他", 0, 0);
                    }
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDuty.Rows.Count; i++)
                    {
                        string strType = dtDuty.Rows[i]["catalogy"].ToString();
                        double dPercent = 0;
                        if (strType == "作業" || strType == "材料" || strType == "設計")
                            dtDuty.Rows[i]["catalogy"] = strType + "不良";
                        DataRow[] dr = dtDutyInfo.Select("DUTY_TYPE='" + strType + "'");
                        if (dr.Length > 0)
                        {
                            dPercent = double.Parse(dr[0]["percents"].ToString());
                            dtDuty.Rows[i]["error_rate"] = double.Parse(dr[0]["fpy"].ToString());

                        }
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtDuty.Rows[i]["percentage"] = dAllPercent;
                    }
                }
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
                exeRes.Anything = dicResult;
            }
            #endregion

            DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroup(plant, sapplant, bydateType, qCond, qLOB, "", qLine, "FPY", "DESC");
            dicResult.Add(bydateType + "FpyGroup", dtGroup);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobByModelPcbLrr(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];
            string strqType = "MODEL";

            if (strDateType == "Daily")
            {
                dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                if (dtDay.Rows.Count == 0)
                    dtDay.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtDay);
            }
            else if (strDateType == "Range")
            {
                if (qCond.IndexOf("-") > -1)
                {
                    string[] cc = qCond.Split('-');
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qModel, "LOB", qLOB, "", "");
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add("N/A", 0, 0, 0);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
            }
            else if (strDateType == "Weekly")
            {
                string strYear, strWeek;
                string strCond = qCond.Replace("W", "");
                strYear = strCond.Substring(0, 4);
                strWeek = strCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                dBaseDate = sTime;
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                if (dtWeek.Rows.Count == 0)
                    dtWeek.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtWeek);
            }
            else
            {
                dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtMonth = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                if (dtMonth.Rows.Count == 0)
                    dtMonth.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtMonth);
            }
            #region Duty Table
            DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDataDutyPcbLrr(plant, sapplant, bydateType, qCond, qLOB, "", qModel, "");
            DataTable dtDuty = new DataTable();
            dtDuty.Columns.Add("catalogy", typeof(string));
            dtDuty.Columns.Add("error_rate", typeof(double));
            dtDuty.Columns.Add("percentage", typeof(double));
            if (dtDutyInfo.Rows.Count > 0)
            {
                double dAllPercent = 0;
                for (int i = 0; i < dtDutyInfo.Rows.Count; i++)
                {
                    string strType = dtDutyInfo.Rows[i]["catalogy"].ToString();
                    double dPercent = 0, dLrr = 0;
                    dPercent = double.Parse(dtDutyInfo.Rows[i]["percents"].ToString());
                    dLrr = double.Parse(dtDutyInfo.Rows[i]["pcblrr"].ToString());
                    dAllPercent += dPercent;
                    if (dAllPercent > 100)
                        dAllPercent = 100;
                    dtDuty.Rows.Add(dtDutyInfo.Rows[i]["catalogy"].ToString(), dLrr, dAllPercent);
                }
            }
            else
                dtDuty.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyDuty", dtDuty);
            #endregion                 
            DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupPcbLrr(plant, sapplant, bydateType, qCond, qLOB, qModel, "", "LRR", "DESC");
            if (dtGroup.Rows.Count == 0)
                dtGroup.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyGroup", dtGroup);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobByLinePcbLrr(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qLine)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];
            string strqType = "LINE";

            if (strDateType == "Daily")
            {
                dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                if (dtDay.Rows.Count == 0)
                    dtDay.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtDay);
            }
            else if (strDateType == "Range")
            {
                if (qCond.IndexOf("-") > -1)
                {
                    string[] cc = qCond.Split('-');
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qLine, "LOB", qLOB, "", "");
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add("N/A", 0, 0, 0);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
            }
            else if (strDateType == "Weekly")
            {
                string strYear, strWeek;
                string strCond = qCond.Replace("W", "");
                strYear = strCond.Substring(0, 4);
                strWeek = strCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                dBaseDate = sTime;
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                if (dtWeek.Rows.Count == 0)
                    dtWeek.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtWeek);
            }
            else
            {
                dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtMonth = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                if (dtMonth.Rows.Count == 0)
                    dtMonth.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtMonth);
            }
            #region Duty Table
            DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDataDutyPcbLrr(plant, sapplant, bydateType, qCond, qLOB, qLine, "", "");
            DataTable dtDuty = new DataTable();
            dtDuty.Columns.Add("catalogy", typeof(string));
            dtDuty.Columns.Add("error_rate", typeof(double));
            dtDuty.Columns.Add("percentage", typeof(double));
            if (dtDutyInfo.Rows.Count > 0)
            {
                double dAllPercent = 0;
                for (int i = 0; i < dtDutyInfo.Rows.Count; i++)
                {
                    string strType = dtDutyInfo.Rows[i]["catalogy"].ToString();
                    double dPercent = 0, dLrr = 0;
                    dPercent = double.Parse(dtDutyInfo.Rows[i]["percents"].ToString());
                    dLrr = double.Parse(dtDutyInfo.Rows[i]["pcblrr"].ToString());
                    dAllPercent += dPercent;
                    if (dAllPercent > 100)
                        dAllPercent = 100;
                    dtDuty.Rows.Add(dtDutyInfo.Rows[i]["catalogy"].ToString(), dLrr, dAllPercent);
                }
            }
            else
                dtDuty.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyDuty", dtDuty);
            #endregion                 
            DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupPcbLrr(plant, sapplant, bydateType, qCond, qLOB, "", qLine, "LRR", "DESC");
            if (dtGroup.Rows.Count == 0)
                dtGroup.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyGroup", dtGroup);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobByStationPcbLrr(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qStation)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];
            string strqType = "STATION";

            if (strDateType == "Daily")
            {
                dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qStation, "LOB", qLOB, "", "");
                if (dtDay.Rows.Count == 0)
                    dtDay.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtDay);
            }
            else if (strDateType == "Range")
            {
                if (qCond.IndexOf("-") > -1)
                {
                    string[] cc = qCond.Split('-');
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qStation, "LOB", qLOB, "", "");
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add("N/A", 0, 0, 0);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
            }
            else if (strDateType == "Weekly")
            {
                string strYear, strWeek;
                string strCond = qCond.Replace("W", "");
                strYear = strCond.Substring(0, 4);
                strWeek = strCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                dBaseDate = sTime;
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qStation, "LOB", qLOB, "", "");
                if (dtWeek.Rows.Count == 0)
                    dtWeek.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtWeek);
            }
            else
            {
                dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtMonth = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qStation, "LOB", qLOB, "", "");
                if (dtMonth.Rows.Count == 0)
                    dtMonth.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtMonth);
            }
            #region Duty Table
            DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDataDutyPcbLrr(plant, sapplant, bydateType, qCond, qLOB, "", "", qStation);
            DataTable dtDuty = new DataTable();
            dtDuty.Columns.Add("catalogy", typeof(string));
            dtDuty.Columns.Add("error_rate", typeof(double));
            dtDuty.Columns.Add("percentage", typeof(double));
            if (dtDutyInfo.Rows.Count > 0)
            {
                double dAllPercent = 0;
                for (int i = 0; i < dtDutyInfo.Rows.Count; i++)
                {
                    string strType = dtDutyInfo.Rows[i]["catalogy"].ToString();
                    double dPercent = 0, dLrr = 0;
                    dPercent = double.Parse(dtDutyInfo.Rows[i]["percents"].ToString());
                    dLrr = double.Parse(dtDutyInfo.Rows[i]["pcblrr"].ToString());
                    dAllPercent += dPercent;
                    if (dAllPercent > 100)
                        dAllPercent = 100;
                    dtDuty.Rows.Add(dtDutyInfo.Rows[i]["catalogy"].ToString(), dLrr, dAllPercent);
                }
            }
            else
                dtDuty.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyDuty", dtDuty);
            #endregion                

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobByDutyPcbLrr(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qDuty)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];
            string strqType = "DUTY";

            if (strDateType == "Daily")
            {
                dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qDuty, "LOB", qLOB, "", "");
                if (dtDay.Rows.Count == 0)
                    dtDay.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtDay);
            }
            else if (strDateType == "Range")
            {
                if (qCond.IndexOf("-") > -1)
                {
                    string[] cc = qCond.Split('-');
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qDuty, "LOB", qLOB, "", "");
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add("N/A", 0, 0, 0);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
            }
            else if (strDateType == "Weekly")
            {
                string strYear, strWeek;
                string strCond = qCond.Replace("W", "");
                strYear = strCond.Substring(0, 4);
                strWeek = strCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                dBaseDate = sTime;
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qDuty, "LOB", qLOB, "", "");
                if (dtWeek.Rows.Count == 0)
                    dtWeek.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtWeek);
            }
            else
            {
                dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtMonth = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qDuty, "LOB", qLOB, "", "");
                if (dtMonth.Rows.Count == 0)
                    dtMonth.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtMonth);
            }

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLineByStationPcbLrr(string plant, string sapplant, string bydateType, string qCond, string qLine, string qStation)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];
            string strqType = "STATION";

            if (strDateType == "Daily")
            {
                dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                if (dtDay.Rows.Count == 0)
                    dtDay.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtDay);
            }
            else if (strDateType == "Range")
            {
                if (qCond.IndexOf("-") > -1)
                {
                    string[] cc = qCond.Split('-');
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qStation, "LINE", qLine, "", "");
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add("N/A", 0, 0, 0);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
            }
            else if (strDateType == "Weekly")
            {
                string strYear, strWeek;
                string strCond = qCond.Replace("W", "");
                strYear = strCond.Substring(0, 4);
                strWeek = strCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                dBaseDate = sTime;
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                if (dtWeek.Rows.Count == 0)
                    dtWeek.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtWeek);
            }
            else
            {
                dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtMonth = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                if (dtMonth.Rows.Count == 0)
                    dtMonth.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtMonth);
            }
            #region Duty Table
            DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDataDutyPcbLrr(plant, sapplant, bydateType, qCond, "", qLine, "", qStation);
            DataTable dtDuty = new DataTable();
            dtDuty.Columns.Add("catalogy", typeof(string));
            dtDuty.Columns.Add("error_rate", typeof(double));
            dtDuty.Columns.Add("percentage", typeof(double));
            if (dtDutyInfo.Rows.Count > 0)
            {
                double dAllPercent = 0;
                for (int i = 0; i < dtDutyInfo.Rows.Count; i++)
                {
                    string strType = dtDutyInfo.Rows[i]["catalogy"].ToString();
                    double dPercent = 0, dLrr = 0;
                    dPercent = double.Parse(dtDutyInfo.Rows[i]["percents"].ToString());
                    dLrr = double.Parse(dtDutyInfo.Rows[i]["pcblrr"].ToString());
                    dAllPercent += dPercent;
                    if (dAllPercent > 100)
                        dAllPercent = 100;
                    dtDuty.Rows.Add(dtDutyInfo.Rows[i]["catalogy"].ToString(), dLrr, dAllPercent);
                }
            }
            else
                dtDuty.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyDuty", dtDuty);
            #endregion                

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLineByDutyPcbLrr(string plant, string sapplant, string bydateType, string qCond, string qLine, string qDuty)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];
            string strqType = "DUTY";

            if (strDateType == "Daily")
            {
                dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qDuty, "LINE", qLine, "", "");
                if (dtDay.Rows.Count == 0)
                    dtDay.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtDay);
            }
            else if (strDateType == "Range")
            {
                if (qCond.IndexOf("-") > -1)
                {
                    string[] cc = qCond.Split('-');
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qDuty, "LINE", qLine, "", "");
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add("N/A", 0, 0, 0);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
            }
            else if (strDateType == "Weekly")
            {
                string strYear, strWeek;
                string strCond = qCond.Replace("W", "");
                strYear = strCond.Substring(0, 4);
                strWeek = strCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                dBaseDate = sTime;
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qDuty, "LINE", qLine, "", "");
                if (dtWeek.Rows.Count == 0)
                    dtWeek.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtWeek);
            }
            else
            {
                dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtMonth = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qDuty, "LINE", qLine, "", "");
                if (dtMonth.Rows.Count == 0)
                    dtMonth.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtMonth);
            }

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByDutyPcbLrr(string plant, string sapplant, string bydateType, string qCond, string qDuty)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];
            string strqType = "DUTY";

            if (strDateType == "Daily")
            {
                dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qDuty, "", "", "", "");
                if (dtDay.Rows.Count == 0)
                    dtDay.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtDay);
            }
            else if (strDateType == "Range")
            {
                if (qCond.IndexOf("-") > -1)
                {
                    string[] cc = qCond.Split('-');
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qDuty, "", "", "", "");
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add("N/A", 0, 0, 0);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
            }
            else if (strDateType == "Weekly")
            {
                string strYear, strWeek;
                string strCond = qCond.Replace("W", "");
                strYear = strCond.Substring(0, 4);
                strWeek = strCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                dBaseDate = sTime;
                dtWeek = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qDuty, "", "", "", "");
                if (dtWeek.Rows.Count == 0)
                    dtWeek.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtWeek);
            }
            else
            {
                dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtMonth = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qDuty, "", "", "", "");
                if (dtMonth.Rows.Count == 0)
                    dtMonth.Rows.Add("N/A", 0, 0, 0);
                dicResult.Add(bydateType + "FpyTrend", dtMonth);
            }

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobByModelSMT(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel)
        {
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            if (qLOB.IndexOf("_") > -1)
            {
                string[] aa = qLOB.Split('_');
                string vDataType = aa[1];
                string strLob = aa[0];
                string[] bb = qModel.Split('_');
                string strModel = bb[0];
                string strProcess = bb[1];
                string strOrderBy = bb[2];
                string strOrderAsc = bb[3];
                if (vDataType == "Lin")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineSMT(plant, sapplant, bydateType, qCond, strProcess, strLob, strModel, strOrderBy, strOrderAsc, 10);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                    exeRes.Anything = dicResult;
                }
                else if (vDataType == "Gro")
                {
                    DataTable dtGroupFpy = fpydeepdiveOperator.GetFpyDataByGroupSMT(plant, sapplant, bydateType, qCond, strLob, strModel, "", strOrderBy, strOrderAsc, 10);
                    dicResult.Add(bydateType + "FpyGroup", dtGroupFpy);
                    exeRes.Anything = dicResult;
                }
            }
            else
            {
                #region             
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "MODEL";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qModel, "LOB", qLOB, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
                #region Duty Table
                DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDutySMT(plant, sapplant, bydateType, qCond, qLOB, "MODEL", qModel);
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("catalogy", typeof(string));
                dtDuty.Columns.Add("error_rate", typeof(double));
                dtDuty.Columns.Add("percentage", typeof(double));

                if (dtDutyInfo.Rows.Count > 0)
                {
                    DataTable dtDutyItem = dtDutyInfo.DefaultView.ToTable(true, "duty_type");
                    for (int i = 0; i < dtDutyItem.Rows.Count; i++)
                    {
                        dtDuty.Rows.Add(dtDutyItem.Rows[i]["duty_type"].ToString(), 0, 0);
                    }
                    if (dtDuty.Rows.Count < 7)
                    {
                        if (dtDuty.Select("catalogy='置件'").Length == 0)
                            dtDuty.Rows.Add("置件", 0, 0);
                        if (dtDuty.Select("catalogy='印刷'").Length == 0)
                            dtDuty.Rows.Add("印刷", 0, 0);
                        if (dtDuty.Select("catalogy='材料'").Length == 0)
                            dtDuty.Rows.Add("材料", 0, 0);
                        if (dtDuty.Select("catalogy='制程'").Length == 0)
                            dtDuty.Rows.Add("制程", 0, 0);
                        if (dtDuty.Select("catalogy='NTF'").Length == 0)
                            dtDuty.Rows.Add("NTF", 0, 0);
                        if (dtDuty.Select("catalogy='AFTP'").Length == 0)
                            dtDuty.Rows.Add("AFTP", 0, 0);
                        if (dtDuty.Select("catalogy='BIOS'").Length == 0)
                            dtDuty.Rows.Add("BIOS", 0, 0);
                    }
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDuty.Rows.Count; i++)
                    {
                        string strType = dtDuty.Rows[i]["catalogy"].ToString();
                        double dPercent = 0;
                        if (strType == "置件" || strType == "材料" || strType == "印刷" || strType == "制程" || strType == "BIOS")
                            dtDuty.Rows[i]["catalogy"] = strType + "不良";
                        else if (strType == "NTF")
                            dtDuty.Rows[i]["catalogy"] = "複判OK";
                        else if (strType == "AFTP")
                            dtDuty.Rows[i]["catalogy"] = "組包造成";
                        DataRow[] dr = dtDutyInfo.Select("DUTY_TYPE='" + strType + "'");
                        if (dr.Length > 0)
                        {
                            dPercent = double.Parse(dr[0]["percents"].ToString());
                            dtDuty.Rows[i]["error_rate"] = double.Parse(dr[0]["fpy"].ToString());
                        }
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtDuty.Rows[i]["percentage"] = dAllPercent;
                    }
                }
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
                #endregion
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupSMT(plant, sapplant, bydateType, qCond, qLOB, qModel, "", "FPY", "DESC", 10);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineSMT(plant, sapplant, bydateType, qCond, "", qLOB, qModel, "FPY", "DESC", 10);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                exeRes.Anything = dicResult;
                #endregion
            }


            return exeRes;
        }

        public ExecutionResult GetFpyByLobByModelInsightSMT(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel)
        {
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            if (qLOB.IndexOf("_") > -1)
            {
                string[] aa = qLOB.Split('_');
                string vDataType = aa[1];
                string strLob = aa[0];
                string[] bb = qModel.Split('_');
                string strModel = bb[0];
                string strProcess = bb[1];
                string strOrderBy = bb[2];
                string strOrderAsc = bb[3];
                if (vDataType == "Lin")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsightSMT(plant, sapplant, bydateType, qCond, strProcess, strLob, strModel, strOrderBy, strOrderAsc, 10);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                    exeRes.Anything = dicResult;
                }
            }
            else
            {
                #region             
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "MODEL";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qModel, "LOB", qLOB, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupInsightSMT(plant, sapplant, bydateType, qCond, qLOB, qModel, "", "FPY", "DESC", 10);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsightSMT(plant, sapplant, bydateType, qCond, "SMT", qLOB, qModel, "FPY", "DESC", 10);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                exeRes.Anything = dicResult;
                #endregion
            }


            return exeRes;
        }

        public ExecutionResult GetFpyByLobByModelInsight(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel)
        {
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            if (qLOB.IndexOf("_") > -1)
            {
                string[] aa = qLOB.Split('_');
                string vDataType = aa[1];
                string strLob = aa[0];
                string[] bb = qModel.Split('_');
                string strModel = bb[0];
                string strProcess = bb[1];
                string strOrderBy = bb[2];
                string strOrderAsc = bb[3];
                if (vDataType == "Lin")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsight(plant, sapplant, bydateType, qCond, strProcess, strLob, strModel, strOrderBy, strOrderAsc, 10);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                    exeRes.Anything = dicResult;
                }
                else if (vDataType == "Gro")
                {
                    DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupInsight(plant, sapplant, bydateType, qCond, strLob, strModel, "", strOrderBy, strOrderAsc, 10);
                    dicResult.Add(bydateType + "FpyGroup", dtGroup);
                    exeRes.Anything = dicResult;
                }
                else if (vDataType == "Gru")
                {
                    DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGru(plant, sapplant, bydateType, qCond, strLob, strModel, "", strOrderBy, strOrderAsc, false);
                    dicResult.Add(bydateType + "FpyGroup2", dtGroup);
                    exeRes.Anything = dicResult;
                }
            }
            else
            {
                #region             
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "MODEL";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qModel, "LOB", qLOB, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qModel, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupInsight(plant, sapplant, bydateType, qCond, qLOB, qModel, "", "FPY", "ASC", 10);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
                DataTable dtGroup2 = fpydeepdiveOperator.GetFpyDataByGru(plant, sapplant, bydateType, qCond, qLOB, qModel, "", "FPY", "ASC", false);
                dicResult.Add(bydateType + "FpyGroup2", dtGroup2);
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsight(plant, sapplant, bydateType, qCond, "ASSY", qLOB, qModel, "FPY", "DESC", 10);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                exeRes.Anything = dicResult;
                #endregion
            }


            return exeRes;
        }

        public ExecutionResult GetFpyByLobByLineSMT(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qLine)
        {
            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region Duty Table
            if (!(qLOB == "" && qLine != ""))
            {
                DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDutySMT(plant, sapplant, bydateType, qCond, qLOB, "LINE", qLine);
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("catalogy", typeof(string));
                dtDuty.Columns.Add("error_rate", typeof(double));
                dtDuty.Columns.Add("percentage", typeof(double));

                if (dtDutyInfo.Rows.Count > 0)
                {
                    DataTable dtDutyItem = dtDutyInfo.DefaultView.ToTable(true, "duty_type");
                    for (int i = 0; i < dtDutyItem.Rows.Count; i++)
                    {
                        dtDuty.Rows.Add(dtDutyItem.Rows[i]["duty_type"].ToString(), 0, 0);
                    }
                    if (dtDuty.Rows.Count < 7)
                    {
                        if (dtDuty.Select("catalogy='置件'").Length == 0)
                            dtDuty.Rows.Add("置件", 0, 0);
                        if (dtDuty.Select("catalogy='印刷'").Length == 0)
                            dtDuty.Rows.Add("印刷", 0, 0);
                        if (dtDuty.Select("catalogy='材料'").Length == 0)
                            dtDuty.Rows.Add("材料", 0, 0);
                        if (dtDuty.Select("catalogy='制程'").Length == 0)
                            dtDuty.Rows.Add("制程", 0, 0);
                        if (dtDuty.Select("catalogy='NTF'").Length == 0)
                            dtDuty.Rows.Add("NTF", 0, 0);
                        if (dtDuty.Select("catalogy='AFTP'").Length == 0)
                            dtDuty.Rows.Add("AFTP", 0, 0);
                        if (dtDuty.Select("catalogy='BIOS'").Length == 0)
                            dtDuty.Rows.Add("BIOS", 0, 0);
                    }
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDuty.Rows.Count; i++)
                    {
                        string strType = dtDuty.Rows[i]["catalogy"].ToString();
                        double dPercent = 0;
                        if (strType == "置件" || strType == "材料" || strType == "印刷" || strType == "制程" || strType == "BIOS")
                            dtDuty.Rows[i]["catalogy"] = strType + "不良";
                        else if (strType == "NTF")
                            dtDuty.Rows[i]["catalogy"] = "複判OK";
                        else if (strType == "AFTP")
                            dtDuty.Rows[i]["catalogy"] = "組包造成";
                        DataRow[] dr = dtDutyInfo.Select("DUTY_TYPE='" + strType + "'");
                        if (dr.Length > 0)
                        {
                            dPercent = double.Parse(dr[0]["percents"].ToString());
                            dtDuty.Rows[i]["error_rate"] = double.Parse(dr[0]["fpy"].ToString());

                        }
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtDuty.Rows[i]["percentage"] = dAllPercent;
                    }
                }
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
                exeRes.Anything = dicResult;
            }
            #endregion

            DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupSMT(plant, sapplant, bydateType, qCond, qLOB, "", qLine, "FPY", "DESC", 10);
            dicResult.Add(bydateType + "FpyGroup", dtGroup);

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetFpyByDuty(string plant, string sapplant, string bydateType, string qCond, string qLob, string qType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            DataTable dtDutyInfo = new DataTable();
            if (qType == "ERRORT")
            {
                qType = "ERROR";
                dtDutyInfo = fpydeepdiveOperator.GetFpyDuty(plant, sapplant, bydateType, qCond, "", "", qType);
                if (dtDutyInfo.Rows.Count == 0)
                    dtDutyInfo.Rows.Add("N/A", 0, 0, 0, 0);
                dicResult.Add(bydateType + "FpyDuty", dtDutyInfo);
            }
            else
            {
                if (qType == "PARTS")
                    dtDutyInfo = fpydeepdiveOperator.GetFpyDuty(plant, sapplant, bydateType, qCond, "", qLob, qType);
                else
                    dtDutyInfo = fpydeepdiveOperator.GetFpyDuty(plant, sapplant, bydateType, qCond, qLob, qType);
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("LOB");
                dtDuty.Columns.Add("INPUT");
                dtDuty.Columns.Add("WP");
                dtDuty.Columns.Add("PCBA_LRR");
                dtDuty.Columns.Add("ML");
                dtDuty.Columns.Add("DS");
                dtDuty.Columns.Add("NTF");
                dtDuty.Columns.Add("OT");
                if (dtDutyInfo.Rows.Count > 0)
                {
                    double iTotalInput = 0, iWPError = 0, iPCBA_LRRError = 0, iMLError = 0, iDSError = 0, iNTFError = 0, iOTError = 0;
                    #region Set               
                    DataTable dtLOB = dtDutyInfo.DefaultView.ToTable(true, "catalogy", "input");
                    for (int i = 0; i < dtLOB.Rows.Count; i++)
                    {
                        DataRow drDuty = dtDuty.NewRow();
                        string strType = dtLOB.Rows[i]["catalogy"].ToString();
                        drDuty["LOB"] = strType;
                        drDuty["INPUT"] = dtLOB.Rows[i]["input"].ToString();
                        drDuty["WP"] = "0%";
                        drDuty["PCBA_LRR"] = "0%";
                        drDuty["ML"] = "0%";
                        drDuty["DS"] = "0%";
                        drDuty["NTF"] = "0%";
                        drDuty["OT"] = "0%";
                        iTotalInput += double.Parse(dtLOB.Rows[i]["input"].ToString());
                        dtDutyInfo.CaseSensitive = true;
                        DataRow[] dr = dtDutyInfo.Select("catalogy='" + strType + "'");
                        if (dr.Length > 0)
                        {
                            for (int idx = 0; idx < dr.Length; idx++)
                            {
                                if (dr[idx]["DUTY_TYPE"].ToString() == "作業")
                                {
                                    drDuty["WP"] = dr[idx]["fpy"].ToString() + "%";
                                    iWPError += double.Parse(dr[idx]["error_input"].ToString());
                                }
                                if (dr[idx]["DUTY_TYPE"].ToString() == "PCBA LRR")
                                {
                                    drDuty["PCBA_LRR"] = dr[idx]["fpy"].ToString() + "%";
                                    iPCBA_LRRError += double.Parse(dr[idx]["error_input"].ToString());
                                }
                                if (dr[idx]["DUTY_TYPE"].ToString() == "材料")
                                {
                                    drDuty["ML"] = dr[idx]["fpy"].ToString() + "%";
                                    iMLError += double.Parse(dr[idx]["error_input"].ToString());
                                }
                                if (dr[idx]["DUTY_TYPE"].ToString() == "設計")
                                {
                                    drDuty["DS"] = dr[idx]["fpy"].ToString() + "%";
                                    iDSError += double.Parse(dr[idx]["error_input"].ToString());
                                }
                                if (dr[idx]["DUTY_TYPE"].ToString() == "NTF")
                                {
                                    drDuty["NTF"] = dr[idx]["fpy"].ToString() + "%";
                                    iNTFError += double.Parse(dr[idx]["error_input"].ToString());
                                }
                                if (dr[idx]["DUTY_TYPE"].ToString() == "其他")
                                {
                                    drDuty["OT"] = dr[idx]["fpy"].ToString() + "%";
                                    iOTError += double.Parse(dr[idx]["error_input"].ToString());
                                }
                            }
                        }
                        dtDuty.Rows.Add(drDuty);
                    }
                    if (qType == "LOB")
                    {
                        DataRow drDuty = dtDuty.NewRow();
                        drDuty["LOB"] = "TTL";
                        drDuty["INPUT"] = iTotalInput.ToString();
                        drDuty["WP"] = iWPError == 0 ? "0%" : Math.Round((iWPError / iTotalInput) * 100, 2).ToString() + "%";
                        drDuty["PCBA_LRR"] = iPCBA_LRRError == 0 ? "0%" : Math.Round((iPCBA_LRRError / iTotalInput) * 100, 2).ToString() + "%";
                        drDuty["ML"] = iMLError == 0 ? "0%" : Math.Round((iMLError / iTotalInput) * 100, 2).ToString() + "%";
                        drDuty["DS"] = iDSError == 0 ? "0%" : Math.Round((iDSError / iTotalInput) * 100, 2).ToString() + "%";
                        drDuty["NTF"] = iNTFError == 0 ? "0%" : Math.Round((iNTFError / iTotalInput) * 100, 2).ToString() + "%";
                        drDuty["OT"] = iOTError == 0 ? "0%" : Math.Round((iOTError / iTotalInput) * 100, 2).ToString() + "%";
                        dtDuty.Rows.Add(drDuty);
                    }
                    #endregion
                }
                else
                {
                    DataRow drDuty = dtDuty.NewRow();
                    drDuty["LOB"] = "N/A";
                    drDuty["INPUT"] = "0";
                    drDuty["WP"] = "0%";
                    drDuty["PCBA_LRR"] = "0%";
                    drDuty["ML"] = "0%";
                    drDuty["DS"] = "0%";
                    drDuty["NTF"] = "0%";
                    drDuty["OT"] = "0%";
                    dtDuty.Rows.Add(drDuty);
                }
                //if (dtDuty.Rows.Count>0)
                //{
                //    DataTable dtCopy = dtDuty.Copy();
                //    DataView dv = dtDuty.DefaultView;
                //    dv.Sort = "ID";
                //    dtCopy = dv.ToTable();
                //}
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByDutySMT(string plant, string sapplant, string bydateType, string qCond, string qLob, string qType)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            DataTable dtDutyInfo = new DataTable();

            if (qType == "PARTS")
                dtDutyInfo = fpydeepdiveOperator.GetFpyDutySMT(plant, sapplant, bydateType, qCond, "", qType, qLob);
            else
                dtDutyInfo = fpydeepdiveOperator.GetFpyDutySMT(plant, sapplant, bydateType, qCond, qLob, qType);
            DataTable dtDuty = new DataTable();
            dtDuty.Columns.Add("LOB");
            dtDuty.Columns.Add("INPUT");
            dtDuty.Columns.Add("MT");
            dtDuty.Columns.Add("PT");
            dtDuty.Columns.Add("ML");
            dtDuty.Columns.Add("PS");
            dtDuty.Columns.Add("NF");
            dtDuty.Columns.Add("FP");
            dtDuty.Columns.Add("BI");
            if (dtDutyInfo.Rows.Count > 0)
            {
                double iTotalInput = 0, iMTError = 0, iPTError = 0, iMLError = 0, iPSError = 0, iNFError = 0, iFPError = 0, iBIError = 0;
                #region Set               
                DataTable dtLOB = dtDutyInfo.DefaultView.ToTable(true, "catalogy", "input");
                for (int i = 0; i < dtLOB.Rows.Count; i++)
                {
                    DataRow drDuty = dtDuty.NewRow();
                    string strType = dtLOB.Rows[i]["catalogy"].ToString();
                    drDuty["LOB"] = strType;
                    drDuty["INPUT"] = dtLOB.Rows[i]["input"].ToString();
                    drDuty["MT"] = "0%";
                    drDuty["PT"] = "0%";
                    drDuty["ML"] = "0%";
                    drDuty["PS"] = "0%";
                    drDuty["NF"] = "0%";
                    drDuty["FP"] = "0%";
                    drDuty["BI"] = "0%";
                    iTotalInput += double.Parse(dtLOB.Rows[i]["input"].ToString());
                    DataRow[] dr = dtDutyInfo.Select("catalogy='" + strType + "'");
                    if (dr.Length > 0)
                    {
                        for (int idx = 0; idx < dr.Length; idx++)
                        {
                            if (dr[idx]["DUTY_TYPE"].ToString() == "置件")
                            {
                                drDuty["MT"] = dr[idx]["fpy"].ToString() + "%";
                                iMTError += double.Parse(dr[idx]["error_input"].ToString());
                            }
                            if (dr[idx]["DUTY_TYPE"].ToString() == "印刷")
                            {
                                drDuty["PT"] = dr[idx]["fpy"].ToString() + "%";
                                iPTError += double.Parse(dr[idx]["error_input"].ToString());
                            }
                            if (dr[idx]["DUTY_TYPE"].ToString() == "材料")
                            {
                                drDuty["ML"] = dr[idx]["fpy"].ToString() + "%";
                                iMLError += double.Parse(dr[idx]["error_input"].ToString());
                            }
                            if (dr[idx]["DUTY_TYPE"].ToString() == "制程")
                            {
                                drDuty["PS"] = dr[idx]["fpy"].ToString() + "%";
                                iPSError += double.Parse(dr[idx]["error_input"].ToString());
                            }
                            if (dr[idx]["DUTY_TYPE"].ToString() == "NTF")
                            {
                                drDuty["NF"] = dr[idx]["fpy"].ToString() + "%";
                                iNFError += double.Parse(dr[idx]["error_input"].ToString());
                            }
                            if (dr[idx]["DUTY_TYPE"].ToString() == "AFTP")
                            {
                                drDuty["FP"] = dr[idx]["fpy"].ToString() + "%";
                                iFPError += double.Parse(dr[idx]["error_input"].ToString());
                            }
                            if (dr[idx]["DUTY_TYPE"].ToString() == "BIOS")
                            {
                                drDuty["BI"] = dr[idx]["fpy"].ToString() + "%";
                                iBIError += double.Parse(dr[idx]["error_input"].ToString());
                            }
                        }
                    }
                    dtDuty.Rows.Add(drDuty);
                }
                if (qType == "LOB")
                {
                    DataRow drDuty = dtDuty.NewRow();
                    drDuty["LOB"] = "TTL";
                    drDuty["INPUT"] = iTotalInput.ToString();
                    drDuty["MT"] = iMTError == 0 ? "0%" : Math.Round((iMTError / iTotalInput) * 100, 2).ToString() + "%";
                    drDuty["PT"] = iPTError == 0 ? "0%" : Math.Round((iPTError / iTotalInput) * 100, 2).ToString() + "%";
                    drDuty["ML"] = iMLError == 0 ? "0%" : Math.Round((iMLError / iTotalInput) * 100, 2).ToString() + "%";
                    drDuty["PS"] = iPSError == 0 ? "0%" : Math.Round((iPSError / iTotalInput) * 100, 2).ToString() + "%";
                    drDuty["NF"] = iNFError == 0 ? "0%" : Math.Round((iNFError / iTotalInput) * 100, 2).ToString() + "%";
                    drDuty["FP"] = iFPError == 0 ? "0%" : Math.Round((iFPError / iTotalInput) * 100, 2).ToString() + "%";
                    drDuty["BI"] = iBIError == 0 ? "0%" : Math.Round((iBIError / iTotalInput) * 100, 2).ToString() + "%";
                    dtDuty.Rows.Add(drDuty);
                }
                #endregion
            }
            else
            {
                DataRow drDuty = dtDuty.NewRow();
                drDuty["LOB"] = "N/A";
                drDuty["INPUT"] = "0";
                drDuty["MT"] = "0%";
                drDuty["PT"] = "0%";
                drDuty["ML"] = "0%";
                drDuty["PS"] = "0%";
                drDuty["NF"] = "0%";
                drDuty["FP"] = "0%";
                drDuty["BI"] = "0%";
                dtDuty.Rows.Add(drDuty);
            }
            dicResult.Add(bydateType + "FpyDuty", dtDuty);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLob(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "ASSY";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
            }
            else
                strLOB = qLOB;
            if (qSub != "")
            {
                #region Sub
                if (qSub == "LINE")
                {
                    if (strLOB != "EE" && strLOB != "ME")
                    {
                        DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLine(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                        if (dtLineFpy.Rows.Count == 0)
                            dtLineFpy.Rows.Add("", 0, 0);
                        dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                    }
                    else
                    {
                        DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByErrorType(plant, sapplant, bydateType, qCond, "ERROR", strLOB, "LINE", strProcess);
                        if (dtLineFpy.Rows.Count == 0)
                            dtLineFpy.Rows.Add("", 0, 0);
                        dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                    }
                }
                else if (qSub == "ERROR")
                {
                    if (qLOB.IndexOf("_") > -1)
                    {
                        string[] aa = qLOB.Split('_');
                        string strvType = "", strvValue = "";
                        if (aa.Length >= 3)
                        {
                            strvType = aa[1];
                            strvValue = aa[2];
                        }
                        DataTable dtStationFpy = fpydeepdiveOperator.GetFpyDataByErrorType(plant, sapplant, bydateType, qCond, "ERROR", strLOB, "GROUP", "", strvType, strvValue);
                        if (dtStationFpy.Rows.Count == 0)
                            dtStationFpy.Rows.Add("", 0, 0);
                        dicResult.Add(bydateType + "FpyStation", dtStationFpy);
                    }
                }
                else if (qSub == "LOB_DUTY" || qSub == "LINE_DUTY" || qSub == "PARTS_DUTY")
                {
                    DateTime dBaseDate = DateTime.Now;
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    string strqType = "", strqCond = "";
                    if (qSub == "LOB_DUTY")
                    {
                        strqType = "LOB";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_DUTY")
                    {
                        strqType = "LINE";
                        strqCond = strLOB;
                    }
                    else if (qSub == "PARTS_DUTY")
                    {
                        strqType = "PARTS";
                        strqCond = strLOB;
                    }
                    if (strDateType == "Daily")
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dBaseDate = DateTime.ParseExact(cc[1], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        }
                        else
                            dBaseDate = DateTime.Now;
                    }
                    else
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);

                    dtDay = fpydeepdiveOperator.GetFpyDutyDPPM(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add(dBaseDate.ToString("yyyyMMdd"), 0, 0);
                    dicResult.Add(bydateType + "FpyDay", dtDay);
                    dtWeek = fpydeepdiveOperator.GetFpyDutyDPPM(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtWeek.Rows.Count == 0)
                        dtWeek.Rows.Add("W01", 0, 0);
                    dicResult.Add(bydateType + "FpyWeek", dtWeek);
                    dtMonth = fpydeepdiveOperator.GetFpyDutyDPPM(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtMonth.Rows.Count == 0)
                        dtMonth.Rows.Add(dBaseDate.ToString("yyyyMM"), 0, 0);
                    dicResult.Add(bydateType + "FpyMonth", dtMonth);
                }
                #endregion
            }
            else
            {
                if (strLOB != "EE" && strLOB != "ME")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLine(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                    DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModel(plant, sapplant, bydateType, qCond, strLOB);
                    #region Duty Table
                    DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDuty(plant, sapplant, bydateType, qCond, strLOB, "LOB");
                    DataTable dtDuty = new DataTable();
                    dtDuty.Columns.Add("catalogy", typeof(string));
                    dtDuty.Columns.Add("error_rate", typeof(double));
                    dtDuty.Columns.Add("percentage", typeof(double));
                    if (dtDutyInfo.Rows.Count > 0)
                    {
                        DataTable dtDutyItem = dtDutyInfo.DefaultView.ToTable(true, "duty_type");
                        for (int i = 0; i < dtDutyItem.Rows.Count; i++)
                        {
                            dtDuty.Rows.Add(dtDutyItem.Rows[i]["duty_type"].ToString(), 0, 0);
                        }
                        if (dtDuty.Rows.Count < 6)
                        {
                            if (dtDuty.Select("catalogy='作業'").Length == 0)
                                dtDuty.Rows.Add("作業", 0, 0);
                            if (dtDuty.Select("catalogy='PCBA LRR'").Length == 0)
                                dtDuty.Rows.Add("PCBA LRR", 0, 0);
                            if (dtDuty.Select("catalogy='材料'").Length == 0)
                                dtDuty.Rows.Add("材料", 0, 0);
                            if (dtDuty.Select("catalogy='設計'").Length == 0)
                                dtDuty.Rows.Add("設計", 0, 0);
                            if (dtDuty.Select("catalogy='NTF'").Length == 0)
                                dtDuty.Rows.Add("NTF", 0, 0);
                            if (dtDuty.Select("catalogy='其他'").Length == 0)
                                dtDuty.Rows.Add("其他", 0, 0);
                        }
                        double dAllPercent = 0;
                        for (int i = 0; i < dtDuty.Rows.Count; i++)
                        {
                            string strType = dtDuty.Rows[i]["catalogy"].ToString();
                            double dPercent = 0;
                            if (strType == "作業" || strType == "材料" || strType == "設計")
                                dtDuty.Rows[i]["catalogy"] = strType + "不良";
                            DataRow[] dr = dtDutyInfo.Select("DUTY_TYPE='" + strType + "'");
                            if (dr.Length > 0)
                            {
                                dPercent = double.Parse(dr[0]["percents"].ToString());
                                dtDuty.Rows[i]["error_rate"] = double.Parse(dr[0]["fpy"].ToString());

                            }
                            dAllPercent += dPercent;
                            if (dAllPercent > 100)
                                dAllPercent = 100;
                            dtDuty.Rows[i]["percentage"] = dAllPercent;
                        }
                    }
                    else
                        dtDuty.Rows.Add("", 0, 0);
                    #endregion
                    DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroup(plant, sapplant, bydateType, qCond, strLOB);
                    DateTime dBaseDate = DateTime.Now;
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    DataTable dtDayM = new DataTable(), dtWeekM = new DataTable(), dtMonthM = new DataTable();
                    DataTable dtDayE = new DataTable(), dtWeekE = new DataTable(), dtMonthE = new DataTable();
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    string strqType = "LOB";
                    if (strDateType == "Daily")
                    {
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, strLOB);
                        dtDayM = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "ERROR", "ME", strqType, strLOB);
                        dtDayE = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "ERROR", "EE", strqType, strLOB);
                        if (dtDayM.Rows.Count == 0)
                            dtDayM.Rows.Add("", 0, 0, "");
                        if (dtDayE.Rows.Count == 0)
                            dtDayE.Rows.Add("", 0, 0, "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                        dicResult.Add(bydateType + "FpyME", dtDayM);
                        dicResult.Add(bydateType + "FpyEE", dtDayE);
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, strLOB);
                            dtDayM = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "ERROR", "ME", strqType, strLOB);
                            dtDayE = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "ERROR", "EE", strqType, strLOB);
                            if (dtDayM.Rows.Count == 0)
                                dtDayM.Rows.Add("", 0, 0, "");
                            if (dtDayE.Rows.Count == 0)
                                dtDayE.Rows.Add("", 0, 0, "");
                            dicResult.Add(bydateType + "FpyTrend", dtDay);
                            dicResult.Add(bydateType + "FpyME", dtDayM);
                            dicResult.Add(bydateType + "FpyEE", dtDayE);
                        }
                    }
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, strLOB);
                        dtWeekM = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "ERROR", "ME", strqType, strLOB);
                        dtWeekE = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "ERROR", "EE", strqType, strLOB);
                        if (dtWeekM.Rows.Count == 0)
                            dtWeekM.Rows.Add("", 0, 0, "");
                        if (dtWeekE.Rows.Count == 0)
                            dtWeekE.Rows.Add("", 0, 0, "");
                        dicResult.Add(bydateType + "FpyTrend", dtWeek);
                        dicResult.Add(bydateType + "FpyME", dtWeekM);
                        dicResult.Add(bydateType + "FpyEE", dtWeekE);
                    }
                    else
                    {
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, strLOB);
                        dtMonthM = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "ERROR", "ME", strqType, strLOB);
                        dtMonthE = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "ERROR", "EE", strqType, strLOB);
                        if (dtMonthM.Rows.Count == 0)
                            dtMonthM.Rows.Add("", 0, 0, "");
                        if (dtMonthE.Rows.Count == 0)
                            dtMonthE.Rows.Add("", 0, 0, "");
                        dicResult.Add(bydateType + "FpyTrend", dtMonth);
                        dicResult.Add(bydateType + "FpyME", dtMonthM);
                        dicResult.Add(bydateType + "FpyEE", dtMonthE);
                    }
                    if (dtLineFpy.Rows.Count == 0)
                        dtLineFpy.Rows.Add("", 0, 0);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                    dicResult.Add(bydateType + "FpyModel", dtModelFpy);
                    dicResult.Add(bydateType + "FpyDuty", dtDuty);
                    dicResult.Add(bydateType + "FpyGroup", dtGroup);
                }
                else
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByErrorType(plant, sapplant, bydateType, qCond, "ERROR", strLOB, "LINE", strProcess);
                    DataTable dtLobFpy = fpydeepdiveOperator.GetFpyDataByErrorType(plant, sapplant, bydateType, qCond, "ERROR", strLOB, "LOB");
                    DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByErrorType(plant, sapplant, bydateType, qCond, "ERROR", strLOB, "MODEL");
                    DataTable dtStationFpy = fpydeepdiveOperator.GetFpyDataByErrorType(plant, sapplant, bydateType, qCond, "ERROR", strLOB, "GROUP", "");

                    DateTime dBaseDate = DateTime.Now;
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    if (strDateType == "Daily")
                    {
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "ERRORT", strLOB);
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "ERRORT", strLOB);
                            dicResult.Add(bydateType + "FpyTrend", dtDay);
                        }
                    }
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "ERRORT", strLOB);
                        dicResult.Add(bydateType + "FpyTrend", dtWeek);
                    }
                    else
                    {
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "ERRORT", strLOB);
                        dicResult.Add(bydateType + "FpyTrend", dtMonth);
                    }
                    if (dtLineFpy.Rows.Count == 0)
                        dtLineFpy.Rows.Add("", 0, 0);
                    if (dtLobFpy.Rows.Count == 0)
                        dtLobFpy.Rows.Add("", 0, 0);
                    if (dtModelFpy.Rows.Count == 0)
                        dtModelFpy.Rows.Add("", 0, 0);
                    if (dtStationFpy.Rows.Count == 0)
                        dtStationFpy.Rows.Add("", 0, 0);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                    dicResult.Add(bydateType + "FpyLob", dtLobFpy);
                    dicResult.Add(bydateType + "FpyModel", dtModelFpy);
                    dicResult.Add(bydateType + "FpyStation", dtStationFpy);
                }
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobSMT(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "SMT";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
                if (qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                    strLOB = qLOB.Split('_')[2] + "_" + qLOB.Split('_')[0];
            }
            else
                strLOB = qLOB;
            if (qSub != "")
            {
                if (qSub == "LINE")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineSMT(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                }
                else if (qSub == "LOB_DUTY" || qSub == "LINE_DUTY" || qSub == "PARTS_DUTY" || qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                {
                    DateTime dBaseDate = DateTime.Now;
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    string strqType = "", strqCond = "";
                    if (qSub == "LOB_DUTY")
                    {
                        strqType = "LOB";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_DUTY")
                    {
                        strqType = "LINE";
                        strqCond = strLOB;
                    }
                    else if (qSub == "PARTS_DUTY")
                    {
                        strqType = "PARTS";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_LOB_DUTY")
                    {
                        strqType = "LINE_LOB";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_MODEL_DUTY")
                    {
                        strqType = "LINE_MODEL";
                        strqCond = strLOB;
                    }
                    if (strDateType == "Daily")
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dBaseDate = DateTime.ParseExact(cc[1], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        }
                        else
                            dBaseDate = DateTime.Now;
                    }
                    else
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);

                    dtDay = fpydeepdiveOperator.GetFpyDutyDPPMSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add(dBaseDate.ToString("yyyyMMdd"), 0, 0);
                    dicResult.Add(bydateType + "FpyDay", dtDay);
                    dtWeek = fpydeepdiveOperator.GetFpyDutyDPPMSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtWeek.Rows.Count == 0)
                        dtWeek.Rows.Add("W01", 0, 0);
                    dicResult.Add(bydateType + "FpyWeek", dtWeek);
                    dtMonth = fpydeepdiveOperator.GetFpyDutyDPPMSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtMonth.Rows.Count == 0)
                        dtMonth.Rows.Add(dBaseDate.ToString("yyyyMM"), 0, 0);
                    dicResult.Add(bydateType + "FpyMonth", dtMonth);
                }
            }
            else
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineSMT(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelSMT(plant, sapplant, bydateType, qCond, strLOB, 10);
                #region Duty Table
                DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDutySMT(plant, sapplant, bydateType, qCond, strLOB, "LOB");
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("catalogy", typeof(string));
                dtDuty.Columns.Add("error_rate", typeof(double));
                dtDuty.Columns.Add("percentage", typeof(double));
                if (dtDutyInfo.Rows.Count > 0)
                {
                    DataTable dtDutyItem = dtDutyInfo.DefaultView.ToTable(true, "duty_type");
                    for (int i = 0; i < dtDutyItem.Rows.Count; i++)
                    {
                        dtDuty.Rows.Add(dtDutyItem.Rows[i]["duty_type"].ToString(), 0, 0);
                    }
                    if (dtDuty.Rows.Count < 7)
                    {
                        if (dtDuty.Select("catalogy='置件'").Length == 0)
                            dtDuty.Rows.Add("置件", 0, 0);
                        if (dtDuty.Select("catalogy='印刷'").Length == 0)
                            dtDuty.Rows.Add("印刷", 0, 0);
                        if (dtDuty.Select("catalogy='材料'").Length == 0)
                            dtDuty.Rows.Add("材料", 0, 0);
                        if (dtDuty.Select("catalogy='制程'").Length == 0)
                            dtDuty.Rows.Add("制程", 0, 0);
                        if (dtDuty.Select("catalogy='NTF'").Length == 0)
                            dtDuty.Rows.Add("NTF", 0, 0);
                        if (dtDuty.Select("catalogy='AFTP'").Length == 0)
                            dtDuty.Rows.Add("AFTP", 0, 0);
                        if (dtDuty.Select("catalogy='BIOS'").Length == 0)
                            dtDuty.Rows.Add("BIOS", 0, 0);
                    }
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDuty.Rows.Count; i++)
                    {
                        string strType = dtDuty.Rows[i]["catalogy"].ToString();
                        double dPercent = 0;
                        if (strType == "置件" || strType == "材料" || strType == "印刷" || strType == "制程" || strType == "BIOS")
                            dtDuty.Rows[i]["catalogy"] = strType + "不良";
                        else if (strType == "NTF")
                            dtDuty.Rows[i]["catalogy"] = "複判OK";
                        else if (strType == "AFTP")
                            dtDuty.Rows[i]["catalogy"] = "組包造成";
                        DataRow[] dr = dtDutyInfo.Select("DUTY_TYPE='" + strType + "'");
                        if (dr.Length > 0)
                        {
                            dPercent = double.Parse(dr[0]["percents"].ToString());
                            dtDuty.Rows[i]["error_rate"] = double.Parse(dr[0]["fpy"].ToString());

                        }
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtDuty.Rows[i]["percentage"] = dAllPercent;
                    }
                }
                else
                    dtDuty.Rows.Add("", 0, 0);
                #endregion
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupSMT(plant, sapplant, bydateType, qCond, strLOB, 10);
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "LOB";
                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, strLOB, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, strLOB, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, strLOB, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, strLOB, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
                if (dtLineFpy.Rows.Count == 0)
                    dtLineFpy.Rows.Add("", 0, 0);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobInsightSMT(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "SMT";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
                if (qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                    strLOB = qLOB.Split('_')[2] + "_" + qLOB.Split('_')[0];
            }
            else
                strLOB = qLOB;
            if (qSub != "")
            {
                if (qSub == "LINE")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsightSMT(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                }
                else if (qSub == "LOB_DUTY" || qSub == "LINE_DUTY" || qSub == "PARTS_DUTY" || qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                {
                    DateTime dBaseDate = DateTime.Now;
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    string strqType = "", strqCond = "";
                    if (qSub == "LOB_DUTY")
                    {
                        strqType = "LOB";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_DUTY")
                    {
                        strqType = "LINE";
                        strqCond = strLOB;
                    }
                    else if (qSub == "PARTS_DUTY")
                    {
                        strqType = "PARTS";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_LOB_DUTY")
                    {
                        strqType = "LINE_LOB";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_MODEL_DUTY")
                    {
                        strqType = "LINE_MODEL";
                        strqCond = strLOB;
                    }
                    if (strDateType == "Daily")
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dBaseDate = DateTime.ParseExact(cc[1], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        }
                        else
                            dBaseDate = DateTime.Now;
                    }
                    else
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);

                    dtDay = fpydeepdiveOperator.GetFpyDutyDPPMInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add(dBaseDate.ToString("yyyyMMdd"), 0, 0);
                    dicResult.Add(bydateType + "FpyDay", dtDay);
                    dtWeek = fpydeepdiveOperator.GetFpyDutyDPPMInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtWeek.Rows.Count == 0)
                        dtWeek.Rows.Add("W01", 0, 0);
                    dicResult.Add(bydateType + "FpyWeek", dtWeek);
                    dtMonth = fpydeepdiveOperator.GetFpyDutyDPPMInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtMonth.Rows.Count == 0)
                        dtMonth.Rows.Add(dBaseDate.ToString("yyyyMM"), 0, 0);
                    dicResult.Add(bydateType + "FpyMonth", dtMonth);
                }
            }
            else
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsightSMT(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelInsightSMT(plant, sapplant, bydateType, qCond, strLOB, 10);
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupInsightSMT(plant, sapplant, bydateType, qCond, strLOB, 10);
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "LOB";
                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, strLOB, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, strLOB, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, strLOB, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, strLOB, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
                if (dtLineFpy.Rows.Count == 0)
                    dtLineFpy.Rows.Add("", 0, 0);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobInsight(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "SMT";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
                if (qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                    strLOB = qLOB.Split('_')[2] + "_" + qLOB.Split('_')[0];
            }
            else
                strLOB = qLOB;
            if (qSub != "")
            {
                if (qSub == "LINE")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsight(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                }
                else if (qSub == "LOB_DUTY" || qSub == "LINE_DUTY" || qSub == "PARTS_DUTY" || qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                {
                    DateTime dBaseDate = DateTime.Now;
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    string strqType = "", strqCond = "";
                    if (qSub == "LOB_DUTY")
                    {
                        strqType = "LOB";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_DUTY")
                    {
                        strqType = "LINE";
                        strqCond = strLOB;
                    }
                    else if (qSub == "PARTS_DUTY")
                    {
                        strqType = "PARTS";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_LOB_DUTY")
                    {
                        strqType = "LINE_LOB";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_MODEL_DUTY")
                    {
                        strqType = "LINE_MODEL";
                        strqCond = strLOB;
                    }
                    if (strDateType == "Daily")
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dBaseDate = DateTime.ParseExact(cc[1], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        }
                        else
                            dBaseDate = DateTime.Now;
                    }
                    else
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);

                    dtDay = fpydeepdiveOperator.GetFpyDutyDPPMInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add(dBaseDate.ToString("yyyyMMdd"), 0, 0);
                    dicResult.Add(bydateType + "FpyDay", dtDay);
                    dtWeek = fpydeepdiveOperator.GetFpyDutyDPPMInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtWeek.Rows.Count == 0)
                        dtWeek.Rows.Add("W01", 0, 0);
                    dicResult.Add(bydateType + "FpyWeek", dtWeek);
                    dtMonth = fpydeepdiveOperator.GetFpyDutyDPPMInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtMonth.Rows.Count == 0)
                        dtMonth.Rows.Add(dBaseDate.ToString("yyyyMM"), 0, 0);
                    dicResult.Add(bydateType + "FpyMonth", dtMonth);
                }
            }
            else
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsight(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelInsight(plant, sapplant, bydateType, qCond, strLOB, 10);
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupInsight(plant, sapplant, bydateType, qCond, strLOB, 10);
                DataTable dtGroup2 = fpydeepdiveOperator.GetFpyDataByGru(plant, sapplant, bydateType, qCond, strLOB);
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "LOB";
                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, strLOB, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, strLOB, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, strLOB, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, strLOB, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
                if (dtLineFpy.Rows.Count == 0)
                    dtLineFpy.Rows.Add("", 0, 0);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
                dicResult.Add(bydateType + "FpyGroup2", dtGroup2);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLobPcbLrr(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "ASSY";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
            }
            else
                strLOB = qLOB;
            if (qSub != "")
            {
                if (qSub == "LINE")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLine(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                }
                else if (qSub == "LOB_DUTY" || qSub == "LINE_DUTY" || qSub == "PARTS_DUTY")
                {
                    DateTime dBaseDate = DateTime.Now;
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    string strqType = "", strqCond = "";
                    if (qSub == "LOB_DUTY")
                    {
                        strqType = "LOB";
                        strqCond = strLOB;
                    }
                    else if (qSub == "LINE_DUTY")
                    {
                        strqType = "LINE";
                        strqCond = strLOB;
                    }
                    else if (qSub == "PARTS_DUTY")
                    {
                        strqType = "PARTS";
                        strqCond = strLOB;
                    }
                    if (strDateType == "Daily")
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dBaseDate = DateTime.ParseExact(cc[1], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        }
                        else
                            dBaseDate = DateTime.Now;
                    }
                    else
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);

                    dtDay = fpydeepdiveOperator.GetFpyDutyDPPM(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtDay.Rows.Count == 0)
                        dtDay.Rows.Add(dBaseDate.ToString("yyyyMMdd"), 0, 0);
                    dicResult.Add(bydateType + "FpyDay", dtDay);
                    dtWeek = fpydeepdiveOperator.GetFpyDutyDPPM(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtWeek.Rows.Count == 0)
                        dtWeek.Rows.Add("W01", 0, 0);
                    dicResult.Add(bydateType + "FpyWeek", dtWeek);
                    dtMonth = fpydeepdiveOperator.GetFpyDutyDPPM(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    if (dtMonth.Rows.Count == 0)
                        dtMonth.Rows.Add(dBaseDate.ToString("yyyyMM"), 0, 0);
                    dicResult.Add(bydateType + "FpyMonth", dtMonth);
                }
            }
            else
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLinePcbLrr(plant, sapplant, bydateType, qCond, strProcess, strLOB, 10);
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelPcbLrr(plant, sapplant, bydateType, qCond, strLOB);
                #region Duty Table
                DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDataDutyPcbLrr(plant, sapplant, bydateType, qCond, strLOB);
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("catalogy", typeof(string));
                dtDuty.Columns.Add("error_rate", typeof(double));
                dtDuty.Columns.Add("percentage", typeof(double));
                if (dtDutyInfo.Rows.Count > 0)
                {
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDutyInfo.Rows.Count; i++)
                    {
                        string strType = dtDutyInfo.Rows[i]["catalogy"].ToString();
                        double dPercent = 0, dLrr = 0;
                        dPercent = double.Parse(dtDutyInfo.Rows[i]["percents"].ToString());
                        dLrr = double.Parse(dtDutyInfo.Rows[i]["pcblrr"].ToString());
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtDuty.Rows.Add(dtDutyInfo.Rows[i]["catalogy"].ToString(), dLrr, dAllPercent);
                    }
                }
                else
                    dtDuty.Rows.Add("", 0, 0);
                #endregion
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupPcbLrr(plant, sapplant, bydateType, qCond, strLOB);
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                DataTable dtDayM = new DataTable(), dtWeekM = new DataTable(), dtMonthM = new DataTable();
                DataTable dtDayE = new DataTable(), dtWeekE = new DataTable(), dtMonthE = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "LOB";
                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, strLOB);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, strLOB);
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, strLOB);
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, strLOB);
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
                if (dtLineFpy.Rows.Count == 0)
                    dtLineFpy.Rows.Add("", 0, 0);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByStationSMT(string plant, string sapplant, string bydateType, string qCond, string qStation, string qSub, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strStation = "";
            string strProcess = "SMT";
            if (qStation.IndexOf("_") > -1)
            {
                strStation = qStation.Split('_')[0];
                strProcess = qStation.Split('_')[1];
                if (qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                    strStation = qStation.Split('_')[2] + "_" + qStation.Split('_')[0];
            }
            else
                strStation = qStation;
            if (qSub != "")
            {
                if (qSub == "LINE")
                {
                    DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByGroupByLineSMT(plant, sapplant, bydateType, qCond, strProcess, strStation, vOrderBy, vOrderAsc, 10);
                    dicResult.Add(bydateType + "FpyLine", dtLineFpy);
                }
                else if (qSub == "LOB_DUTY" || qSub == "LINE_DUTY" || qSub == "PARTS_DUTY" || qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                {
                    DateTime dBaseDate = DateTime.Now;
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    string strqType = "", strqCond = "";
                    if (qSub == "LOB_DUTY")
                    {
                        strqType = "LOB";
                        strqCond = strStation;
                    }
                    else if (qSub == "LINE_DUTY")
                    {
                        strqType = "LINE";
                        strqCond = strStation;
                    }
                    else if (qSub == "PARTS_DUTY")
                    {
                        strqType = "PARTS";
                        strqCond = strStation;
                    }
                    else if (qSub == "LINE_LOB_DUTY")
                    {
                        strqType = "LINE_LOB";
                        strqCond = strStation;
                    }
                    else if (qSub == "LINE_MODEL_DUTY")
                    {
                        strqType = "LINE_MODEL";
                        strqCond = strStation;
                    }
                    if (strDateType == "Daily")
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dBaseDate = DateTime.ParseExact(cc[1], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        }
                        else
                            dBaseDate = DateTime.Now;
                    }
                    else
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);

                    dtDay = fpydeepdiveOperator.GetFpyDutyDPPMSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    dicResult.Add(bydateType + "FpyDay", dtDay);
                    dtWeek = fpydeepdiveOperator.GetFpyDutyDPPMSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    dicResult.Add(bydateType + "FpyWeek", dtWeek);
                    dtMonth = fpydeepdiveOperator.GetFpyDutyDPPMSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqCond, strqType, strProcess);
                    dicResult.Add(bydateType + "FpyMonth", dtMonth);
                }
            }
            else
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByGroupByLineSMT(plant, sapplant, bydateType, qCond, strProcess, strStation, vOrderBy, vOrderAsc, 10);
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "STATION";
                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, strStation, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, strStation, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, strStation, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, strStation, "", "", "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
                if (dtLineFpy.Rows.Count == 0)
                    dtLineFpy.Rows.Add("", 0, 0);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByStation(string plant, string sapplant, string bydateType, string qCond, string qStation, string qSub, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strStation = "";
            string strProcess = "FATP";
            if (qStation.IndexOf("_") > -1)
            {
                string[] strSplite = qStation.Split('_');
                if (strSplite.Length == 3 && qSub == "")
                {
                    strStation = strSplite[0] + "_" + strSplite[1];
                    strProcess = strSplite[2];
                }
                else
                {
                    strStation = strSplite[0];
                    strProcess = strSplite[1];
                    if (qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                        strStation = strSplite[2] + "_" + strSplite[0];
                }
            }
            else
                strStation = qStation;

            DateTime dBaseDate = DateTime.Now;
            DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
            string[] aa = bydateType.Split('_');
            string strDateType = aa[0];
            string strShift = aa[1];
            string strqType = "STATION";
            if (strDateType == "Daily")
            {
                dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, strStation);

                dicResult.Add(bydateType + "FpyTrend", dtDay);
            }
            else if (strDateType == "Range")
            {
                if (qCond.IndexOf("-") > -1)
                {
                    string[] cc = qCond.Split('-');
                    dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, strStation);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
            }
            else if (strDateType == "Weekly")
            {
                string strYear, strWeek;
                string strCond = qCond.Replace("W", "");
                strYear = strCond.Substring(0, 4);
                strWeek = strCond.Substring(4, 2);
                DateTime sTime, eTime;
                if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                {
                    sTime = DateTime.Now.AddDays(-7);
                    eTime = DateTime.Now;
                }
                dBaseDate = sTime;
                dtWeek = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, strStation);
                dicResult.Add(bydateType + "FpyTrend", dtWeek);
            }
            else
            {
                dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                dtMonth = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, strStation);
                dicResult.Add(bydateType + "FpyTrend", dtMonth);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByStationInsightSMT(string plant, string sapplant, string bydateType, string qCond, string qStation, string qSub)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strStation = "";
            string strProcess = "SMT";
            if (qStation.IndexOf("_") > -1)
            {
                strStation = qStation.Split('_')[0];
                strProcess = qStation.Split('_')[1];
                if (qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                    strStation = qStation.Split('_')[2] + "_" + qStation.Split('_')[0];
            }
            else
                strStation = qStation;
            if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
            {
                GregorianCalendar gc = new GregorianCalendar();
                int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                if (Int32.Parse(qCond.Substring(1)) > w)
                    qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                else
                    qCond = dtNow.Year.ToString() + qCond.Substring(1);
            }
            if (qSub != "")
            {
                if (qSub == "LINE")
                {
                    DataTable dtGap = fpydeepdiveOperator.GetFpyDataByGroupByLineGapInsightSMT(plant, sapplant, bydateType, qCond, "", "", "", strProcess, strStation, "", "", 10);
                    dicResult.Add(bydateType + "FpyStation", dtGap);
                }
            }
            else
            {
                if (strStation != "")
                {
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    DateTime dBaseDate = DateTime.Now;
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    string strqType = "STATIONGAP";
                    if (strDateType == "Daily")
                    {
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, strStation, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, strStation, "", "", "", "");
                            dicResult.Add(bydateType + "FpyTrend", dtDay);
                        }
                    }
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, strStation, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtWeek);
                    }
                    else
                    {
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, strStation, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtMonth);
                    }
                    DataTable dtGap = fpydeepdiveOperator.GetFpyDataByGroupByLineGapInsightSMT(plant, sapplant, bydateType, qCond, "", "", "", strProcess, strStation, "", "", 10);
                    dicResult.Add(bydateType + "FpyStation", dtGap);
                }
                else
                {
                    DataTable dtGap = fpydeepdiveOperator.GetFpyDataByGroupGapInsightSMT(plant, sapplant, bydateType, qCond, "", "", "", "", "", 10);
                    dicResult.Add(bydateType + "FpyStation", dtGap);
                }

            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByStationInsight(string plant, string sapplant, string bydateType, string qCond, string qStation, string qSub)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strStation = "";
            string strProcess = "FATP";
            if (qStation.IndexOf("_") > -1)
            {
                strStation = qStation.Split('_')[0];
                strProcess = qStation.Split('_')[1];
                if (qSub == "LINE_LOB_DUTY" || qSub == "LINE_MODEL_DUTY")
                    strStation = qStation.Split('_')[2] + "_" + qStation.Split('_')[0];
            }
            else
                strStation = qStation;
            if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
            {
                GregorianCalendar gc = new GregorianCalendar();
                int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                if (Int32.Parse(qCond.Substring(1)) > w)
                    qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                else
                    qCond = dtNow.Year.ToString() + qCond.Substring(1);
            }
            if (qSub != "")
            {
                if (qSub == "LINE")
                {
                    DataTable dtGap = fpydeepdiveOperator.GetFpyDataByGroupByLineGapInsight(plant, sapplant, bydateType, qCond, "", "", "", strProcess, strStation, "", "", 10);
                    dicResult.Add(bydateType + "FpyStation", dtGap);
                }
            }
            else
            {
                if (strStation != "")
                {
                    DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                    DateTime dBaseDate = DateTime.Now;
                    string[] aa = bydateType.Split('_');
                    string strDateType = aa[0];
                    string strShift = aa[1];
                    string strqType = "STATIONGAP";
                    if (strDateType == "Daily")
                    {
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, strStation, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, strStation, "", "", "", "");
                            dicResult.Add(bydateType + "FpyTrend", dtDay);
                        }
                    }
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, strStation, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtWeek);
                    }
                    else
                    {
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, strStation, "", "", "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtMonth);
                    }
                    DataTable dtGap = fpydeepdiveOperator.GetFpyDataByGroupByLineGapInsight(plant, sapplant, bydateType, qCond, "", "", "", strProcess, strStation, "", "", 10);
                    dicResult.Add(bydateType + "FpyStation", dtGap);
                }
                else
                {
                    DataTable dtGap = fpydeepdiveOperator.GetFpyDataByGroupGapInsight(plant, sapplant, bydateType, qCond, "", "", "", "", "遞減", 10);
                    dicResult.Add(bydateType + "FpyStation", dtGap);
                    DataTable dtGap2 = fpydeepdiveOperator.GetFpyDataByGru(plant, sapplant, bydateType, qCond, "", "", "", "", "遞減", false);
                    dicResult.Add(bydateType + "FpyStation2", dtGap2);
                }

            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetDeepDiveDataStationLv3SMT(string plant, string sapplant, string bydateType, string qCond, string qStation, string qLine, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            #region

            if (qType == "Tre")
            {
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "STATION";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qStation, "LINE", qLine, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetDeepDiveDataStationLv3InsightSMT(string plant, string sapplant, string bydateType, string qCond, string qStation, string qLine, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            #region
            if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
            {
                GregorianCalendar gc = new GregorianCalendar();
                int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                if (Int32.Parse(qCond.Substring(1)) > w)
                    qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                else
                    qCond = dtNow.Year.ToString() + qCond.Substring(1);
            }
            if (qType == "Tre")
            {
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "STATIONGAP";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qStation, "LINE", qLine, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");

                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetDeepDiveDataStationLv3Insight(string plant, string sapplant, string bydateType, string qCond, string qStation, string qLine, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            #region
            if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
            {
                GregorianCalendar gc = new GregorianCalendar();
                int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                if (Int32.Parse(qCond.Substring(1)) > w)
                    qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                else
                    qCond = dtNow.Year.ToString() + qCond.Substring(1);
            }
            if (qType == "Tre")
            {
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "STATIONGAP";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qStation, "LINE", qLine, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");

                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsight(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qStation, "LINE", qLine, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetDeepDiveDataLobLv2(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "ASSY";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
            }
            else
                strLOB = qLOB;
            if (qType == "Lin")
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLine(plant, sapplant, bydateType, qCond, strProcess, strLOB, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            }
            else if (qType == "Mod")
            {
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModel(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
            }
            else if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroup(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetDeepDiveDataLobLv2SMT(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "SMT";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
            }
            else
                strLOB = qLOB;
            if (qType == "Lin")
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineSMT(plant, sapplant, bydateType, qCond, strProcess, strLOB, "", vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            }
            else if (qType == "Mod")
            {
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelSMT(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
            }
            else if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupSMT(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetDeepDiveDataLobLv2InsightSMT(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "SMT";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
            }
            else
                strLOB = qLOB;
            if (qType == "Lin")
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsightSMT(plant, sapplant, bydateType, qCond, strProcess, strLOB, "", vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            }
            else if (qType == "Mod")
            {
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelInsightSMT(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
            }
            else if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupInsightSMT(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetDeepDiveDataLobLv2Insight(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "ASSY";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
            }
            else
                strLOB = qLOB;
            if (qType == "Lin")
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsight(plant, sapplant, bydateType, qCond, strProcess, strLOB, "", vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            }
            else if (qType == "Mod")
            {
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelInsight(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
            }
            else if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupInsight(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }
            else if (qType == "Gru")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGru(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc);
                dicResult.Add(bydateType + "FpyGroup2", dtGroup);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetDeepDiveDataLobLv2PcbLrr(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qSub, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            string strLOB = "";
            string strProcess = "ASSY";
            if (qLOB.IndexOf("_") > -1)
            {
                strLOB = qLOB.Split('_')[0];
                strProcess = qLOB.Split('_')[1];
            }
            else
                strLOB = qLOB;
            if (qType == "Lin")
            {
                DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLinePcbLrr(plant, sapplant, bydateType, qCond, strProcess, strLOB, "", vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            }
            else if (qType == "Mod")
            {
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelPcbLrr(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
            }
            else if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupPcbLrr(plant, sapplant, bydateType, qCond, strLOB, vOrderBy, vOrderAsc);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }

            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetDeepDiveDataLobLv3(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel, string qLine, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            #region
            if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroup(plant, sapplant, bydateType, qCond, qLOB, "", qLine, vOrderBy, vOrderAsc);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }
            else if (qType == "Tre")
            {
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                DataTable dtDayM = new DataTable(), dtWeekM = new DataTable(), dtMonthM = new DataTable();
                DataTable dtDayE = new DataTable(), dtWeekE = new DataTable(), dtMonthE = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "LINE";

                if (qCond.IndexOf("_") > -1)
                {
                    string[] bb = qCond.Split('_');
                    string qDuty = bb[1];
                    qCond = bb[0];
                    if (strDateType == "Daily")
                    {
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "DUTY", qDuty, "LOB", qLOB, "MODEL", qModel);
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "DUTY", qDuty, "LOB", qLOB, "MODEL", qModel);
                            dicResult.Add(bydateType + "FpyTrend", dtDay);
                        }
                    }
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "DUTY", qDuty, "LOB", qLOB, "MODEL", qModel);
                        dicResult.Add(bydateType + "FpyTrend", dtWeek);
                    }
                    else
                    {
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "DUTY", qDuty, "LOB", qLOB, "MODEL", qModel);
                        dicResult.Add(bydateType + "FpyTrend", dtMonth);
                    }
                }
                else if (qModel != "")
                {
                    if (strDateType == "Daily")
                    {
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qLine, "MODEL", qModel);
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qLine, "MODEL", qModel);
                            dicResult.Add(bydateType + "FpyTrend", dtDay);
                        }
                    }
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qLine, "MODEL", qModel);
                        dicResult.Add(bydateType + "FpyTrend", dtWeek);
                    }
                    else
                    {
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qLine, "MODEL", qModel);
                        dicResult.Add(bydateType + "FpyTrend", dtMonth);
                    }
                }
                else
                {
                    if (strDateType == "Daily")
                    {
                        dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qLine);
                        dtDayM = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "ERROR", "ME", strqType, qLine);
                        dtDayE = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "ERROR", "EE", strqType, qLine);
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                        dicResult.Add(bydateType + "FpyME", dtDayM);
                        dicResult.Add(bydateType + "FpyEE", dtDayE);
                    }
                    else if (strDateType == "Range")
                    {
                        if (qCond.IndexOf("-") > -1)
                        {
                            string[] cc = qCond.Split('-');
                            dtDay = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qLine);
                            dtDayM = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "ERROR", "ME", strqType, qLine);
                            dtDayE = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "ERROR", "EE", strqType, qLine);
                            dicResult.Add(bydateType + "FpyTrend", dtDay);
                            dicResult.Add(bydateType + "FpyME", dtDayM);
                            dicResult.Add(bydateType + "FpyEE", dtDayE);
                        }
                    }
                    else if (strDateType == "Weekly")
                    {
                        string strYear, strWeek;
                        string strCond = qCond.Replace("W", "");
                        strYear = strCond.Substring(0, 4);
                        strWeek = strCond.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        dBaseDate = sTime;
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qLine);
                        dtWeekM = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "ERROR", "ME", strqType, qLine);
                        dtWeekE = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "ERROR", "EE", strqType, qLine);
                        dicResult.Add(bydateType + "FpyTrend", dtWeek);
                        dicResult.Add(bydateType + "FpyME", dtWeekM);
                        dicResult.Add(bydateType + "FpyEE", dtWeekE);
                    }
                    else
                    {
                        dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qLine);
                        dtMonthM = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "ERROR", "ME", strqType, qLine);
                        dtMonthE = fpydeepdiveOperator.GetFpyDataTrend(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "ERROR", "EE", strqType, qLine);
                        dicResult.Add(bydateType + "FpyTrend", dtMonth);
                        dicResult.Add(bydateType + "FpyME", dtMonthM);
                        dicResult.Add(bydateType + "FpyEE", dtMonthE);
                    }
                }
            }
            else if (qType == "Dut")
            {
                DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDuty(plant, sapplant, bydateType, qCond, qLOB, qLine, "LINE");
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("catalogy", typeof(string));
                dtDuty.Columns.Add("error_rate", typeof(double));
                dtDuty.Columns.Add("percentage", typeof(double));

                if (dtDutyInfo.Rows.Count > 0)
                {
                    DataTable dtDutyItem = dtDutyInfo.DefaultView.ToTable(true, "duty_type");
                    for (int i = 0; i < dtDutyItem.Rows.Count; i++)
                    {
                        dtDuty.Rows.Add(dtDutyItem.Rows[i]["duty_type"].ToString(), 0, 0);
                    }
                    if (dtDuty.Rows.Count < 6)
                    {
                        if (dtDuty.Select("catalogy='作業'").Length == 0)
                            dtDuty.Rows.Add("作業", 0, 0);
                        if (dtDuty.Select("catalogy='PCBA LRR'").Length == 0)
                            dtDuty.Rows.Add("PCBA LRR", 0, 0);
                        if (dtDuty.Select("catalogy='材料'").Length == 0)
                            dtDuty.Rows.Add("材料", 0, 0);
                        if (dtDuty.Select("catalogy='設計'").Length == 0)
                            dtDuty.Rows.Add("設計", 0, 0);
                        if (dtDuty.Select("catalogy='NTF'").Length == 0)
                            dtDuty.Rows.Add("NTF", 0, 0);
                        if (dtDuty.Select("catalogy='其他'").Length == 0)
                            dtDuty.Rows.Add("其他", 0, 0);
                    }
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDuty.Rows.Count; i++)
                    {
                        string strType = dtDuty.Rows[i]["catalogy"].ToString();
                        double dPercent = 0;
                        if (strType == "作業" || strType == "材料" || strType == "設計")
                            dtDuty.Rows[i]["catalogy"] = strType + "不良";
                        DataRow[] dr = dtDutyInfo.Select("DUTY_TYPE='" + strType + "'");
                        if (dr.Length > 0)
                        {
                            dPercent = double.Parse(dr[0]["percents"].ToString());
                            dtDuty.Rows[i]["error_rate"] = double.Parse(dr[0]["fpy"].ToString());

                        }
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtDuty.Rows[i]["percentage"] = dAllPercent;
                    }
                }
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetDeepDiveDataLobLv3SMT(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel, string qLine, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            #region
            if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupSMT(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }
            else if (qType == "Lob")
            {
                DataTable dtLob = fpydeepdiveOperator.GetFpyDataByLobSMT(plant, sapplant, bydateType, qCond, qLine, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyLob", dtLob);
            }
            else if (qType == "Mod")
            {
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelSMT(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
            }
            else if (qType == "Dut")
            {
                DataTable dtDutyInfo = new DataTable();
                if (qLOB != "")
                {
                    if (qModel != "")
                        dtDutyInfo = fpydeepdiveOperator.GetFpyDutySMT(plant, sapplant, bydateType, qCond, qLOB, "LINE", qLine, "LOB", qLOB, "MODEL", qModel);
                    else
                        dtDutyInfo = fpydeepdiveOperator.GetFpyDutySMT(plant, sapplant, bydateType, qCond, qLOB, "LINE", qLine, "LOB", qLOB);
                }
                else
                {
                    if (qModel != "")
                        dtDutyInfo = fpydeepdiveOperator.GetFpyDutySMT(plant, sapplant, bydateType, qCond, qLOB, "LINE", qLine, "MODEL", qModel, "", "");
                    else
                        dtDutyInfo = fpydeepdiveOperator.GetFpyDutySMT(plant, sapplant, bydateType, qCond, qLOB, "LINE", qLine);
                }
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("catalogy", typeof(string));
                dtDuty.Columns.Add("error_rate", typeof(double));
                dtDuty.Columns.Add("percentage", typeof(double));

                if (dtDutyInfo.Rows.Count > 0)
                {
                    DataTable dtDutyItem = dtDutyInfo.DefaultView.ToTable(true, "duty_type");
                    for (int i = 0; i < dtDutyItem.Rows.Count; i++)
                    {
                        dtDuty.Rows.Add(dtDutyItem.Rows[i]["duty_type"].ToString(), 0, 0);
                    }
                    if (dtDuty.Rows.Count < 7)
                    {
                        if (dtDuty.Select("catalogy='置件'").Length == 0)
                            dtDuty.Rows.Add("置件", 0, 0);
                        if (dtDuty.Select("catalogy='印刷'").Length == 0)
                            dtDuty.Rows.Add("印刷", 0, 0);
                        if (dtDuty.Select("catalogy='材料'").Length == 0)
                            dtDuty.Rows.Add("材料", 0, 0);
                        if (dtDuty.Select("catalogy='制程'").Length == 0)
                            dtDuty.Rows.Add("制程", 0, 0);
                        if (dtDuty.Select("catalogy='NTF'").Length == 0)
                            dtDuty.Rows.Add("NTF", 0, 0);
                        if (dtDuty.Select("catalogy='AFTP'").Length == 0)
                            dtDuty.Rows.Add("AFTP", 0, 0);
                        if (dtDuty.Select("catalogy='BIOS'").Length == 0)
                            dtDuty.Rows.Add("BIOS", 0, 0);
                    }
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDuty.Rows.Count; i++)
                    {
                        string strType = dtDuty.Rows[i]["catalogy"].ToString();
                        double dPercent = 0;
                        if (strType == "置件" || strType == "材料" || strType == "印刷" || strType == "制程" || strType == "BIOS")
                            dtDuty.Rows[i]["catalogy"] = strType + "不良";
                        else if (strType == "NTF")
                            dtDuty.Rows[i]["catalogy"] = "複判OK";
                        else if (strType == "AFTP")
                            dtDuty.Rows[i]["catalogy"] = "組包造成";
                        DataRow[] dr = dtDutyInfo.Select("DUTY_TYPE='" + strType + "'");
                        if (dr.Length > 0)
                        {
                            dPercent = double.Parse(dr[0]["percents"].ToString());
                            dtDuty.Rows[i]["error_rate"] = double.Parse(dr[0]["fpy"].ToString());

                        }
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtDuty.Rows[i]["percentage"] = dAllPercent;
                    }
                }
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
            }
            else if (qType == "Tre")
            {
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "LINE";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    if (qModel != "")
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                    else
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        if (qModel != "")
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                        else
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qLine, "LOB", qLOB, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    if (qModel != "")
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                    else
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    if (qModel != "")
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                    else
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrendSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }

            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetDeepDiveDataLobLv3InsightSMT(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel, string qLine, string qStation, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            #region
            if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupInsightSMT(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, vOrderBy, vOrderAsc, 10);
                if (dtGroup.Rows.Count == 0)
                    dtGroup.Rows.Add("N/A", 0, 0);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }
            else if (qType == "Lob")
            {
                DataTable dtLob = fpydeepdiveOperator.GetFpyDataByLobInsightSMT(plant, sapplant, bydateType, qCond, qLine, vOrderBy, vOrderAsc, 10);
                if (dtLob.Rows.Count == 0)
                    dtLob.Rows.Add("N/A", 0, 0);
                dicResult.Add(bydateType + "FpyLob", dtLob);
            }
            else if (qType == "Mod")
            {
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelInsightSMT(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, vOrderBy, vOrderAsc, 10);
                if (dtModelFpy.Rows.Count == 0)
                    dtModelFpy.Rows.Add("N/A", 0, 0);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
            }
            else if (qType == "Tre")
            {
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "LINE";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    if (qModel != "" && qLine != "" && qStation == "")
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                    else if (qStation != "")
                    {
                        if (qLine != "")
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "STATION", qStation, strqType, qLine, "LOB", qLOB);
                        else if (qModel != "")
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "STATION", qStation, "MODEL", qModel, "LOB", qLOB);
                    }
                    else
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        if (qModel != "" && qLine != "" && qStation == "")
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                        else if (qStation != "")
                        {
                            if (qLine != "")
                                dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "STATION", qStation, strqType, qLine, "LOB", qLOB);
                            else if (qModel != "")
                                dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "STATION", qStation, "MODEL", qModel, "LOB", qLOB);
                        }
                        else
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qLine, "LOB", qLOB, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    if (qModel != "" && qLine != "" && qStation == "")
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                    else if (qStation != "")
                    {
                        if (qLine != "")
                            dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "STATION", qStation, strqType, qLine, "LOB", qLOB);
                        else if (qModel != "")
                            dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "STATION", qStation, "MODEL", qModel, "LOB", qLOB);
                    }
                    else
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    if (qModel != "" && qLine != "" && qStation == "")
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                    else if (qStation != "")
                    {
                        if (qLine != "")
                            dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "STATION", qStation, strqType, qLine, "LOB", qLOB);
                        else if (qModel != "")
                            dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "STATION", qStation, "MODEL", qModel, "LOB", qLOB);
                    }
                    else
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrendInsightSMT(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }

            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetDeepDiveDataLobLv3Insight(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel, string qLine, string qStation, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            #region
            if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupInsight(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, vOrderBy, vOrderAsc, 10);
                if (dtGroup.Rows.Count == 0)
                    dtGroup.Rows.Add("N/A", 0, 0);
                //  DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupGapInsight(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }
            else if (qType == "Gru")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGru(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, vOrderBy, vOrderAsc, false);
                if (dtGroup.Rows.Count == 0)
                    dtGroup.Rows.Add("N/A", 0, 0);
                dicResult.Add(bydateType + "FpyGroup2", dtGroup);
            }
            else if (qType == "Lob")
            {
                DataTable dtLob = fpydeepdiveOperator.GetFpyDataByLobInsight(plant, sapplant, bydateType, qCond, qLine, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyLob", dtLob);
            }
            else if (qType == "Mod")
            {
                DataTable dtModelFpy = fpydeepdiveOperator.GetFpyDataByModelInsight(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, vOrderBy, vOrderAsc, 10);
                dicResult.Add(bydateType + "FpyModel", dtModelFpy);
            }
            else if (qType == "Tre")
            {
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "LINE";

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    if (qModel != "")
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                    else if (qStation != "")
                    {
                        if (qLine != "")
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "STATIONGAP", qStation, strqType, qLine, "LOB", qLOB);
                        else if (qModel != "")
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), "STATIONGAP", qStation, "MODEL", qModel, "LOB", qLOB);
                    }
                    else
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        if (qModel != "")
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                        else if (qStation != "")
                        {
                            if (qLine != "")
                                dtDay = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "STATIONGAP", qStation, strqType, qLine, "LOB", qLOB);
                            else if (qModel != "")
                                dtDay = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], "STATIONGAP", qStation, "MODEL", qModel, "LOB", qLOB);
                        }
                        else
                            dtDay = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qLine, "LOB", qLOB, "", "");
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    if (qModel != "" && qLine != "")
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                    else if (qStation != "")
                    {
                        if (qLine != "")
                            dtWeek = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "STATIONGAP", qStation, strqType, qLine, "LOB", qLOB);
                        else if (qModel != "")
                            dtWeek = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), "STATIONGAP", qStation, "MODEL", qModel, "LOB", qLOB);
                    }
                    else
                        dtWeek = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    if (qModel != "")
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "MODEL", qModel);
                    else if (qStation != "")
                    {
                        if (qLine != "")
                            dtMonth = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "STATIONGAP", qStation, strqType, qLine, "LOB", qLOB);
                        else if (qModel != "")
                            dtMonth = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), "STATIONGAP", qStation, "MODEL", qModel, "LOB", qLOB);
                    }
                    else
                        dtMonth = fpydeepdiveOperator.GetFpyDataTrendGapInsight(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qLine, "LOB", qLOB, "", "");
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }

            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }
        public ExecutionResult GetDeepDiveDataLobLv3PcbLrr(string plant, string sapplant, string bydateType, string qCond, string qLOB, string qModel, string qLine, string qStation, string qType, string vOrderBy, string vOrderAsc)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            #region
            if (qType == "Gro")
            {
                DataTable dtGroup = fpydeepdiveOperator.GetFpyDataByGroupPcbLrr(plant, sapplant, bydateType, qCond, qLOB, "", qLine, vOrderBy, vOrderAsc);
                dicResult.Add(bydateType + "FpyGroup", dtGroup);
            }
            else if (qType == "Tre")
            {
                DateTime dBaseDate = DateTime.Now;
                DataTable dtDay = new DataTable(), dtWeek = new DataTable(), dtMonth = new DataTable();
                DataTable dtDayM = new DataTable(), dtWeekM = new DataTable(), dtMonthM = new DataTable();
                DataTable dtDayE = new DataTable(), dtWeekE = new DataTable(), dtMonthE = new DataTable();
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0];
                string strShift = aa[1];
                string strqType = "LINE";
                string qContent = qLine;
                if (qStation != "")
                {
                    strqType = "STATION";
                    qContent = qStation;
                }

                if (strDateType == "Daily")
                {
                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"), strqType, qContent);
                    dicResult.Add(bydateType + "FpyTrend", dtDay);
                }
                else if (strDateType == "Range")
                {
                    if (qCond.IndexOf("-") > -1)
                    {
                        string[] cc = qCond.Split('-');
                        dtDay = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Daily_" + strShift, cc[0], cc[1], strqType, qContent);
                        dicResult.Add(bydateType + "FpyTrend", dtDay);
                    }
                }
                else if (strDateType == "Weekly")
                {
                    string strYear, strWeek;
                    string strCond = qCond.Replace("W", "");
                    strYear = strCond.Substring(0, 4);
                    strWeek = strCond.Substring(4, 2);
                    DateTime sTime, eTime;
                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                    {
                        sTime = DateTime.Now.AddDays(-7);
                        eTime = DateTime.Now;
                    }
                    dBaseDate = sTime;
                    dtWeek = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Weekly_" + strShift, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"), strqType, qContent);
                    dicResult.Add(bydateType + "FpyTrend", dtWeek);
                }
                else
                {
                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    dtMonth = fpydeepdiveOperator.GetFpyDataTrendPcbLrr(plant, sapplant, "Monthly_" + strShift, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"), strqType, qContent);
                    dicResult.Add(bydateType + "FpyTrend", dtMonth);
                }
            }
            else if (qType == "Dut")
            {
                DataTable dtDutyInfo = new DataTable();
                if (plant.IndexOf("_IOP") > -1)
                {
                    plant = plant.Substring(0, plant.IndexOf("_IOP"));
                    dtDutyInfo = fpydeepdiveOperator.GetFpyDataDutyIOP(plant, sapplant, bydateType, qCond, qLOB, qLine, qStation);
                }
                else
                    dtDutyInfo = fpydeepdiveOperator.GetFpyDataDutyPcbLrr(plant, sapplant, bydateType, qCond, qLOB, qLine, qModel, qStation);
                DataTable dtDuty = new DataTable();
                dtDuty.Columns.Add("catalogy", typeof(string));
                dtDuty.Columns.Add("error_rate", typeof(double));
                dtDuty.Columns.Add("percentage", typeof(double));
                if (dtDutyInfo.Rows.Count > 0)
                {
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDutyInfo.Rows.Count; i++)
                    {
                        string strType = dtDutyInfo.Rows[i]["catalogy"].ToString();
                        double dPercent = 0, dLrr = 0;
                        dPercent = double.Parse(dtDutyInfo.Rows[i]["percents"].ToString());
                        dLrr = double.Parse(dtDutyInfo.Rows[i]["pcblrr"].ToString());
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtDuty.Rows.Add(dtDutyInfo.Rows[i]["catalogy"].ToString(), dLrr, dAllPercent);
                    }
                }
                else
                    dtDuty.Rows.Add("", 0, 0);
                dicResult.Add(bydateType + "FpyDuty", dtDuty);
            }
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLine(string plant, string sapplant, string bydateType, string qCond, string qProcess, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = new DataTable();
            if (bMinus)
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineMinus(plant, sapplant, bydateType, qCond, qProcess, "", vOrderBy, vOrderAsc, vRows);
            else
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLine(plant, sapplant, bydateType, qCond, qProcess, vOrderBy, vOrderAsc, vRows);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLine(string plant, string sapplant, string bydateType, string qCond, string qProcess)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLine(plant, sapplant, bydateType, qCond, qProcess, 10);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLineSMT(string plant, string sapplant, string bydateType, string qCond, string qProcess, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = new DataTable();
            if (bMinus)
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineSMTMinus(plant, sapplant, bydateType, qCond, qProcess, "", "", vOrderBy, vOrderAsc, vRows);
            else
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineSMT(plant, sapplant, bydateType, qCond, qProcess, vOrderBy, vOrderAsc, vRows);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLineSMT(string plant, string sapplant, string bydateType, string qCond, string qProcess)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineSMT(plant, sapplant, bydateType, qCond, qProcess, 10);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLineInsightSMT(string plant, string sapplant, string bydateType, string qCond, string qProcess, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = new DataTable();
            if (bMinus)
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsightSMTMinus(plant, sapplant, bydateType, qCond, qProcess, "", "", vOrderBy, vOrderAsc, vRows);
            else
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsightSMT(plant, sapplant, bydateType, qCond, qProcess, vOrderBy, vOrderAsc, vRows);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLineInsightSMT(string plant, string sapplant, string bydateType, string qCond, string qProcess)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsightSMT(plant, sapplant, bydateType, qCond, qProcess, 10);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0, 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLineInsight(string plant, string sapplant, string bydateType, string qCond, string qProcess, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = new DataTable();
            if (bMinus)
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsightMinus(plant, sapplant, bydateType, qCond, qProcess, "", "", vOrderBy, vOrderAsc, vRows);
            else
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsight(plant, sapplant, bydateType, qCond, qProcess, vOrderBy, vOrderAsc, vRows);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLineInsight(string plant, string sapplant, string bydateType, string qCond, string qProcess)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLineInsight(plant, sapplant, bydateType, qCond, qProcess, 10);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0, 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLinePcbLrr(string plant, string sapplant, string bydateType, string qCond, string qProcess, string vOrderBy, string vOrderAsc, int vRows, bool bMinus = false)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = new DataTable();
            if (bMinus)
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLinePcbLrrMinus(plant, sapplant, bydateType, qCond, qProcess, "", "", vOrderBy, vOrderAsc, vRows);
            else
                dtLineFpy = fpydeepdiveOperator.GetFpyDataByLinePcbLrr(plant, sapplant, bydateType, qCond, qProcess, vOrderBy, vOrderAsc, vRows);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public ExecutionResult GetFpyByLinePcbLrr(string plant, string sapplant, string bydateType, string qCond, string qProcess)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;

            #region
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            DataTable dtLineFpy = fpydeepdiveOperator.GetFpyDataByLinePcbLrr(plant, sapplant, bydateType, qCond, qProcess, 10);
            if (dtLineFpy.Rows.Count == 0)
                dtLineFpy.Rows.Add("N/A", 0, 0, 0);
            dicResult.Add(bydateType + "FpyLine", dtLineFpy);
            exeRes.Anything = dicResult;
            #endregion

            return exeRes;
        }

        public static int GetWeekOfYear(DateTime dt)
        {
            System.Globalization.GregorianCalendar gc = new System.Globalization.GregorianCalendar();
            int weekOfYear = gc.GetWeekOfYear(dt, System.Globalization.CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            return weekOfYear;
        }

        public string GetSapPlant(string plant)
        {
            return fpydeepdiveOperator.getSapPlant(plant);
        }

        public string GetWeekFromEnd(string strWeekCond)
        {
            string strYear, strWeek, strReuslt = "";
            strYear = strWeekCond.Substring(0, 4);
            strWeek = strWeekCond.Substring(4, 2);
            DateTime sTime, eTime;
            if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
            {
                sTime = DateTime.Now.AddDays(-7);
                eTime = DateTime.Now;
            }
            strReuslt = sTime.ToString("yyyyMMdd") + ";" + eTime.ToString("yyyyMMdd");
            return strReuslt;
        }

        public DateTime GetMondayOfDay(DateTime date)
        {
            DateTime date0 = date;
            switch (date.DayOfWeek)
            {
                case System.DayOfWeek.Monday:
                    date0 = date;
                    break;
                case System.DayOfWeek.Tuesday:
                    date0 = date.AddDays(-1);
                    break;
                case System.DayOfWeek.Wednesday:
                    date0 = date.AddDays(-2);
                    break;
                case System.DayOfWeek.Thursday:
                    date0 = date.AddDays(-3);
                    break;
                case System.DayOfWeek.Friday:
                    date0 = date.AddDays(-4);
                    break;
                case System.DayOfWeek.Saturday:
                    date0 = date.AddDays(-5);
                    break;
                case System.DayOfWeek.Sunday:
                    date0 = date.AddDays(-6);
                    break;
            }
            return date0;
        }

        public DataRow GetDataRow(DataTable dt, string filter)
        {
            foreach (DataRow dr in dt.Rows)
            {
                if (filter.Contains(dr["date"].ToString().Replace("/", "")))
                {
                    return dr;
                }
            }
            return null;
        }

        public ExecutionResult GetErrorTableByError(string plant, string sapplant, string bydateType, string qCond, string qProcess, string qError)
        {
            List<FpyDeepDiveModels> modellist = new List<FpyDeepDiveModels>();
            DataTable dtError = new DataTable(), dtError2 = new DataTable();
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();

            if (qError == "IOP")
            {
                dtError = fpydeepdiveOperator.GetFpyDataIOPPcbLrr(plant, sapplant, bydateType, qCond);
                if (dtError.Rows.Count == 0)
                    dtError.Rows.Add("N/A", 0, 0, 0);
            }
            else if (qError == "DUTY")
            {
                #region Duty Table
                DataTable dtDutyInfo = fpydeepdiveOperator.GetFpyDataDutyPcbLrr(plant, sapplant, bydateType, qCond, "");
                dtError = new DataTable();
                dtError.Columns.Add("catalogy", typeof(string));
                dtError.Columns.Add("error_rate", typeof(double));
                dtError.Columns.Add("percentage", typeof(double));
                dtError.Columns.Add("divpercentage", typeof(double));
                if (dtDutyInfo.Rows.Count > 0)
                {
                    double dAllPercent = 0;
                    for (int i = 0; i < dtDutyInfo.Rows.Count; i++)
                    {
                        string strType = dtDutyInfo.Rows[i]["catalogy"].ToString();
                        double dPercent = 0, dLrr = 0;
                        dPercent = double.Parse(dtDutyInfo.Rows[i]["percents"].ToString());
                        dLrr = double.Parse(dtDutyInfo.Rows[i]["pcblrr"].ToString());
                        dAllPercent += dPercent;
                        if (dAllPercent > 100)
                            dAllPercent = 100;
                        dtError.Rows.Add(dtDutyInfo.Rows[i]["catalogy"].ToString(), dLrr, dAllPercent, dPercent);
                    }
                }
                else
                    dtError.Rows.Add("N/A", 0, 0, 0);
                #endregion               
            }
            else
            {
                dtError = fpydeepdiveOperator.GetErrorTableExtend(plant, sapplant, bydateType, qCond, qProcess, qError);
                if (dtError.Rows.Count == 0)
                    dtError.Rows.Add("N/A", 0, 0, 0);
            }

            dicResult.Add(bydateType + "FpyDuty", dtError);
            exeRes.Anything = dicResult;

            return exeRes;
        }

        public object[] GetErrorTable(object[] objParam)
        {
            List<FpyDeepDiveModels> modellist = new List<FpyDeepDiveModels>();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qModel = "", qLine = "", qStation = "", qDuty = "";
            string categories = "", strMetType = "ALL";
            int iTop = 5;
            bool bExcel = false;
            plant = objParam[0].ToString();
            sapplant = objParam[1].ToString();
            bydateType = objParam[2].ToString();
            qCond = objParam[3].ToString();
            qLOB = objParam[4].ToString();
            qModel = objParam[5].ToString();
            if (objParam[6].ToString().IndexOf("_") > -1)
            {
                string[] bb = objParam[6].ToString().Split('_');
                if (bb[0].ToString() == "2" || bb[0].ToString() == "ME")
                    strMetType = "ME";
                else if (bb[0].ToString() == "3" || bb[0].ToString() == "EE")
                    strMetType = "EE";
                else
                    strMetType = "ALL";
                iTop = Int32.Parse(bb[1].ToString());
            }
            else
            {
                if (objParam[6].ToString() == "EXCEL")
                {
                    bExcel = true;
                    iTop = 9999;
                }
                else
                    iTop = Int32.Parse(objParam[6].ToString());
            }
            plant = plant.Replace("TW01", "CTY1");
            if (objParam.Length > 7)
                qStation = objParam[7].ToString();
            if (objParam.Length > 8)
                qLine = objParam[8].ToString();
            if (objParam.Length > 9)
                qDuty = objParam[9].ToString();
            if (iTop > 0)
            {
                DataTable dtError = new DataTable();
                string strQType = "", strQCond = "";
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0].Substring(0, 1);
                string strShift = aa[1];
                //if (qModel != "" && qLine == "")
                //    dtError = fpydeepdiveOperator.GetErrorTable(plant, sappalnt, bydateType, qCond, qLOB, qModel, "MODEL", qStation, iTop);
                //else if (qLine != "" && qModel == "")
                //    dtError = fpydeepdiveOperator.GetErrorTable(plant, sappalnt, bydateType, qCond, qLOB, qLine, "LINE", qStation, iTop);
                //else if (qLOB != "" && qLine == "" && qModel == "")
                //    dtError = fpydeepdiveOperator.GetErrorTable(plant, sappalnt, bydateType, qCond, qLOB, qLine, "LOB", qStation, iTop);
                if (qModel != "" && qLine == "")
                {
                    strQType = "MODEL";
                    strQCond = qModel;
                    if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
                    {
                        GregorianCalendar gc = new GregorianCalendar();
                        int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                        if (Int32.Parse(qCond.Substring(1)) > w)
                            qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                        else
                            qCond = dtNow.Year.ToString() + qCond.Substring(1);
                    }
                }
                else if (qLine != "" && qModel == "")
                {
                    strQType = "LINE";
                    strQCond = qLine;
                    if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
                    {
                        GregorianCalendar gc = new GregorianCalendar();
                        int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                        if (Int32.Parse(qCond.Substring(1)) > w)
                            qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                        else
                            qCond = dtNow.Year.ToString() + qCond.Substring(1);
                    }
                }
                else if (qLine != "" && qModel != "")
                {
                    strQType = "MODELLINE";
                    strQCond = qModel + ";" + qLine;
                    if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
                    {
                        GregorianCalendar gc = new GregorianCalendar();
                        int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                        if (Int32.Parse(qCond.Substring(1)) > w)
                            qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                        else
                            qCond = dtNow.Year.ToString() + qCond.Substring(1);
                    }
                }
                else if (qLine == "" && qModel == "" && qStation == "")
                {
                    strQType = "LOB";
                    strQCond = "";
                    if (strDateType == "W")
                    {
                        if (qCond.Length <= 3)
                        {
                            GregorianCalendar gc = new GregorianCalendar();
                            int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            if (Int32.Parse(qCond.Substring(1)) > w)
                                qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                            else
                                qCond = dtNow.Year.ToString() + qCond.Substring(1);
                        }
                    }
                }
                else if (qLine == "" && qModel == "" && qStation != "")
                {
                    if (strDateType == "W")
                    {
                        if (qCond.Length <= 3)
                        {
                            GregorianCalendar gc = new GregorianCalendar();
                            int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            if (Int32.Parse(qCond.Substring(1)) > w)
                                qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                            else
                                qCond = dtNow.Year.ToString() + qCond.Substring(1);
                        }
                    }
                }
                if (iTop == 9999)
                {
                    #region special                    
                    string strType = "MODEL";
                    string strLineType = "";
                    int iTops = 3;
                    if (qLine != "" && qLine.IndexOf("_") > -1)
                    {
                        string[] cc = qLine.Split('_');
                        strType = cc[0];
                        iTops = Int32.Parse(cc[1].ToString());
                        if (bExcel)
                            iTops = 0;
                        if (strType.IndexOf("-") > -1)
                        {
                            string[] dd = strType.Split('-');
                            strType = dd[0];
                            strLineType = dd[1];
                            if (strLineType == "ALL")
                                strLineType = "";
                        }
                    }
                    dtError = fpydeepdiveOperator.GetErrorTableExtendModel(plant, sapplant, bydateType, qCond, qLOB, qStation, "", strType, strLineType, iTops);
                    DataTable dtResult = new DataTable();
                    dtResult.Columns.Add("ERROR_CODE");
                    dtResult.Columns.Add("QTY");
                    dtResult.Columns.Add("FR");
                    dtResult.Columns.Add("PERCENTS");
                    dtResult.Columns.Add("W1");
                    dtResult.Columns.Add("W2");
                    dtResult.Columns.Add("W3");
                    dtResult.Columns.Add("W4");
                    dtResult.Columns.Add("LOCAL_DESC");
                    categories = GetCategories(strDateType, qCond, strQType);
                    if (dtError.Rows.Count < iTop)
                        iTop = dtError.Rows.Count;
                    if (bExcel)
                    {
                        FpyDeepDiveModels model = new FpyDeepDiveModels();
                        model.ID = "ChartE";
                        for (int i = 0; i < dtError.Rows.Count; i++)
                        {
                            DataRow dr = dtResult.NewRow();
                            dr["ERROR_CODE"] = dtError.Rows[i]["test_code"];
                            dr["LOCAL_DESC"] = dtError.Rows[i]["location_code"];
                            dr["QTY"] = dtError.Rows[i]["qty"];
                            dr["FR"] = dtError.Rows[i]["FR"];
                            dr["PERCENTS"] = dtError.Rows[i]["percents"];
                            if (categories != "")
                            {
                                for (int k = 0; k < categories.Split(';').Length; k++)
                                {
                                    DataTable dtTemp = new DataTable();
                                    if (strDateType == "R")
                                        dtTemp = fpydeepdiveOperator.GetErrorTableExtendModel(plant, sapplant, "Daily_" + strShift, categories.Split(';')[k], qLOB, qStation, dtError.Rows[i]["test_code"].ToString(), strType, strLineType);
                                    else
                                        dtTemp = fpydeepdiveOperator.GetErrorTableExtendModel(plant, sapplant, bydateType, categories.Split(';')[k], qLOB, qStation, dtError.Rows[i]["test_code"].ToString(), strType, strLineType);
                                    double dTemp = 0;
                                    if (dtTemp.Rows.Count > 0)
                                        dTemp = double.Parse(dtTemp.Rows[0]["FR"].ToString());
                                    dr["W" + (k + 1).ToString()] = dTemp.ToString() + "%";
                                }
                            }
                            dtResult.Rows.Add(dr);
                            if (dtResult.Rows.Count == 0)
                            {
                                dr = dtResult.NewRow();
                                dr["ERROR_CODE"] = "N/A";
                                dr["LOCAL_DESC"] = "N/A";
                                dr["QTY"] = 0;
                                dr["FR"] = 0;
                                dr["PERCENTS"] = 0;
                                dr["W1"] = 0;
                                dr["W2"] = 0;
                                dr["W3"] = 0;
                                dr["W4"] = 0;
                                dtResult.Rows.Add(dr);
                            }
                        }
                        model.DtError = dtResult.Copy();
                        modellist.Add(model);
                    }
                    else
                    {
                        for (int i = 0; i < dtError.Rows.Count; i++)
                        {
                            dtResult.Rows.Clear();
                            FpyDeepDiveModels model = new FpyDeepDiveModels();
                            model.ID = "ChartE" + i;
                            DataRow dr = dtResult.NewRow();
                            dr["ERROR_CODE"] = dtError.Rows[i]["test_code"];
                            dr["LOCAL_DESC"] = dtError.Rows[i]["location_code"];
                            dr["QTY"] = dtError.Rows[i]["qty"];
                            dr["FR"] = dtError.Rows[i]["FR"];
                            dr["PERCENTS"] = dtError.Rows[i]["percents"];
                            if (categories != "")
                            {
                                for (int k = 0; k < categories.Split(';').Length; k++)
                                {
                                    DataTable dtTemp = new DataTable();
                                    if (strDateType == "R")
                                        dtTemp = fpydeepdiveOperator.GetErrorTableExtendModel(plant, sapplant, "Daily_" + strShift, categories.Split(';')[k], qLOB, qStation, dtError.Rows[i]["test_code"].ToString(), strType, strLineType);
                                    else
                                        dtTemp = fpydeepdiveOperator.GetErrorTableExtendModel(plant, sapplant, bydateType, categories.Split(';')[k], qLOB, qStation, dtError.Rows[i]["test_code"].ToString(), strType, strLineType);
                                    double dTemp = 0;
                                    if (dtTemp.Rows.Count > 0)
                                        dTemp = double.Parse(dtTemp.Rows[0]["FR"].ToString());
                                    dr["W" + (k + 1).ToString()] = dTemp.ToString() + "%";
                                    model.data.Add(dTemp);
                                }
                            }
                            dtResult.Rows.Add(dr);
                            if (dtResult.Rows.Count == 0)
                            {
                                dr = dtResult.NewRow();
                                dr["ERROR_CODE"] = "N/A";
                                dr["LOCAL_DESC"] = "N/A";
                                dr["QTY"] = 0;
                                dr["FR"] = 0;
                                dr["PERCENTS"] = 0;
                                dr["W1"] = 0;
                                dr["W2"] = 0;
                                dr["W3"] = 0;
                                dr["W4"] = 0;
                                dtResult.Rows.Add(dr);
                            }
                            model.DtError = dtResult.Copy();
                            modellist.Add(model);
                        }
                    }
                    #endregion
                }
                else
                {
                    #region Normal
                    dtError = fpydeepdiveOperator.GetErrorTable(plant, sapplant, bydateType, qCond, qLOB, strQCond, strQType, qStation, iTop, "", "", strMetType, qDuty);
                    DataTable dtResult = new DataTable();
                    dtResult.Columns.Add("TOP");
                    dtResult.Columns.Add("ERROR_CODE");
                    dtResult.Columns.Add("ERROR_DESC");
                    dtResult.Columns.Add("QTY");
                    dtResult.Columns.Add("FR");
                    dtResult.Columns.Add("PERCENTS");
                    dtResult.Columns.Add("W1");
                    dtResult.Columns.Add("W2");
                    dtResult.Columns.Add("W3");
                    dtResult.Columns.Add("W4");
                    dtResult.Columns.Add("LOCAL_DESC");
                    dtResult.Columns.Add("MODEL_NAME");
                    categories = GetCategories(strDateType, qCond, strQType);
                    if (dtError.Rows.Count < iTop)
                        iTop = dtError.Rows.Count;
                    for (int i = 0; i < dtError.Rows.Count; i++)
                    {
                        dtResult.Rows.Clear();
                        FpyDeepDiveModels model = new FpyDeepDiveModels();
                        if (plant.IndexOf("PcbLrr") > -1)
                            model.ID = "ChartE" + plant + i;
                        else if (plant.IndexOf("ItemCode") > -1)
                            model.ID = "ChartEI" + i;
                        else
                            model.ID = "ChartE" + i;
                        DataRow dr = dtResult.NewRow();
                        dr["TOP"] = "TOP" + (i + 1).ToString();
                        dr["ERROR_CODE"] = dtError.Rows[i]["test_code"];
                        if (dtError.Rows[i]["error_desc"].ToString() == "NULL" || dtError.Rows[i]["error_desc"].ToString() == "")
                        {
                            string strErrorCode = dtError.Rows[i]["test_code"].ToString();
                            if (strErrorCode.Length > 3 && strErrorCode.Substring(0, 3) == "WBT")
                                dr["ERROR_DESC"] = "WBT Fail";
                            else if (strErrorCode.Length > 2 && strErrorCode.Substring(0, 2) == "FI")
                                dr["ERROR_DESC"] = "FI Fail";
                        }
                        else
                            dr["ERROR_DESC"] = dtError.Rows[i]["error_desc"];
                        dr["LOCAL_DESC"] = dtError.Rows[i]["location_code"];
                        dr["QTY"] = dtError.Rows[i]["qty"];
                        dr["FR"] = dtError.Rows[i]["FR"];
                        dr["PERCENTS"] = dtError.Rows[i]["percents"];
                        if (categories != "")
                        {
                            for (int k = 0; k < categories.Split(';').Length; k++)
                            {
                                DataTable dtTemp = new DataTable();
                                //if (strQType == "LOB")
                                if (strDateType == "R")
                                    dtTemp = fpydeepdiveOperator.GetErrorTable(plant, sapplant, "Daily_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qStation, iTop, dtError.Rows[i]["test_code"].ToString(), "", strMetType, qDuty);
                                else
                                    dtTemp = fpydeepdiveOperator.GetErrorTable(plant, sapplant, bydateType, categories.Split(';')[k], qLOB, strQCond, strQType, qStation, iTop, dtError.Rows[i]["test_code"].ToString(), "", strMetType, qDuty);
                                //else
                                //    dtTemp = fpydeepdiveOperator.GetErrorTable(plant, sappalnt, "Weekly_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qStation, iTop, dtError.Rows[i]["test_code"].ToString(), "");
                                double dTemp = 0;
                                if (dtTemp.Rows.Count > 0)
                                    dTemp = double.Parse(dtTemp.Rows[0]["FR"].ToString());
                                dr["W" + (k + 1).ToString()] = dTemp.ToString() + "%";
                                model.data.Add(dTemp);
                            }
                        }
                        if (dtError.Columns.IndexOf("MODEL_NAME") > -1)
                            dr["MODEL_NAME"] = dtError.Rows[i]["MODEL_NAME"];
                        dtResult.Rows.Add(dr);
                        //   model.categories = categories;
                        model.DtError = dtResult.Copy();
                        modellist.Add(model);
                    }
                    #endregion
                }

            }

            return new object[] { categories, modellist };
        }

        public object[] GetPartsTable(object[] objParam)
        {
            List<FpyDeepDiveModels> modellist = new List<FpyDeepDiveModels>();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qModel = "", qLine = "", qDuty = "", qStation = "", strMetType = "ALL";
            string categories = "";
            int iTop = 5;
            plant = objParam[0].ToString();
            sapplant = objParam[1].ToString();
            bydateType = objParam[2].ToString();
            qCond = objParam[3].ToString();
            qLOB = objParam[4].ToString();
            qModel = objParam[5].ToString();
            if (objParam[6].ToString().IndexOf("_") > -1)
            {
                string[] bb = objParam[6].ToString().Split('_');
                if (bb[0].ToString() == "2" || bb[0].ToString() == "ME")
                    strMetType = "ME";
                else if (bb[0].ToString() == "3" || bb[0].ToString() == "EE")
                    strMetType = "EE";
                else
                    strMetType = "ALL";
                iTop = Int32.Parse(bb[1].ToString());
            }
            else
                iTop = Int32.Parse(objParam[6].ToString());
            plant = plant.Replace("TW01", "CTY1");
            if (objParam.Length > 7)
                qDuty = objParam[7].ToString();
            if (objParam.Length > 8)
                qLine = objParam[8].ToString();
            if (objParam.Length > 9)
                qStation = objParam[9].ToString();
            if (iTop > 0)
            {
                DataTable dtError = new DataTable();
                string strQType = "", strQCond = "";
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0].Substring(0, 1);
                string strShift = aa[1];
                //if (qModel != "" && qLine == "")
                //    dtError = fpydeepdiveOperator.GetPartsTable(plant, sappalnt, bydateType, qCond, qLOB, qModel, "MODEL", qDuty, iTop);
                //else if (qLine != "" && qModel == "")
                //    dtError = fpydeepdiveOperator.GetPartsTable(plant, sappalnt, bydateType, qCond, qLOB, qLine, "LINE", qDuty, iTop);
                //else if (qLOB != "" && qLine == "" && qModel == "")
                //    dtError = fpydeepdiveOperator.GetPartsTable(plant, sappalnt, bydateType, qCond, qLOB, qLine, "LOB", qDuty, iTop);
                if (qModel != "" && qLine == "")
                {
                    strQType = "MODEL";
                    strQCond = qModel;
                    if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
                    {
                        GregorianCalendar gc = new GregorianCalendar();
                        int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                        if (Int32.Parse(qCond.Substring(1)) > w)
                            qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                        else
                            qCond = dtNow.Year.ToString() + qCond.Substring(1);
                    }
                }
                else if (qLine != "" && qModel == "")
                {
                    strQType = "LINE";
                    strQCond = qLine;
                    if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
                    {
                        if (qCond.Length <= 3)
                        {
                            GregorianCalendar gc = new GregorianCalendar();
                            int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            if (Int32.Parse(qCond.Substring(1)) > w)
                                qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                            else
                                qCond = dtNow.Year.ToString() + qCond.Substring(1);
                        }
                    }
                }
                else if (qLine != "" && qModel != "")
                {
                    strQType = "MODELLINE";
                    strQCond = qModel + ";" + qLine;
                    if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
                    {
                        GregorianCalendar gc = new GregorianCalendar();
                        int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                        if (Int32.Parse(qCond.Substring(1)) > w)
                            qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                        else
                            qCond = dtNow.Year.ToString() + qCond.Substring(1);
                    }
                }
                else if (qLine == "" && qModel == "" && qDuty == "" && qStation == "")
                {
                    strQType = "LOB";
                    strQCond = "";
                    if (strDateType == "W")
                    {
                        if (qCond.Length <= 3)
                        {
                            GregorianCalendar gc = new GregorianCalendar();
                            int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            if (Int32.Parse(qCond.Substring(1)) > w)
                                qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                            else
                                qCond = dtNow.Year.ToString() + qCond.Substring(1);
                        }
                    }
                }
                else if (qLine == "" && qModel == "" && qDuty == "" && qStation != "")
                {
                    if (strDateType == "W")
                    {
                        if (qCond.Length <= 3)
                        {
                            GregorianCalendar gc = new GregorianCalendar();
                            int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            if (Int32.Parse(qCond.Substring(1)) > w)
                                qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                            else
                                qCond = dtNow.Year.ToString() + qCond.Substring(1);
                        }
                    }
                }
                dtError = fpydeepdiveOperator.GetPartsTable(plant, sapplant, bydateType, qCond, qLOB, strQCond, strQType, qDuty, qStation, iTop, "", strMetType);
                DataTable dtResult = new DataTable();
                dtResult.Columns.Add("TOP");
                dtResult.Columns.Add("LOCAL_DESC");
                dtResult.Columns.Add("QTY");
                dtResult.Columns.Add("FR");
                dtResult.Columns.Add("PERCENTS");
                dtResult.Columns.Add("W1");
                dtResult.Columns.Add("W2");
                dtResult.Columns.Add("W3");
                dtResult.Columns.Add("W4");
                dtResult.Columns.Add("ERROR_DESC");
                categories = GetCategories(strDateType, qCond, strQType);
                if (dtError.Rows.Count < iTop)
                    iTop = dtError.Rows.Count;
                for (int i = 0; i < dtError.Rows.Count; i++)
                {
                    dtResult.Rows.Clear();
                    FpyDeepDiveModels model = new FpyDeepDiveModels();
                    model.ID = "ChartP" + i;
                    DataRow dr = dtResult.NewRow();
                    dr["TOP"] = "TOP" + (i + 1).ToString();
                    dr["LOCAL_DESC"] = dtError.Rows[i]["location_code"];
                    dr["QTY"] = dtError.Rows[i]["qty"];
                    dr["FR"] = dtError.Rows[i]["FR"];
                    dr["PERCENTS"] = dtError.Rows[i]["percents"];
                    dr["ERROR_DESC"] = dtError.Rows[i]["error_desc"];
                    if (categories != "")
                    {
                        for (int k = 0; k < categories.Split(';').Length; k++)
                        {
                            DataTable dtTemp = new DataTable();
                            //if (strQType == "LOB")
                            if (strDateType == "R")
                                dtTemp = fpydeepdiveOperator.GetPartsTable(plant, sapplant, "Daily_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qDuty, qStation, iTop, dtError.Rows[i]["location_code"].ToString(), strMetType);
                            else
                                dtTemp = fpydeepdiveOperator.GetPartsTable(plant, sapplant, bydateType, categories.Split(';')[k], qLOB, strQCond, strQType, qDuty, qStation, iTop, dtError.Rows[i]["location_code"].ToString(), strMetType);
                            //else
                            //    dtTemp = fpydeepdiveOperator.GetPartsTable(plant, sappalnt, "Weekly_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qDuty, qStation, iTop, dtError.Rows[i]["location_code"].ToString());
                            double dTemp = 0;
                            if (dtTemp.Rows.Count > 0)
                                dTemp = double.Parse(dtTemp.Rows[0]["FR"].ToString());
                            dr["W" + (k + 1).ToString()] = dTemp.ToString() + "%";
                            model.data.Add(dTemp);
                        }
                    }
                    dtResult.Rows.Add(dr);
                    //   model.categories = categories;
                    model.DtError = dtResult.Copy();
                    modellist.Add(model);
                }
            }

            return new object[] { categories, modellist };
        }

        public object[] GetComponentTable(object[] objParam)
        {
            List<FpyDeepDiveModels> modellist = new List<FpyDeepDiveModels>();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qModel = "", qLine = "", qDuty = "", qStation = "";
            string categories = "";
            int iTop = 5;
            plant = objParam[0].ToString();
            sapplant = objParam[1].ToString();
            bydateType = objParam[2].ToString();
            qCond = objParam[3].ToString();
            qLOB = objParam[4].ToString();
            qModel = objParam[5].ToString();
            iTop = Int32.Parse(objParam[6].ToString());
            plant = plant.Replace("TW01", "CTY1");
            if (objParam.Length > 7)
                qDuty = objParam[7].ToString();
            if (objParam.Length > 8)
                qLine = objParam[8].ToString();
            if (objParam.Length > 9)
                qStation = objParam[9].ToString();
            if (iTop > 0)
            {
                DataTable dtError = new DataTable();
                string strQType = "", strQCond = "";
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0].Substring(0, 1);
                string strShift = aa[1];
                //if (qModel != "" && qLine == "")
                //    dtError = fpydeepdiveOperator.GetPartsTable(plant, sappalnt, bydateType, qCond, qLOB, qModel, "MODEL", qDuty, iTop);
                //else if (qLine != "" && qModel == "")
                //    dtError = fpydeepdiveOperator.GetPartsTable(plant, sappalnt, bydateType, qCond, qLOB, qLine, "LINE", qDuty, iTop);
                //else if (qLOB != "" && qLine == "" && qModel == "")
                //    dtError = fpydeepdiveOperator.GetPartsTable(plant, sappalnt, bydateType, qCond, qLOB, qLine, "LOB", qDuty, iTop);
                if (qModel != "" && qLine == "")
                {
                    strQType = "MODEL";
                    strQCond = qModel;
                }
                else if (qLine != "" && qModel == "")
                {
                    strQType = "LINE";
                    strQCond = qLine;
                    if (qCond.IndexOf("W") > -1 && qCond.Length == 3)
                    {
                        if (qCond.Length <= 3)
                        {
                            GregorianCalendar gc = new GregorianCalendar();
                            int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            if (Int32.Parse(qCond.Substring(1)) > w)
                                qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                            else
                                qCond = dtNow.Year.ToString() + qCond.Substring(1);
                        }
                    }
                }
                else if (qLine != "" && qModel != "")
                {
                    strQType = "MODELLINE";
                    strQCond = qModel + ";" + qLine;
                }
                else if (qLine == "" && qModel == "" && qDuty == "" && qStation == "")
                {
                    strQType = "LOB";
                    strQCond = "";
                    if (strDateType == "W")
                    {
                        if (qCond.Length <= 3)
                        {
                            GregorianCalendar gc = new GregorianCalendar();
                            int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            if (Int32.Parse(qCond.Substring(1)) > w)
                                qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                            else
                                qCond = dtNow.Year.ToString() + qCond.Substring(1);
                        }
                    }
                }
                else if (qLine == "" && qModel == "" && qDuty == "" && qStation != "")
                {
                    if (strDateType == "W")
                    {
                        if (qCond.Length <= 3)
                        {
                            GregorianCalendar gc = new GregorianCalendar();
                            int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            if (Int32.Parse(qCond.Substring(1)) > w)
                                qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                            else
                                qCond = dtNow.Year.ToString() + qCond.Substring(1);
                        }
                    }
                }
                dtError = fpydeepdiveOperator.GetComponentTable(plant, sapplant, bydateType, qCond, qLOB, strQCond, strQType, qDuty, qStation, iTop);
                DataTable dtResult = new DataTable();
                dtResult.Columns.Add("TOP");
                dtResult.Columns.Add("LOCAL_DESC");
                dtResult.Columns.Add("QTY");
                dtResult.Columns.Add("FR");
                dtResult.Columns.Add("PERCENTS");
                dtResult.Columns.Add("W1");
                dtResult.Columns.Add("W2");
                dtResult.Columns.Add("W3");
                dtResult.Columns.Add("W4");
                dtResult.Columns.Add("ERROR_DESC");
                categories = GetCategories(strDateType, qCond, strQType);
                if (dtError.Rows.Count < iTop)
                    iTop = dtError.Rows.Count;
                for (int i = 0; i < dtError.Rows.Count; i++)
                {
                    dtResult.Rows.Clear();
                    FpyDeepDiveModels model = new FpyDeepDiveModels();
                    model.ID = "ChartC" + i;
                    DataRow dr = dtResult.NewRow();
                    dr["TOP"] = "TOP" + (i + 1).ToString();
                    dr["LOCAL_DESC"] = dtError.Rows[i]["location_code"];
                    dr["QTY"] = dtError.Rows[i]["qty"];
                    dr["FR"] = dtError.Rows[i]["FR"];
                    dr["PERCENTS"] = dtError.Rows[i]["percents"];
                    dr["ERROR_DESC"] = dtError.Rows[i]["error_desc"];
                    if (categories != "")
                    {
                        for (int k = 0; k < categories.Split(';').Length; k++)
                        {
                            DataTable dtTemp = new DataTable();
                            if (strDateType == "R")
                                dtTemp = fpydeepdiveOperator.GetComponentTable(plant, sapplant, "Daily_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qDuty, qStation, iTop, dtError.Rows[i]["location_code"].ToString());
                            else
                                dtTemp = fpydeepdiveOperator.GetComponentTable(plant, sapplant, bydateType, categories.Split(';')[k], qLOB, strQCond, strQType, qDuty, qStation, iTop, dtError.Rows[i]["location_code"].ToString());
                            //   else
                            //        dtTemp = fpydeepdiveOperator.GetComponentTable(plant, sappalnt, "Weekly_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qDuty, qStation, iTop, dtError.Rows[i]["location_code"].ToString());
                            double dTemp = 0;
                            if (dtTemp.Rows.Count > 0)
                                dTemp = double.Parse(dtTemp.Rows[0]["FR"].ToString());
                            dr["W" + (k + 1).ToString()] = dTemp.ToString() + "%";
                            model.data.Add(dTemp);
                        }
                    }
                    dtResult.Rows.Add(dr);
                    //   model.categories = categories;
                    model.DtError = dtResult.Copy();
                    modellist.Add(model);
                }
            }

            return new object[] { categories, modellist };
        }

        public object[] GetHeadSlotTable(object[] objParam)
        {
            List<FpyDeepDiveModels> modellist = new List<FpyDeepDiveModels>();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qModel = "", qLine = "", qStation = "", qType = "";
            string categories = "";
            int iTop = 5;
            plant = objParam[0].ToString();
            sapplant = objParam[1].ToString();
            bydateType = objParam[2].ToString();
            qCond = objParam[3].ToString();
            qLOB = objParam[4].ToString();
            qModel = objParam[5].ToString();
            iTop = Int32.Parse(objParam[6].ToString());

            plant = plant.Replace("TW01", "CTY1");
            if (objParam.Length > 7)
                qStation = objParam[7].ToString();
            if (objParam.Length > 8)
                qLine = objParam[8].ToString();
            if (objParam.Length > 9)
                qType = objParam[9].ToString();
            if (iTop > 0)
            {
                DataTable dtError = new DataTable();
                string strQType = "", strQCond = "";
                string[] aa = bydateType.Split('_');
                string strDateType = aa[0].Substring(0, 1);
                string strShift = aa[1];
                if (qModel != "" && qLine == "")
                {
                    strQType = "MODEL";
                    strQCond = qModel;
                }
                else if (qLine != "" && qModel == "")
                {
                    strQType = "LINE";
                    strQCond = qLine;
                }
                else if (qLine != "" && qModel != "")
                {
                    strQType = "MODELLINE";
                    strQCond = qModel + ";" + qLine;
                }
                else if (qLine == "" && qModel == "" && qStation == "")
                {
                    strQType = "LOB";
                    strQCond = "";
                    if (strDateType == "W")
                    {
                        GregorianCalendar gc = new GregorianCalendar();
                        int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                        if (Int32.Parse(qCond.Substring(1)) > w)
                            qCond = dtNow.AddYears(-1).Year.ToString() + qCond.Substring(1);
                        else
                            qCond = dtNow.Year.ToString() + qCond.Substring(1);
                    }
                }
                if (qType == "H")
                    dtError = fpydeepdiveOperator.GetHeadTable(plant, sapplant, bydateType, qCond, qLOB, strQCond, strQType, qStation, iTop);
                else if (qType == "S")
                    dtError = fpydeepdiveOperator.GetSlotTable(plant, sapplant, bydateType, qCond, qLOB, strQCond, strQType, qStation, iTop);
                DataTable dtResult = new DataTable();
                dtResult.Columns.Add("TOP");
                dtResult.Columns.Add("HS");
                dtResult.Columns.Add("QTY");
                dtResult.Columns.Add("FR");
                dtResult.Columns.Add("PERCENTS");
                dtResult.Columns.Add("W1");
                dtResult.Columns.Add("W2");
                dtResult.Columns.Add("W3");
                dtResult.Columns.Add("W4");
                categories = GetCategories(strDateType, qCond, strQType);
                if (dtError.Rows.Count < iTop)
                    iTop = dtError.Rows.Count;
                for (int i = 0; i < dtError.Rows.Count; i++)
                {
                    dtResult.Rows.Clear();
                    FpyDeepDiveModels model = new FpyDeepDiveModels();
                    model.ID = "Chart" + qType + i;
                    DataRow dr = dtResult.NewRow();
                    dr["TOP"] = "TOP" + (i + 1).ToString();
                    dr["HS"] = dtError.Rows[i]["hs"];
                    dr["QTY"] = dtError.Rows[i]["qty"];
                    dr["FR"] = dtError.Rows[i]["FR"];
                    dr["PERCENTS"] = dtError.Rows[i]["percents"];
                    if (categories != "")
                    {
                        for (int k = 0; k < categories.Split(';').Length; k++)
                        {
                            DataTable dtTemp = new DataTable();
                            //   if (strQType == "LOB")
                            //    {
                            if (qType == "H")
                            {
                                if (strDateType == "R")
                                    dtTemp = fpydeepdiveOperator.GetHeadTable(plant, sapplant, "Daily_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qStation, iTop, "", "", dtError.Rows[i]["hs"].ToString());
                                else
                                    dtTemp = fpydeepdiveOperator.GetHeadTable(plant, sapplant, bydateType, categories.Split(';')[k], qLOB, strQCond, strQType, qStation, iTop, "", "", dtError.Rows[i]["hs"].ToString());
                            }
                            else if (qType == "S")
                            {
                                if (strDateType == "R")
                                    dtTemp = fpydeepdiveOperator.GetSlotTable(plant, sapplant, "Daily_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qStation, iTop, "", "", dtError.Rows[i]["hs"].ToString());
                                else
                                    dtTemp = fpydeepdiveOperator.GetSlotTable(plant, sapplant, bydateType, categories.Split(';')[k], qLOB, strQCond, strQType, qStation, iTop, "", "", dtError.Rows[i]["hs"].ToString());
                            }
                            //      }
                            //       else
                            //        {
                            //             if (qType == "H")
                            //                  dtTemp = fpydeepdiveOperator.GetHeadTable(plant, sappalnt, "Weekly_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qStation, iTop, "", "", dtError.Rows[i]["hs"].ToString());
                            //              else if (qType == "S")
                            //                  dtTemp = fpydeepdiveOperator.GetSlotTable(plant, sappalnt, "Weekly_" + strShift, categories.Split(';')[k], qLOB, strQCond, strQType, qStation, iTop, "", "", dtError.Rows[i]["hs"].ToString());
                            //        }
                            double dTemp = 0;
                            if (dtTemp.Rows.Count > 0)
                                dTemp = double.Parse(dtTemp.Rows[0]["FR"].ToString());
                            dr["W" + (k + 1).ToString()] = dTemp.ToString() + "%";
                            model.data.Add(dTemp);
                        }
                    }
                    dtResult.Rows.Add(dr);
                    //   model.categories = categories;
                    model.DtError = dtResult.Copy();
                    modellist.Add(model);
                }
            }

            return new object[] { categories, modellist };
        }

        public ExecutionResult GetPidData(object[] objParam)
        {
            ExecutionResult exeRes = new ExecutionResult();
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            string[] strMtype = { "ME Parts", "Key Parts", "LCD", "N/A" };
            string[] strPlant = { "KSP2", "KSP3", "KSP4", "CDP1", "CQP1", "CTY1", "VNP1" };
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLV = "", qMType = "", qPtype = "", qSubType = "", qSubValue = "", qPhotoType = "";


            int iTop = 0;
            if (objParam.Length >= 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLV = objParam[4].ToString();
                qMType = objParam[5].ToString();
                qPtype = objParam[6].ToString();
                qSubType = objParam[7].ToString();
                qSubValue = objParam[8].ToString();
                qPhotoType = objParam[9].ToString();
                iTop = Int32.Parse(objParam[10].ToString());
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    switch (qLV)
                    {
                        case "INDEX":
                            DataTable dtMP = new DataTable();
                            DataTable dtNonMP = new DataTable();
                            dtMP = fpydeepdiveOperator.GetPidIndexData(plant, sapplant, bydateType, qCond, qMType);
                            if (qMType == "TOTAL")
                            {
                                dtNonMP = fpydeepdiveOperator.GetPidIndexData(plant, sapplant, bydateType, qCond, qMType, false);
                                DataTable dtTemp = new DataTable();
                                dtTemp.Columns.Add("catalogy");
                                dtTemp.Columns.Add("MP", typeof(double));
                                dtTemp.Columns.Add("None MP", typeof(double));
                                dtTemp.Columns.Add("Total", typeof(double));
                                for (int i = 0; i < strMtype.Length; i++)
                                {
                                    string strType = strMtype[i].ToString();
                                    double iMP = 0, iNonMP = 0;
                                    DataRow[] drMP = dtMP.Select("catalogy='" + strType + "'");
                                    DataRow[] drNonMP = dtNonMP.Select("catalogy='" + strType + "'");
                                    if (drMP.Length > 0)
                                        iMP = double.Parse(drMP[0]["DPPM"].ToString());
                                    if (drNonMP.Length > 0)
                                        iNonMP = double.Parse(drNonMP[0]["DPPM"].ToString());
                                    if (iMP > 0 || iNonMP > 0)
                                        dtTemp.Rows.Add(strType, iMP, iNonMP, iMP + iNonMP);
                                }
                                dtMP = dtTemp;
                            }
                            dicResult.Add(bydateType + qMType, dtMP);
                            exeRes.Anything = dicResult;
                            break;
                        case "LV2":
                            switch (qPhotoType)
                            {
                                case "CrossPlant":
                                    #region Cross Plant
                                    DataTable dtCross = new DataTable();
                                    dtCross.Columns.Add("PLANT_CODE");
                                    dtCross.Columns.Add("DATE_CODE");
                                    dtCross.Columns.Add("DPPM");
                                    string[] aa = bydateType.Split('_');
                                    string strDateType = aa[0].Substring(0, 1);
                                    string strShift = aa[1];
                                    string strCata = "";
                                    bool bIsMP = true;
                                    strCata = GetCategories(strDateType, qCond, "", 6);
                                    string[] Cata = strCata.Split(';');
                                    for (int i = 0; i < Cata.Length; i++)
                                    {
                                        DataTable dtTemp = new DataTable();
                                        if (qMType == "None MP" || qMType == "MP")
                                        {
                                            if (qMType == "None MP")
                                                bIsMP = false;
                                            dtTemp = fpydeepdiveOperator.GetPidCrossData("", bydateType, Cata[i], "TOTAL", qPtype, "", "", bIsMP);
                                        }
                                        else
                                            dtTemp = fpydeepdiveOperator.GetPidCrossData("", bydateType, Cata[i], qMType, qPtype, "", "");
                                        if (dtTemp.Rows.Count > 0)
                                        {
                                            for (int j = 0; j < strPlant.Length; j++)
                                            {
                                                string strPlantCode = strPlant[j];
                                                DataRow dr = dtCross.NewRow();
                                                int iQty = 0;
                                                DataRow[] drQty = dtTemp.Select("plant_code='" + strPlantCode + "'");
                                                if (drQty.Length > 0)
                                                    iQty = Int32.Parse(drQty[0]["DPPM"].ToString());
                                                if (strPlantCode == "CDP1")
                                                    dr["PLANT_CODE"] = "CD";
                                                else if (strPlantCode == "CQP1")
                                                    dr["PLANT_CODE"] = "CQ";
                                                else if (strPlantCode == "VNP1")
                                                    dr["PLANT_CODE"] = "CHV";
                                                else if (strPlantCode == "CTY1")
                                                    dr["PLANT_CODE"] = "CTY";
                                                else
                                                    dr["PLANT_CODE"] = strPlantCode;
                                                dr["DATE_CODE"] = Cata[i].Substring(4);
                                                dr["DPPM"] = iQty;
                                                dtCross.Rows.Add(dr);
                                            }
                                        }
                                    }
                                    dicResult.Add(bydateType + qMType + qPhotoType, dtCross);
                                    exeRes.Anything = dicResult;
                                    break;
                                #endregion
                                case "TrendChart":
                                    #region Trend Chart
                                    DataTable dtChart = new DataTable();
                                    dtChart.Columns.Add("catalogy");
                                    dtChart.Columns.Add("INPUT", typeof(double));
                                    dtChart.Columns.Add("DPPM", typeof(double));
                                    aa = bydateType.Split('_');
                                    strDateType = aa[0].Substring(0, 1);
                                    strShift = aa[1];
                                    strCata = "";
                                    bIsMP = true;
                                    strCata = GetCategories(strDateType, qCond, "", 6);
                                    Cata = strCata.Split(';');
                                    for (int i = 0; i < Cata.Length; i++)
                                    {
                                        DataTable dtTemp = new DataTable();
                                        if (qMType == "None MP" || qMType == "MP")
                                        {
                                            if (qMType == "None MP")
                                                bIsMP = false;
                                            if (strDateType == "R")
                                                dtTemp = fpydeepdiveOperator.GetPidTrenChartData(plant, sapplant, "Daily_" + strShift, Cata[i], "TOTAL", qPtype, "", "", bIsMP);
                                            else
                                                dtTemp = fpydeepdiveOperator.GetPidTrenChartData(plant, sapplant, bydateType, Cata[i], "TOTAL", qPtype, "", "", bIsMP);
                                        }
                                        else
                                        {
                                            if (strDateType == "R")
                                                dtTemp = fpydeepdiveOperator.GetPidTrenChartData(plant, sapplant, "Daily_" + strShift, Cata[i], qMType, qPtype, "", "");
                                            else
                                                dtTemp = fpydeepdiveOperator.GetPidTrenChartData(plant, sapplant, bydateType, Cata[i], qMType, qPtype, "", "");
                                        }
                                        if (dtTemp.Rows.Count > 0)
                                        {
                                            DataRow dr = dtChart.NewRow();
                                            dr["catalogy"] = Cata[i].Substring(4);
                                            dr["INPUT"] = double.Parse(dtTemp.Rows[0]["QTY"].ToString());
                                            dr["DPPM"] = double.Parse(dtTemp.Rows[0]["DPPM"].ToString());
                                            dtChart.Rows.Add(dr);
                                        }
                                    }
                                    dicResult.Add(bydateType + qMType + qPhotoType, dtChart);
                                    exeRes.Anything = dicResult;
                                    break;
                                #endregion
                                default:
                                    DataTable dtInfo = new DataTable(), dtAll = new DataTable();
                                    bIsMP = true;
                                    if (qMType == "None MP" || qMType == "MP")
                                    {
                                        if (qMType == "None MP")
                                            bIsMP = false;
                                        dtInfo = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, "TOTAL", qPtype, qSubType, qSubValue, bIsMP);
                                    }
                                    else
                                        dtInfo = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue);
                                    //   dtAll = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, qMType, qPtype, "");

                                    //     dtAll.Merge(dtInfo);
                                    dicResult.Add(bydateType + qMType + qSubType + qPhotoType, dtInfo);
                                    exeRes.Anything = dicResult;
                                    break;
                            }
                            break;
                        case "LV3":
                            switch (qPhotoType)
                            {
                                case "CrossPlant":
                                    #region Cross Plant
                                    DataTable dtCross = new DataTable();
                                    dtCross.Columns.Add("PLANT_CODE");
                                    dtCross.Columns.Add("DATE_CODE");
                                    dtCross.Columns.Add("DPPM");
                                    string[] aa = bydateType.Split('_');
                                    string strDateType = aa[0].Substring(0, 1);
                                    string strShift = aa[1];
                                    string strCata = "";
                                    bool bIsMP = true;
                                    strCata = GetCategories(strDateType, qCond, "", 6);
                                    string[] Cata = strCata.Split(';');
                                    for (int i = 0; i < Cata.Length; i++)
                                    {
                                        DataTable dtTemp = new DataTable();
                                        if (qMType == "None MP" || qMType == "MP")
                                        {
                                            if (qMType == "None MP")
                                                bIsMP = false;
                                            dtTemp = fpydeepdiveOperator.GetPidCrossData(sapplant, bydateType, Cata[i], "TOTAL", qPtype, qSubType, qSubValue, bIsMP);
                                        }
                                        else
                                            dtTemp = fpydeepdiveOperator.GetPidCrossData(sapplant, bydateType, Cata[i], qMType, qPtype, qSubType, qSubValue);
                                        if (dtTemp.Rows.Count > 0)
                                        {
                                            for (int j = 0; j < dtTemp.Rows.Count; j++)
                                            {
                                                DataRow dr = dtCross.NewRow();
                                                dr["PLANT_CODE"] = dtTemp.Rows[j]["plant_code"].ToString();
                                                dr["DATE_CODE"] = Cata[i].Substring(4);
                                                dr["DPPM"] = Int32.Parse(dtTemp.Rows[j]["DPPM"].ToString());
                                                dtCross.Rows.Add(dr);
                                            }
                                            //for (int j = 0; j < strPlant.Length; j++)
                                            //{
                                            //    string strPlantCode = strPlant[j];
                                            //    DataRow dr = dtCross.NewRow();
                                            //    int iQty = 0;
                                            //    DataRow[] drQty = dtTemp.Select("plant_code='" + strPlantCode + "'");
                                            //    if (drQty.Length > 0)
                                            //        iQty = Int32.Parse(drQty[0]["QTY"].ToString());
                                            //    if (strPlantCode == "CDP1")
                                            //        dr["PLANT_CODE"] = "CD";
                                            //    else if (strPlantCode == "CQP1")
                                            //        dr["PLANT_CODE"] = "CQ";
                                            //    else if (strPlantCode == "VNP1")
                                            //        dr["PLANT_CODE"] = "CHV";
                                            //    else if (strPlantCode == "CTY1")
                                            //        dr["PLANT_CODE"] = "CTY";
                                            //    else
                                            //        dr["PLANT_CODE"] = strPlantCode;
                                            //    dr["DATE_CODE"] = Cata[i].Substring(4);
                                            //    dr["QTY"] = iQty;
                                            //    dtCross.Rows.Add(dr);
                                            //}
                                        }
                                    }
                                    dicResult.Add(bydateType + qMType + qPhotoType, dtCross);
                                    exeRes.Anything = dicResult;
                                    break;
                                #endregion
                                case "TrendChart":
                                    #region Trend Chart
                                    DataTable dtChart = new DataTable();
                                    dtChart.Columns.Add("catalogy");
                                    dtChart.Columns.Add("INPUT", typeof(double));
                                    dtChart.Columns.Add("DPPM", typeof(double));
                                    aa = bydateType.Split('_');
                                    strDateType = aa[0].Substring(0, 1);
                                    strShift = aa[1];
                                    strCata = "";
                                    bIsMP = true;
                                    strCata = GetCategories(strDateType, qCond, "", 6);
                                    Cata = strCata.Split(';');
                                    for (int i = 0; i < Cata.Length; i++)
                                    {
                                        DataTable dtTemp = new DataTable();
                                        if (qMType == "None MP" || qMType == "MP")
                                        {
                                            if (qMType == "None MP")
                                                bIsMP = false;
                                            if (strDateType == "R")
                                                dtTemp = fpydeepdiveOperator.GetPidTrenChartData(plant, sapplant, "Daily_" + strShift, Cata[i], "TOTAL", qPtype, qSubType, qSubValue, bIsMP);
                                            else
                                                dtTemp = fpydeepdiveOperator.GetPidTrenChartData(plant, sapplant, bydateType, Cata[i], "TOTAL", qPtype, qSubType, qSubValue, bIsMP);
                                        }
                                        else
                                        {
                                            if (strDateType == "R")
                                                dtTemp = fpydeepdiveOperator.GetPidTrenChartData(plant, sapplant, "Daily_" + strShift, Cata[i], qMType, qPtype, qSubType, qSubValue);
                                            else
                                                dtTemp = fpydeepdiveOperator.GetPidTrenChartData(plant, sapplant, bydateType, Cata[i], qMType, qPtype, qSubType, qSubValue);
                                        }
                                        if (dtTemp.Rows.Count > 0)
                                        {
                                            DataRow dr = dtChart.NewRow();
                                            dr["catalogy"] = Cata[i].Substring(4);
                                            dr["INPUT"] = double.Parse(dtTemp.Rows[0]["QTY"].ToString());
                                            dr["DPPM"] = double.Parse(dtTemp.Rows[0]["DPPM"].ToString());
                                            dtChart.Rows.Add(dr);
                                        }
                                    }
                                    dicResult.Add(bydateType + qMType + qPhotoType, dtChart);
                                    exeRes.Anything = dicResult;
                                    break;
                                #endregion
                                case "LineCrossPlant":
                                    #region Line CrossPlant
                                    DataTable dtLineCross = new DataTable();
                                    dtLineCross.Columns.Add("PLANT_CODE");
                                    dtLineCross.Columns.Add("DATE_CODE");
                                    dtLineCross.Columns.Add("DPPM");
                                    aa = bydateType.Split('_');
                                    strDateType = aa[0].Substring(0, 1);
                                    strShift = aa[1];
                                    strCata = "";
                                    bIsMP = true;
                                    strCata = GetCategories(strDateType, qCond, "", 6);
                                    Cata = strCata.Split(';');
                                    for (int i = 0; i < Cata.Length; i++)
                                    {
                                        DataTable dtTemp = new DataTable();
                                        if (qMType == "None MP" || qMType == "MP")
                                        {
                                            if (qMType == "None MP")
                                                bIsMP = false;
                                            dtTemp = fpydeepdiveOperator.GetPidCrossData(sapplant, bydateType, Cata[i], "TOTAL", qPtype, qSubType + "LINE", qSubValue, bIsMP);
                                        }
                                        else
                                            dtTemp = fpydeepdiveOperator.GetPidCrossData(sapplant, bydateType, Cata[i], qMType, qPtype, qSubType + "LINE", qSubValue);

                                        if (dtTemp.Rows.Count > 0)
                                        {
                                            for (int j = 0; j < dtTemp.Rows.Count; j++)
                                            {
                                                DataRow dr = dtLineCross.NewRow();
                                                dr["PLANT_CODE"] = dtTemp.Rows[j]["plant_code"].ToString();
                                                dr["DATE_CODE"] = Cata[i].Substring(4);
                                                dr["DPPM"] = Int32.Parse(dtTemp.Rows[j]["DPPM"].ToString());
                                                dtLineCross.Rows.Add(dr);
                                            }
                                        }
                                    }
                                    dicResult.Add(bydateType + qMType + qPhotoType, dtLineCross);
                                    exeRes.Anything = dicResult;
                                    break;
                                #endregion
                                case "ErrorTable":
                                    #region Error
                                    List<FpyDeepDiveModels> modellist = new List<FpyDeepDiveModels>();
                                    aa = bydateType.Split('_');
                                    strDateType = aa[0].Substring(0, 1);
                                    strShift = aa[1];
                                    bIsMP = true;
                                    DataTable dtError = new DataTable();
                                    if (qMType == "None MP" || qMType == "MP")
                                    {
                                        if (qMType == "None MP")
                                            bIsMP = false;
                                        dtError = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, "TOTAL", qPtype, qSubType, qSubValue, "REPAIR", "", bIsMP);
                                    }
                                    else
                                        dtError = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue, "REPAIR");
                                    DataTable dtResult = new DataTable();
                                    dtResult.Columns.Add("TOP");
                                    dtResult.Columns.Add("ERROR_DESC");
                                    dtResult.Columns.Add("QTY");
                                    dtResult.Columns.Add("DPPM");
                                    dtResult.Columns.Add("PERCENTS");
                                    dtResult.Columns.Add("W1");
                                    dtResult.Columns.Add("W2");
                                    dtResult.Columns.Add("W3");
                                    dtResult.Columns.Add("W4");
                                    dtResult.Columns.Add("TOP1");
                                    dtResult.Columns.Add("TOPDPPM");
                                    string categories = GetCategories(strDateType, qCond, "");
                                    if (dtError.Rows.Count < iTop)
                                        iTop = dtError.Rows.Count;
                                    double dPercent = 0;
                                    for (int i = 0; i < iTop; i++)
                                    {
                                        //   dPercent += double.Parse(dtError.Rows[i]["percents"].ToString());
                                        dtResult.Rows.Clear();
                                        FpyDeepDiveModels model = new FpyDeepDiveModels();
                                        model.ID = "ChartE" + i;
                                        DataRow dr = dtResult.NewRow();
                                        dr["TOP"] = "TOP" + (i + 1).ToString();
                                        dr["ERROR_DESC"] = dtError.Rows[i]["catalogy"];
                                        dr["QTY"] = dtError.Rows[i]["PartQty"];
                                        dr["DPPM"] = dtError.Rows[i]["DPPM"];
                                        dr["PERCENTS"] = double.Parse(dtError.Rows[i]["percents"].ToString());
                                        //  dr["PERCENTS"] = dPercent;
                                        if (categories != "")
                                        {
                                            for (int k = 0; k < categories.Split(';').Length; k++)
                                            {
                                                DataTable dtTemp = new DataTable();
                                                if (qMType == "None MP" || qMType == "MP")
                                                    dtTemp = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, categories.Split(';')[k], "TOTAL", qPtype, qSubType, qSubValue, "REPAIR", dtError.Rows[i]["catalogy"].ToString(), bIsMP);
                                                else
                                                    dtTemp = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, categories.Split(';')[k], qMType, qPtype, qSubType, qSubValue, "REPAIR", dtError.Rows[i]["catalogy"].ToString());
                                                double dTemp = 0;
                                                if (dtTemp.Rows.Count > 0)
                                                    dTemp = double.Parse(dtTemp.Rows[0]["DPPM"].ToString());
                                                dr["W" + (k + 1).ToString()] = dTemp.ToString();
                                                model.data.Add(dTemp);
                                            }
                                        }
                                        DataTable dtTop = new DataTable();
                                        if (qMType == "None MP" || qMType == "MP")
                                            dtTop = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, "TOTAL", qPtype, qSubType, qSubValue, "TOP", dtError.Rows[i]["catalogy"].ToString(), bIsMP);
                                        else
                                            dtTop = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue, "TOP", dtError.Rows[i]["catalogy"].ToString());
                                        if (dtTop.Rows.Count > 0)
                                        {
                                            dr["TOP1"] = dtTop.Rows[0]["catalogy"].ToString();
                                            dr["TOPDPPM"] = dtTop.Rows[0]["DPPM"].ToString();
                                        }
                                        dtResult.Rows.Add(dr);
                                        //   model.categories = categories;
                                        model.DtError = dtResult.Copy();
                                        modellist.Add(model);
                                    }
                                    exeRes.Anything = new object[] { categories, modellist };
                                    break;
                                #endregion
                                default:
                                    DataTable dtInfo = new DataTable(), dtAll = new DataTable();
                                    //   dtAll = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, qMType, qPtype, "");
                                    bIsMP = true;
                                    if (qMType == "None MP" || qMType == "MP")
                                    {
                                        if (qMType == "None MP")
                                            bIsMP = false;
                                        dtInfo = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, "TOTAL", qPtype, qSubType, qSubValue, qPhotoType, "", bIsMP);
                                    }
                                    else
                                        dtInfo = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue, qPhotoType);
                                    if (dtInfo.Rows.Count > 0)
                                    {
                                        dPercent = 0;
                                        for (int i = 0; i < dtInfo.Rows.Count; i++)
                                        {
                                            dPercent += double.Parse(dtInfo.Rows[i]["percents"].ToString());
                                            dtInfo.Rows[i]["Error_Percent"] = dPercent.ToString();
                                        }
                                    }
                                    //     dtAll.Merge(dtInfo);
                                    dicResult.Add(bydateType + qMType + qSubType + qPhotoType, dtInfo);
                                    exeRes.Anything = dicResult;
                                    break;
                            }
                            break;
                        default:
                            break;
                    }
                }
                if (qPhotoType != "ErrorTable")
                    exeRes.Anything = dicResult;
            }
            else
            {
                exeRes.Status = false;
                exeRes.Message = "Para Error";
            }

            return exeRes;
        }

        public ExecutionResult GetErrLogData(object[] objParam)
        {
            ExecutionResult exeRes = new ExecutionResult();
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLV = "", qMType = "", qPtype = "", qSubType = "", qSubValue = "", qPhotoType = "", qOrderBy = "", qOrderAsc = "";


            int iTop = 0;
            if (objParam.Length >= 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLV = objParam[4].ToString();
                qMType = objParam[5].ToString();
                qPtype = objParam[6].ToString();
                qSubType = objParam[7].ToString();
                qSubValue = objParam[8].ToString();
                qPhotoType = objParam[9].ToString();
                iTop = Int32.Parse(objParam[10].ToString());
                qOrderBy = objParam[11].ToString();
                qOrderAsc = objParam[12].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    switch (qLV)
                    {
                        case "INDEX":
                            DataTable dtIndex = new DataTable();
                            dtIndex = fpydeepdiveOperator.GetErrLogIndexData(plant, sapplant, bydateType, qCond, qMType, qPtype, qPhotoType, iTop, qOrderBy, qOrderAsc);
                            if (dtIndex.Rows.Count == 0)
                                dtIndex.Rows.Add("N/A", 0, 0, 0);
                            dicResult.Add(bydateType + qMType, dtIndex);
                            exeRes.Anything = dicResult;
                            break;
                        case "LV2":
                            DataTable dtLV2 = new DataTable();
                            if (qSubType == "TREND")
                            {
                                DateTime dBaseDate = DateTime.Now;
                                string[] aa = bydateType.Split('_');
                                string strDateType = aa[0];
                                string strShift = aa[1];
                                if (strDateType == "Daily")
                                {
                                    dBaseDate = DateTime.ParseExact(qCond, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                                    dtLV2 = fpydeepdiveOperator.GetErrLogLv2Data(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue, qPhotoType, iTop, qOrderBy, qOrderAsc, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"));
                                }
                                else if (strDateType == "Range")
                                {
                                    if (qCond.IndexOf("-") > -1)
                                    {
                                        string[] cc = qCond.Split('-');
                                        dBaseDate = DateTime.ParseExact(cc[1], "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                                    }
                                    else
                                        dBaseDate = DateTime.Now;
                                    dtLV2 = fpydeepdiveOperator.GetErrLogLv2Data(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue, qPhotoType, iTop, qOrderBy, qOrderAsc, dBaseDate.AddDays(-6).ToString("yyyyMMdd"), dBaseDate.ToString("yyyyMMdd"));
                                }
                                else if (strDateType == "Weekly")
                                {
                                    string strYear, strWeek;
                                    string strCond = qCond.Replace("W", "");
                                    strYear = strCond.Substring(0, 4);
                                    strWeek = strCond.Substring(4, 2);
                                    DateTime sTime, eTime;
                                    if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                                    {
                                        sTime = DateTime.Now.AddDays(-7);
                                        eTime = DateTime.Now;
                                    }
                                    dBaseDate = sTime;
                                    dtLV2 = fpydeepdiveOperator.GetErrLogLv2Data(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue, qPhotoType, iTop, qOrderBy, qOrderAsc, dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(-6 * 7).ToString("yyyyMMdd"), dBaseDate.AddDays(1 - Convert.ToInt32(dBaseDate.DayOfWeek.ToString("d"))).AddDays(6).ToString("yyyyMMdd"));
                                }
                                else
                                {
                                    dBaseDate = DateTime.ParseExact(qCond.Replace("M", "") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                                    dtLV2 = fpydeepdiveOperator.GetErrLogLv2Data(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue, qPhotoType, iTop, qOrderBy, qOrderAsc, dBaseDate.AddMonths(-6).ToString("yyyyMM") + "01", DateTime.ParseExact(dBaseDate.AddMonths(1).ToString("yyyyMM") + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(-1).ToString("yyyyMMdd"));
                                }
                            }
                            else
                                dtLV2 = fpydeepdiveOperator.GetErrLogLv2Data(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue, qPhotoType, iTop, qOrderBy, qOrderAsc);
                            if (dtLV2.Rows.Count == 0)
                                dtLV2.Rows.Add("N/A", 0, 0, 0);
                            dicResult.Add(bydateType + qMType + qSubType, dtLV2);
                            break;
                        case "LV3":
                            #region Error
                            List<FpyDeepDiveModels> modellist = new List<FpyDeepDiveModels>();
                            string[] aaLv3 = bydateType.Split('_');
                            string strLv3DateType = aaLv3[0].Substring(0, 1);
                            string strLv3Shift = aaLv3[1];
                            if (qSubType == "TREND")
                            {
                                if (qSubValue.IndexOf("W") > -1 && qSubValue.Length == 3)
                                {
                                    GregorianCalendar gc = new GregorianCalendar();
                                    int w = gc.GetWeekOfYear(dtNow, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                                    if (Int32.Parse(qSubValue.Substring(1)) > w)
                                        qCond = dtNow.AddYears(-1).Year.ToString() + qSubValue.Substring(1);
                                    else
                                        qCond = dtNow.Year.ToString() + qSubValue.Substring(1);
                                }
                            }
                            DataTable dtError = fpydeepdiveOperator.GetErrLogLv3Data(plant, sapplant, bydateType, qCond, qMType, qPtype, qSubType, qSubValue, qPhotoType, iTop, qOrderBy, qOrderAsc);
                            DataTable dtResult = new DataTable();
                            dtResult.Columns.Add("TOP");
                            dtResult.Columns.Add("ERROR_DESC");
                            dtResult.Columns.Add("QTY");
                            dtResult.Columns.Add("FR");
                            dtResult.Columns.Add("PERCENTS");
                            dtResult.Columns.Add("W1");
                            dtResult.Columns.Add("W2");
                            dtResult.Columns.Add("W3");
                            dtResult.Columns.Add("W4");
                            string categories = GetCategories(strLv3DateType, qCond, "");
                            if (dtError.Rows.Count < iTop)
                                iTop = dtError.Rows.Count;
                            for (int i = 0; i < iTop; i++)
                            {
                                dtResult.Rows.Clear();
                                FpyDeepDiveModels model = new FpyDeepDiveModels();
                                model.ID = "Chart" + qPhotoType + i;
                                DataRow dr = dtResult.NewRow();
                                dr["TOP"] = "TOP" + (i + 1).ToString();
                                dr["ERROR_DESC"] = dtError.Rows[i]["catalogy"];
                                dr["QTY"] = dtError.Rows[i]["ErrorQty"];
                                dr["FR"] = double.Parse(dtError.Rows[i]["fr"].ToString());
                                dr["PERCENTS"] = double.Parse(dtError.Rows[i]["error_rate"].ToString());
                                //  dr["PERCENTS"] = dPercent;
                                if (categories != "")
                                {
                                    for (int k = 0; k < categories.Split(';').Length; k++)
                                    {
                                        DataTable dtTemp = new DataTable();
                                        dtTemp = fpydeepdiveOperator.GetErrLogLv3Data(plant, sapplant, bydateType, categories.Split(';')[k], qMType, qPtype, qSubType, qSubValue, qPhotoType, iTop, qOrderBy, qOrderAsc, dtError.Rows[i]["catalogy"].ToString());
                                        //dtTemp = fpydeepdiveOperator.GetPidDPPMChartData(plant, sapplant, bydateType, categories.Split(';')[k], qMType, qPtype, qSubType, qSubValue, "REPAIR", dtError.Rows[i]["catalogy"].ToString());
                                        double dTemp = 0;
                                        if (dtTemp.Rows.Count > 0)
                                            dTemp = double.Parse(dtTemp.Rows[0]["fr"].ToString());
                                        dr["W" + (k + 1).ToString()] = dTemp.ToString();
                                        model.data.Add(dTemp);
                                    }
                                }
                                dtResult.Rows.Add(dr);
                                //   model.categories = categories;
                                model.DtError = dtResult.Copy();
                                modellist.Add(model);
                            }
                            exeRes.Anything = new object[] { categories, modellist };

                            #endregion
                            break;
                        default:
                            break;
                    }
                }
                if (qLV != "LV3")
                    exeRes.Anything = dicResult;
            }
            else
            {
                exeRes.Status = false;
                exeRes.Message = "Para Error";
            }

            return exeRes;
        }

        public ExecutionResult GetHsrDeepDiveData(object[] objParam)
        {
            ExecutionResult exeRes = new ExecutionResult();
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            string plant = "", sapplant = "", bydateType = "", qCond = "", qMProcess, qProcess = "", qType = "", qOrderBy = "", qOrderAsc = "";
            string qLv2Type = "", qLv2Cond = "", qLv3Type = "", qLv3Cond = "";

            if (objParam.Length == 9)
            {
                #region LV1
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qType = objParam[4].ToString();
                qMProcess = objParam[5].ToString();
                qProcess = objParam[6].ToString();
                qOrderBy = objParam[7].ToString();
                qOrderAsc = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    if(qType == "Dab"||qType== "DabExcel")
                    {
                        DataTable dtHSR = fpydeepdiveOperator.GetHsrDataLV1(plant, sapplant, bydateType, qCond, qType, qMProcess, qProcess, qOrderBy, qOrderAsc);
                        DataTable dthsr = new DataTable();                        
                        if (dtHSR.Rows.Count > 0)
                        {
                            //获取LINE_NAME
                            DataTable dtline = dtHSR.DefaultView.ToTable(true, "LINE_NAME");
                            double[] sum = new double[dtline.Rows.Count];
                            dthsr.Columns.Add("LOB");
                            for(int i = 0; i < dtline.Rows.Count; i++)
                            {
                                dthsr.Columns.Add(dtline.Rows[i]["LINE_NAME"].ToString());
                                //记录当前linename的值的总和，i为linename的位置的前一位
                                sum[i] = 0;                                
                            }
                            
                            DataTable dtlob = dtHSR.DefaultView.ToTable(true, "LOB");
                            for (int i = 0; i < dtlob.Rows.Count; i++)
                            {
                                DataRow drhsr = dthsr.NewRow();
                                string lob = dtlob.Rows[i]["LOB"].ToString();
                                drhsr["LOB"] = lob;
                                for (int j = 1; j < dthsr.Columns.Count; j++)
                                {
                                    drhsr[j] = "0";
                                }
                                dtHSR.CaseSensitive = true;
                                DataRow[] dr = dtHSR.Select("LOB='" + lob + "'");
                                if (dr.Length > 0)
                                {
                                    for (int idx = 0; idx < dr.Length; idx++)
                                    {
                                        for(int j =1;j< dthsr.Columns.Count; j++)
                                        {
                                            if (dr[idx]["LINE_NAME"].ToString() == dthsr.Columns[j].ColumnName)
                                            {
                                                drhsr[j] = dr[idx]["QTY"].ToString();
                                                sum[j - 1] += double.Parse(dr[idx]["QTY"].ToString());
                                            }
                                        }
                                    }
                                }
                                dthsr.Rows.Add(drhsr);
                            }
                            if (qType == "Dab")
                            {
                                DataRow drhsr = dthsr.NewRow();
                                drhsr["LOB"] = "TTL";
                                for (int j = 1; j < dthsr.Columns.Count; j++)
                                {
                                    drhsr[j] = sum[j - 1];
                                }
                                dthsr.Rows.Add(drhsr);
                            }
                        }
                        else
                        {
                            DataRow drhsr = dthsr.NewRow();
                            drhsr["LOB"] = "N/A";
                            for (int j = 1; j < dthsr.Columns.Count; j++)
                            {
                                drhsr[j] = "0";
                            }
                            dthsr.Rows.Add(drhsr);
                        }
                        dicResult.Add(bydateType + "FpyHSR", dthsr);
                        exeRes.Anything = dicResult;
                    }
                    else
                    {
                        DataTable dtHSR = fpydeepdiveOperator.GetHsrDataLV1(plant, sapplant, bydateType, qCond, qType, qMProcess, qProcess, qOrderBy, qOrderAsc);
                        if (dtHSR.Rows.Count == 0)
                            dtHSR.Rows.Add("N/A", 0, 0);
                        dicResult.Add(bydateType + "FpyHSR", dtHSR);
                        exeRes.Anything = dicResult;
                    }
                    
                }
                #endregion
            }
            else if (objParam.Length == 11)
            {
                #region LV2
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qType = objParam[4].ToString();
                qMProcess = objParam[5].ToString();
                qProcess = objParam[6].ToString();
                qLv2Type = objParam[7].ToString();
                qLv2Cond = objParam[8].ToString();                
                qOrderBy = objParam[9].ToString();
                qOrderAsc = objParam[10].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    DataTable dtHSR = fpydeepdiveOperator.GetHsrDetailData(plant, sapplant, bydateType, qCond, qType, qMProcess, qProcess, qLv2Type, qLv2Cond, qLv3Type, qLv3Cond, qOrderBy, qOrderAsc);
                    if (dtHSR.Rows.Count == 0)
                        dtHSR.Rows.Add("N/A", 0, 0);
                    dicResult.Add(bydateType + "FpyHSR", dtHSR);
                    exeRes.Anything = dicResult;
                }
                #endregion
            }
            else if (objParam.Length == 12)
            {
                #region LV3
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();

                qType = objParam[4].ToString();
                qMProcess = objParam[5].ToString();
                qProcess = objParam[6].ToString();
                qOrderBy = objParam[7].ToString();
                qOrderAsc = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    DataTable dtHSR = fpydeepdiveOperator.GetHsrDataLV1(plant, sapplant, bydateType, qCond, qType, qMProcess, qProcess, qOrderBy, qOrderAsc);
                    if (dtHSR.Rows.Count == 0)
                        dtHSR.Rows.Add("N/A", 0, 0);
                    dicResult.Add(bydateType + "FpyHSR", dtHSR);
                    exeRes.Anything = dicResult;
                }
                #endregion
            }
            else if (objParam.Length == 13)
            {
                #region Detail
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qType = objParam[4].ToString();
                qMProcess = objParam[5].ToString();
                qProcess = objParam[6].ToString();
                qLv2Type = objParam[7].ToString();
                qLv2Cond = objParam[8].ToString();
                qLv3Type = objParam[9].ToString();
                qLv3Cond = objParam[10].ToString();
                qOrderBy = objParam[11].ToString();
                qOrderAsc = objParam[12].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    DataTable dtHSR = fpydeepdiveOperator.GetHsrDetailData(plant, sapplant, bydateType, qCond, qType, qMProcess, qProcess, qLv2Type, qLv2Cond, qLv3Type, qLv3Cond, qOrderBy, qOrderAsc);
                    dicResult.Add(bydateType + "FpyHSR", dtHSR);
                    exeRes.Anything = dicResult;
                }
                #endregion
            }
            return exeRes;
        }

        public string GetNowWeek()
        {
            string strWeek = "";
            System.Globalization.GregorianCalendar gc = new System.Globalization.GregorianCalendar();
            int weekOfYear = gc.GetWeekOfYear(DateTime.Now.AddDays(-(7 * 0)), System.Globalization.CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            strWeek = DateTime.Now.AddDays(-(7 * 0)).Year.ToString() + "W" + weekOfYear.ToString("00");

            return strWeek;
        }

        public string GetCategories(string dateType, string qDate, string CataType, int iRange = 3)
        {
            string Categories = "";
            string strYear, strWeek;
            GregorianCalendar gc = new GregorianCalendar();
            DateTime dDate = DateTime.Now;
            if (CataType == "LOB")
            {
                switch (dateType)
                {
                    case "D":
                        dDate = DateTime.ParseExact(qDate, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        for (int i = iRange; i >= 0; i--)
                        {
                            Categories += dDate.AddDays(-i).ToString("yyyyMMdd") + ";";
                        }
                        break;
                    case "W":
                        qDate = qDate.Replace("W", "");
                        strYear = qDate.Substring(0, 4);
                        strWeek = qDate.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        for (int i = iRange; i >= 0; i--)
                        {
                            strWeek = "";
                            int weekOfYear = gc.GetWeekOfYear(sTime.AddDays(-(7 * i)), System.Globalization.CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            strWeek = sTime.AddDays(-(7 * i)).Year.ToString() + "W" + weekOfYear.ToString("00");
                            Categories += strWeek + ";";
                        }
                        break;
                    case "M":
                        qDate = qDate.Replace("M", "");
                        dDate = DateTime.ParseExact(qDate + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        for (int i = iRange; i >= 0; i--)
                        {
                            Categories += dDate.AddMonths(-i).Year.ToString() + "M" + dDate.AddMonths(-i).Month.ToString("00") + ";";
                        }
                        break;
                    case "R":
                        if (qDate.IndexOf("-") > -1)
                        {
                            string[] cc = qDate.Split('-');
                            qDate = cc[1];
                        }
                        dDate = DateTime.ParseExact(qDate, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        for (int i = iRange; i >= 0; i--)
                        {
                            Categories += dDate.AddDays(-i).ToString("yyyyMMdd") + ";";
                        }
                        break;
                }
            }
            else
            {
                switch (dateType)
                {
                    case "D":
                        dDate = DateTime.ParseExact(qDate, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        //for (int i = iRange; i >= 0; i--)
                        //{
                        //    //strWeek = "";
                        //    //int weekOfYear = gc.GetWeekOfYear(dDate.AddDays(-(7 * i)), System.Globalization.CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                        //    //strWeek = DateTime.Now.AddDays(-(7 * i)).Year.ToString() + "W" + weekOfYear.ToString("00");
                        //    //Categories += strWeek + ";";                          
                        //}
                        for (int i = iRange; i >= 0; i--)
                        {
                            Categories += dDate.AddDays(-i).ToString("yyyyMMdd") + ";";
                        }
                        break;
                    case "W":
                        qDate = qDate.Replace("W", "");
                        strYear = qDate.Substring(0, 4);
                        strWeek = qDate.Substring(4, 2);
                        DateTime sTime, eTime;
                        if (!WorkDateConvert.GetDaysOfWeeks(Int32.Parse(strYear), Int32.Parse(strWeek), System.Globalization.CalendarWeekRule.FirstFourDayWeek, out sTime, out eTime, mClientInfo))
                        {
                            sTime = DateTime.Now.AddDays(-7);
                            eTime = DateTime.Now;
                        }
                        for (int i = iRange; i >= 0; i--)
                        {
                            strWeek = "";
                            int weekOfYear = gc.GetWeekOfYear(sTime.AddDays(-(7 * i)), System.Globalization.CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            strWeek = sTime.AddDays(-(7 * i)).Year.ToString() + "W" + weekOfYear.ToString("00");
                            Categories += strWeek + ";";
                        }
                        break;
                    case "M":
                        qDate = qDate.Replace("M", "");
                        dDate = DateTime.ParseExact(qDate + "01", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        for (int i = iRange; i >= 0; i--)
                        {
                            Categories += dDate.AddMonths(-i).Year.ToString() + "M" + dDate.AddMonths(-i).Month.ToString("00") + ";";
                        }
                        break;
                    case "R":
                        if (qDate.IndexOf("-") > -1)
                        {
                            string[] cc = qDate.Split('-');
                            qDate = cc[1];
                        }
                        dDate = DateTime.ParseExact(qDate, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                        for (int i = iRange; i >= 0; i--)
                        {
                            Categories += dDate.AddDays(-i).ToString("yyyyMMdd") + ";";
                        }
                        break;
                }
            }

            return Categories.Substring(0, Categories.Length - 1);
        }
    }
}
