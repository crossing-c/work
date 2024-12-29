using Srvtools;
using Compal.MESComponent;
using sDEM2100.Core;
using Newtonsoft.Json;
using sDEM2100.Beans;
using System;
using System.Data;
using System.Net.Mail;
using System.Threading;
using System.Collections.Generic;
using System.Text;

namespace sDEM2100
{
    /// <summary>
    /// Summary description for Component.
    /// </summary>
    public class Component : DataModule
    {
        private MESLog srvLog;
        private ServiceManager serviceManager;
        private InfoConnection InfoConnection1;
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components;

        public Component(System.ComponentModel.IContainer container)
        {
            ///
            /// Required for Windows.Forms Class Composition Designer support
            ///
            container.Add(this);
            InitializeComponent();
            object[] objs = (object[])Srvtools.CliUtils.GetBaseClientInfo();
            //
            // TODO: Add any constructor code after InitializeComponent call
            //
        }

        public Component()
        {
            ///
            /// This call is required by the Windows.Forms Designer.
            ///
            InitializeComponent();
            this.srvLog = new MESLog(this.GetType().Name);
            //
            // TODO: Add any constructor code after InitializeComponent call
            //
        }

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (components != null)
                {
                    components.Dispose();
                }
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code
        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            Srvtools.Service service1 = new Srvtools.Service();
            Srvtools.Service service2 = new Srvtools.Service();
            Srvtools.Service service3 = new Srvtools.Service();
            Srvtools.Service service4 = new Srvtools.Service();
            Srvtools.Service service5 = new Srvtools.Service();
            Srvtools.Service service6 = new Srvtools.Service();
            Srvtools.Service service7 = new Srvtools.Service();
            Srvtools.Service service8 = new Srvtools.Service();
            Srvtools.Service service9 = new Srvtools.Service();
            Srvtools.Service service10 = new Srvtools.Service();
            Srvtools.Service service11 = new Srvtools.Service();
            Srvtools.Service service12 = new Srvtools.Service();
            Srvtools.Service service13 = new Srvtools.Service();
            Srvtools.Service service14 = new Srvtools.Service();
            Srvtools.Service service15 = new Srvtools.Service();
            Srvtools.Service service16 = new Srvtools.Service();
            Srvtools.Service service17 = new Srvtools.Service();
            Srvtools.Service service18 = new Srvtools.Service();
            Srvtools.Service service19 = new Srvtools.Service();
            Srvtools.Service service20 = new Srvtools.Service();
            Srvtools.Service service21 = new Srvtools.Service();
            Srvtools.Service service22 = new Srvtools.Service();
            Srvtools.Service service23 = new Srvtools.Service();
            Srvtools.Service service24 = new Srvtools.Service();
            Srvtools.Service service25 = new Srvtools.Service();
            Srvtools.Service service26 = new Srvtools.Service();
            Srvtools.Service service27 = new Srvtools.Service();
            Srvtools.Service service28 = new Srvtools.Service();
            Srvtools.Service service29 = new Srvtools.Service();
            Srvtools.Service service30 = new Srvtools.Service();
            Srvtools.Service service31 = new Srvtools.Service();
            Srvtools.Service service32 = new Srvtools.Service();
            Srvtools.Service service33 = new Srvtools.Service();
            Srvtools.Service service34 = new Srvtools.Service();
            Srvtools.Service service35 = new Srvtools.Service();
            Srvtools.Service service36 = new Srvtools.Service();
            Srvtools.Service service37 = new Srvtools.Service();
            Srvtools.Service service38 = new Srvtools.Service();
            Srvtools.Service service39 = new Srvtools.Service();
            Srvtools.Service service40 = new Srvtools.Service();
            Srvtools.Service service41 = new Srvtools.Service();
            Srvtools.Service service42 = new Srvtools.Service();
            Srvtools.Service service43 = new Srvtools.Service();
            Srvtools.Service service44 = new Srvtools.Service();
            Srvtools.Service service45 = new Srvtools.Service();
            Srvtools.Service service46 = new Srvtools.Service();
            Srvtools.Service service47 = new Srvtools.Service();
            Srvtools.Service service48 = new Srvtools.Service();
            Srvtools.Service service49 = new Srvtools.Service();
            Srvtools.Service service50 = new Srvtools.Service();
            Srvtools.Service service51 = new Srvtools.Service();
            Srvtools.Service service52 = new Srvtools.Service();
            Srvtools.Service service53 = new Srvtools.Service();
            Srvtools.Service service54 = new Srvtools.Service();
            Srvtools.Service service55 = new Srvtools.Service();
            Srvtools.Service service56 = new Srvtools.Service();
            Srvtools.Service service57 = new Srvtools.Service();
            Srvtools.Service service58 = new Srvtools.Service();
            Srvtools.Service service59 = new Srvtools.Service();
            Srvtools.Service service60 = new Srvtools.Service();
            Srvtools.Service service61 = new Srvtools.Service();
            Srvtools.Service service62 = new Srvtools.Service();
            Srvtools.Service service63 = new Srvtools.Service();
            Srvtools.Service service64 = new Srvtools.Service();
            Srvtools.Service service65 = new Srvtools.Service();
            Srvtools.Service service66 = new Srvtools.Service();
            Srvtools.Service service67 = new Srvtools.Service();
            Srvtools.Service service68 = new Srvtools.Service();
            Srvtools.Service service69 = new Srvtools.Service();
            Srvtools.Service service70 = new Srvtools.Service();
            Srvtools.Service service71 = new Srvtools.Service();
            Srvtools.Service service72 = new Srvtools.Service();
            Srvtools.Service service73 = new Srvtools.Service();
            Srvtools.Service service74 = new Srvtools.Service();
            Srvtools.Service service75 = new Srvtools.Service();
            this.serviceManager = new Srvtools.ServiceManager(this.components);
            this.InfoConnection1 = new Srvtools.InfoConnection(this.components);
            ((System.ComponentModel.ISupportInitialize)(this.InfoConnection1)).BeginInit();
            // 
            // serviceManager
            // 
            service1.DelegateName = "GetScoreCardChart";
            service1.NonLogin = false;
            service1.ServiceName = "GetScoreCardChart";
            service2.DelegateName = "GetScoreCardDetail";
            service2.NonLogin = false;
            service2.ServiceName = "GetScoreCardDetail";
            service3.DelegateName = "GetScoreCardRadar";
            service3.NonLogin = false;
            service3.ServiceName = "GetScoreCardRadar";
            service4.DelegateName = "GetUserInfo";
            service4.NonLogin = false;
            service4.ServiceName = "GetUserInfo";
            service5.DelegateName = "AccountList";
            service5.NonLogin = false;
            service5.ServiceName = "AccountList";
            service6.DelegateName = "AccountEdit";
            service6.NonLogin = false;
            service6.ServiceName = "AccountEdit";
            service7.DelegateName = "AccountCreate";
            service7.NonLogin = false;
            service7.ServiceName = "AccountCreate";
            service8.DelegateName = "AccountDelete";
            service8.NonLogin = false;
            service8.ServiceName = "AccountDelete";
            service9.DelegateName = "GetPlantAndRoleList";
            service9.NonLogin = false;
            service9.ServiceName = "GetPlantAndRoleList";
            service10.DelegateName = "DqmsWeekly";
            service10.NonLogin = false;
            service10.ServiceName = "DqmsWeekly";
            service11.DelegateName = "DqmsMonthly";
            service11.NonLogin = false;
            service11.ServiceName = "DqmsMonthly";
            service12.DelegateName = "DqmsQuarter";
            service12.NonLogin = false;
            service12.ServiceName = "DqmsQuarter";
            service13.DelegateName = "MpIndexData";
            service13.NonLogin = false;
            service13.ServiceName = "MpIndexData";
            service14.DelegateName = "MpOfflineData";
            service14.NonLogin = false;
            service14.ServiceName = "MpOfflineData";
            service15.DelegateName = "MpWfrData";
            service15.NonLogin = false;
            service15.ServiceName = "MpWfrData";
            service16.DelegateName = "MpCtqData";
            service16.NonLogin = false;
            service16.ServiceName = "MpCtqData";
            service17.DelegateName = "MpPidData";
            service17.NonLogin = false;
            service17.ServiceName = "MpPidData";
            service18.DelegateName = "MpScrapData";
            service18.NonLogin = false;
            service18.ServiceName = "MpScrapData";
            service19.DelegateName = "MpFpyData";
            service19.NonLogin = false;
            service19.ServiceName = "MpFpyData";
            service20.DelegateName = "MpLrrData";
            service20.NonLogin = false;
            service20.ServiceName = "MpLrrData";
            service21.DelegateName = "MpHsrData";
            service21.NonLogin = false;
            service21.ServiceName = "MpHsrData";
            service22.DelegateName = "CustomerIndexData";
            service22.NonLogin = false;
            service22.ServiceName = "CustomerIndexData";
            service23.DelegateName = "GetCloudRoom";
            service23.NonLogin = false;
            service23.ServiceName = "GetCloudRoom";
            service24.DelegateName = "DoLogin";
            service24.NonLogin = false;
            service24.ServiceName = "DoLogin";
            service25.DelegateName = "LoginStatistics";
            service25.NonLogin = false;
            service25.ServiceName = "LoginStatistics";
            service26.DelegateName = "UserLoginDetail";
            service26.NonLogin = false;
            service26.ServiceName = "UserLoginDetail";
            service27.DelegateName = "UserLogintTrendChart";
            service27.NonLogin = false;
            service27.ServiceName = "UserLogintTrendChart";
            service28.DelegateName = "GetCrossCheckData";
            service28.NonLogin = false;
            service28.ServiceName = "GetCrossCheckData";
            service29.DelegateName = "MatrixIndexData";
            service29.NonLogin = false;
            service29.ServiceName = "MatrixIndexData";
            service30.DelegateName = "MatrixQualityData";
            service30.NonLogin = false;
            service30.ServiceName = "MatrixQualityData";
            service31.DelegateName = "UserClickLog";
            service31.NonLogin = false;
            service31.ServiceName = "UserClickLog";
            service32.DelegateName = "NpiData";
            service32.NonLogin = false;
            service32.ServiceName = "NpiData";
            service33.DelegateName = "MatrixQualityData2";
            service33.NonLogin = false;
            service33.ServiceName = "MatrixQualityData2";
            service34.DelegateName = "MatrixRejectRateData";
            service34.NonLogin = false;
            service34.ServiceName = "MatrixRejectRateData";
            service35.DelegateName = "MatrixRjtRateLine";
            service35.NonLogin = false;
            service35.ServiceName = "MatrixRjtRateLine";
            service36.DelegateName = "ClickStatistics";
            service36.NonLogin = false;
            service36.ServiceName = "ClickStatistics";
            service37.DelegateName = "OverallQuality";
            service37.NonLogin = false;
            service37.ServiceName = "OverallQuality";
            service38.DelegateName = "ClickRate";
            service38.NonLogin = false;
            service38.ServiceName = "ClickRate";
            service39.DelegateName = "SMTBadAnalysis";
            service39.NonLogin = false;
            service39.ServiceName = "SMTBadAnalysis";
            service40.DelegateName = "CollisionParts";
            service40.NonLogin = false;
            service40.ServiceName = "CollisionParts";
            service41.DelegateName = "GetErrorCount";
            service41.NonLogin = false;
            service41.ServiceName = "GetErrorCount";
            service42.DelegateName = "SetErrorCount";
            service42.NonLogin = false;
            service42.ServiceName = "SetErrorCount";
            service43.DelegateName = "GetDeepDiveData";
            service43.NonLogin = false;
            service43.ServiceName = "GetDeepDiveData";
            service44.DelegateName = "GetDeepDiveDataByLOB";
            service44.NonLogin = false;
            service44.ServiceName = "GetDeepDiveDataByLOB";
            service45.DelegateName = "GetDeepDiveDataByLobLV3";
            service45.NonLogin = false;
            service45.ServiceName = "GetDeepDiveDataByLobLV3";
            service46.DelegateName = "GetErrorTableTop";
            service46.NonLogin = false;
            service46.ServiceName = "GetErrorTableTop";
            service47.DelegateName = "GetPartsTableTop";
            service47.NonLogin = false;
            service47.ServiceName = "GetPartsTableTop";
            service48.DelegateName = "GetSapPlant";
            service48.NonLogin = false;
            service48.ServiceName = "GetSapPlant";
            service49.DelegateName = "GetDeepDiveDataSMT";
            service49.NonLogin = false;
            service49.ServiceName = "GetDeepDiveDataSMT";
            service50.DelegateName = "GetDeepDiveDataByLOBSMT";
            service50.NonLogin = false;
            service50.ServiceName = "GetDeepDiveDataByLOBSMT";
            service51.DelegateName = "GetDeepDiveDataByLobLV3SMT";
            service51.NonLogin = false;
            service51.ServiceName = "GetDeepDiveDataByLobLV3SMT";
            service52.DelegateName = "GetHeadSlotTableTop";
            service52.NonLogin = false;
            service52.ServiceName = "GetHeadSlotTableTop";
            service53.DelegateName = "GetKnownIssueData";
            service53.NonLogin = false;
            service53.ServiceName = "GetKnownIssueData";
            service54.DelegateName = "GetDeepDiveDataByStationSMT";
            service54.NonLogin = false;
            service54.ServiceName = "GetDeepDiveDataByStationSMT";
            service55.DelegateName = "GetComponentTableTop";
            service55.NonLogin = false;
            service55.ServiceName = "GetComponentTableTop";
            service56.DelegateName = "GetPIDData";
            service56.NonLogin = false;
            service56.ServiceName = "GetPIDData";
            service57.DelegateName = "GetWeekFromEnd";
            service57.NonLogin = false;
            service57.ServiceName = "GetWeekFromEnd";
            service58.DelegateName = "GetDeepDiveDataByStation";
            service58.NonLogin = false;
            service58.ServiceName = "GetDeepDiveDataByStation";
            service59.DelegateName = "SendLoginMail";
            service59.NonLogin = false;
            service59.ServiceName = "SendLoginMail";
            service60.DelegateName = "GetDeepDiveDataInsightSMT";
            service60.NonLogin = false;
            service60.ServiceName = "GetDeepDiveDataInsightSMT";
            service61.DelegateName = "GetDeepDiveDataByLOBInsightSMT";
            service61.NonLogin = false;
            service61.ServiceName = "GetDeepDiveDataByLOBInsightSMT";
            service62.DelegateName = "GetDeepDiveDataByLobLV3InsightSMT";
            service62.NonLogin = false;
            service62.ServiceName = "GetDeepDiveDataByLobLV3InsightSMT";
            service63.DelegateName = "GetDeepDiveDataByStationInsightSMT";
            service63.NonLogin = false;
            service63.ServiceName = "GetDeepDiveDataByStationInsightSMT";
            service64.DelegateName = "GetDeepDiveDataInsight";
            service64.NonLogin = false;
            service64.ServiceName = "GetDeepDiveDataInsight";
            service65.DelegateName = "GetDeepDiveDataByLOBInsight";
            service65.NonLogin = false;
            service65.ServiceName = "GetDeepDiveDataByLOBInsight";
            service66.DelegateName = "GetDeepDiveDataByLobLV3Insight";
            service66.NonLogin = false;
            service66.ServiceName = "GetDeepDiveDataByLobLV3Insight";
            service67.DelegateName = "GetDeepDiveDataByStationInsight";
            service67.NonLogin = false;
            service67.ServiceName = "GetDeepDiveDataByStationInsight";
            service68.DelegateName = "GetDeepDiveDataPcbLrr";
            service68.NonLogin = false;
            service68.ServiceName = "GetDeepDiveDataPcbLrr";
            service69.DelegateName = "GetDeepDiveDataByLOBPcbLrr";
            service69.NonLogin = false;
            service69.ServiceName = "GetDeepDiveDataByLOBPcbLrr";
            service70.DelegateName = "GetDeepDiveDataByLobLV3PcbLrr";
            service70.NonLogin = false;
            service70.ServiceName = "GetDeepDiveDataByLobLV3PcbLrr";
            service71.DelegateName = "GetErrLogData";
            service71.NonLogin = false;
            service71.ServiceName = "GetErrLogData";
            service72.DelegateName = "PlantLoginDetail";
            service72.NonLogin = false;
            service72.ServiceName = "PlantLoginDetail";
            service73.DelegateName = "GetMicroManagementData";
            service73.NonLogin = false;
            service73.ServiceName = "GetMicroManagementData";
            service74.DelegateName = "GetUserInfoByEmp";
            service74.NonLogin = false;
            service74.ServiceName = "GetUserInfoByEmp";
            service75.DelegateName = "GetHsrDeepDiveData";
            service75.NonLogin = false;
            service75.ServiceName = "GetHsrDeepDiveData";
            this.serviceManager.ServiceCollection.Add(service1);
            this.serviceManager.ServiceCollection.Add(service2);
            this.serviceManager.ServiceCollection.Add(service3);
            this.serviceManager.ServiceCollection.Add(service4);
            this.serviceManager.ServiceCollection.Add(service5);
            this.serviceManager.ServiceCollection.Add(service6);
            this.serviceManager.ServiceCollection.Add(service7);
            this.serviceManager.ServiceCollection.Add(service8);
            this.serviceManager.ServiceCollection.Add(service9);
            this.serviceManager.ServiceCollection.Add(service10);
            this.serviceManager.ServiceCollection.Add(service11);
            this.serviceManager.ServiceCollection.Add(service12);
            this.serviceManager.ServiceCollection.Add(service13);
            this.serviceManager.ServiceCollection.Add(service14);
            this.serviceManager.ServiceCollection.Add(service15);
            this.serviceManager.ServiceCollection.Add(service16);
            this.serviceManager.ServiceCollection.Add(service17);
            this.serviceManager.ServiceCollection.Add(service18);
            this.serviceManager.ServiceCollection.Add(service19);
            this.serviceManager.ServiceCollection.Add(service20);
            this.serviceManager.ServiceCollection.Add(service21);
            this.serviceManager.ServiceCollection.Add(service22);
            this.serviceManager.ServiceCollection.Add(service23);
            this.serviceManager.ServiceCollection.Add(service24);
            this.serviceManager.ServiceCollection.Add(service25);
            this.serviceManager.ServiceCollection.Add(service26);
            this.serviceManager.ServiceCollection.Add(service27);
            this.serviceManager.ServiceCollection.Add(service28);
            this.serviceManager.ServiceCollection.Add(service29);
            this.serviceManager.ServiceCollection.Add(service30);
            this.serviceManager.ServiceCollection.Add(service31);
            this.serviceManager.ServiceCollection.Add(service32);
            this.serviceManager.ServiceCollection.Add(service33);
            this.serviceManager.ServiceCollection.Add(service34);
            this.serviceManager.ServiceCollection.Add(service35);
            this.serviceManager.ServiceCollection.Add(service36);
            this.serviceManager.ServiceCollection.Add(service37);
            this.serviceManager.ServiceCollection.Add(service38);
            this.serviceManager.ServiceCollection.Add(service39);
            this.serviceManager.ServiceCollection.Add(service40);
            this.serviceManager.ServiceCollection.Add(service41);
            this.serviceManager.ServiceCollection.Add(service42);
            this.serviceManager.ServiceCollection.Add(service43);
            this.serviceManager.ServiceCollection.Add(service44);
            this.serviceManager.ServiceCollection.Add(service45);
            this.serviceManager.ServiceCollection.Add(service46);
            this.serviceManager.ServiceCollection.Add(service47);
            this.serviceManager.ServiceCollection.Add(service48);
            this.serviceManager.ServiceCollection.Add(service49);
            this.serviceManager.ServiceCollection.Add(service50);
            this.serviceManager.ServiceCollection.Add(service51);
            this.serviceManager.ServiceCollection.Add(service52);
            this.serviceManager.ServiceCollection.Add(service53);
            this.serviceManager.ServiceCollection.Add(service54);
            this.serviceManager.ServiceCollection.Add(service55);
            this.serviceManager.ServiceCollection.Add(service56);
            this.serviceManager.ServiceCollection.Add(service57);
            this.serviceManager.ServiceCollection.Add(service58);
            this.serviceManager.ServiceCollection.Add(service59);
            this.serviceManager.ServiceCollection.Add(service60);
            this.serviceManager.ServiceCollection.Add(service61);
            this.serviceManager.ServiceCollection.Add(service62);
            this.serviceManager.ServiceCollection.Add(service63);
            this.serviceManager.ServiceCollection.Add(service64);
            this.serviceManager.ServiceCollection.Add(service65);
            this.serviceManager.ServiceCollection.Add(service66);
            this.serviceManager.ServiceCollection.Add(service67);
            this.serviceManager.ServiceCollection.Add(service68);
            this.serviceManager.ServiceCollection.Add(service69);
            this.serviceManager.ServiceCollection.Add(service70);
            this.serviceManager.ServiceCollection.Add(service71);
            this.serviceManager.ServiceCollection.Add(service72);
            this.serviceManager.ServiceCollection.Add(service73);
            this.serviceManager.ServiceCollection.Add(service74);
            this.serviceManager.ServiceCollection.Add(service75);
            // 
            // InfoConnection1
            // 
            this.InfoConnection1.EEPAlias = "CON";
            ((System.ComponentModel.ISupportInitialize)(this.InfoConnection1)).EndInit();

        }

        #endregion
        public object DoLogin(object[] objParam)
        {
            object resObj;
            ExecutionResult result = new ExecutionResult();
            result.Status = true;
            try
            {
                UserInfoControl uc = new UserInfoControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                string userid = objParam[0].ToString();
                DataTable dt = uc.GetUserInfo(userid);
                if (dt.Rows.Count > 0)
                {
                    result = uc.ExecuteLog(userid);
                    List<string> Permissions = new List<string>();
                    for (int i = 0; i < dt.Rows.Count; i++)
                        Permissions.Add(dt.Rows[i]["controller"].ToString());
                    var user = new
                    {
                        UserID = dt.Rows[0]["user_id"].ToString(),
                        UserName = dt.Rows[0]["user_name"].ToString(),
                        PlantCode = dt.Rows[0]["plant_code"].ToString(),
                        Role = dt.Rows[0]["role_id"].ToString(),
                        Permissions = Permissions
                    };
                    resObj = new object[] { 0, JsonConvert.SerializeObject(user) };
                }
                else
                    resObj = new object[] { 0, null };
                //srvLog.Info("DEM2100.GetUserInfo", dt.Rows.Count.ToString());
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            if (result.Status == false)
                srvLog.Info("DEM2100.InsertLOGIN_LOG Error", result.Message);
            return resObj;
        }

        public object UserClickLog(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl ac = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                string result = ac.ExecuteUserClickLog(objParam);
                resObj = new object[] { 0, result };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
                srvLog = new MESLog("QMS.UserClickLog");
                srvLog.Error(ex.Message);
            }
            return resObj;
        }
        public object LoginStatistics(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl ac = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                AccountModels model = ac.LoginStatisticsExecute(objParam);
                resObj = new object[] { 0, JsonConvert.SerializeObject(model) };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object ClickStatistics(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl ac = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                AccountModels model = ac.ClickStatisticsExecute(objParam);
                resObj = new object[] { 0, JsonConvert.SerializeObject(model) };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object UserLoginDetail(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl ac = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                DataTable dt = ac.UserLoginDetail(objParam);
                resObj = new object[] { 0, JsonConvert.SerializeObject(dt) };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object UserLogintTrendChart(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl ac = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                AccountModels model = ac.UserLogintTrendChart(objParam);
                resObj = new object[] { 0, JsonConvert.SerializeObject(model) };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object PlantLoginDetail(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes = new ExecutionResult();

            AccountControl ac = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
            if (objParam.Length >= 6)
            {
                try
                {
                    string plant = objParam[0].ToString();
                    string qDays = objParam[5].ToString().Replace(" Days", "");
                    DataTable dt = ac.GetLastDetail(plant, qDays);
                    resObj = new object[] { 0, dt };
                }
                catch (Exception ex)
                {
                    resObj = new object[] { 1, ex.Message };
                }
            }
            else
            {
                execRes = ac.PlantLoginDetail(objParam);
                if (execRes.Status)
                {
                    string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                    resObj = new object[] { 0, json };
                }
                else
                {
                    resObj = new object[] { 1, "" };
                }
            }

            return resObj;
        }
        public object GetScoreCardChart(object[] objParam)
        {
            object resObj;
            try
            {
                ScoreCardControl scc = new ScoreCardControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                Chart chart1 = scc.GetScoreCardChar();
                resObj = new object[] { 0, JsonConvert.SerializeObject(chart1) };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object GetScoreCardDetail(object[] objParam)
        {
            object resObj;
            try
            {
                ScoreCardControl scc = new ScoreCardControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                DataTable dt = scc.GetScoreCardDetail();
                resObj = new object[] { 0, JsonConvert.SerializeObject(dt) };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object GetScoreCardRadar(object[] objParam)
        {
            object resObj;
            try
            {
                ScoreCardControl scc = new ScoreCardControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                string plant = objParam[0].ToString();
                if (plant == "ALL")
                {
                    List<Chart> chartlist = scc.GetScoreCardRadarChar();
                    resObj = new object[] { 0, JsonConvert.SerializeObject(chartlist) };
                }
                else
                {
                    Chart chart = scc.GetScoreCardRadarChar(plant);
                    resObj = new object[] { 0, JsonConvert.SerializeObject(chart) };
                }
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object GetUserInfo(object[] objParam)
        {
            object resObj;
            try
            {
                UserInfoControl uc = new UserInfoControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                string userid = objParam[0].ToString();
                DataTable dt = uc.GetUserInfo(userid);
                if (dt.Rows.Count > 0)
                {
                    List<string> Permissions = new List<string>();
                    for (int i = 0; i < dt.Rows.Count; i++)
                        Permissions.Add(dt.Rows[i]["controller"].ToString());
                    var user = new
                    {
                        UserID = dt.Rows[0]["user_id"].ToString(),
                        UserName = dt.Rows[0]["user_name"].ToString(),
                        PlantCode = dt.Rows[0]["plant_code"].ToString(),
                        Role = dt.Rows[0]["role_id"].ToString(),
                        DeptName = dt.Rows[0]["dept_name"].ToString(),
                        WorkPlace = dt.Rows[0]["work_place"].ToString(),
                        Permissions = Permissions
                    };
                    resObj = new object[] { 0, JsonConvert.SerializeObject(user) };
                }
                else
                    resObj = new object[] { 0, null };
                srvLog.Info("DEM2100.GetUserInfo", dt.Rows.Count.ToString());
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }

            return resObj;
        }

        public object GetUserInfoByEmp(object[] objParam)
        {
            object resObj;
            try
            {
                UserInfoControl uc = new UserInfoControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                string userid = objParam[0].ToString();
                srvLog.Info("DEM2100.GetUserInfoByEmp", "User Id:" + userid);
                DataTable dt = uc.GetUserInfoByEmp(userid);
                srvLog.Info("DEM2100.GetUserInfoByEmp", "get dt OK:" + dt.Rows.Count);
                if (dt.Rows.Count > 0)
                    resObj = new object[] { 0, JsonConvert.SerializeObject(dt.Rows[0]["user_id"].ToString()) };
                else
                    resObj = new object[] { 0, null };
                srvLog.Info("DEM2100.GetUserInfoByEmp", dt.Rows.Count.ToString());
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }

            return resObj;
        }
        public object AccountList(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl pc = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                DataTable dt = pc.QueryExecute();
                resObj = new object[] { 0, dt };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object GetPlantAndRoleList(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl pc = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                DataSet ds = pc.GetPlantAndRoleExecute();
                resObj = new object[] { 0, ds };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object AccountEdit(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl pc = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                ExecutionResult result = pc.EditExecute(objParam);
                if (result.Status)
                    resObj = new object[] { 0, "OK" };
                else
                {
                    resObj = new object[] { 1, result.Message };
                }
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object AccountCreate(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl pc = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                ExecutionResult result = pc.CreateExecute(objParam);
                if (result.Status)
                    resObj = new object[] { 0, "OK" };
                else
                {
                    resObj = new object[] { 1, result.Message };
                }
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object AccountDelete(object[] objParam)
        {
            object resObj;
            try
            {
                AccountControl pc = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                ExecutionResult result = pc.DeleteExecute(objParam);
                if (result.Status)
                    resObj = new object[] { 0, "OK" };
                else
                {
                    resObj = new object[] { 1, result.Message };
                }
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }
        public object DqmsWeekly(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "KSP2";
            for (int i = 1; i < objParam.Length; i++)
            {
                plant = objParam[i].ToString();
                DqmsMainLogic dml = new DqmsMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = dml.ExecuteWeekly(plant);
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, execRes.Message };
            }
            else
            {
                resObj = new object[] { 1, execRes.Message };
            }
            return resObj;
        }

        public object DqmsMonthly(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "KSP2";
            for (int i = 1; i < objParam.Length; i++)
            {
                plant = objParam[i].ToString();
                DqmsMainLogic dml = new DqmsMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = dml.ExecuteMonthly(plant);
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, execRes.Message };
            }
            else
            {
                resObj = new object[] { 1, execRes.Message };
            }
            return resObj;
        }

        public object DqmsQuarter(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "KSP2";
            for (int i = 1; i < objParam.Length; i++)
            {
                plant = objParam[i].ToString();
                DqmsMainLogic dml = new DqmsMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = dml.ExecuteQuarter(plant);
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, execRes.Message };
            }
            else
            {
                resObj = new object[] { 1, execRes.Message };
            }
            return resObj;
        }

        public object MpIndexData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", type = "";
            plant = objParam[0].ToString();
            type = objParam[1].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                MpMainLogic mp = new MpMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = mp.ExecuteMpIndex(plant, type);
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, execRes.Anything };
            }
            else
            {
                resObj = new object[] { 1, execRes.Anything };
            }
            return resObj;
        }

        public object MpOfflineData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", type = "";
            plant = objParam[0].ToString();
            type = objParam[1].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                MpMainLogic mp = new MpMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = mp.ExecuteMpOffline(plant, type);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MpWfrData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", type = "";
            plant = objParam[0].ToString();
            type = objParam[1].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                MpMainLogic mp = new MpMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = mp.ExecuteMpWfr(plant, type);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MpCtqData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", type = "";
            plant = objParam[0].ToString();
            type = objParam[1].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                MpMainLogic mp = new MpMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = mp.ExecuteMpCtq(plant, type);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MpPidData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", type = "";
            plant = objParam[0].ToString();
            type = objParam[1].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                MpMainLogic mp = new MpMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = mp.ExecuteMpPid(plant, type);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MpScrapData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            MpMainLogic mp = new MpMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
            execRes = mp.ExecuteMpScrap();
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MpFpyData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", type = "", fpytype = "";
            plant = objParam[0].ToString();
            type = objParam[1].ToString();
            fpytype = objParam[2].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                MpMainLogic mp = new MpMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = mp.ExecuteMpFpy(plant, type, fpytype);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MpLrrData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            MpMainLogic mp = new MpMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
            execRes = mp.ExecuteMpLrr(objParam);
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MpHsrData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            if (objParam.Length >= 3)
            {
                MpMainLogic mp = new MpMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = mp.ExecuteMpHsr(objParam);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object CustomerIndexData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "";
            plant = objParam[0].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                CustomerMainLogic mp = new CustomerMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = mp.ExecuteCustomerIndex(plant);
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, execRes.Anything };
            }
            else
            {
                resObj = new object[] { 1, execRes.Anything };
            }
            return resObj;
        }

        public object NpiData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "";
            plant = objParam[0].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                NpiMainLogic npi = new NpiMainLogic(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = npi.ExecuteNpiCleanLaunch(plant);
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, Newtonsoft.Json.JsonConvert.SerializeObject(execRes.Anything) };
            }
            else
            {
                resObj = new object[] { 1, execRes.Anything };
            }
            return resObj;
        }

        public object GetCloudRoom(object[] objParam)
        {
            object resObj;
            try
            {
                CloudRoomControl crc = new CloudRoomControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                string strjson = crc.GetClouRoomModels(objParam);
                resObj = new object[] { 0, strjson };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object GetCrossCheckData(object[] objParam)
        {
            object resObj;
            try
            {
                CrossCheckControl Cc = new CrossCheckControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                object[] Data = new object[2];
                Data = Cc.GetData(objParam);
                resObj = new object[] { 0, JsonConvert.SerializeObject(Data) };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object GetMicroManagementData(object[] objParam)
        {
            object resObj;
            try
            {
                MicroManagementControl MC = new MicroManagementControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                if (objParam.Length > 2)
                {
                    object[] Data = new object[2];
                    Data = MC.GetData(objParam);
                    resObj = new object[] { 0, JsonConvert.SerializeObject(Data) };
                }
                else
                {
                    string jsonStr = MC.getCustList(objParam);
                    resObj = new object[] { 0, jsonStr };
                }
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object MatrixIndexData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "";
            plant = objParam[0].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                MatrixMainLogic matrix = new MatrixMainLogic(ClientInfo, GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = matrix.ExecuteMatrixIndex(plant);
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, execRes.Anything };
            }
            else
            {
                resObj = new object[] { 1, execRes.Anything };
            }
            return resObj;
        }

        public object MatrixQualityData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", type = "";
            plant = objParam[0].ToString();
            type = objParam[1].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                var matrix = new MatrixMainLogic(ClientInfo, GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = matrix.ExecuteMatrixQuality(plant, type);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MatrixQualityData2(object[] objParam)
        {
            object resObj;
            var execRes = new ExecutionResult();
            var plant = objParam[0].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                var matrix = new MatrixMainLogic(ClientInfo, GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = matrix.ExecuteMatrixQuality2(plant);
            }
            if (execRes.Status)
            {
                var json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MatrixRejectRateData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", type = "";
            plant = objParam[0].ToString();
            type = objParam[1].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                var matrix = new MatrixMainLogic(ClientInfo, GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = matrix.ExecuteMatrixRejectRate(plant, type);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object MatrixRjtRateLine(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", type = "", typeDate = "";
            plant = objParam[0].ToString();
            type = objParam[1].ToString();
            typeDate = objParam[2].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                var matrix = new MatrixMainLogic(ClientInfo, GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = matrix.MatrixRjtRateLine(plant, type, typeDate);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object OverallQuality(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "";
            plant = objParam[0].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                var overallQuality = new OverallQualityControl(ClientInfo, GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = overallQuality.ExecuteOverallQuality(plant);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object ClickRate(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = objParam[0].ToString();
            string dateType = objParam[1].ToString();
            string date = objParam[2].ToString();
            string dateSpan = objParam[3].ToString();
            if (!string.IsNullOrEmpty(plant))
            {
                var clickRate = new ClickRateMainLogic(ClientInfo, GetClientInfo(ClientInfoType.LoginDB).ToString());
                execRes = clickRate.ExecuteClickRateIndex(plant, dateType, date, dateSpan);
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object SMTBadAnalysis(object[] objParam)
        {
            object resObj;
            try
            {
                SMTBadAnalysisControl CP = new SMTBadAnalysisControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                string jsonStr = CP.getSMTBadAnalysis(objParam);
                resObj = new object[] { 0, jsonStr };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object CollisionParts(object[] objParam)
        {
            object resObj;
            try
            {
                CollisionPartsControl CP = new CollisionPartsControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                string jsonStr = CP.getCollisionParts(objParam);
                resObj = new object[] { 0, jsonStr };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object GetErrorCount(object[] objParam)
        {
            object resObj;
            string result = "";

            try
            {
                UserInfoControl uc = new UserInfoControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                string userid = objParam[0].ToString();
                DataTable dt = uc.GetUserInfo(userid);
                if (dt.Rows.Count > 0)
                {
                    result = dt.Rows[0]["error_count"].ToString() + "_" + dt.Rows[0]["diff"].ToString();
                    resObj = new object[] { 0, JsonConvert.SerializeObject(result) };
                }
                else
                    resObj = new object[] { 0, null };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
          ;
            return resObj;
        }

        public object SetErrorCount(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes = new ExecutionResult();

            string userid = objParam[0].ToString();
            int iErrorCount = Int32.Parse(objParam[1].ToString());
            AccountControl ac = new AccountControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
            execRes = ac.UpdateErrorCount(userid, iErrorCount);

            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }

            return resObj;
        }

        public object GetDeepDiveData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qProcess = "", qDuty = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qDuty = objParam[5].ToString();

                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qProcess == "" && qDuty == "")
                        execRes = Dd.GetFpyByLob(plant, sapplant, bydateType, qCond);
                    else if (qProcess != "" && qDuty == "")
                        execRes = Dd.GetFpyByLine(plant, sapplant, bydateType, qCond, qProcess);
                    else
                        execRes = Dd.GetFpyByDuty(plant, sapplant, bydateType, qCond, qProcess, qDuty);
                }
            }
            if (objParam.Length == 8)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qType = objParam[5].ToString();
                qOrderBy = objParam[6].ToString();
                qOrderAsc = objParam[7].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qType == "Lob")
                        execRes = Dd.GetFpyLobData(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Lin")
                        execRes = Dd.GetFpyByLine(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Gro")
                        execRes = Dd.GetFpyByGroup(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc);
                    else if (qType == "Las")
                        execRes = Dd.GetFpyByLast(plant, sapplant, bydateType, qCond);
                    else if (qType == "LobExcel")
                        execRes = Dd.GetFpyLobData(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "LinExcel")
                        execRes = Dd.GetFpyByLine(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "Trend" || qType == "TreExcel")
                        execRes = Dd.GetFpyByTrend(plant, sapplant, bydateType, qCond);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByLOB(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qSub = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetFpyByLob(plant, sapplant, bydateType, qCond, qLOB, qSub);
                }
            }
            else if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                qType = objParam[6].ToString();
                qOrderBy = objParam[7].ToString();
                qOrderAsc = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataLobLv2(plant, sapplant, bydateType, qCond, qLOB, qSub, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByLOBSMT(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qSub = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetFpyByLobSMT(plant, sapplant, bydateType, qCond, qLOB, qSub);
                }
            }
            else if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                qType = objParam[6].ToString();
                qOrderBy = objParam[7].ToString();
                qOrderAsc = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataLobLv2SMT(plant, sapplant, bydateType, qCond, qLOB, qSub, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByLobLV3(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qModel = "", qLine = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 7)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qModel = objParam[5].ToString();
                qLine = objParam[6].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qModel != "" && qLine == "")
                        execRes = Dd.GetFpyByLobByModel(plant, sapplant, bydateType, qCond, qLOB, qModel);
                    else if (qModel == "" && qLine != "")
                        execRes = Dd.GetFpyByLobByLine(plant, sapplant, bydateType, qCond, qLOB, qLine);
                }
            }
            if (objParam.Length == 10)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qModel = objParam[5].ToString();
                qLine = objParam[6].ToString();
                qType = objParam[7].ToString();
                qOrderBy = objParam[8].ToString();
                qOrderAsc = objParam[9].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataLobLv3(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataSMT(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qProcess = "", qDuty = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qDuty = objParam[5].ToString();

                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qProcess == "" && qDuty == "")
                        execRes = Dd.GetFpyByLobSMT(plant, sapplant, bydateType, qCond);
                    else if (qProcess != "" && qDuty == "")
                        execRes = Dd.GetFpyByLineSMT(plant, sapplant, bydateType, qCond, qProcess);
                    else
                        execRes = Dd.GetFpyByDutySMT(plant, sapplant, bydateType, qCond, qProcess, qDuty);
                }
            }
            if (objParam.Length == 8)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qType = objParam[5].ToString();
                qOrderBy = objParam[6].ToString();
                qOrderAsc = objParam[7].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qType == "Lob")
                        execRes = Dd.GetFpyLobDataSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Lin")
                        execRes = Dd.GetFpyByLineSMT(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Gro")
                        execRes = Dd.GetFpyByGroupSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Las")
                        execRes = Dd.GetFpyByLastSMT(plant, sapplant, bydateType, qCond);
                    else if (qType == "LobExcel")
                        execRes = Dd.GetFpyLobDataSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "LinExcel")
                        execRes = Dd.GetFpyByLineSMT(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "GroExcel")
                        execRes = Dd.GetFpyByGroupSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "Trend" || qType == "TreExcel")
                        execRes = Dd.GetFpyByTrendSMT(plant, sapplant, bydateType, qCond);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByLobLV3SMT(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qModel = "", qLine = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 7)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qModel = objParam[5].ToString();
                qLine = objParam[6].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qModel != "" && qLine == "")
                        execRes = Dd.GetFpyByLobByModelSMT(plant, sapplant, bydateType, qCond, qLOB, qModel);
                    else if (qModel == "" && qLine != "")
                        execRes = Dd.GetFpyByLobByLineSMT(plant, sapplant, bydateType, qCond, qLOB, qLine);
                }
            }
            if (objParam.Length == 10)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qModel = objParam[5].ToString();
                qLine = objParam[6].ToString();
                qType = objParam[7].ToString();
                qOrderBy = objParam[8].ToString();
                qOrderAsc = objParam[9].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataLobLv3SMT(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByStation(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qStation = "", qSub = "", qLine = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 8)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qStation = objParam[4].ToString();
                qSub = objParam[5].ToString();
                qOrderBy = objParam[6].ToString();
                qOrderAsc = objParam[7].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetFpyByStation(plant, sapplant, bydateType, qCond, qStation, qSub, qOrderBy, qOrderAsc);
                }
            }
            else if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qStation = objParam[4].ToString();
                qLine = objParam[5].ToString();
                qType = objParam[6].ToString();
                qOrderBy = objParam[7].ToString();
                qOrderAsc = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataStationLv3SMT(plant, sapplant, bydateType, qCond, qStation, qLine, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByStationSMT(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qStation = "", qSub = "", qLine = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 8)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qStation = objParam[4].ToString();
                qSub = objParam[5].ToString();
                qOrderBy = objParam[6].ToString();
                qOrderAsc = objParam[7].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetFpyByStationSMT(plant, sapplant, bydateType, qCond, qStation, qSub, qOrderBy, qOrderAsc);
                }
            }
            else if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qStation = objParam[4].ToString();
                qLine = objParam[5].ToString();
                qType = objParam[6].ToString();
                qOrderBy = objParam[7].ToString();
                qOrderAsc = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataStationLv3SMT(plant, sapplant, bydateType, qCond, qStation, qLine, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByLOBInsightSMT(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qSub = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetFpyByLobInsightSMT(plant, sapplant, bydateType, qCond, qLOB, qSub);
                }
            }
            else if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                qType = objParam[6].ToString();
                qOrderBy = objParam[7].ToString();
                qOrderAsc = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetDeepDiveDataLobLv2InsightSMT(plant, sapplant, bydateType, qCond, qLOB, qSub, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataInsightSMT(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qProcess = "", qError = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qError = objParam[5].ToString();

                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qProcess == "" && qError == "")
                        execRes = Dd.GetFpyByLobInsightSMT(plant, sapplant, bydateType, qCond);
                    else if (qProcess != "" && qError == "")
                        execRes = Dd.GetFpyByLineInsightSMT(plant, sapplant, bydateType, qCond, qProcess);
                    else
                        execRes = Dd.GetErrorTableByError(plant, sapplant, bydateType, qCond, qProcess, qError);
                }
            }
            if (objParam.Length == 8)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qType = objParam[5].ToString();
                qOrderBy = objParam[6].ToString();
                qOrderAsc = objParam[7].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qType == "Lob")
                        execRes = Dd.GetFpyLobDataInsightSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Lin")
                        execRes = Dd.GetFpyByLineInsightSMT(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Gap")
                        execRes = Dd.GetFpyByGapInsightSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Las")
                        execRes = Dd.GetFpyByLastInsightSMT(plant, sapplant, bydateType, qCond);
                    else if (qType == "LobExcel")
                        execRes = Dd.GetFpyLobDataInsightSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "LinExcel")
                        execRes = Dd.GetFpyByLineInsightSMT(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "GapExcel")
                        execRes = Dd.GetFpyByGapInsightSMT(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "Trend" || qType == "TreExcel")
                        execRes = Dd.GetFpyByTrendSMTInsight(plant, sapplant, bydateType, qCond);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByLobLV3InsightSMT(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qModel = "", qLine = "", qType = "", qStation = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 7)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qModel = objParam[5].ToString();
                qLine = objParam[6].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qModel != "" && qLine == "")
                        execRes = Dd.GetFpyByLobByModelInsightSMT(plant, sapplant, bydateType, qCond, qLOB, qModel);
                    else if (qModel == "" && qLine != "")
                        execRes = Dd.GetFpyByLobByLineSMT(plant, sapplant, bydateType, qCond, qLOB, qLine);
                }
            }
            if (objParam.Length == 11)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qModel = objParam[5].ToString();
                qLine = objParam[6].ToString();
                qStation = objParam[7].ToString();
                qType = objParam[8].ToString();
                qOrderBy = objParam[9].ToString();
                qOrderAsc = objParam[10].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataLobLv3InsightSMT(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, qStation, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByStationInsightSMT(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qStation = "", qSub = "", qLine = "", qType = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qStation = objParam[4].ToString();
                qSub = objParam[5].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetFpyByStationInsightSMT(plant, sapplant, bydateType, qCond, qStation, qSub);
                }
            }
            else if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qStation = objParam[4].ToString();
                qLine = objParam[5].ToString();
                qType = objParam[6].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataStationLv3InsightSMT(plant, sapplant, bydateType, qCond, qStation, qLine, qType, "", "");
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByLOBInsight(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qSub = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetFpyByLobInsight(plant, sapplant, bydateType, qCond, qLOB, qSub);
                }
            }
            else if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                qType = objParam[6].ToString();
                qOrderBy = objParam[7].ToString();
                qOrderAsc = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetDeepDiveDataLobLv2Insight(plant, sapplant, bydateType, qCond, qLOB, qSub, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataInsight(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qProcess = "", qError = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qError = objParam[5].ToString();

                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qProcess == "" && qError == "")
                        execRes = Dd.GetFpyByLobInsight(plant, sapplant, bydateType, qCond);
                    else if (qProcess != "" && qError == "")
                        execRes = Dd.GetFpyByLineInsight(plant, sapplant, bydateType, qCond, qProcess);
                    else
                        execRes = Dd.GetErrorTableByError(plant, sapplant, bydateType, qCond, qProcess, qError);
                }
            }
            if (objParam.Length == 8)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qType = objParam[5].ToString();
                qOrderBy = objParam[6].ToString();
                qOrderAsc = objParam[7].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qType == "Lob")
                        execRes = Dd.GetFpyLobDataInsight(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Lin")
                        execRes = Dd.GetFpyByLineInsight(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Gap")
                        execRes = Dd.GetFpyByGapInsight(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
                    else if (qType == "Las")
                        execRes = Dd.GetFpyByLastInsight(plant, sapplant, bydateType, qCond);
                    else if (qType == "LobExcel")
                        execRes = Dd.GetFpyLobDataInsight(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "LinExcel")
                        execRes = Dd.GetFpyByLineInsight(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "GapExcel")
                        execRes = Dd.GetFpyByGapInsight(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 1000, true);
                    else if (qType == "Trend" || qType == "TreExcel")
                        execRes = Dd.GetFpyByTrendInsight(plant, sapplant, bydateType, qCond);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataPcbLrr(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qProcess = "", qError = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qError = objParam[5].ToString();

                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qProcess == "" && qError == "")
                        execRes = Dd.GetFpyByLobPcbLrr(plant, sapplant, bydateType, qCond);
                    else if (qProcess != "" && qError == "")
                        execRes = Dd.GetFpyByLinePcbLrr(plant, sapplant, bydateType, qCond, qProcess);
                    else
                        execRes = Dd.GetErrorTableByError(plant, sapplant, bydateType, qCond, qProcess, qError);
                }
            }
            if (objParam.Length == 8)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qProcess = objParam[4].ToString();
                qType = objParam[5].ToString();
                qOrderBy = objParam[6].ToString();
                qOrderAsc = objParam[7].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (plant.IndexOf("_IOP") > -1)
                    {
                        plant = plant.Substring(0, plant.IndexOf("_IOP"));
                        //  sapplant = Dd.GetSapPlant(plant);
                        if (qType == "Lob")
                            execRes = Dd.GetFpyDataPcbLrrIOP(plant, sapplant, bydateType, qCond, "LOB", qOrderBy, qOrderAsc, 10);
                        else if (qType == "Lin")
                            execRes = Dd.GetFpyDataPcbLrrIOP(plant, sapplant, bydateType, qCond, "LINE_" + qProcess, qOrderBy, qOrderAsc, 10);
                        else if (qType == "LobExcel")
                            execRes = Dd.GetFpyDataPcbLrrIOP(plant, sapplant, bydateType, qCond, "LOB", qOrderBy, qOrderAsc, 1000, true);
                        else if (qType == "LinExcel")
                            execRes = Dd.GetFpyDataPcbLrrIOP(plant, sapplant, bydateType, qCond, "LINE_" + qProcess, qOrderBy, qOrderAsc, 1000, true);
                    }
                    else
                    {
                        if (qType == "Lob")
                            execRes = Dd.GetFpyLobDataPcbLrr(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10);
                        else if (qType == "LobFloor")
                            execRes = Dd.GetFpyLobDataPcbLrr(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 10, false, true);
                        else if (qType == "Lin")
                            execRes = Dd.GetFpyByLinePcbLrr(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 10);
                        else if (qType == "Gro")
                            execRes = Dd.GetFpyByGroupPcbLrr(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc);
                        else if (qType == "Las")
                            execRes = Dd.GetFpyByLastPcbLrr(plant, sapplant, bydateType, qCond);
                        else if (qType == "LobExcel")
                            execRes = Dd.GetFpyLobDataPcbLrr(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 1000, true);
                        else if (qType == "LobExcelFloor")
                            execRes = Dd.GetFpyLobDataPcbLrr(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, 1000, true, true);
                        else if (qType == "LinExcel")
                            execRes = Dd.GetFpyByLinePcbLrr(plant, sapplant, bydateType, qCond, qProcess, qOrderBy, qOrderAsc, 1000, true);
                        else if (qType == "GroExcel")
                            execRes = Dd.GetFpyByGroupPcbLrr(plant, sapplant, bydateType, qCond, qOrderBy, qOrderAsc, true);
                    }
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByLOBPcbLrr(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qSub = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetFpyByLobPcbLrr(plant, sapplant, bydateType, qCond, qLOB, qSub);
                }
            }
            else if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qSub = objParam[5].ToString();
                qType = objParam[6].ToString();
                qOrderBy = objParam[7].ToString();
                qOrderAsc = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetDeepDiveDataLobLv2PcbLrr(plant, sapplant, bydateType, qCond, qLOB, qSub, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByLobLV3PcbLrr(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qModel = "", qLine = "", qStation = "", qDuty = "", qType = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qLine = objParam[5].ToString();
                qModel = objParam[6].ToString();
                qStation = objParam[7].ToString();
                if (qStation == "AFT")
                    qStation = "AFT ";
                qDuty = objParam[8].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (plant.IndexOf("_IOP") > -1)
                    {
                        plant = plant.Substring(0, plant.IndexOf("_IOP"));
                        sapplant = Dd.GetSapPlant(plant);
                    }
                    if (qLOB != "" && qModel != "" && qLine == "" && qStation == "" && qDuty == "")
                        execRes = Dd.GetFpyByLobByModelPcbLrr(plant, sapplant, bydateType, qCond, qLOB, qModel);
                    else if (qLOB != "" && qModel == "" && qLine != "" && qStation == "" && qDuty == "")
                        execRes = Dd.GetFpyByLobByLinePcbLrr(plant, sapplant, bydateType, qCond, qLOB, qLine);
                    else if (qLOB != "" && qModel == "" && qLine == "" && qStation != "" && qDuty == "")
                        execRes = Dd.GetFpyByLobByStationPcbLrr(plant, sapplant, bydateType, qCond, qLOB, qStation);
                    else if (qLOB == "" && qModel == "" && qLine != "" && qStation != "" && qDuty == "")
                        execRes = Dd.GetFpyByLineByStationPcbLrr(plant, sapplant, bydateType, qCond, qLine, qStation);
                    else if (qLOB != "" && qModel == "" && qLine == "" && qStation == "" && qDuty != "")
                        execRes = Dd.GetFpyByLobByDutyPcbLrr(plant, sapplant, bydateType, qCond, qLOB, qDuty);
                    else if (qLOB == "" && qModel == "" && qLine != "" && qStation == "" && qDuty != "")
                        execRes = Dd.GetFpyByLineByDutyPcbLrr(plant, sapplant, bydateType, qCond, qLine, qDuty);
                    else if (qLOB == "" && qModel == "" && qLine == "" && qStation == "" && qDuty != "")
                        execRes = Dd.GetFpyByDutyPcbLrr(plant, sapplant, bydateType, qCond, qDuty);
                }
            }
            if (objParam.Length == 11)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qModel = objParam[5].ToString();
                qLine = objParam[6].ToString();
                qStation = objParam[7].ToString();
                if (qStation == "AFT")
                    qStation = "AFT ";
                qType = objParam[8].ToString();
                qOrderBy = objParam[9].ToString();
                qOrderAsc = objParam[10].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetDeepDiveDataLobLv3PcbLrr(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, qStation, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }
        public object GetDeepDiveDataByLobLV3Insight(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qLOB = "", qModel = "", qLine = "", qType = "", qStation = "", qOrderBy = "", qOrderAsc = "";
            if (objParam.Length == 7)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qModel = objParam[5].ToString();
                qLine = objParam[6].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    if (qModel != "" && qLine == "")
                        execRes = Dd.GetFpyByLobByModelInsight(plant, sapplant, bydateType, qCond, qLOB, qModel);
                    //else if (qModel == "" && qLine != "")
                    //    execRes = Dd.GetFpyByLobByLine(plant, sapplant, bydateType, qCond, qLOB, qLine);
                }
            }
            if (objParam.Length == 11)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qLOB = objParam[4].ToString();
                qModel = objParam[5].ToString();
                qLine = objParam[6].ToString();
                qStation = objParam[7].ToString();
                qType = objParam[8].ToString();
                qOrderBy = objParam[9].ToString();
                qOrderAsc = objParam[10].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataLobLv3Insight(plant, sapplant, bydateType, qCond, qLOB, qModel, qLine, qStation, qType, qOrderBy, qOrderAsc);
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetDeepDiveDataByStationInsight(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "", sapplant = "", bydateType = "", qCond = "", qStation = "", qSub = "", qLine = "", qType = "";
            if (objParam.Length == 6)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qStation = objParam[4].ToString();
                qSub = objParam[5].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    execRes = Dd.GetFpyByStationInsight(plant, sapplant, bydateType, qCond, qStation, qSub);
                }
            }
            else if (objParam.Length == 9)
            {
                plant = objParam[0].ToString();
                sapplant = objParam[1].ToString();
                bydateType = objParam[2].ToString();
                qCond = objParam[3].ToString();
                qStation = objParam[4].ToString();
                qLine = objParam[5].ToString();
                qType = objParam[6].ToString();
                if (!string.IsNullOrEmpty(plant))
                {
                    plant = plant.Replace("TW01", "CTY1");
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());

                    execRes = Dd.GetDeepDiveDataStationLv3Insight(plant, sapplant, bydateType, qCond, qStation, qLine, qType, "", "");
                }
            }
            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetPIDData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes = new ExecutionResult();

            FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
            execRes = Dd.GetPidData(objParam);

            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetErrLogData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes = new ExecutionResult();

            FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
            execRes = Dd.GetErrLogData(objParam);

            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }

        public object GetHsrDeepDiveData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes = new ExecutionResult();

            FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
            execRes = Dd.GetHsrDeepDiveData(objParam);

            if (execRes.Status)
            {
                string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                resObj = new object[] { 0, json };
            }
            else
            {
                resObj = new object[] { 1, "" };
            }
            return resObj;
        }
        

        public object GetErrorTableTop(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "";
            if (objParam.Length >= 7)
                plant = objParam[0].ToString();
            try
            {
                if (!string.IsNullOrEmpty(plant))
                {
                    object[] Data = new object[2];
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    Data = Dd.GetErrorTable(objParam);
                    //          Thread.Sleep(1000);
                    resObj = new object[] { 0, JsonConvert.SerializeObject(Data) };
                }
                else
                    resObj = new object[] { 1, "No Data" };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object GetPartsTableTop(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "";
            if (objParam.Length >= 7)
                plant = objParam[0].ToString();
            try
            {
                if (!string.IsNullOrEmpty(plant))
                {
                    object[] Data = new object[2];
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    Data = Dd.GetPartsTable(objParam);
                    //        Thread.Sleep(1000);
                    resObj = new object[] { 0, JsonConvert.SerializeObject(Data) };
                }
                else
                    resObj = new object[] { 1, "No Data" };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object GetComponentTableTop(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "";
            if (objParam.Length >= 7)
                plant = objParam[0].ToString();
            try
            {
                if (!string.IsNullOrEmpty(plant))
                {
                    object[] Data = new object[2];
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    Data = Dd.GetComponentTable(objParam);
                    //      Thread.Sleep(1000);
                    resObj = new object[] { 0, JsonConvert.SerializeObject(Data) };
                }
                else
                    resObj = new object[] { 1, "No Data" };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object GetHeadSlotTableTop(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes;
            execRes = new ExecutionResult();
            string plant = "";
            if (objParam.Length >= 7)
                plant = objParam[0].ToString();
            try
            {
                if (!string.IsNullOrEmpty(plant))
                {
                    object[] Data = new object[2];
                    FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                    Data = Dd.GetHeadSlotTable(objParam);
                    //      Thread.Sleep(1000);
                    resObj = new object[] { 0, JsonConvert.SerializeObject(Data) };
                }
                else
                    resObj = new object[] { 1, "No Data" };
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object GetKnownIssueData(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes = new ExecutionResult();

            try
            {
                object[] Data = new object[2];
                KnownIssueControl KI = new KnownIssueControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                if (objParam.Length < 4)
                {
                    Data = KI.GetKnownIssueTable(objParam);
                    resObj = new object[] { 0, JsonConvert.SerializeObject(Data) };
                }
                else
                {
                    execRes = KI.GetKnownIssueDPPMData(objParam);
                    if (execRes.Status)
                    {
                        string json = JsonConvert.SerializeObject(execRes.Anything, Formatting.Indented);
                        resObj = new object[] { 0, json };
                    }
                    else
                    {
                        resObj = new object[] { 1, "" };
                    }
                }
            }
            catch (Exception ex)
            {
                resObj = new object[] { 1, ex.Message };
            }
            return resObj;
        }

        public object GetWeekFromEnd(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes = new ExecutionResult();
            string strWeek = "", strResult = "";
            if (objParam.Length > 0)
                strWeek = objParam[0].ToString();

            if (!string.IsNullOrEmpty(strWeek))
            {
                FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                strResult = Dd.GetWeekFromEnd(strWeek);
                if (strResult != "")
                    execRes.Status = true;
                else
                    execRes.Status = false;
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, strResult };
            }
            else
            {
                resObj = new object[] { 1, "Week Format Error" };
            }
            return resObj;
        }

        public object GetSapPlant(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes = new ExecutionResult();
            string plant = "", strSapPlant = "";

            if (objParam.Length > 0)
                plant = objParam[0].ToString();

            if (!string.IsNullOrEmpty(plant))
            {
                plant = plant.Replace("TW01", "CTY1");
                FPYDeepDiveControl Dd = new FPYDeepDiveControl(base.ClientInfo, base.GetClientInfo(ClientInfoType.LoginDB).ToString());
                if (plant.IndexOf("-") > -1)
                    plant = plant.Split('-')[0];
                strSapPlant = Dd.GetSapPlant(plant);
                if (strSapPlant != "")
                    execRes.Status = true;
                else
                    execRes.Status = false;
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, strSapPlant };
            }
            else
            {
                resObj = new object[] { 1, "No Sap Plant" };
            }
            return resObj;
        }

        public object SendLoginMail(object[] objParam)
        {
            object resObj;
            ExecutionResult execRes = new ExecutionResult();
            string strReceiver = "", strSender = "MES", strSubject = "QMS", strContent = @"您好,<br />";
            bool bSuccess = true;
            if (objParam.Length > 0)
                strReceiver = objParam[0].ToString();
            if (objParam.Length > 1)
                strSender = objParam[1].ToString();
            if (objParam.Length > 2)
                strSubject = objParam[2].ToString();
            if (objParam.Length > 3)
                strContent = objParam[3].ToString();
            if (objParam.Length > 4)
            {
                if (objParam[4].ToString() == "Y")
                    bSuccess = true;
                else
                    bSuccess = false;
            }
            this.srvLog.Info("strReceiver:" + strReceiver + " strSender:" + strSender);
            if (!string.IsNullOrEmpty(strReceiver))
            {
                try
                {
                    if (strSender == "QMS")
                    {
                        strSubject = "【" + strSender + "】";
                        if (bSuccess)
                        {
                            strSubject += "登入成功通知";
                            strContent += "您於系統時間" + DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + "，成功登入【" + strSender + "】，如非您本人登入，請立即修改AD密碼。<br />";
                        }
                        else
                        {
                            strSubject += "登入錯誤通知";
                            strContent += @"您於系統時間" + DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + "，登入【" + strSender + "】錯誤，為確保您的交易安全，如非您本人登入，請立即修改AD密碼。,<br />";
                        }
                        strContent += @"本電子郵件係由系統自動發送，請勿直接回覆本郵件。";
                        if (strReceiver.Trim().Length > 0)
                        {
                            strReceiver = strReceiver + "@compal.com";
                        }
                        SmtpClient smtp = new SmtpClient("10.128.2.181", 25);
                        smtp.Credentials = new System.Net.NetworkCredential("MAIL_MES", "12345678");

                        // smtp.Credentials = new System.Net.NetworkCredential();

                        MailMessage msgMail = new MailMessage();
                        msgMail.From = new MailAddress("MAIL_MES@COMPAL.COM", strSender);
                        msgMail.To.Add(strReceiver);
                        msgMail.Subject = strSubject;
                        msgMail.Body = strContent;
                        msgMail.BodyEncoding = Encoding.UTF8;
                        msgMail.IsBodyHtml = true;
                        //AlternateView alt = AlternateView.CreateAlternateViewFromString(strContent, null, "text/html");
                        //msgMail.AlternateViews.Add(alt);
                        smtp.Send(msgMail);
                        execRes.Status = true;
                        execRes.Message = strReceiver + " Send OK!";
                    }
                }
                catch (Exception ex)
                {
                    execRes.Status = false;
                    execRes.Message = ex.Message;
                    this.srvLog.Info("Exception:" + ex.Message);
                }
            }
            if (execRes.Status)
            {
                resObj = new object[] { 0, execRes.Message };
            }
            else
            {
                resObj = new object[] { 1, execRes.Message };
            }
            return resObj;
        }
    }
}
