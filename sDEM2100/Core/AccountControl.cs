using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using sDEM2100.DataGateway;
using Compal.MESComponent;
using sDEM2100.Beans;

namespace sDEM2100.Core
{
    class AccountControl
    {
        private static AccountOperator accountOperator;
        public AccountControl(object[] clientInfo, string DBName)
        {
            accountOperator = new AccountOperator(clientInfo, DBName);
        }
        public DataTable QueryExecute()
        {
            DataTable dt = new DataTable();
            dt = accountOperator.GetData();
            return dt;
        }

        public DataSet GetPlantAndRoleExecute()
        {
            DataSet ds = new DataSet();
            DataTable dtplant = accountOperator.GetPlant();
            dtplant.TableName = "PLANT";
            ds.Tables.Add(dtplant.Copy());
            DataTable dtRole = accountOperator.GetRole();
            dtRole.TableName = "ROLE";
            ds.Tables.Add(dtRole.Copy());
            return ds;
        }

        public ExecutionResult EditExecute(object[] objParam)
        {
            ExecutionResult result = new ExecutionResult();
            string loginuser = objParam[0].ToString();
            string userid = objParam[1].ToString();
            string username = objParam[2].ToString();
            string plantcode = objParam[3].ToString();
            string roleid = objParam[4].ToString();
            result = accountOperator.UpdateUserInfo(loginuser, userid, username, plantcode, roleid);
            return result;
        }

        public ExecutionResult CreateExecute(object[] objParam)
        {
            ExecutionResult result = new ExecutionResult();
            string loginuser = objParam[0].ToString();
            string userid = objParam[1].ToString();
            string username = objParam[2].ToString();
            string plantcode = objParam[3].ToString();
            string roleid = objParam[4].ToString();
            result = accountOperator.InsertUserInfo(loginuser, userid, username, plantcode, roleid);
            return result;
        }

        public ExecutionResult DeleteExecute(object[] objParam)
        {
            ExecutionResult result = new ExecutionResult();
            string userid = objParam[0].ToString();
            result = accountOperator.DeleteUser(userid);
            return result;
        }

        public AccountModels LoginStatisticsExecute(object[] objParam)
        {
            AccountModels model = new AccountModels();
            try
            {
                string userid = objParam[0].ToString();
                string fromdate = objParam[1].ToString();
                string enddate = objParam[2].ToString();
                model.logincount = accountOperator.GetloginCount();
                DataTable dt = accountOperator.GetLoginStatistics(userid, fromdate, enddate);
                model.dt = dt;
            }
            catch 
            {
            }
            return model;
        }

        public AccountModels ClickStatisticsExecute(object[] objParam)
        {
            AccountModels model = new AccountModels();
            try
            {
                string userid = objParam[0].ToString();
                string fromdate = objParam[1].ToString();
                string enddate = objParam[2].ToString();
                string controller = objParam[3].ToString();
                model.logincount = accountOperator.GetloginCount();
                DataTable dt = accountOperator.GetClickStatistics(userid, fromdate, enddate, controller);
                model.dt = dt;
            }
            catch 
            {
            }
            return model;
        }

        public DataTable UserLoginDetail(object[] objParam)
        {
            DataTable dt = new DataTable();
            try
            {
                string userid = objParam[0].ToString();
                string fromdate = objParam[1].ToString();
                string enddate = objParam[2].ToString();
                dt = accountOperator.GetUserLoginDetail(userid, fromdate, enddate);
            }
            catch  
            {
            }
            return dt;
        }

        public AccountModels UserLogintTrendChart(object[] objParam)
        {
            AccountModels model = new AccountModels();
            model.logincount = accountOperator.GetloginCount();
            LoginTrendChart chart = new LoginTrendChart();
            DataTable dt = new DataTable();
            string type = objParam[0].ToString();
            string fromdate = objParam[1].ToString();
            string enddate = objParam[2].ToString();
            string controller = objParam[3].ToString();

            switch (type)
            {
                case "ByMonthly":
                    dt = accountOperator.UserLoginByMonthly(controller);
                    chart.title = "By Monthly";
                    break;
                case "ByPlant":
                    dt = accountOperator.UserLoginByPlant(fromdate, enddate, controller);
                    chart.title = "By Plant";
                    break;
                case "ByDepartment":
                    dt = accountOperator.UserLoginByDepartment(fromdate, enddate, controller);
                    chart.title = "By Department";
                    break;
                case "ByName":
                    dt = accountOperator.UserLoginByName(fromdate, enddate, controller);
                    chart.title = "By Name";
                    break;
                case "ByFunction":
                    dt = accountOperator.UserClickByFunction(fromdate, enddate, controller);
                    chart.title = "By Function";
                    break;
            }
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    chart.categories += dt.Rows[i][0].ToString() + ";";
                    chart.data.Add(Convert.ToDouble(dt.Rows[i][1]));
                }
            }
            model.chart = chart;

            return model;
        }

        public string ExecuteUserClickLog(object[] objParam)
        {
            ExecutionResult result = new ExecutionResult();
            string userid = objParam[0].ToString();
            string Controller = objParam[1].ToString();
            string Action = objParam[2].ToString();
            string Plant = objParam[3].ToString();
            result = accountOperator.InsertClickLog(userid, Controller, Action, Plant);
            if (result.Status == false)
                return result.Message;
            else
                return "OK";
        }

        public ExecutionResult UpdateErrorCount(string userid, int errorcount)
        {
            ExecutionResult result = new ExecutionResult();

            result = accountOperator.UpdateErrorCount(userid, errorcount);

            return result;
        }

        public ExecutionResult PlantLoginDetail(object[] objParam)
        {
            ExecutionResult exeRes = new ExecutionResult();
            Dictionary<string, DataTable> dicResult = new Dictionary<string, DataTable>();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            string plant = "", fromdate = "", enddate = "", controller = "", qPhotoType = "";

            if (objParam.Length >= 5)
            {
                plant = objParam[0].ToString();
                fromdate = objParam[1].ToString();
                enddate = objParam[2].ToString();
                controller = objParam[3].ToString();
                qPhotoType = objParam[4].ToString();

                if (!string.IsNullOrEmpty(qPhotoType))
                {
                    switch (qPhotoType)
                    {
                        case "DAILY":
                            DataTable dtDaily = new DataTable();
                            dtDaily = accountOperator.UserLoginTrendDaily(plant, controller);
                            dicResult.Add(qPhotoType, dtDaily);
                            exeRes.Anything = dicResult;
                            break;
                        case "WEEKLY":
                            DataTable dtWeekly = new DataTable();
                            dtWeekly = accountOperator.UserLoginTrendWeekly(plant, controller);
                            dicResult.Add(qPhotoType, dtWeekly);
                            exeRes.Anything = dicResult;
                            break;
                        case "MONTHLY":
                            DataTable dtMonth = new DataTable();
                            dtMonth = accountOperator.UserLoginTrendMonthly(plant, controller);
                            dicResult.Add(qPhotoType, dtMonth);
                            exeRes.Anything = dicResult;
                            break;
                        case "DEPT":
                            DataTable dtDept = new DataTable();
                            dtDept = accountOperator.UserLoginByDepartment(plant, fromdate, enddate, controller);
                            dicResult.Add(qPhotoType, dtDept);
                            exeRes.Anything = dicResult;
                            break;
                        case "USER":
                            DataTable dtUser = new DataTable();
                            dtUser = accountOperator.UserLoginByName(plant, fromdate, enddate, controller);
                            dicResult.Add(qPhotoType, dtUser);
                            exeRes.Anything = dicResult;
                            break;
                        case "LAST":
                            DataTable dtLAST = new DataTable();
                            dtLAST = accountOperator.UserLoginByLast(plant, fromdate, enddate, controller);
                            dicResult.Add(qPhotoType, dtLAST);
                            exeRes.Anything = dicResult;
                            break;
                        default:
                            break;
                    }
                }
                exeRes.Anything = dicResult;
            }
            else
            {
                exeRes.Status = false;
                exeRes.Message = "Para Error";
            }

            return exeRes;
        }

        public DataTable GetLastDetail(string qPlant, string qDays)
        {
            DataTable dt = new DataTable();
            dt = accountOperator.UserLoginByLastDetail(qPlant, qDays);
            return dt;
        }
    }
}
