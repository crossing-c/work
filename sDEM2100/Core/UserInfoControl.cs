using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using sDEM2100.DataGateway;
using Compal.MESComponent;

namespace sDEM2100.Core
{
    class UserInfoControl
    {
        private static UserInfoOperator userinfoOperator;
        public UserInfoControl(object[] clientInfo, string DBName)
        {
            userinfoOperator = new UserInfoOperator(clientInfo, DBName);
        }
        public DataTable GetUserInfo(string userid)
        {
            DataTable dt = new DataTable();
            ExecutionResult result = new ExecutionResult();
            result = userinfoOperator.GetData(userid);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable GetUserInfoByEmp(string userid)
        {
            DataTable dt = new DataTable();
            ExecutionResult result = new ExecutionResult();
            result = userinfoOperator.GetDataByEmp(userid);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public ExecutionResult ExecuteLog(string userid)
        {
            ExecutionResult result = new ExecutionResult();
            try
            {
                result = userinfoOperator.GenerateLog(userid);
            }
            catch(Exception ex)
            {
                result.Status = false;
                result.Message = ex.Message;
            }
            return result;
        }
    }
}
