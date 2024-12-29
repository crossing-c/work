using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class UserInfoOperator
    {
        private InfoLightDBTools mDBTools;
        private Hashtable ht;
        private MESLog meslog;
        public UserInfoOperator(object[] clientInfo, string dbName)
        {
            mDBTools = new InfoLightDBTools(clientInfo, dbName);
            ht = new Hashtable();
            meslog = new MESLog(this.GetType().Name);
        }
        public ExecutionResult GetData(string userid)
        {
            ExecutionResult result = new ExecutionResult();
            string sql = @"select t.user_id,t.user_name,t.plant_code,w.role_id,p.controller,nvl(t.error_count,0) as error_count,nvl(ROUND(TO_NUMBER(sysdate-t.update_time)  * 24 * 60),0) as diff,
                                           t.dept_name,
                                           t.work_place    
                                from dqms.pms_permission_t p,dqms.pms_role_permission_t w,dqms.pms_user_t t
                                where t.role_id=w.role_id and w.pms_id=p.pms_id and t.user_id=:userid ";
            ht.Clear();
            ht.Add("userid", userid);
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == false)
                meslog.Error("GetUserInfo Select error:", result.Message);
            return result;
        }

        public ExecutionResult GetDataByEmp(string userid)
        {
            ExecutionResult result = new ExecutionResult();
            string sql = @"select t.user_id,t.user_name,t.plant_code,w.role_id,p.controller,nvl(t.error_count,0) as error_count,nvl(ROUND(TO_NUMBER(sysdate-t.update_time)  * 24 * 60),0) as diff,
                                           t.dept_name,
                                           t.work_place    
                                from dqms.pms_permission_t p,dqms.pms_role_permission_t w,dqms.pms_user_t t,SFIS1.C_EMPLOYEE_T@DC_LINK e
                                where t.role_id=w.role_id and w.pms_id=p.pms_id and t.user_id=lower(e.emp_eng_name) and e.emp_no=:userid ";
            ht.Clear();
            ht.Add("userid", userid);
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == false)
                meslog.Error("GetUserInfoEmp Select error:", result.Message);
            return result;
        }

        public ExecutionResult GenerateLog(string userid)
        {
            string sql = @"INSERT INTO DQMS.PMS_LOGIN_LOG_T (USER_ID,LOGIN_FROM)VALUES(:userid,'N/A')";
            ht.Clear();
            ht.Add("userid", userid);
            ExecutionResult result = mDBTools.ExecuteUpdateHt(sql, ht);
            return result;
        }
    }
}
