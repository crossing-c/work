using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using sDEM2100.Utils;

namespace sDEM2100.DataGateway
{
    class AccountOperator
    {
        private InfoLightDBTools mDBTools;
        private Hashtable ht;
        private object[] mClientInfo;
        public AccountOperator(object[] clientInfo, string dbName)
        {
            mDBTools = new InfoLightDBTools(clientInfo, dbName);
            ht = new Hashtable();
            mClientInfo = clientInfo;
        }
        public DataTable GetData()
        {
            DataTable dt = new DataTable();
            string sql = @" select u.user_id,
                                           u.user_name,
                                           u.plant_code,
                                           r.role_name,
                                           u.dept_name,
                                           u.work_place
                                     from dqms.pms_user_t u,dqms.pms_role_t r,SFIS1.C_EMPLOYEE_T@DC_LINK W
                                     where u.role_id=r.role_id and W.EMP_ENG_NAME(+)=UPPER(u.USER_ID) AND W.RESIGN_FLAG(+)='N'
                                     order by U.USER_ID DESC";
            ExecutionResult result = mDBTools.ExecuteQueryDS(sql);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable GetPlant()
        {
            DataTable dt = new DataTable();
            string sql = @" select 'ALL' plant_code from dual
                                            UNION
                                            select 'KSP2' plant_code from dual
                                            UNION
                                            select 'KSP3' plant_code from dual
                                            UNION
                                            select 'KSP4' plant_code from dual
                                            UNION
                                            select 'KSP5' plant_code from dual
                                            UNION
                                            select 'CDP1' plant_code from dual
                                            UNION
                                            select 'CQP1' plant_code from dual
                                            UNION
                                            select 'TW01' plant_code from dual ";
            ExecutionResult result = mDBTools.ExecuteQueryDS(sql);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable GetRole()
        {
            DataTable dt = new DataTable();
            string sql = @" select distinct role_id, role_name
                                          from dqms.pms_role_t
                                         order by role_id asc ";
            ExecutionResult result = mDBTools.ExecuteQueryDS(sql);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public ExecutionResult UpdateUserInfo(string loginuser, string userid, string username, string plantcode, string roleid)
        {
            ExecutionResult result = new ExecutionResult();
            string sql = @" update dqms.pms_user_t u
                            set user_name=:username,plant_code=:plantcode, u.role_id =:roleid,u.update_user=:loginuser,u.update_time=sysdate 
                        where user_id=:userid ";
            ht.Clear();
            ht.Add("loginuser", loginuser);
            ht.Add("userid", userid);
            ht.Add("username", username);
            ht.Add("plantcode", plantcode);
            ht.Add("roleid", roleid);
            result = mDBTools.ExecuteUpdateHt(sql, ht);
            return result;
        }
        public ExecutionResult InsertUserInfo(string loginuser, string userid, string username, string plantcode, string roleid)
        {
            ExecutionResult result = new ExecutionResult();
            string sql = @"  insert into dqms.pms_user_t (user_id,user_name,plant_code,role_id,create_user,create_time,update_user,update_time,dept_name,work_place,error_count)
                                         values
                                           (:userid,:username,:plantcode,:roleid,:loginuser, sysdate,'','',:dept_name,:WORK_PLACE,0) ";
            ht.Clear();
            ht.Add("loginuser", loginuser);
            ht.Add("userid", userid.ToLower());
            ht.Add("username", username);
            ht.Add("plantcode", plantcode);
            ht.Add("roleid", roleid);
            ht.Add("dept_name", GetUserinfo(userid, "WORKING_PLACE"));
            ht.Add("WORK_PLACE", GetUserinfo(userid, "WORKING_PLACE"));
            result = mDBTools.ExecuteUpdateHt(sql, ht);
            return result;
        }
        public ExecutionResult DeleteUser(string userid)
        {
            ExecutionResult result = new ExecutionResult();
            string sql = @"  delete from dqms.pms_user_t where user_id=:userid ";
            ht.Clear();
            ht.Add("userid", userid);
            result = mDBTools.ExecuteUpdateHt(sql, ht);
            return result;
        }
        public int GetloginCount()
        {
            string sql = @" select COUNT(l.USER_ID) AS LoginCount
  from dqms.pms_login_log_t l, DQMS.PMS_USER_T u, dqms.pms_permission_t p
 where l.user_id = u.user_id
   and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
   and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue')
   and l.controller != 'Login'
   and l.controller = p.controller ";
            int a = mDBTools.ExecuteQueryInt(sql);
            return a;
        }
        public DataTable GetLoginStatistics(string userid, string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            ht.Clear();
            string sql = @"select l.USER_ID, COUNT(l.USER_ID) AS LoginCount,u.USER_NAME,u.DEPT_NAME,u.WORK_PLACE
                                               from dqms.pms_login_log_t l,DQMS.PMS_USER_T u 
                                                  where l.user_id=u.user_id
                                                  and l.controller='Login'
                                                  and login_time between to_date(:fromdate,'yyyyMMddHH24miss') 
                                                              and to_date(:enddate,'yyyyMMddHH24miss') ";
            if (userid != "ALL")
            {
                sql += " and l.user_id=:userid ";
                ht.Add("userid", userid);
            }
            sql += @"GROUP BY l.USER_ID,u.USER_NAME,u.DEPT_NAME,u.WORK_PLACE
                                                 order by logincount desc";
            ht.Add("fromdate", fromdate.Replace("-", "") + "080000");
            //    ht.Add("enddate", enddate.Replace("-", "") + "075959");
            ht.Add("enddate", Convert.ToDateTime(enddate.Replace("-", "/")).AddDays(1).ToString("yyyyMMdd") + "075959");
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable GetClickStatistics(string userid, string fromdate, string enddate, string controller)
        {
            DataTable dt = new DataTable();
            ht.Clear();
            ht.Add("fromdate", fromdate.Replace("-", "") + "080000");
            ht.Add("enddate", Convert.ToDateTime(enddate.Replace("-", "/")).AddDays(1).ToString("yyyyMMdd") + "075959");
            string sql1 = string.Empty;
            string sql2 = string.Empty;
            string sql3 = string.Empty;
            if (userid != "ALL")
            {
                sql1 += " and l.user_id = :user_id ";
                sql2 += " and l.user_id = :user_id ";
                sql3 += " and u.user_id = :user_id ";
                ht.Add("user_id", userid.ToLower());
            }
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sql2 += " and l.controller = :controller and l.action=:action ";
                    sql3 += " and t2.controller = :controller and t2.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sql2 += " and l.controller= :controller ";
                    sql3 += " and t2.controller= :controller ";
                    ht.Add("controller", controller);
                }
            }
            string sql = @" 
select u.user_id,
       case u.user_name
         when null then
          e.emp_name
         else
          u.user_name
       end user_name,
       e.dept_name,
       e.working_place work_place,
       nvl(t1.logincount, 0) logincount,
       t2.pms_name,
       t2.action,
       nvl(t2.clickcount, 0) clickcount
  from DQMS.PMS_USER_T u,
       (select e.emp_name,upper(e.emp_eng_name) emp_eng_name,e.dept_name,e.working_place from      sfis1.c_employee_t@dc_link e where resign_flag='N') e,
       (select l.user_id, count(*) logincount
          from dqms.pms_login_log_t L
         where l.user_id not in ('yuhua_lu',
                                 'haiyan_qiao',
                                 'lin_yuan',
                                 'miracle_chen',
                                 'william_jin',
                                 'shiuan_huang',
                                 'harry_zhu',
                                 'yatao_zhu',
                                 'jiajia_xue')
           and l.controller = 'Login'
           " + sql1 + @"
           and l.login_time between
               to_date(:fromdate,'yyyymmddhh24miss') and 
               to_date(:enddate,'yyyymmddhh24miss')
         group by l.user_id) t1,
       (select l.USER_ID,
               l.controller,
               p.pms_name,
               l.action,
               count(*) clickcount
          from dqms.pms_login_log_t L, dqms.pms_permission_t p
         where l.controller = p.controller
           and l.controller != 'Login'
           " + sql2 + @"
           and l.user_id not in ('yuhua_lu',
                                 'haiyan_qiao',
                                 'lin_yuan',
                                 'miracle_chen',
                                 'william_jin',
                                 'shiuan_huang',
                                 'harry_zhu',
                                 'yatao_zhu',
                                 'jiajia_xue')
           and l.login_time between
               to_date(:fromdate,'yyyymmddhh24miss') and 
               to_date(:enddate,'yyyymmddhh24miss')
         group by l.USER_ID, l.controller, l.action, p.pms_name) t2
 where u.user_id = t1.user_id(+)
   and upper(u.user_id) = e.emp_eng_name(+)
   and u.user_id = t2.user_id(+)
   " + sql3;
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public string GetUserinfo(string userid, string Column)
        {
            string info = string.Empty;
            string sql = string.Empty;
            sql = @"select EMP_NAME,upper(EMP_ENG_NAME) EMP_ENG_NAME ,DEPT_NAME ,WORKING_PLACE 
                                    from sfis1.c_employee_t@dc_link where upper(EMP_ENG_NAME) = upper(:userid) AND RESIGN_FLAG='N'";
            Hashtable HT = new Hashtable();
            HT.Clear();
            HT.Add("userid", userid.ToUpper());
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, HT);
            if (result.Status == true)
            {
                DataTable dt = ((DataSet)result.Anything).Tables[0];
                if (dt.Rows.Count > 0)
                {
                    info = dt.Rows[0][Column].ToString();
                }
            }
            return info;
        }

        public DataTable GetUserLoginDetail(string userid, string fromdate, string enddate)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            string sql = string.Empty;
            sql = @"select l.USER_ID,l.LOGIN_TIME
                                  from dqms.pms_login_log_t l,dqms.pms_permission_t p
                                  where l.user_id=:userid
                                  and l.controller = p.controller
                                  and l.login_time between to_date(:fromdate,'yyyyMMddHH24miss') 
                                                      and to_date(:enddate,'yyyyMMddHH24miss')
                                  ORDER BY l.LOGIN_TIME DESC";
            ht.Clear();
            ht.Add("userid", userid);
            ht.Add("fromdate", fromdate.Replace("-", "") + "080000");
            ht.Add("enddate", Convert.ToDateTime(enddate.Replace("-", "/")).AddDays(1).ToString("yyyyMMdd") + "075959");
            //ht.Add("enddate", enddate.Replace("-", "") + "075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable UserLoginByMonthly(string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            }
            string sql = string.Empty;
            sql = @"select SUBSTR(:fromdate1,0,6) as month,count(*) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id
                                        and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate1,'yyyyMMddhh24miss') and to_date(:enddate1,'yyyyMMddhh24miss')
                            union
                            select SUBSTR(:fromdate2,0,6) as month,count(*) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                    where l.user_id=u.user_id
                                        and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue')
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @" and l.login_time between to_date(:fromdate2,'yyyyMMddhh24miss') and to_date(:enddate2,'yyyyMMddhh24miss')
                            union
                            select SUBSTR(:fromdate3,0,6) as month,count(*) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                  where l.user_id=u.user_id
                                        and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue')
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @" and l.login_time between to_date(:fromdate3,'yyyyMMddhh24miss') and to_date(:enddate3,'yyyyMMddhh24miss')
                            union
                            select SUBSTR(:fromdate4,0,6) as month,count(*) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                    where l.user_id=u.user_id
                                        and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue')
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @" and l.login_time between to_date(:fromdate4,'yyyyMMddhh24miss') and to_date(:enddate4,'yyyyMMddhh24miss')
                            union
                           select SUBSTR(:fromdate5,0,6) as month,count(*) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                    where l.user_id=u.user_id
                                        and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue')
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @" and l.login_time between to_date(:fromdate5,'yyyyMMddhh24miss') and to_date(:enddate5,'yyyyMMddhh24miss')
                            union
                            select SUBSTR(:fromdate6,0,6) as month,count(*) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                    where l.user_id=u.user_id
                                        and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue')
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @" and l.login_time between to_date(:fromdate6,'yyyyMMddhh24miss') and to_date(:enddate6,'yyyyMMddhh24miss')
                            union
                            select SUBSTR(:fromdate7,0,6) as month,count(*) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                    where l.user_id=u.user_id
                                        and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue')
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @" and l.login_time between to_date(:fromdate7,'yyyyMMddhh24miss') and to_date(:enddate7,'yyyyMMddhh24miss')";

            ht.Add("fromdate1", DateTime.Now.ToString("yyyyMM") + "01080000");
            ht.Add("enddate1", DateTime.Now.AddMonths(1).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate2", DateTime.Now.AddMonths(-1).ToString("yyyyMM") + "01080000");
            ht.Add("enddate2", DateTime.Now.ToString("yyyyMM") + "01075959");
            ht.Add("fromdate3", DateTime.Now.AddMonths(-2).ToString("yyyyMM") + "01080000");
            ht.Add("enddate3", DateTime.Now.AddMonths(-1).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate4", DateTime.Now.AddMonths(-3).ToString("yyyyMM") + "01080000");
            ht.Add("enddate4", DateTime.Now.AddMonths(-2).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate5", DateTime.Now.AddMonths(-4).ToString("yyyyMM") + "01080000");
            ht.Add("enddate5", DateTime.Now.AddMonths(-3).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate6", DateTime.Now.AddMonths(-5).ToString("yyyyMM") + "01080000");
            ht.Add("enddate6", DateTime.Now.AddMonths(-4).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate7", DateTime.Now.AddMonths(-6).ToString("yyyyMM") + "01080000");
            ht.Add("enddate7", DateTime.Now.AddMonths(-5).ToString("yyyyMM") + "01075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable UserLoginByPlant(string fromdate, string enddate, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            }
            string sql = string.Empty;
            sql = @" 
select case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end  as work_place, sum(t.qty) qty
  from (select l.user_id, count(*) qty
          from dqms.pms_login_log_t  l,
               DQMS.PMS_USER_T       u,
               dqms.pms_permission_t p
         where l.user_id = u.user_id
           and l.controller != 'Login'
           and l.controller = p.controller
         " + sqlWhere + @"
           and l.login_time between to_date(:fromdate, 'yyyyMMddHH24miss') and
               to_date(:enddate, 'yyyyMMddHH24miss')
           and u.user_id not in ('yuhua_lu',
                                 'haiyan_qiao',
                                 'lin_yuan',
                                 'miracle_chen',
                                 'william_jin',
                                 'shiuan_huang',
                                 'harry_zhu',
                                 'yatao_zhu',
                                 'jiajia_xue')
         group by l.user_id) t,
       (select upper(e.emp_eng_name) emp_eng_name,
               e.dept_name,
               e.working_place
          from sfis1.c_employee_t@dc_link e
         where e.resign_flag = 'N'
           and e.emp_eng_name is not null) e
 where upper(t.user_id) = e.emp_eng_name(+)
   and e.dept_name not like '%QOO%'
   and e.dept_name !='製造系統處'
   and e.dept_name not like '%智能%' 
   and (e.working_place in ('KS Plant1',
                           'KS Plant2',
                           'KS Plant3',
                           'KS Plant4',
                           'KS Plant5',
                           'CD Plant',
                           'CQ Plant',
                           'CQSHQ',
                           'PCP',
                           'CKY') 
        or e.working_place like 'F%')
 group by case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end 
 order by qty desc
 ";
            ht.Add("fromdate", fromdate.Replace("-", "") + "080000");
            ht.Add("enddate", Convert.ToDateTime(enddate.Replace("-", "/")).AddDays(1).ToString("yyyyMMdd") + "075959");
            //ht.Add("enddate", enddate.Replace("-", "") + "075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable UserLoginByDepartment(string fromdate, string enddate, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            }
            string sql = string.Empty;
            sql = @"
select *
  from (select  e.dept_name||'_'||case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end dept_name, sum(t.qty) qty
          from (select l.user_id, count(*) qty
                  from dqms.pms_login_log_t  l,
                       DQMS.PMS_USER_T       u,
                       dqms.pms_permission_t p
                 where l.user_id = u.user_id
                   and l.controller != 'Login'
                   and l.controller = p.controller
                      " + sqlWhere + @"
                   and l.login_time between
                       to_date(:fromdate, 'yyyyMMddHH24miss') and
                       to_date(:enddate, 'yyyyMMddHH24miss')
                   and u.user_id not in ('yuhua_lu',
                                         'haiyan_qiao',
                                         'lin_yuan',
                                         'miracle_chen',
                                         'william_jin',
                                         'shiuan_huang',
                                         'harry_zhu',
                                         'yatao_zhu',
                                         'jiajia_xue')
                 group by l.user_id) t,
               (select upper(e.emp_eng_name) emp_eng_name, e.dept_name,e.working_place 
                  from sfis1.c_employee_t@dc_link e 
                 where e.resign_flag = 'N'
                   and e.emp_eng_name is not null) e
         where upper(t.user_id) = e.emp_eng_name(+)
           and e.dept_name not like '%QOO%'
           and e.dept_name !='製造系統處'
           and e.dept_name not like '%智能%' 
           and e.dept_name is not null
         group by e.dept_name,e.working_place 
         order by qty desc)
 where rownum < 21  ";
            ht.Add("fromdate", fromdate.Replace("-", "") + "080000");
            ht.Add("enddate", Convert.ToDateTime(enddate.Replace("-", "/")).AddDays(1).ToString("yyyyMMdd") + "075959");
            //ht.Add("enddate", enddate.Replace("-", "") + "075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable UserLoginByName(string fromdate, string enddate, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            }
            string sql = string.Empty;
            sql = @"select * from(select u.user_name,count(*) qty,u.user_id
                                  from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                             where l.user_id=u.user_id
                              and l.controller!='Login'
                              and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
                              and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue')
                              and l.controller = p.controller " + sqlWhere + @"
                                     and login_time between to_date(:fromdate,'yyyyMMddHH24miss') 
                                                                           and to_date(:enddate,'yyyyMMddHH24miss')
                                 group by u.user_id,u.user_name
                                 order by qty desc)where rownum < 21";
            ht.Add("fromdate", fromdate.Replace("-", "") + "080000");
            ht.Add("enddate", Convert.ToDateTime(enddate.Replace("-", "/")).AddDays(1).ToString("yyyyMMdd") + "075959");
            //ht.Add("enddate", enddate.Replace("-", "") + "075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable UserClickByFunction(string fromdate, string enddate, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            }
            string sql = string.Empty;
            //sql = @" select case when p.pms_name, count(*) qty
            //             from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
            //                 where l.user_id=u.user_id
            //                  and l.controller!='Login'
            //                  and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
            //                  and u.user_id not in ('yuhua_lu',
            //             'haiyan_qiao',
            //             'lin_yuan',
            //             'miracle_chen',
            //             'william_jin',
            //             'shiuan_huang',
            //             'harry_zhu',
            //             'yatao_zhu',
            //             'jiajia_xue')
            //                  and l.controller = p.controller
            //            and l.controller != 'Login' " + sqlWhere + @"
            //            and l.login_time between to_date(:fromdate, 'yyyyMMddHH24miss') and
            //                to_date(:enddate, 'yyyyMMddHH24miss')
            //            group by p.pms_name ";
            sql = @" select case when l.controller='FPYDeepDive' then case when instr(l.action,'Insight')>0 then replace(l.action,'Insight',' Insight FPY') else l.action || '-'||p.pms_name end else p.pms_name end as pms_name, count(*) qty
                         from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                             where l.user_id=u.user_id
                              and l.controller!='Login'
                              and u.dept_name not like '%QOO%' and u.dept_name not like '%智能%' 
                              and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue')
                              and l.controller = p.controller
                        and l.controller != 'Login' " + sqlWhere + @"
                        and l.login_time between to_date(:fromdate, 'yyyyMMddHH24miss') and
                            to_date(:enddate, 'yyyyMMddHH24miss')
                        group by case when l.controller='FPYDeepDive' then case when instr(l.action,'Insight')>0 then replace(l.action,'Insight',' Insight FPY') else l.action || '-'||p.pms_name end else p.pms_name end  ";
            ht.Add("fromdate", fromdate.Replace("-", "") + "080000");
            ht.Add("enddate", Convert.ToDateTime(enddate.Replace("-", "/")).AddDays(1).ToString("yyyyMMdd") + "075959");
            //ht.Add("enddate", enddate.Replace("-", "") + "075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public ExecutionResult InsertClickLog(string userid, string controller, string action, string plant)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            string sql = string.Empty;
            sql = @" insert into dqms.pms_login_log_t
              (user_id,login_from,login_time,controller,action,plant_code)
                    values
                        (:userid, 'N/A', sysdate, :controller,:action,:plant) ";
            ht.Clear();
            ht.Add("userid", userid);
            ht.Add("controller", controller);
            ht.Add("action", action);
            ht.Add("plant", plant.ToUpper());
            result = mDBTools.ExecuteUpdateHt(sql, ht);
            return result;
        }

        public ExecutionResult UpdateErrorCount(string userid, int errorcount)
        {
            ExecutionResult result = new ExecutionResult();
            string sql = @" update dqms.pms_user_t u
                            set u.error_count=:errorcount,update_time=sysdate
                        where user_id=:userid ";
            ht.Clear();
            ht.Add("errorcount", errorcount);
            ht.Add("userid", userid);
            result = mDBTools.ExecuteUpdateHt(sql, ht);
            return result;
        }

        public DataTable UserLoginTrendDaily(string plant, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            } 
            ht.Add("work_place", plant);
            string sql = string.Empty;
            sql = @"  select substr(:fromdate1,0,8) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate1,'yyyyMMddhh24miss') and to_date(:enddate1,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                             union
                            select substr(:fromdate2,0,8) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate2,'yyyyMMddhh24miss') and to_date(:enddate2,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select substr(:fromdate3,0,8) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate3,'yyyyMMddhh24miss') and to_date(:enddate3,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select substr(:fromdate4,0,8) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate4,'yyyyMMddhh24miss') and to_date(:enddate4,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                           select substr(:fromdate5,0,8) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate5,'yyyyMMddhh24miss') and to_date(:enddate5,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select substr(:fromdate6,0,8) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate6,'yyyyMMddhh24miss') and to_date(:enddate6,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select substr(:fromdate7,0,8) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate7,'yyyyMMddhh24miss') and to_date(:enddate7,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place";

            ht.Add("fromdate1", DateTime.Now.AddDays(-6).ToString("yyyyMMdd") + "080000");
            ht.Add("enddate1", DateTime.Now.AddDays(-5).ToString("yyyyMMdd") + "075959");
            ht.Add("fromdate2", DateTime.Now.AddDays(-5).ToString("yyyyMMdd") + "075959");
            ht.Add("enddate2", DateTime.Now.AddDays(-4).ToString("yyyyMMdd") + "075959");
            ht.Add("fromdate3", DateTime.Now.AddDays(-4).ToString("yyyyMMdd") + "075959");
            ht.Add("enddate3", DateTime.Now.AddDays(-3).ToString("yyyyMMdd") + "075959");
            ht.Add("fromdate4", DateTime.Now.AddDays(-3).ToString("yyyyMMdd") + "080000");
            ht.Add("enddate4", DateTime.Now.AddDays(-2).ToString("yyyyMMdd") + "075959");
            ht.Add("fromdate5", DateTime.Now.AddDays(-2).ToString("yyyyMMdd") + "080000");
            ht.Add("enddate5", DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + "075959");
            ht.Add("fromdate6", DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + "080000");
            ht.Add("enddate6", DateTime.Now.AddDays(-0).ToString("yyyyMMdd") + "075959");
            ht.Add("fromdate7", DateTime.Now.AddDays(-0).ToString("yyyyMMdd") + "080000");
            ht.Add("enddate7", DateTime.Now.AddDays(1).ToString("yyyyMMdd") + "075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable UserLoginTrendWeekly(string plant, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            }          
            ht.Add("work_place", plant);
            string sql = string.Empty;
            sql = @"select :weekcode1 as catalogy,nvl(sum(t.qty),0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                       
                                        and u.user_id not in ('yuhua_lu',
                                                             'haiyan_qiao',
                                                             'lin_yuan',
                                                             'miracle_chen',
                                                             'william_jin',
                                                             'shiuan_huang',
                                                             'harry_zhu',
                                                             'yatao_zhu',
                                                             'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate1,'yyyyMMddhh24miss') and to_date(:enddate1,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                          union
                            select :weekcode2 as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate2,'yyyyMMddhh24miss') and to_date(:enddate2,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select :weekcode3 as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate3,'yyyyMMddhh24miss') and to_date(:enddate3,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select :weekcode4 as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate4,'yyyyMMddhh24miss') and to_date(:enddate4,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                           select :weekcode5 as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate5,'yyyyMMddhh24miss') and to_date(:enddate5,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select :weekcode6 as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate6,'yyyyMMddhh24miss') and to_date(:enddate6,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select :weekcode7 as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate7,'yyyyMMddhh24miss') and to_date(:enddate7,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place";
            DataTable dtWeeks = WorkDateConvert.GetRangeWeeks(System.Globalization.CalendarWeekRule.FirstFourDayWeek, 6, mClientInfo);

            if (dtWeeks.Rows.Count > 0)
            {
                for (int i = 0; i < dtWeeks.Rows.Count; i++)
                {
                    string strWeek = dtWeeks.Rows[i]["WEEKS"].ToString();
                    ht.Add("weekcode" + (i + 1).ToString(), "W" + strWeek.Substring(4, 2));
                    ht.Add("fromdate" + (i + 1).ToString(), dtWeeks.Rows[i]["fromday"].ToString().Replace("/", "") + "080000");
                    ht.Add("enddate" + (i + 1).ToString(), DateTime.Parse(dtWeeks.Rows[i]["today"].ToString()).AddDays(1).ToString("yyyyMMdd") + "075959");
                } 
                result = mDBTools.ExecuteQueryDSHt(sql, ht);
                if (result.Status == true)
                    dt = ((DataSet)result.Anything).Tables[0];
            }

            return dt;
        }
        public DataTable UserLoginTrendMonthly(string plant, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            }           
            ht.Add("work_place", plant);
            string sql = string.Empty;         
                                              
            sql = @"select SUBSTR(:fromdate1,0,6) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate1,'yyyyMMddhh24miss') and to_date(:enddate1,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select SUBSTR(:fromdate2,0,6) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate2,'yyyyMMddhh24miss') and to_date(:enddate2,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select SUBSTR(:fromdate3,0,6) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate3,'yyyyMMddhh24miss') and to_date(:enddate3,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select SUBSTR(:fromdate4,0,6) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate4,'yyyyMMddhh24miss') and to_date(:enddate4,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                           select SUBSTR(:fromdate5,0,6) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate5,'yyyyMMddhh24miss') and to_date(:enddate5,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select SUBSTR(:fromdate6,0,6) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate6,'yyyyMMddhh24miss') and to_date(:enddate6,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place 
                            union
                            select SUBSTR(:fromdate7,0,6) as catalogy,nvl(sum(t.qty), 0) as qty from
                        (select l.user_id, nvl(count(*), 0) as qty from dqms.pms_login_log_t l,DQMS.PMS_USER_T u  ,dqms.pms_permission_t p
                                      where l.user_id=u.user_id                                        
                                        and u.user_id not in ('yuhua_lu',
                         'haiyan_qiao',
                         'lin_yuan',
                         'miracle_chen',
                         'william_jin',
                         'shiuan_huang',
                         'harry_zhu',
                         'yatao_zhu',
                         'jiajia_xue') 
                                        and l.controller!='Login'
                                        and l.controller = p.controller " + sqlWhere + @"   and l.login_time between to_date(:fromdate7,'yyyyMMddhh24miss') and to_date(:enddate7,'yyyyMMddhh24miss') group by l.user_id )t,
                                       (select upper(e.emp_eng_name) emp_eng_name,
                                               e.dept_name,
                                               e.working_place
                                          from sfis1.c_employee_t @dc_link e
                                          where e.resign_flag = 'N'
                                           and e.emp_eng_name is not null) e
                                 where upper(t.user_id) = e.emp_eng_name(+)
                                   and e.dept_name not like '%QOO%'
                                   and e.dept_name != '製造系統處'
                                   and e.dept_name not like '%智能%'
                                   and case when e.working_place like 'F%' then 'CHV' else case when e.working_place = 'PCP' then 'CTY' else e.working_place end end =:work_place ";

            ht.Add("fromdate1", DateTime.Now.ToString("yyyyMM") + "01080000");
            ht.Add("enddate1", DateTime.Now.AddMonths(1).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate2", DateTime.Now.AddMonths(-1).ToString("yyyyMM") + "01080000");
            ht.Add("enddate2", DateTime.Now.ToString("yyyyMM") + "01075959");
            ht.Add("fromdate3", DateTime.Now.AddMonths(-2).ToString("yyyyMM") + "01080000");
            ht.Add("enddate3", DateTime.Now.AddMonths(-1).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate4", DateTime.Now.AddMonths(-3).ToString("yyyyMM") + "01080000");
            ht.Add("enddate4", DateTime.Now.AddMonths(-2).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate5", DateTime.Now.AddMonths(-4).ToString("yyyyMM") + "01080000");
            ht.Add("enddate5", DateTime.Now.AddMonths(-3).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate6", DateTime.Now.AddMonths(-5).ToString("yyyyMM") + "01080000");
            ht.Add("enddate6", DateTime.Now.AddMonths(-4).ToString("yyyyMM") + "01075959");
            ht.Add("fromdate7", DateTime.Now.AddMonths(-6).ToString("yyyyMM") + "01080000");
            ht.Add("enddate7", DateTime.Now.AddMonths(-5).ToString("yyyyMM") + "01075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable UserLoginByDepartment(string plant, string fromdate, string enddate, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            }                       
            string sql = string.Empty;
            sql = @"
select *
  from (select  e.dept_name||'_'||case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end catalogy, sum(t.qty) qty
          from (select l.user_id, count(*) qty
                  from dqms.pms_login_log_t  l,
                       DQMS.PMS_USER_T       u,
                       dqms.pms_permission_t p
                 where l.user_id = u.user_id
                   and l.controller != 'Login'
                   and l.controller = p.controller
                      " + sqlWhere + @"
                   and l.login_time between
                       to_date(:fromdate, 'yyyyMMddHH24miss') and
                       to_date(:enddate, 'yyyyMMddHH24miss')
                   and u.user_id not in ('yuhua_lu',
                                         'haiyan_qiao',
                                         'lin_yuan',
                                         'miracle_chen',
                                         'william_jin',
                                         'shiuan_huang',
                                         'harry_zhu',
                                         'yatao_zhu',
                                         'jiajia_xue')
                 group by l.user_id) t,
               (select upper(e.emp_eng_name) emp_eng_name, e.dept_name,e.working_place 
                  from sfis1.c_employee_t@dc_link e 
                 where e.resign_flag = 'N'
                   and e.emp_eng_name is not null) e
         where upper(t.user_id) = e.emp_eng_name(+)
           and e.dept_name not like '%QOO%'
           and e.dept_name !='製造系統處'
           and e.dept_name not like '%智能%' 
           and e.dept_name is not null
           and case when e.working_place  like 'F%' then 'CHV' else case when e.working_place ='PCP' then 'CTY' else e.working_place end end =:work_place 
         group by e.dept_name,e.working_place 
         order by qty desc)
 where rownum <= 10  ";
            ht.Add("fromdate", fromdate.Replace("-", "") + "080000");
            ht.Add("enddate", Convert.ToDateTime(enddate.Replace("-", "/")).AddDays(1).ToString("yyyyMMdd") + "075959");
            ht.Add("work_place", plant);
            //ht.Add("enddate", enddate.Replace("-", "") + "075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable UserLoginByName(string plant, string fromdate, string enddate, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;
            if (controller != "ALL")
            {
                if (controller.IndexOf("FPYDeepDive") > -1)
                {
                    sqlWhere = " and l.controller = :controller and l.action=:action ";
                    ht.Add("controller", controller.Split('-')[1]);
                    ht.Add("action", controller.Split('-')[0]);
                }
                else
                {
                    sqlWhere = " and l.controller = :controller ";
                    ht.Add("controller", controller);
                }
            }
          
          
            string sql = string.Empty;
            sql = @"
select *
  from (select t.user_name as catalogy, sum(t.qty) qty
          from (select l.user_id, count(*) qty,u.user_name 
                  from dqms.pms_login_log_t  l,
                       DQMS.PMS_USER_T       u,
                       dqms.pms_permission_t p
                 where l.user_id = u.user_id
                   and l.controller != 'Login'
                   and l.controller = p.controller
                      " + sqlWhere + @"
                   and l.login_time between
                       to_date(:fromdate, 'yyyyMMddHH24miss') and
                       to_date(:enddate, 'yyyyMMddHH24miss')
                   and u.user_id not in ('yuhua_lu',
                                         'haiyan_qiao',
                                         'lin_yuan',
                                         'miracle_chen',
                                         'william_jin',
                                         'shiuan_huang',
                                         'harry_zhu',
                                         'yatao_zhu',
                                         'jiajia_xue')
                 group by l.user_id,u.user_name) t,
               (select upper(e.emp_eng_name) emp_eng_name, e.dept_name,e.working_place 
                  from sfis1.c_employee_t@dc_link e 
                 where e.resign_flag = 'N'
                   and e.emp_eng_name is not null) e
         where upper(t.user_id) = e.emp_eng_name(+)
           and e.dept_name not like '%QOO%'
           and e.dept_name !='製造系統處'
           and e.dept_name not like '%智能%' 
           and e.dept_name is not null
           and case when e.working_place  like 'F%' then 'CHV' else case when e.working_place ='PCP' then 'CTY' else e.working_place end end =:work_place 
         group by t.user_name 
         order by qty desc)
        where rownum <= 10  ";
            ht.Add("fromdate", fromdate.Replace("-", "") + "080000");
            ht.Add("enddate", Convert.ToDateTime(enddate.Replace("-", "/")).AddDays(1).ToString("yyyyMMdd") + "075959");
            ht.Add("work_place", plant);
            //ht.Add("enddate", enddate.Replace("-", "") + "075959");
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable UserLoginByLast(string plant, string fromdate, string enddate, string controller)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear();
            string sqlWhere = string.Empty;          
            string sql = string.Empty;
            //sql = @"select a.loginfrom ||' Days' as catalogy,count(*) as qty from (
            //        select case when lastlogin>=180 then 180 else case when lastlogin >=90 then 90 else case when lastlogin >=60 then 60 
            //        else case when lastlogin >=30 then 30 else case when lastlogin >=15 then 15 else 7 end end end end end as loginfrom,a.* from 
            //        (select trunc(sysdate- nvl(a.dd,sysdate-180)) as lastlogin, a.* from 
            //        (select u.*,(select max(login_time) from dqms.pms_login_log_t where user_id=u.user_id and controller = 'Login' and login_time>=sysdate-181) as dd
            //        from dqms.pms_user_t u,dqms.pms_role_t r,SFIS1.C_EMPLOYEE_T@DC_LINK e
            //        where u.role_id=r.role_id and e.EMP_ENG_NAME(+)=UPPER(u.USER_ID) AND e.RESIGN_FLAG(+)='N' and e.emp_eng_name is not null and e.dept_name not like '%QOO%'
            //            and e.dept_name !='製造系統處'
            //            and e.dept_name not like '智能%' 
            //        and case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end =:work_place and u.user_id not in ('yuhua_lu',
            //        'haiyan_qiao',
            //        'lin_yuan',
            //        'miracle_chen',
            //        'william_jin',
            //        'shiuan_huang',
            //        'harry_zhu',
            //        'yatao_zhu',
            //        'jiajia_xue')
            //        )a)a where a.lastlogin>=7) a group by a.loginfrom order by loginfrom desc ";       
            sql = @"        select * from (  
                                     select 90 ||' Days' as catalogy,count(*) as qty from 
                                     (select trunc(sysdate- nvl(a.dd,sysdate-90)) as lastlogin, a.* from 
                                     (select u.*,(select max(login_time) from dqms.pms_login_log_t where user_id=u.user_id and controller = 'Login' and login_time>=sysdate-91) as dd
                                     from dqms.pms_user_t u,dqms.pms_role_t r,SFIS1.C_EMPLOYEE_T@DC_LINK e
                                     where u.role_id=r.role_id and e.EMP_ENG_NAME(+)=UPPER(u.USER_ID) AND e.RESIGN_FLAG(+)='N' and e.emp_eng_name is not null and e.dept_name not like '%QOO%'
                                         and e.dept_name !='製造系統處'
                                         and e.dept_name not like '智能%' 
                                     and case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end =:work_place and u.user_id not in ('yuhua_lu',
                                       'haiyan_qiao',
                                       'lin_yuan',
                                       'miracle_chen',
                                       'william_jin',
                                       'shiuan_huang',
                                       'harry_zhu',
                                       'yatao_zhu',
                                       'jiajia_xue')
                                     	)a)a where a.lastlogin>=90
                                      union
                                      select 60 ||' Days' as catalogy,count(*) as qty from 
                                     (select trunc(sysdate- nvl(a.dd,sysdate-60)) as lastlogin, a.* from 
                                     (select u.*,(select max(login_time) from dqms.pms_login_log_t where user_id=u.user_id and controller = 'Login' and login_time>=sysdate-61) as dd
                                     from dqms.pms_user_t u,dqms.pms_role_t r,SFIS1.C_EMPLOYEE_T@DC_LINK e
                                     where u.role_id=r.role_id and e.EMP_ENG_NAME(+)=UPPER(u.USER_ID) AND e.RESIGN_FLAG(+)='N' and e.emp_eng_name is not null and e.dept_name not like '%QOO%'
                                         and e.dept_name !='製造系統處'
                                         and e.dept_name not like '智能%' 
                                     and case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end =:work_place and u.user_id not in ('yuhua_lu',
                                       'haiyan_qiao',
                                       'lin_yuan',
                                       'miracle_chen',
                                       'william_jin',
                                       'shiuan_huang',
                                       'harry_zhu',
                                       'yatao_zhu',
                                       'jiajia_xue')
                                     	)a)a where a.lastlogin>=60
                                      union
                                      select 30 ||' Days' as catalogy,count(*) as qty from 
                                     (select trunc(sysdate- nvl(a.dd,sysdate-30)) as lastlogin, a.* from 
                                     (select u.*,(select max(login_time) from dqms.pms_login_log_t where user_id=u.user_id and controller = 'Login' and login_time>=sysdate-31) as dd
                                     from dqms.pms_user_t u,dqms.pms_role_t r,SFIS1.C_EMPLOYEE_T@DC_LINK e
                                     where u.role_id=r.role_id and e.EMP_ENG_NAME(+)=UPPER(u.USER_ID) AND e.RESIGN_FLAG(+)='N' and e.emp_eng_name is not null and e.dept_name not like '%QOO%'
                                         and e.dept_name !='製造系統處'
                                         and e.dept_name not like '智能%' 
                                     and case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end =:work_place and u.user_id not in ('yuhua_lu',
                                       'haiyan_qiao',
                                       'lin_yuan',
                                       'miracle_chen',
                                       'william_jin',
                                       'shiuan_huang',
                                       'harry_zhu',
                                       'yatao_zhu',
                                       'jiajia_xue')
                                     	)a)a where a.lastlogin>=30
                                      union
                                      select 15 ||' Days' as catalogy,count(*) as qty from 
                                     (select trunc(sysdate- nvl(a.dd,sysdate-15)) as lastlogin, a.* from 
                                     (select u.*,(select max(login_time) from dqms.pms_login_log_t where user_id=u.user_id and controller = 'Login' and login_time>=sysdate-16) as dd
                                     from dqms.pms_user_t u,dqms.pms_role_t r,SFIS1.C_EMPLOYEE_T@DC_LINK e
                                     where u.role_id=r.role_id and e.EMP_ENG_NAME(+)=UPPER(u.USER_ID) AND e.RESIGN_FLAG(+)='N' and e.emp_eng_name is not null and e.dept_name not like '%QOO%'
                                         and e.dept_name !='製造系統處'
                                         and e.dept_name not like '智能%' 
                                     and case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end =:work_place and u.user_id not in ('yuhua_lu',
                                       'haiyan_qiao',
                                       'lin_yuan',
                                       'miracle_chen',
                                       'william_jin',
                                       'shiuan_huang',
                                       'harry_zhu',
                                       'yatao_zhu',
                                       'jiajia_xue')
                                     	)a)a where a.lastlogin>=15) order by catalogy desc";
            ht.Add("work_place", plant);        
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable UserLoginByLastDetail(string qPlant,string qDays)
        {
            ExecutionResult result = new ExecutionResult();
            DataTable dt = new DataTable();
            ht.Clear(); 
            string sql = string.Empty;
            sql = @"select a.user_id,a.user_name,a.DEPT_NAME,a.working_place as WORK_PLACE from (
                                     select trunc(sysdate- nvl(a.dd,sysdate-:qdays)) as lastlogin, a.* from 
                                     (select u.*,case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end as working_place,(select max(login_time) from dqms.pms_login_log_t where user_id=u.user_id and controller = 'Login' and login_time>=sysdate-:qdays-1) as dd
                                     from dqms.pms_user_t u,dqms.pms_role_t r,SFIS1.C_EMPLOYEE_T@DC_LINK e
                                     where u.role_id=r.role_id and e.EMP_ENG_NAME(+)=UPPER(u.USER_ID) AND e.RESIGN_FLAG(+)='N' and e.emp_eng_name is not null and e.dept_name not like '%QOO%'
                                         and e.dept_name !='製造系統處'
                                         and e.dept_name not like '智能%' 
                                     and case when e.working_place like 'F%' then 'CHV' else case when e.working_place='PCP' then 'CTY' else e.working_place end end =:work_place and u.user_id not in ('yuhua_lu',
                                       'haiyan_qiao',
                                       'lin_yuan',
                                       'miracle_chen',
                                       'william_jin',
                                       'shiuan_huang',
                                       'harry_zhu',
                                       'yatao_zhu',
                                       'jiajia_xue')
                                     	)a)a where a.lastlogin>=:qdays  ";
            ht.Add("work_place", qPlant);
            ht.Add("qdays", qDays);
            result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status == true)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
    }
}
