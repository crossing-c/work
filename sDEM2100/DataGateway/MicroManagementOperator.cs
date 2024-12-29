using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class MicroManagementOperator
    {
        private InfoLightDBTools mDBTools;
        private InfoLightDBTools mDCDBTools;
        private InfoLightMSTools mDBmsTools;
        private InfoLightDBTools mHGPGTools;
        public MicroManagementOperator(object[] clientInfo, string dbName)
        {
            mDBTools = new InfoLightDBTools(clientInfo, dbName);
            mDCDBTools = new InfoLightDBTools(clientInfo, "DC");
            mDBmsTools = new InfoLightMSTools(clientInfo, "WARROOM");
            mHGPGTools = new InfoLightDBTools(clientInfo, "HGPG");
        }
        public DataTable getCustList(string vPlant)
        {
            ExecutionResult exeRes = new ExecutionResult();
            DataTable dtResult = new DataTable();
            Hashtable ht = new Hashtable();
            string sql = @" select b.data_value as CUSTOMER 
                            from sfis1.c_data_dictionary_t b 
                            where b.data_code=:plant and b.data_type='PLANT_CUST' and b.data_desc='PCBG'  ";
            ht.Clear();
            ht.Add("plant", vPlant);

            exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
            if (exeRes.Status)
            {
                DataSet ds = (DataSet)exeRes.Anything;
                if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                {
                    dtResult = ds.Tables[0];
                }
            }

            return dtResult;
        }

        private string getCust(string vPlant)
        {
            string res = "";
            Hashtable ht = new Hashtable();
            string sql = @"select b.data_value 
                            from sfis1.c_data_dictionary_t b 
                            where b.data_code=:plant and b.data_type='PLANT_CUST' and b.data_desc='PCBG'  ";
            ht.Clear();
            ht.Add("plant", vPlant);
            try
            {
                res = mDCDBTools.ExecuteQueryStrHt(sql, ht);
            }
            catch  
            {
                res = "";
            }
            return res;
        }

        public string getSapPlant(string vPlant)
        {
            string res = "";
            Hashtable ht = new Hashtable();
            string sql = @"select b.sap_plant 
                            from sfis1.c_plant_def_t b 
                            where b.rms_plant=:plant and b.bu_code='PCBG'  ";
            ht.Clear();
            ht.Add("plant", vPlant);
            try
            {
                res = mDCDBTools.ExecuteQueryStrHt(sql, ht);
            }
            catch 
            {
                res = "";
            }
            return res;
        }

        public DataTable GetFailRateTrend(string vPlant, string vDateType, string vFDate, string vEdate, string vQType, string vQCust)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable();
            string sqlType1 = "";
            string strCust = "", strSap = "";
            string strExSql = "", strExType = "";
            if (vPlant.IndexOf("-") > -1)
            {
                string[] bb = vPlant.Split('-');
                vPlant = bb[0];
                strExType = bb[1];
                if (strExType == "DOCK")
                    strExSql = " and (a.prod_type in ('DOCKING') or a.cust_no='A76')";
                else
                    strExSql = " and (a.prod_type IN ('PAD', 'NB', 'AIO') or a.cust_no='A76')";
            }
            strCust = getCust(vPlant);
            strSap = getSapPlant(vPlant);
            if (vDateType == "Daily")
                sqlType1 = " a.work_date ";
            else if (vDateType == "Weekly")
                sqlType1 = " to_char(to_date(a.work_date,'YYYYMMDD'),'IYYY') ||'W'|| to_char(to_date(a.work_date,'YYYYMMDD'),'IW') ";
            else
                sqlType1 = " substr(a.work_date,1,4)|| 'M'|| substr(a.work_date,5,2) ";
            if (vQCust != "ALL")
                strExSql += " and a.cust_no= :cust ";
            else
                strExSql += " and instr(:cust,a.cust_no ) >0 ";
            string sql = "";
            #region SQL          
            sql = @"select  x.date1,
                            case when (y.input>0) then
                            round(x.qty / y.input, 4) * 100 
                            else 
                            0 
                            end as value 
                          from (select " + sqlType1 + @" as date1, count(0) qty
                                  from (select a.*,nvl((select d.model_serial from sfis1.c_model_serial_def_t d where a.model_name = d.model_name and d.plant_code=:plant and d.bu_code='PCBG'            
                                          ),a.model_name) as model_serial from sfism4.r_error_info_t a)a, sfis1.c_data_dictionary_t b 
                                 where 1=1 and a.work_date between :qcond and :qcond2 " + strExSql;
            if (vPlant == "CQP1" && (vQCust == "ABO" || vQCust == "N88"))
                sql += " AND b.create_user=:qcust ";
            else
                sql += " AND b.create_user='OTHER' ";
            sql += @"              and a.plant_code like :sap           
                                   and a.process='FATP' AND a.test_code=b.data_value AND b.data_code=:qtype 
                                 group by " + sqlType1 + @",b.data_code) x,
                               (select  " + sqlType1 + @" as date1,nvl(sum(a.output_qty),0) input
                                  from (select a.*,nvl((select d.model_serial from sfis1.c_model_serial_def_t d where a.model_name=d.model_name and d.plant_code=:plant and d.bu_code=a.bu_code
                                          ),a.model_name) as model_serial from sfism4.r_kpi_fpy_t a)a 
                                 where a.bu_code = 'PCBG'
                                   and a.plant_code like :sap and a.work_date between :qcond and :qcond2 " + strExSql + @"      
                                   and a.process = 'FATP' and a.sub_process in ('ASSY', 'CELL', 'REPAIR', 'TNB', 'DOCK') 
                                   group by " + sqlType1 + @") y 
                                where x.date1 = y.date1 order by x.date1";
            #endregion

            ht.Clear();
            if (vQCust != "ALL")
                ht.Add("cust", vQCust);
            else
                ht.Add("cust", strCust);
            if (strSap.IndexOf("CQ") > -1)
                ht.Add("sap", "CQ%");
            else if (strSap.IndexOf("CD") > -1)
                ht.Add("sap", "CD%");
            else
                ht.Add("sap", strSap);
            ht.Add("plant", vPlant.Replace("VN76", "VNP1"));
            ht.Add("qcond", vFDate);
            ht.Add("qcond2", vEdate);
            ht.Add("qtype", vQType);
            if (vPlant == "CQP1" && (vQCust == "ABO" || vQCust == "N88"))
                ht.Add("qcust", vQCust);
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        public DataTable GetErrorTable(string vPlant, string vDataType, string vFDate, string vEdate, string vQType, string vQCust, string vTop)
        {
            ExecutionResult exeRes = new ExecutionResult();
            exeRes.Status = true;
            exeRes.Message = string.Empty;
            Hashtable ht = new Hashtable();
            DataTable dtResult = new DataTable(); 
            string strCust = "", strSap = "";
            string strExSql = "", strExType = "";
            if (vPlant.IndexOf("-") > -1)
            {
                string[] bb = vPlant.Split('-');
                vPlant = bb[0];
                strExType = bb[1];
            }
            if (strExType == "DOCK")
                strExSql = " and (a.prod_type in ('DOCKING') or a.cust_no='A76')";
            else
                strExSql = " and (a.prod_type IN ('PAD', 'NB', 'AIO') or a.cust_no='A76')";
            strCust = getCust(vPlant);
            strSap = getSapPlant(vPlant);

            if (vQCust != "ALL")
                strExSql += " and a.cust_no= :cust ";
            else
                strExSql += " and instr(:cust,a.cust_no ) >0 ";
            string sql = "";
            #region SQL     
            if (vDataType == "MODEL")
            {
                sql = @"SELECT * FROM (   
                            select x.catalogy,
                               case when (y.input>0) then
                               round(x.qty / y.input, 4) * 100 
                               else 
                               0 
                               end as FR,x.qty,x.percents
                          from (select a.model_name AS catalogy,count(0) qty,round(ratio_to_report(count(0)) over(), 4) * 100 percents 
                                  from (select a.*,nvl((select d.model_serial from sfis1.c_model_serial_def_t d where a.model_name = d.model_name and d.plant_code=:plant and d.bu_code='PCBG'            
                                          ),a.model_name) as model_serial from sfism4.r_error_info_t a)a, sfis1.c_data_dictionary_t b 
                                 where 1=1 and a.work_date between :qcond and :qcond2 " + strExSql;
                if (vPlant == "CQP1" && (vQCust == "ABO" || vQCust == "N88"))
                    sql += " AND b.create_user=:qcust ";
                else
                    sql += " AND b.create_user='OTHER' ";
                sql += @"              and a.plant_code like :sap           
                                   and a.process='FATP' AND a.test_code=b.data_value AND b.data_code=:qtype 
                                 group by a.model_name) x,
                               (select nvl(sum(a.output_qty),0) input
                                  from (select a.*,nvl((select d.model_serial from sfis1.c_model_serial_def_t d where a.model_name=d.model_name and d.plant_code=:plant and d.bu_code=a.bu_code
                                          ),a.model_name) as model_serial from sfism4.r_kpi_fpy_t a)a 
                                 where a.bu_code = 'PCBG'
                                   and a.plant_code like :sap and a.work_date between :qcond and :qcond2 " + strExSql + @"      
                                   and a.process = 'FATP' and a.sub_process in ('ASSY', 'CELL', 'REPAIR', 'TNB', 'DOCK') 
                                  ) y 
                                 ORDER by x.qty desc,x.catalogy) WHERE rownum <= :itop";
            }
            else if (vDataType == "STATION")
            {
                sql = @"SELECT * FROM (   
                            select x.catalogy,
                               case when (y.input>0) then
                               round(x.qty / y.input, 4) * 100 
                               else 
                               0 
                               end as FR,x.qty,x.percents
                          from (select d.data_value AS catalogy,count(0) qty,round(ratio_to_report(count(0)) over(), 4) * 100 percents 
                                  from (select a.*,nvl((select d.model_serial from sfis1.c_model_serial_def_t d where a.model_name = d.model_name and d.plant_code=:plant and d.bu_code='PCBG'            
                                          ),a.model_name) as model_serial from sfism4.r_error_info_t a)a, sfis1.c_data_dictionary_t b,sfis1.c_data_dictionary_t d   
                                 where 1=1 and a.work_date between :qcond and :qcond2 " + strExSql;
                if (vPlant == "CQP1" && (vQCust == "ABO" || vQCust == "N88"))
                    sql += " AND b.create_user=:qcust ";
                else
                    sql += " AND b.create_user='OTHER' ";
                sql += @"          and a.plant_code like :sap  and a.group_name = d.data_code and d.data_type = 'FPY_GROUP'           
                                   and a.process='FATP' AND a.test_code=b.data_value AND b.data_code=:qtype 
                                 group by d.data_value) x,
                (select nvl(sum(a.output_qty),0) input
                                  from (select a.*,nvl((select d.model_serial from sfis1.c_model_serial_def_t d where a.model_name=d.model_name and d.plant_code=:plant and d.bu_code=a.bu_code
                                          ),a.model_name) as model_serial from sfism4.r_kpi_fpy_t a)a 
                                 where a.bu_code = 'PCBG'
                                   and a.plant_code like :sap and a.work_date between :qcond and :qcond2 " + strExSql + @"      
                                   and a.process = 'FATP' and a.sub_process in ('ASSY', 'CELL', 'REPAIR', 'TNB', 'DOCK') 
                                  ) y 
                                 ORDER by x.qty desc,x.catalogy) WHERE rownum <= :itop";
                //sql = @"SELECT * FROM (   
                //            select x.catalogy,
                //               case when (y.input>0) then
                //               round(x.qty / y.input, 4) * 100 
                //               else 
                //               0 
                //               end as FR,x.qty,x.percents
                //          from (select d.data_value AS catalogy,count(0) qty,round(ratio_to_report(count(0)) over(), 4) * 100 percents 
                //                  from (select a.*,nvl((select d.model_serial from sfis1.c_model_serial_def_t d where a.model_name = d.model_name and d.plant_code=:plant and d.bu_code='PCBG'            
                //                          ),a.model_name) as model_serial from sfism4.r_error_info_t a)a, sfis1.c_data_dictionary_t b,sfis1.c_data_dictionary_t d   
                //                 where 1=1 and a.work_date between :qcond and :qcond2 " + strExSql;
                //if (strCust != "A76")
                //    sql += " and a.FPY_FLAG = 'Y' ";
                //if (vPlant == "CQP1" && (vQCust == "ABO" || vQCust == "N88"))
                //    sql += " AND b.create_user=:qcust ";
                //sql += @"          and a.plant_code like :sap  and a.group_name = d.data_code and d.data_type = 'FPY_GROUP'           
                //                   and a.process='FATP' AND a.test_code=b.data_value AND b.data_code=:qtype 
                //                 group by d.data_value) x,
                //               (select d.data_value as catalogy,
                //                         sum(a.pass_qty + a.repass_qty+a.fail_qty+a.refail_qty) as input
                //                    from sfis1.c_data_dictionary_t d, (select a.*,nvl((select d.model_serial from sfis1.c_model_serial_def_t d where a.model_name = d.model_name and d.plant_code =:plant and d.bu_code = 'PCBG'
                //                          ),a.model_name) as model_serial from sfism4.r_station_summary_t a)a
                //                    where a.plant_code like :sap
                //                          and a.date_type = 'D'
                //                          and a.mo_type = 'ZP01'
                //                          and instr(:cust,a.cust_no ) > 0
                //                          and a.from_db not like '%SMT%' " + strExSql + @"
                //                          and a.group_name = d.data_code
                //                          and d.data_type = 'FPY_GROUP'
                //                          and((a.line_name not like '%REP%') or
                //                        (a.line_name like '%REP%' and
                //                         TRIM(a.group_name) not in
                //                         ('STRU', 'STRU2', 'AFT', 'TFF', 'TPDL', 'TPDL_IN')))
                //                         and a.group_name not like 'REPAIR%'
                //                         and a.group_name not like 'OFFLINE%'
                //                         and a.group_name not like '%OFF'
                //                         and a.group_name not like '%_BR%'
                //                         and a.line_name not like '%TNB%'
                //                      group by d.data_value) y 
                //                where x.catalogy = y.catalogy ORDER by x.qty desc,x.catalogy) WHERE rownum <= :itop";
            }
            #endregion
            ht.Clear();
            if (vQCust != "ALL")
                ht.Add("cust", vQCust);
            else
                ht.Add("cust", strCust);
            if (strSap.IndexOf("CQ") > -1)
                ht.Add("sap", "CQ%");
            else if (strSap.IndexOf("CD") > -1)
                ht.Add("sap", "CD%");
            else
                ht.Add("sap", strSap);
            ht.Add("plant", vPlant.Replace("VN76", "VNP1"));
            ht.Add("qcond", vFDate);
            ht.Add("qcond2", vEdate);
            ht.Add("qtype", vQType);
            ht.Add("itop", vTop);
            if (vPlant == "CQP1" && (vQCust == "ABO" || vQCust == "N88"))
                ht.Add("qcust", vQCust);
            try
            {
                exeRes = mDCDBTools.ExecuteQueryDSHt(sql, ht);
                if (exeRes.Status)
                {
                    DataSet ds = (DataSet)exeRes.Anything;
                    if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
                    {
                        dtResult = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                exeRes.Status = false;
                exeRes.Message = ex.Message;
            }
            return dtResult;
        }

        private string getSubProcess(string vMProcess, string vCust)
        {
            string res = "";
            Hashtable ht = new Hashtable();
            string sql = @" select b.data_value 
                            from sfis1.c_data_dictionary_t b 
                            where  b.data_type=:datatype and b.data_code=:cust and b.data_desc='PCBG'  ";
            ht.Clear();
            ht.Add("datatype", "PROCESS_" + vMProcess);
            ht.Add("cust", vCust);
            try
            {
                res = mDCDBTools.ExecuteQueryStrHt(sql, ht);
                if (res == "" || res == null)
                {
                    if (vMProcess == "SMT")
                        res = " and a.sub_process in ('SMT')  ";
                    else if (vMProcess == "PCB")
                        res = " and a.sub_process in ('PCB')  ";
                    else if (vMProcess == "PACK")
                        res = " and a.sub_process in ('PACK')  ";
                    else
                        res = " and a.sub_process in ('ASSY', 'CELL', 'REPAIR', 'TNB', 'DOCK')  ";
                }
                else if (res == "N/A")
                    res = "";
            }
            catch 
            {
                res = "";
            }
            return res;
        }
    }
}
