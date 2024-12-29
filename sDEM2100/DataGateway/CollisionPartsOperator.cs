using Compal.MESComponent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace sDEM2100.DataGateway
{
    class CollisionPartsOperator
    {
        private InfoLightDBTools mDBTools;
        private InfoLightDBTools mDCDBTools;
        private Hashtable ht;
        public CollisionPartsOperator(object[] clientInfo, string dbName)
        {
            mDBTools = new InfoLightDBTools(clientInfo, dbName);
            mDCDBTools = new InfoLightDBTools(clientInfo, "DC");
            ht = new Hashtable();
        }
        public DataTable GetCustList(string plant)
        {
            DataTable dt = new DataTable();
            string sql = @" select distinct a.cust_no CUSTOMER
                              FROM sfism4.r_repair_location_summary_t a
                             where  A.workdate_id BETWEEN to_char(sysdate - 90, 'yyyymmdd')||'080000' AND
                                   to_char(sysdate, 'yyyymmdd')||'080000' and a.plant_code=:plant_code 
                                       order by a.cust_no ";
            ht.Clear();
            ht.Add("plant_code", plant);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable GetLineList(string plant)
        {
            DataTable dt = new DataTable();
            string sql = @" select distinct A.LINE_NAME from sfis1.c_line_desc_t a where A.CONTR_TYPE='SMT' and a.factory_id=:plant_code";
            ht.Clear();
            ht.Add("plant_code", plant);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql,ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable GetModelList(string cust_no,string plant,string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            string sql = @" select distinct a.model_name MODEL
  FROM sfism4.r_repair_location_summary_t a,sfis1.c_pcb_board_map_t p
 where A.WORK_DATE BETWEEN :fromdate AND :enddate 
   and a.cust_no = :cust_no
   and a.plant_code =:plant
   AND UPPER(A.REASON_CODE) like '%CRACK%'
   AND A.BOARD_NO = P.PCB_VERSION
   and P.EOP_FALG = 'N'
   /*AND A.BOARD_NO NOT IN
       (SELECT P.VR_VALUE
          FROM SFIS1.C_PARAMETER_INI P
         WHERE P.PRG_NAME = 'BDM2169'
           AND P.VR_ITEM = :plant
           AND P.VR_CLASS = 'QMS_CRACK')*/
  order by a.model_name";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("cust_no", cust_no);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql,ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable GetPcbNoList(string cust_no, string model, string plant, string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            string sql = @"  select distinct a.board_no PCB_NO
  FROM sfism4.r_repair_location_summary_t a,sfis1.c_pcb_board_map_t p
 where A.WORK_DATE BETWEEN :fromdate AND :enddate 
   and a.cust_no = :cust_no
   and a.plant_code=:plant
   and a.model_name like :model ";
            if (cust_no != "A95")
                sql += @" AND A.BOARD_NO LIKE 'LA%' ";
            sql += @" AND UPPER(A.REASON_CODE) like '%CRACK%'
   AND A.BOARD_NO = P.PCB_VERSION
   and P.EOP_FALG = 'N'
   AND a.plant_code=P.plant_code
  order by a.board_no";
            ht.Clear();
            ht.Add("cust_no", cust_no);
            ht.Add("plant", plant);
            ht.Add("model", model + "%");
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable getPointData(string cust_no, string line, string lineside, string model, string board_no,string location, string fromdate, string enddate,string plant)
        {
            
            DataTable dt = new DataTable();
            ht.Clear();
            string sql = @" select t.location, t2.position_x, t2.position_y, sum(t.qty) qty
  from (select distinct a.*
          FROM sfism4.r_repair_location_summary_t a,
               sfism4.r_repair_board_info_t       b
         WHERE a.workdate_id  between  :fromdate  AND  :enddate
           and a.board_no = b.board_no
           and a.location = b.location
           AND A.CUST_NO = :cust_no
            AND A.MODEL_NAME = :model 
            AND A.board_no = :board_no
            AND A.LINE_SIDE = :lineside ";
            if(location!="")
            {
                sql += " AND A.location = :location ";
                ht.Add("location", location);
            }
            sql+=@" and a.plant_code=:plant_code
            AND UPPER(A.REASON_CODE) LIKE '%CRACK%' ) t,
       sfism4.r_repair_board_info_t t2
 where t.board_no = t2.board_no
   and t.location = t2.location
 group by t.location, t2.position_x, t2.position_y 
 ORDER BY QTY DESC";
            //string sql = @" select A.PLANT_CODE,
            //                A.LINE_SIDE,
            //                a.model_name,
            //                a.board_no,
            //                a.location,
            //                b.board_length,
            //                b.board_width,
            //                b.position_x,
            //                b.position_y,
            //                sum(a.qty) qty
            //            FROM sfism4.r_repair_location_summary_t a,
            //                sfism4.r_repair_board_info_t       b
            //            WHERE A.WORK_DATE BETWEEN :fromdate AND :enddate 
            //            and a.board_no = b.board_no
            //            and a.location = b.location
            //            AND A.CUST_NO = :cust_no
            //            AND A.MODEL_NAME = :model 
            //            AND A.board_no = :board_no
            //            AND A.LINE_SIDE = :lineside
            //            and a.plant_code=:plant_code ";
            //if (line != "")
            //{
            //    sql += " AND A.LINE_NAME=:line ";
            //    ht.Add("line", line);
            //}
            //sql += @"   AND UPPER(A.REASON_CODE) LIKE '%CRACK%'
            //            group by A.PLANT_CODE,
            //                    A.LINE_SIDE,
            //                    a.model_name,
            //                    a.board_no,
            //                    a.location,
            //                    b.board_length,
            //                    b.board_width,
            //                    b.position_x,
            //                    b.position_y  
            //            order by qty desc   ";

            ht.Add("cust_no", cust_no);
            ht.Add("lineside", lineside);
            ht.Add("model", model);
            ht.Add("board_no", board_no);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ht.Add("plant_code", plant);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public DataTable getChartData(string cust_no, string line,string lineside, string model, string board_no, string fromdate, string enddate,string plant)
        {
            DataTable dt = new DataTable();
            ht.Clear();
            string sql = @"select r.location, sum(r.qty) qty
  from sfism4.r_repair_location_summary_t r,
       (select *
          from (select a.location, sum(a.qty) as qty
                  from (select distinct a.*
                          FROM sfism4.r_repair_location_summary_t a,
                               sfism4.r_repair_board_info_t       b
                         WHERE A.workdate_id between  :fromdate  AND  :enddate
                           and a.location = b.location
                           and a.board_no = b.board_no
                            AND A.CUST_NO = :cust_no
                            AND A.MODEL_NAME = :model 
                            AND A.BOARD_NO = :board_no
                            AND A.LINE_SIDE = :lineside 
                            and a.plant_code=:plant_code
                            AND UPPER(A.REASON_CODE) LIKE '%CRACK%' ) a
            group by a.location
                 order by qty desc)
         where rownum < 11) t
 where r.workdate_id between  :fromdate  AND  :enddate
   and r.location = t.location
    AND r.CUST_NO = :cust_no
                            AND r.MODEL_NAME = :model 
                            AND r.BOARD_NO = :board_no
                            AND r.LINE_SIDE = :lineside 
                            and r.plant_code=:plant_code
                            AND UPPER(R.REASON_CODE) LIKE '%CRACK%'
group by r.location 
 ORDER BY QTY DESC";
            //string sql = @" select * from(select A.PLANT_CODE,
            //                       A.LINE_SIDE,
            //                       a.model_name,
            //                       a.BOARD_NO,
            //                       a.location,
            //                       sum(a.qty) as qty
            //                  FROM sfism4.r_repair_location_summary_t a
            //                 WHERE A.WORK_DATE BETWEEN :fromdate AND :enddate 
            //                   AND A.CUST_NO = :cust_no
            //                   AND A.MODEL_NAME = :model 
            //                   AND A.BOARD_NO = :board_no
            //                   AND A.LINE_SIDE = :lineside 
            //                   and a.plant_code=:plant_code ";
            //if (line != "")
            //{
            //    sql += " AND A.LINE_NAME=:line ";
            //    ht.Add("line", line);
            //}
            //sql += @"  AND UPPER(A.REASON_CODE) LIKE '%CRACK%'
            //                group by A.PLANT_CODE,
            //                        A.LINE_SIDE,
            //                        a.model_name,
            //                        a.BOARD_NO,
            //                        a.location 
            //                order by qty desc )where rownum < 11";

            ht.Add("cust_no", cust_no);
            ht.Add("lineside", lineside);
            ht.Add("model", model);
            ht.Add("board_no", board_no);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ht.Add("plant_code", plant);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable GetPictureUploadRate(string plant, string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            string sql = @"select  case
                when qty is null then
                 0
                when total_qty is null then
                 100
                when total_qty = 0 then
                 100
                else
                 round(qty / total_qty * 100, 2)
              end rate
  from (select count(*) qty
          from (SELECT t.model_name, t.pcb_version, t.mount_side
                  FROM SFIS1.C_PCB_BOARD_MAP_T t
                 where t.upload_time is not null
                   AND t.EOP_FALG = 'N'
                   and t.PCB_VERSION LIKE 'LA%'
                   and t.plant_code = :plant
                   and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
                   and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')
                 group by t.model_name, t.pcb_version, t.mount_side)) a,
       (select count(*) total_qty
          from (SELECT t.model_name, t.pcb_version, t.mount_side
                  FROM SFIS1.C_PCB_BOARD_MAP_T t
                 where t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
                   and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')
                   and t.PCB_VERSION LIKE 'LA%'
                   AND t.EOP_FALG = 'N'
                   and t.plant_code = :plant
                 group by t.model_name, t.pcb_version, t.mount_side)) b    ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable GetOver24PictureUploadRate(string plant, string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            string sql = @"select case
                when qty is null then
                 0
                when total_qty is null then
                 100
                when total_qty = 0 then
                 100
                else
                 round(qty / total_qty * 100, 2)
              end rate
  from (select count(*) qty
          from (SELECT ceil((t.upload_time - t.alert_time) * 24) TAT,
                       t.model_name,
                       t.pcb_version,
                       t.mount_side
                  FROM SFIS1.C_PCB_BOARD_MAP_T t
                 where t.upload_time is not null
                   and t.PCB_VERSION LIKE 'LA%'
                   AND t.EOP_FALG = 'N'
                   and t.plant_code = :plant
                   and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
                   and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss'))
         WHERE TAT <= 24) a,
       (SELECT count(*) total_qty
          FROM SFIS1.C_PCB_BOARD_MAP_T t
         where t.plant_code = :plant
           and t.PCB_VERSION LIKE 'LA%'
           AND t.EOP_FALG = 'N'
           and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
           and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')) b     ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable GetPictureUploadRateByDay(string plant, string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            string sql = @"select substr(b.create_time, 5, 2) || '/' || substr(b.create_time, 7, 2) create_time, qty, total_qty, case
                when qty is null then
                 0
                when total_qty is null then
                 100
                when total_qty = 0 then
                 100
                else
                 round(qty / total_qty * 100, 2)
              end rate
  from (select create_time, count(*) qty
          from (SELECT TO_CHAR(create_time, 'yyyymmdd') create_time,
                       t.model_name,
                       t.pcb_version,
                       t.mount_side
                  FROM SFIS1.C_PCB_BOARD_MAP_T t
                 where t.upload_time is not null
                   and t.PCB_VERSION LIKE 'LA%'
                   AND t.EOP_FALG = 'N'
                   and t.plant_code = :plant
                   and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
                and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')
                )
         group by create_time) a,
       (select create_time, count(*) total_qty
          from (SELECT TO_CHAR(create_time, 'yyyymmdd') create_time,
                       t.model_name,
                       t.pcb_version,
                       t.mount_side
                  FROM SFIS1.C_PCB_BOARD_MAP_T t
                 where t.plant_code = :plant
                   and t.PCB_VERSION LIKE 'LA%'
                   AND t.EOP_FALG = 'N'
                     and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
                and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')
                )
         group by create_time) b
 WHERE b.create_time = a.create_time(+)     
       order by b.create_time";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable GetPictureUploadRateByWeek(string plant, string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            string sql = @"select 'W' || substr(b.create_time, 5, 2) create_time, qty, total_qty, 
                case
                when qty is null then
                 0
                when total_qty is null then
                 100
                when total_qty = 0 then
                 100
                else
                 round(qty / total_qty * 100, 2)
              end rate
  from (select create_time, count(*) qty
          from (SELECT TO_CHAR(TO_DATE(TO_CHAR(create_time,'yyyymmdd') || '010000', 'YYYYMMDDHH24MISS'),
                  'IYYYIW') create_time,
                       t.model_name,
                       t.pcb_version,
                       t.mount_side
                  FROM SFIS1.C_PCB_BOARD_MAP_T t
                 where t.upload_time is not null
                   and t.PCB_VERSION LIKE 'LA%'
                   AND t.EOP_FALG = 'N'
                   and t.plant_code = :plant
                   and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
                and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')
                )
         group by create_time) a,
       (select create_time, count(*) total_qty
          from (SELECT TO_CHAR(TO_DATE(TO_CHAR(create_time,'yyyymmdd') || '010000', 'YYYYMMDDHH24MISS'),
                  'IYYYIW') create_time,
                       t.model_name,
                       t.pcb_version,
                       t.mount_side
                  FROM SFIS1.C_PCB_BOARD_MAP_T t
                 where t.plant_code = :plant
                   and t.PCB_VERSION LIKE 'LA%'
                   AND t.EOP_FALG = 'N'
                     and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
                and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')
                )
         group by create_time) b
 WHERE b.create_time = a.create_time(+)     
       order by b.create_time   ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable GetPictureUploadRateByMonth(string plant, string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            string sql = @"select 'M' || substr(b.create_time, 5, 2) create_time, qty, total_qty,case
                when qty is null then
                 0
                when total_qty is null then
                 100
                when total_qty = 0 then
                 100
                else
                 round(qty / total_qty * 100, 2)
              end rate
  from (select create_time, count(*) qty
          from (SELECT TO_CHAR(create_time, 'yyyymm') create_time,
                       t.model_name,
                       t.pcb_version,
                       t.mount_side
                  FROM SFIS1.C_PCB_BOARD_MAP_T t
                 where t.upload_time is not null
                   and t.PCB_VERSION LIKE 'LA%'
                   AND t.EOP_FALG = 'N'
                   and t.plant_code = :plant
                   and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
                and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')
                )
         group by create_time) a,
       (select create_time, count(*) total_qty
          from (SELECT TO_CHAR(create_time, 'yyyymm') create_time,
                       t.model_name,
                       t.pcb_version,
                       t.mount_side
                  FROM SFIS1.C_PCB_BOARD_MAP_T t
                 where t.plant_code = :plant
                   and t.PCB_VERSION LIKE 'LA%'
                   AND t.EOP_FALG = 'N'
                     and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
                and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')
                )
         group by create_time) b
 WHERE b.create_time = a.create_time(+)     
       order by b.create_time   ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable GetPictureUploadRateByModel(string plant, string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            string sql = @"SELECT model_name, count(*) qty
          FROM SFIS1.C_PCB_BOARD_MAP_T t
         WHERE UPLOAD_TIME IS NULL
                   and t.PCB_VERSION LIKE 'LA%'
                   AND t.EOP_FALG = 'N'
           and t.plant_code = :plant
           and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
           and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')
         group by model_name
         order by qty desc   ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable GetPictureUploadRateByModelExcel(string plant, string fromdate, string enddate)
        {
            DataTable dt = new DataTable();
            string sql = @"SELECT t.plant_code,
       t.cust_no,
       t.model_name,
       t.pcb_version,
       t.mount_side,
       to_char(t.alert_time,'yyyy-mm-dd hh24:mi:ss') alert_time,
       to_char(t.upload_time,'yyyy-mm-dd hh24:mi:ss') upload_time,
       ROUND((sysdate - t.alert_time) * 24, 1) TAT
  FROM SFIS1.C_PCB_BOARD_MAP_T t
 where t.upload_time is null
   and t.PCB_VERSION LIKE 'LA%'
   AND t.EOP_FALG = 'N'
   and t.plant_code = :plant
   and t.create_time >= to_date(:fromdate ||'000000','yyyymmddhh24miss')
   and t.create_time < to_date(:enddate ||'000000','yyyymmddhh24miss')   ";
            ht.Clear();
            ht.Add("plant", plant);
            ht.Add("fromdate", fromdate);
            ht.Add("enddate", enddate);
            ExecutionResult result = mDBTools.ExecuteQueryDSHt(sql, ht);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }

        public DataTable getFTPParameter()
        {
            DataTable dt = new DataTable();
            string sql = @" select t.component_name path, t.item_key username, t.item_value pwd
  from sfis1.c_menu_parameter_t t
 where t.function_name = 'SMT_BadAnalysis' ";
            ExecutionResult result = mDBTools.ExecuteQueryDS(sql);
            if (result.Status)
                dt = ((DataSet)result.Anything).Tables[0];
            return dt;
        }
        public string ToMESPlant(string rmsplant,ref string cust)
        {
            if (rmsplant == "A95")
            {
                if (cust == "KSA95")
                {
                    cust = "A95";
                    return "KS2";
                }
                if (cust == "VNA95")
                {
                    cust = "A95";
                    return "VN2";
                }
            }
            if (rmsplant == "TW01")
                return "CTY";
            string sqlplant = @"SELECT MES_PLANT FROM SFIS1.C_PLANT_DEF_T T WHERE BU_CODE='PCBG' and rms_plant=:rmsplant";
            ht.Clear();
            ht.Add("rmsplant", rmsplant);
            string mesplant = mDCDBTools.ExecuteQueryStrHt(sqlplant, ht);
            return mesplant;
        }
    }
}
