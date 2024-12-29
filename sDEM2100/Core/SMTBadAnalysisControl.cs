using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using sDEM2100.DataGateway;
using System.Data;
using sDEM2100.Beans;
using Compal.MESComponent;
using System.Windows.Forms;
using System.IO;
using Newtonsoft.Json;

namespace sDEM2100.Core
{
    class SMTBadAnalysisControl
    {
        private static SMTBadAnalysisOperator SMTBadAnalysisOperator;
        public SMTBadAnalysisControl(object[] clientInfo, string DBName)
        {
            SMTBadAnalysisOperator = new SMTBadAnalysisOperator(clientInfo, DBName);
        }
        public string getSMTBadAnalysis(object[] objParam)
        {
            string jsonStr = "";
            string type= objParam[0].ToString();
            switch (type)
            {
                case "cust":
                    jsonStr =JsonConvert.SerializeObject(GetCustList(objParam));
                    break;
                case "line":
                    jsonStr = JsonConvert.SerializeObject(GetLineList(objParam));
                    break;
                case "model":
                    jsonStr = JsonConvert.SerializeObject(GetModelList(objParam));
                    break;
                case "board_no":
                    jsonStr = JsonConvert.SerializeObject(GetBoard_noList(objParam));
                    break;
                case "data":
                    jsonStr = JsonConvert.SerializeObject(GetSMTBadAnalysisData(objParam));
                    break;
                case "pictureuploadrate":
                    jsonStr = JsonConvert.SerializeObject(GetPictureUploadRate(objParam));
                    break;
                //case "bydatetrendchart":
                //    jsonStr = JsonConvert.SerializeObject(GetPictureUploadBydateTrendChartRate(objParam));
                //    break;
                //case "bymodeltrendchart":
                //    jsonStr = JsonConvert.SerializeObject(GetPictureUploadByModelTrendChartRate(objParam));
                //    break;
                //case "bymodelexceldata":
                //    jsonStr = JsonConvert.SerializeObject(GetPictureUploadByModelExcelData(objParam));
                //    break;
            }
            return jsonStr;
        }
        public DataTable GetCustList(object[] objParam)
        {
            string plant = objParam[1].ToString();
            DataTable dt = new DataTable();
            switch (plant)
            {
                case "A95":
                    dt.Columns.Add("CUSTOMER");
                    dt.Rows.Add(new object[] { "KSA95" });
                    dt.Rows.Add(new object[] { "VNA95" });
                    break;
                case "KSP2":
                    dt.Columns.Add("CUSTOMER");
                    dt.Rows.Add(new object[] { "A32" });
                    break;
                case "KSP3":
                    dt.Columns.Add("CUSTOMER");
                    dt.Rows.Add(new object[] { "A31" });
                    dt.Rows.Add(new object[] { "RMAA31" });
                    break;
                case "KSP4":
                    dt.Columns.Add("CUSTOMER");
                    dt.Rows.Add(new object[] { "A38" });
                    dt.Rows.Add(new object[] { "A39" });
                    dt.Rows.Add(new object[] { "C38" });
                    dt.Rows.Add(new object[] { "A85" });
                    dt.Rows.Add(new object[] { "N88" });
                    dt.Rows.Add(new object[] { "T12" });
                    dt.Rows.Add(new object[] { "C85" });
                    break;
                case "CQP1":
                    dt.Columns.Add("CUSTOMER");
                    dt.Rows.Add(new object[] { "ABO" });
                    break;
                case "CDP1":
                    dt.Columns.Add("CUSTOMER");
                    dt.Rows.Add(new object[] { "A31" });
                    break;
                case "TW01":
                    dt.Columns.Add("CUSTOMER");
                    dt.Rows.Add(new object[] { "A31" });
                    break;
                case "VNP1":
                    dt.Columns.Add("CUSTOMER");
                    dt.Rows.Add(new object[] { "A31" });
                    break;
                default:
                    string cust="";
                    plant = SMTBadAnalysisOperator.ToMESPlant(plant,ref cust);
                    dt = SMTBadAnalysisOperator.GetCustList(plant);
                    break;
            }
            return dt;
        }
        public DataTable GetLineList(object[] objParam)
        {
            string plant = objParam[1].ToString();
            string cust_no = objParam[2].ToString();
            string fromdate = objParam[3].ToString().Replace("-", "") + "000000";
            string enddate = objParam[4].ToString().Replace("-", "") + "999999";
            plant = SMTBadAnalysisOperator.ToMESPlant(plant,ref cust_no);
            DataTable dt = new DataTable();
            dt = SMTBadAnalysisOperator.GetLineList(plant, cust_no, fromdate,enddate);
            return dt;
        }
        public DataTable GetModelList(object[] objParam)
        {
            string cust_no = objParam[1].ToString();
            string plant = objParam[2].ToString();
            string line = objParam[3].ToString();
            string fromdate = objParam[4].ToString().Replace("-", "") + "000000";
            string enddate = objParam[5].ToString().Replace("-", "") + "999999";
            plant = SMTBadAnalysisOperator.ToMESPlant(plant, ref cust_no);
            DataTable dt = new DataTable();
            dt = SMTBadAnalysisOperator.GetModelList(cust_no, plant,line,fromdate, enddate);
            return dt;
        }
        public DataTable GetBoard_noList(object[] objParam)
        {
            string cust_no = objParam[1].ToString();
            string model = objParam[2].ToString();
            string plant = objParam[3].ToString();
            string line = objParam[4].ToString();
            string fromdate = objParam[5].ToString().Replace("-", "") + "000000";
            string enddate = objParam[6].ToString().Replace("-", "") + "999999";
            plant = SMTBadAnalysisOperator.ToMESPlant(plant, ref cust_no);
            DataTable dt = new DataTable();
            dt = SMTBadAnalysisOperator.GetBoard_noList(cust_no,model, plant,line, fromdate, enddate);
            return dt;
        }
        //public DataTable GetMailfileList(object[] objParam)
        //{
        //    string cust_no = objParam[0].ToString();
        //    string model = objParam[1].ToString();
        //    string pcbno = objParam[2].ToString();
        //    DataTable dt = new DataTable();
        //    dt = SMTBadAnalysisOperator.GetMailfileList(cust_no, model,pcbno);
        //    return dt;
        //}
        public CollisionPartsModels GetSMTBadAnalysisData(object[] objParam)
        {
            string cust_no = objParam[1].ToString();
            string line = objParam[2].ToString();
            string lineside = objParam[3].ToString();
            string model = objParam[4].ToString();
            string pcbno = objParam[5].ToString();
            string station = objParam[6].ToString();
            string location = objParam[7].ToString();
            string fromdate = objParam[8].ToString().Replace("-","") + "000000";
            string enddate = objParam[9].ToString().Replace("-", "") + "999999";
            string plant = objParam[10].ToString().Split('-')[0];
            string imgName = string.Empty;
            string dir = string.Empty;
            if (cust_no == "KSA95")
            {
                dir = "KS2A95";
                plant = SMTBadAnalysisOperator.ToMESPlant(plant, ref cust_no);
            }
            else if (cust_no == "VNA95")
            {
                dir = "VN2A95";
                plant = SMTBadAnalysisOperator.ToMESPlant(plant, ref cust_no);
            }
            else
            {
                plant= SMTBadAnalysisOperator.ToMESPlant(plant, ref cust_no);
                dir = plant;
            }
            CollisionPartsModels TK = new CollisionPartsModels();
            #region FTP
            DataTable dtFTPParameter = SMTBadAnalysisOperator.getFTPParameter();
            if (dtFTPParameter.Rows.Count > 0)
            {
                TK.ftpinfo.ftpUser = dtFTPParameter.Rows[0]["username"].ToString();
                TK.ftpinfo.ftpPwd = dtFTPParameter.Rows[0]["pwd"].ToString();
                //imgName = pcbno + "_" + lineside;// +".png";
                //TK.ImgName = imgName;
                    TK.ftpinfo.ftpPath = dtFTPParameter.Rows[0]["path"].ToString() + "/PCB_Photo/" + dir + "/" + lineside + "/";
            }
            //DownLoad Image
            string Localpath = Application.StartupPath + "\\DQMS\\SMTBadAnalysis";
            if (!System.IO.Directory.Exists(Localpath))
                System.IO.Directory.CreateDirectory(Localpath);
            FTPOperation ftp = new FTPOperation(new Uri(TK.ftpinfo.ftpPath), TK.ftpinfo.ftpUser, TK.ftpinfo.ftpPwd);
            foreach (FileStruct file in ftp.ListFiles())
            {
                //file.Name ：pcbno_lineside_length_width
                if (file.Name.Contains(pcbno + "_" + lineside) && file.Name != (pcbno + "_" + lineside))
                {
                    if (!System.IO.File.Exists(Localpath + "/" + file.Name))
                    {
                        bool flag = ftp.DownloadFile(file.Name, Localpath);
                    }
                    byte[] bytes = File.ReadAllBytes(Localpath + "/" + file.Name);
                    TK.ImgBytes = bytes;
                    File.Delete(Localpath + "/" + file.Name);
                    TK.ImgName = file.Name;
                    TK.ImgWidth = Convert.ToDouble(file.Name.Split('_')[2]);
                    string h = file.Name.Split('_')[3];
                    TK.ImgHeight = Convert.ToDouble(h.Substring(0, h.Length - 4));

                    break;
                }
            }
            #endregion
            DataTable dt = SMTBadAnalysisOperator.getPointData(cust_no,line,lineside,model, pcbno,station,location, fromdate,enddate,plant);
            if (dt.Rows.Count > 0)
            {
                string plant_code = plant;
                string line_side = lineside;
                double Top1Qty= Convert.ToDouble(dt.Rows[0]["QTY"].ToString());
                for (int i=0;i<dt.Rows.Count;i++)
                {
                    double qty= Convert.ToDouble(dt.Rows[i]["QTY"].ToString());
                    CPdata data = new CPdata();
                    data.x = Convert.ToDouble(dt.Rows[i]["POSITION_X"].ToString());
                    data.y = Convert.ToDouble(dt.Rows[i]["POSITION_Y"].ToString());
                    if (data.x < TK.ImgWidth-10 && data.y < TK.ImgHeight-10&&TK.CollisionPartsData.Count<100)
                    {
                        data.country = dt.Rows[i]["LOCATION"].ToString();
                        data.name = dt.Rows[i]["QTY"].ToString();
                        if (i == 0)
                        {
                            if (qty > 1)
                                data.color = "red";
                            else
                                data.color = "yellow";
                        }
                        else
                        {
                            if (Top1Qty == Convert.ToDouble(dt.Rows[i]["QTY"].ToString()))
                            {
                                if (qty > 1)
                                    data.color = "red";
                                else
                                    data.color = "yellow";
                            }
                            else
                            {
                                if (qty > 1)
                                    data.color = "pink";
                                else
                                    data.color = "yellow";
                            }
                        }
                        TK.CollisionPartsData.Add(data);
                    }
                    else
                    {

                    }
                }
            }
            //chart2
            #region
            CPchart chart = new CPchart();
            DataTable dt2 = SMTBadAnalysisOperator.getChartData(cust_no, line, lineside, model, pcbno, station, fromdate, enddate, plant);
            if (dt2.Rows.Count > 0)
            {
                var LOCATIONList = from t in dt2.AsEnumerable()
                            group t by new { t1 = t.Field<string>("LOCATION") } into m
                            select new
                            {
                                location = m.Key.t1,
                                qty = m.Sum(n => n.Field<decimal>("QTY"))
                            };
                LOCATIONList=LOCATIONList.ToList().OrderByDescending(q => q.qty);
               
                DataTable dtReason_Code = dt2.DefaultView.ToTable(true, "REASON_CODE");
                foreach (DataRow drReason_Code in dtReason_Code.Rows)
                {
                    CPchart chart2 = new Beans.CPchart();
                    chart2.name = drReason_Code["REASON_CODE"].ToString();
                    chart.categories = "";
                    if (LOCATIONList.ToList().Count > 0)
                    {
                        LOCATIONList.ToList().ForEach(q =>
                        {
                                chart.categories += q.location + ";";
                                CPchart2data data0 = new Beans.CPchart2data();
                                var result = dt2.AsEnumerable().Where(a => a.Field<string>("REASON_CODE") == drReason_Code["REASON_CODE"].ToString()
                                && a.Field<string>("LOCATION") == q.location).ToArray();
                                if (result.Length > 0)
                                    data0.y = Convert.ToDouble(result[0]["QTY"].ToString());
                                else
                                    data0.y = null;
                                chart2.data.Add(data0);
                        });
                    }
                    TK.CollisionPartsChart2.Add(chart2);
                }
            }
                    //DataTable dt2 = SMTBadAnalysisOperator.getChartData(cust_no, line,lineside, model, pcbno,station, fromdate, enddate,plant);
                    //if(dt2.Rows.Count>0)
                    //{
                    //    double Top1Qty = Convert.ToDouble(dt2.Rows[0]["QTY"].ToString());
                    //    for (int i=0;i<dt2.Rows.Count;i++)
                    //    {
                    //        CPchart2data data = new CPchart2data();
                    //        double qty = Convert.ToDouble(dt2.Rows[i]["QTY"].ToString());
                    //        chart.categories += dt2.Rows[i]["LOCATION"].ToString() + ";";
                    //        data.y=Convert.ToDouble(dt2.Rows[i]["QTY"]);
                    //        if (i == 0)
                    //        {
                    //            if (qty > 1)
                    //                data.color = "red";
                    //            else
                    //                data.color = "yellow";
                    //        }
                    //        else
                    //        {
                    //            if (Top1Qty == Convert.ToDouble(dt2.Rows[i]["QTY"].ToString()))
                    //            {
                    //                if (qty > 1)
                    //                    data.color = "red";
                    //                else
                    //                    data.color = "yellow";
                    //            }
                    //            else
                    //            {
                    //                if (qty > 1)
                    //                    data.color = "pink";
                    //                else
                    //                    data.color = "yellow";
                    //            }
                    //        }
                    //        chart.data.Add(data);
                    //    }
                    //}
                    #endregion
                    TK.CollisionPartsChart = chart;
            return TK;
        }
        public object GetPictureUploadRate(object[] objParam)
        {
            string plant = objParam[1].ToString();
            string date = objParam[2].ToString();
            string cust = "";
            plant = SMTBadAnalysisOperator.ToMESPlant(plant,ref cust);
            double rate1 = 0.0, rate2 = 0.0;
            string fromdate = "", enddate = "";
            if(date.ToUpper().Contains("M"))
            {

            }
            if(date.ToUpper().Contains("W"))
            {

            }
            DataTable dt1 = SMTBadAnalysisOperator.GetPictureUploadRate(plant,fromdate,enddate);
            if (dt1.Rows.Count > 0)
                rate1 = Convert.ToDouble(dt1.Rows[0][0].ToString());
            DataTable dt2 = SMTBadAnalysisOperator.GetOver24PictureUploadRate(plant, fromdate, enddate);
            if (dt2.Rows.Count > 0)
                rate2 = Convert.ToDouble(dt2.Rows[0][0].ToString());
            var data = new
            {
                rate1 = rate1,
                rate2 = rate2
            };
            return data;
        }

    }
}
