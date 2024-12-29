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
using System.Globalization;

namespace sDEM2100.Core
{
    class CollisionPartsControl
    {
        private static CollisionPartsOperator collisionPartsOperator;
        public CollisionPartsControl(object[] clientInfo, string DBName)
        {
            collisionPartsOperator = new CollisionPartsOperator(clientInfo, DBName);
        }
        public string getCollisionParts(object[] objParam)
        {
            string jsonStr = "";
            string type = objParam[0].ToString();
            switch (type)
            {
                case "cust":
                    jsonStr = JsonConvert.SerializeObject(GetCustList(objParam));
                    break;
                //case "line":
                //    jsonStr = JsonConvert.SerializeObject(GetLineList(objParam));
                //    break;
                case "model":
                    jsonStr = JsonConvert.SerializeObject(GetModelList(objParam));
                    break;
                case "board_no":
                    jsonStr = JsonConvert.SerializeObject(GetPcbNoList(objParam));
                    break;
                case "data":
                    jsonStr = JsonConvert.SerializeObject(GetCollisionPartsData(objParam));
                    break;
                case "pictureuploadrate":
                    jsonStr = JsonConvert.SerializeObject(GetPictureUploadRate(objParam));
                    break;
                case "bydatetrendchart":
                    jsonStr = JsonConvert.SerializeObject(GetPictureUploadRateTrendChart(objParam));
                    break;
                case "bymodelexceldata":
                    jsonStr = JsonConvert.SerializeObject(GetPictureUploadByModelExcelData(objParam));
                    break;
            }
            return jsonStr;
        }
    
        public DataTable GetCustList(object[] objParam)
        {
            DataTable dt = new DataTable();
            string plant = objParam[1].ToString();
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
                    string cust = "";
                    plant = collisionPartsOperator.ToMESPlant(plant,ref cust);
                    dt = collisionPartsOperator.GetCustList(plant);
                    break;
            }
            return dt;
        }
        public DataTable GetLineList(object[] objParam)
        {
            string cust_no = objParam[1].ToString();
            string plant = objParam[2].ToString();
            plant = collisionPartsOperator.ToMESPlant(plant,ref cust_no);
            DataTable dt = new DataTable();
            dt = collisionPartsOperator.GetLineList(plant);
            return dt;
        }
        public DataTable GetModelList(object[] objParam)
        {
            string cust_no = objParam[1].ToString();
            string plant = objParam[2].ToString();
            string fromdate = objParam[3].ToString().Replace("-", "");
            string enddate = objParam[4].ToString().Replace("-", "");
            plant = collisionPartsOperator.ToMESPlant(plant,ref cust_no);
            DataTable dt = new DataTable();
            dt = collisionPartsOperator.GetModelList(cust_no, plant,fromdate, enddate);
            return dt;
        }
        public DataTable GetPcbNoList(object[] objParam)
        {
            string cust_no = objParam[1].ToString();
            string model = objParam[2].ToString();
            string plant = objParam[3].ToString();
            string fromdate = objParam[4].ToString().Replace("-", "");
            string enddate = objParam[5].ToString().Replace("-", "");
            plant = collisionPartsOperator.ToMESPlant(plant,ref cust_no);
            DataTable dt = new DataTable();
            dt = collisionPartsOperator.GetPcbNoList(cust_no,model, plant, fromdate, enddate);
            return dt;
        }
        public CollisionPartsModels GetCollisionPartsData(object[] objParam)
        {
            string cust_no = objParam[1].ToString();
            string line = objParam[2].ToString();
            string lineside = objParam[3].ToString();
            string model = objParam[4].ToString();
            string pcbno = objParam[5].ToString();
            string location = objParam[6].ToString();
            string fromdate = objParam[7].ToString().Replace("-","");
            string enddate = objParam[8].ToString().Replace("-", "");
            string plant = objParam[9].ToString().Split('-')[0];
            string imgName = string.Empty;
            string dir = string.Empty;
            if (cust_no == "KSA95")
            {
                dir = "KS2A95";
                plant = collisionPartsOperator.ToMESPlant(plant, ref cust_no);
            }
            else if (cust_no == "VNA95")
            {
                dir = "VN2A95";
                plant = collisionPartsOperator.ToMESPlant(plant, ref cust_no);
            }
            else
            {
                plant = collisionPartsOperator.ToMESPlant(plant, ref cust_no);
                dir = plant;
            }
            CollisionPartsModels TK = new CollisionPartsModels();
            #region FTP
            DataTable dtFTPParameter = collisionPartsOperator.getFTPParameter();
            if (dtFTPParameter.Rows.Count > 0)
            {
                TK.ftpinfo.ftpUser = dtFTPParameter.Rows[0]["username"].ToString();
                TK.ftpinfo.ftpPwd = dtFTPParameter.Rows[0]["pwd"].ToString();
                //imgName = pcbno + "_" + lineside;// +".png";
                //TK.ImgName = imgName;
                TK.ftpinfo.ftpPath = dtFTPParameter.Rows[0]["path"].ToString() + "/PCB_Photo/" + dir + "/" + lineside + "/";
            }
            //DownLoad Image
            string Localpath = Application.StartupPath + "\\DQMS\\CollisionParts";
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
            DataTable dt = collisionPartsOperator.getPointData(cust_no,line,lineside,model, pcbno, location, fromdate,enddate,plant);
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
                    data.country = dt.Rows[i]["LOCATION"].ToString();
                    data.name= dt.Rows[i]["QTY"].ToString();
                    if(i==0)
                    {
                        if (qty > 1)
                            data.color = "red";
                        else
                            data.color = "yellow";
                    }
                    else
                    {
                        if(Top1Qty== Convert.ToDouble(dt.Rows[i]["QTY"].ToString()))
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
            }
            //chart2
            #region
            CPchart chart = new CPchart();
            DataTable dt2 = collisionPartsOperator.getChartData(cust_no, line,lineside, model, pcbno, fromdate, enddate,plant);
            if(dt2.Rows.Count>0)
            {
                double Top1Qty = Convert.ToDouble(dt2.Rows[0]["QTY"].ToString());
                for (int i=0;i<dt2.Rows.Count;i++)
                {
                    CPchart2data data = new CPchart2data();
                    double qty = Convert.ToDouble(dt2.Rows[i]["QTY"].ToString());
                    chart.categories += dt2.Rows[i]["LOCATION"].ToString() + ";";
                    data.y=Convert.ToDouble(dt2.Rows[i]["QTY"]);
                    if (i == 0)
                    {
                        if (qty > 1)
                            data.color = "red";
                        else
                            data.color = "yellow";
                    }
                    else
                    {
                        if (Top1Qty == Convert.ToDouble(dt2.Rows[i]["QTY"].ToString()))
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
                    chart.data.Add(data);
                }
            }
            #endregion
            TK.CollisionPartsChart = chart;
            return TK;
        }
        public object GetPictureUploadRate(object[] objParam)
        {
            string plant = objParam[1].ToString();
            string date = objParam[2].ToString();
            string cust_no = objParam[3].ToString();
            double rate1 = 0.0, rate2 = 0.0;
            string[] date0 = GetDate(date);
            string fromTime = date0[0];
            string endTime = date0[1];
            plant = collisionPartsOperator.ToMESPlant(plant,ref cust_no);
            DataTable dt1 = collisionPartsOperator.GetPictureUploadRate(plant, fromTime, endTime);
            if (dt1.Rows.Count > 0)
                rate1 = Convert.ToDouble(dt1.Rows[0][0].ToString());
            DataTable dt2 = collisionPartsOperator.GetOver24PictureUploadRate(plant, fromTime, endTime);
            if (dt2.Rows.Count > 0)
                rate2 = Convert.ToDouble(dt2.Rows[0][0].ToString());
            var data = new
            {
                rate1 = rate1,
                rate2 = rate2
            };
            return data;
        }
        public object GetPictureUploadRateTrendChart(object[] objParam)
        {
            string plant = objParam[1].ToString();
            string cust_no = objParam[2].ToString();
            plant = collisionPartsOperator.ToMESPlant(plant,ref cust_no);
            string fromTime = "";
            string endTime = "";
            DateTime dtNow = DateTime.Now;

            fromTime = dtNow.AddDays(-6).ToString("yyyyMMdd");
            endTime = dtNow.AddDays(1).ToString("yyyyMMdd");
            DataTable dtDaily = collisionPartsOperator.GetPictureUploadRateByDay(plant, fromTime, endTime);
            var data1 = dtDaily.AsEnumerable().Select(t => t.Field<decimal>("rate")).ToList();
            var categories1 = dtDaily.AsEnumerable().Select(t => t.Field<string>("create_time")).ToList();

            GregorianCalendar gc = new GregorianCalendar();
            DateTime  dtNow1 = dtNow.AddDays(-7 *6);
            fromTime = dtNow1.AddDays(1 - (((int)(gc.GetDayOfWeek(dtNow1))) == 0 ? 7 : ((int)(gc.GetDayOfWeek(dtNow1))))).ToString("yyyyMMdd");
            endTime = dtNow.AddDays(8 - (((int)(gc.GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)(gc.GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
            DataTable dtWeekly = collisionPartsOperator.GetPictureUploadRateByWeek(plant, fromTime, endTime);
            var data2 = dtWeekly.AsEnumerable().Select(t => t.Field<decimal>("rate")).ToList();
            var categories2 = dtWeekly.AsEnumerable().Select(t => t.Field<string>("create_time")).ToList();

            fromTime = dtNow.AddMonths(-6).ToString("yyyyMM")+"01";
            endTime = dtNow.AddMonths(1).ToString("yyyyMM")+"01";
            DataTable dtMonthly = collisionPartsOperator.GetPictureUploadRateByMonth(plant, fromTime, endTime);
            var data3 = dtMonthly.AsEnumerable().Select(t => t.Field<decimal>("rate")).ToList();
            var categories3 = dtMonthly.AsEnumerable().Select(t => t.Field<string>("create_time")).ToList();
            
            string date = objParam[2].ToString();
            string[] date0 = GetDate(date);
            fromTime = date0[0];
            endTime = date0[1];
            DataTable dtModel = collisionPartsOperator.GetPictureUploadRateByModel(plant, fromTime, endTime);
            var data4 = dtModel.AsEnumerable().Select(t => t.Field<decimal>("qty")).ToList();
            var categories4 = dtModel.AsEnumerable().Select(t => t.Field<string>("model_name")).ToList();

            var data = new
            {
                chart1 = new { SubTitle = "Daily Trend Chart", data = data1, categories = categories1 },
                chart2 = new { SubTitle = "Weekly Trend Chart", data = data2, categories = categories2 },
                chart3 = new { SubTitle = "Monthly Trend Chart", data = data3, categories = categories3 },
                chart4 = new { SubTitle = "图片未上传率解析By Model", data = data4, categories = categories4 },
            };
            return data;
        }
        public object GetPictureUploadByModelExcelData(object[] objParam)
        {
            string plant = objParam[1].ToString();
            string date = objParam[2].ToString();
            string cust_no = objParam[3].ToString();
            string[] date0 = GetDate(date);
            string fromTime = date0[0];
            string endTime = date0[1];
            plant = collisionPartsOperator.ToMESPlant(plant,ref cust_no);
            var data = collisionPartsOperator.GetPictureUploadRateByModelExcel(plant, fromTime, endTime);
            return data;
        }


        public string[] GetDate(string date)
        {
            string fromTime = "", endTime = "";
            DateTime dtNow = DateTime.Now;
            GregorianCalendar gc = new GregorianCalendar();
            if (date.ToLower().Contains("m"))
            {
                fromTime = dtNow.AddMonths(-1 * Convert.ToInt32(date.Substring(2))).ToString("yyyyMM") + "01";
                endTime = dtNow.AddMonths(-1 * Convert.ToInt32(date.Substring(2)) + 1).ToString("yyyyMM") + "01";

            }
            if (date.ToLower().Contains("w"))
            {

                if ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow)) == 1)
                    dtNow = dtNow.AddDays(-7 - 7 * Convert.ToInt32(date.Substring(2)));
                else
                    dtNow = dtNow.AddDays(-7 * Convert.ToInt32(date.Substring(2)));
                fromTime = dtNow.AddDays(1 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
                endTime = dtNow.AddDays(8 - (((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))) == 0 ? 7 : ((int)((new GregorianCalendar()).GetDayOfWeek(dtNow))))).ToString("yyyyMMdd");
            }
            return new string[] { fromTime, endTime };
        }
    }
}
