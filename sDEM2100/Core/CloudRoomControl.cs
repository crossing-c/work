using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using sDEM2100.DataGateway;
using sDEM2100.Beans;
using Newtonsoft.Json;
using System.Data;
using System.Drawing;

namespace sDEM2100.Core
{
    class CloudRoomControl
    {
        private static CloudRoomOperator CloudRoomOperator;
        public CloudRoomControl(object[] clientInfo, string DBName)
        {
            CloudRoomOperator = new CloudRoomOperator(clientInfo, DBName);
        }
        public string GetClouRoomModels(Object[] parameters)
        {
            string strjson = "";
            if(parameters.Length>1)//plant&kpi_name  单个
            {
                ChartModels model = new ChartModels();
                string plant = parameters[0].ToString();
                string kpi_name = parameters[1].ToString();
                string datetype = parameters[2].ToString();
                string serialcolor = parameters[3].ToString();
                model.kpi_name = kpi_name;
                model.ID = kpi_name + "Chart";
                if (kpi_name == "QBR")
                    model.button = new string[] { "M" };
                else
                    model.button = new string[] { "W", "M" };
                serial serial1 = new serial();
                serial1.name = "";
                serial1.color = serialcolor;
                if (datetype == "")
                    datetype = model.button[0];
                DataTable dt = CloudRoomOperator.GetData(plant, kpi_name, datetype);
                if (dt.Rows.Count > 0)
                {
                    for (int j = 0; j < dt.Rows.Count; j++)
                    {
                        model.categories += dt.Rows[j][1].ToString().Substring(4) + ";";
                        if (kpi_name == "QBR" && (Convert.ToInt32(Convert.ToDouble(dt.Rows[j][2]))) == 1)
                            serial1.data.Add(3);
                        else if (kpi_name == "QBR" && (Convert.ToInt32(Convert.ToDouble(dt.Rows[j][2]))) == 3)
                            serial1.data.Add(1);
                        else
                            serial1.data.Add(Math.Round(Convert.ToDouble(dt.Rows[j][2]), 2));
                    }
                }
                model.series.Add(serial1);
                if (kpi_name == "MWDC" || kpi_name == "IFIR"
                    || kpi_name == "FPY+" || kpi_name == "FIR")
                {
                    //serial serial2 = new serial();
                    //serial2.name = "";
                    //serial2.color = "red";
                    //serial2.type = "line";
                    //double y = 0.0;
                    switch (kpi_name)
                    {
                        case "MWDC":
                            model.Goal = 30;
                            break;
                        case "IFIR":
                            model.Goal = 80;
                            break;
                        case "FPY+":
                            model.Goal = 98.5;
                            break;
                        case "FIR":
                            model.Goal = 80;
                            break;
                    }
                }
                //model.series.Add(serial2);
                strjson = JsonConvert.SerializeObject(model);
            }
            else//plant 6个kpi_name
            {
                List<ChartModels> listmodel = new List<ChartModels>();
                string plant = parameters[0].ToString();
                DataTable dtKpi_name = CloudRoomOperator.GetKpi_Name(plant);
                if(dtKpi_name.Rows.Count>0)
                {
                    for(int i=0;i< dtKpi_name.Rows.Count;i++)
                    {
                        string kpi_name = dtKpi_name.Rows[i][0].ToString();
                        ChartModels model = new ChartModels();
                        model.kpi_name = kpi_name;
                        model.ID = kpi_name + "Chart";
                        if (kpi_name == "QBR")
                            model.button = new string[] {"M"};
                        else
                            model.button = new string[] { "W","M" };
                        serial serial1 = new serial();
                        serial1.name = "";
                        serial1.color = GetRandomColor(i);
                        //serial serial2 = new serial();
                        //serial2.name = "利用率";
                        //serial2.type = "line";
                        //serial2.yAxis = 1;
                        //dataLabels labels2 = new dataLabels();
                        // labels2.enabled = true;
                        //labels2.format = "{y}%";
                        //serial2.dataLabels = labels2;
                        DataTable dt = CloudRoomOperator.GetData(plant, kpi_name, model.button[0]);
                        if(dt.Rows.Count >0)
                        {
                            for(int j=0;j< dt.Rows.Count;j++)
                            {
                                model.categories += dt.Rows[j][1].ToString().Substring(4) + ";";
                                //serial1.data.Add(Math.Round(Convert.ToDouble(dt.Rows[j][2]), 2));
                                if (kpi_name == "QBR" && (Convert.ToInt32(Convert.ToDouble(dt.Rows[j][2]))) == 1)
                                    serial1.data.Add(3);
                                else if (kpi_name == "QBR" && (Convert.ToInt32(Convert.ToDouble(dt.Rows[j][2]))) == 3)
                                    serial1.data.Add(1);
                                else
                                    serial1.data.Add(Math.Round(Convert.ToDouble(dt.Rows[j][2]), 2));
                            }
                        }
                        model.series.Add(serial1);
                        if (kpi_name == "MWDC" || kpi_name == "IFIR"
                    || kpi_name == "FPY+" || kpi_name == "FIR")
                        {
                            //serial serial2 = new serial();
                            //serial2.name = "";
                            //serial2.color = "red";
                            //serial2.type = "line";
                            //double y = 0.0;
                            switch (kpi_name)
                            {
                                case "MWDC":
                                    model.Goal = 30;
                                    break;
                                case "IFIR":
                                    model.Goal = 80;
                                    break;
                                case "FPY+":
                                    model.Goal = 98.5;
                                    break;
                                case "FIR":
                                    model.Goal = 80;
                                    break;
                            }
                            //for (int k= 0; k< dt.Rows.Count;k++)
                            //{
                            //    serial2.data.Add(y);
                            //}
                            //model.series.Add(serial2);
                        }
                        //model.series.Add(serial2);
                        listmodel.Add(model);
                    }
                }
                strjson = JsonConvert.SerializeObject(listmodel);
            }
            return strjson;
        }
        private string GetRandomColor(int a)
        {
            //int a = (new Random()).Next(1, 8);
            switch (a)
            {
                case 1:
                    return "chartreuse";
                case 2:
                    return "cyan";
                case 3:
                    return "darkorchid";
                case 4:
                    return "darkorange";
                case 5:
                    return "lightskyblue";
                case 6:
                    return "palegreen";
                case 7:
                    return "rosybrown";
                default:
                    return "yellowgreen";
            }
        }
    }
}
