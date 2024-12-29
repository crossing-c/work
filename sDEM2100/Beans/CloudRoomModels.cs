using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Compal.MESComponent;

namespace sDEM2100.Beans
{
    public class CloudRoomModels
    {
        private const string dbName = "HGPG";
        public CloudRoomModels()
        {

        }
    }
    public class ChartModels
    {
        public ChartModels()
        {
            series = new List<serial>();
        }
        public string kpi_name { get; set; }
        public string ID { get; set; }
        public string[] button { get; set; }
        public string categories { get; set; }
        public List<serial> series { get; set; }
        public double Goal { get; set; }
    }
    public class serial
    {
        public serial()
        {
            data = new List<double>();
        }
        public string name { get; set; }
        public string type { get; set; }
        public string color { get; set; }
        public int yAxis { get; set; }
        public List<double> data { get; set; }

    }
}