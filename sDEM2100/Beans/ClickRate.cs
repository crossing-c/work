using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sDEM2100.Beans
{
    class ClickRate
    {
        public ClickRate()
        {
            PieChartList = new List<Beans.ClickRatePie>();
            chart1 = new Beans.ClickRateChart1();
            chart2 = new List<Beans.ClickRateChart2>();
            plant_color = new List<string>();
        }
        public List<ClickRatePie> PieChartList { get; set; }
        public ClickRateChart1 chart1 { get; set; }
        public List<ClickRateChart2> chart2 { get; set; }
        public List<string> plant_color { get; set; }
    }
    public class ClickRatePie
    {
        public ClickRatePie ()
        {
            data = new List<Beans.ClickRatePieData>();
        }
        public string subtitle { get; set; }
        public List<ClickRatePieData> data { get; set; }
    }
    public class ClickRatePieData
    {
        public string name { get; set; }
        public double? y { get; set; }
        public double? qty { get; set; }
    }

    public class ClickRateChart1
    {
        public ClickRateChart1()
        {
            series = new List<Beans.ClickRateSeries>();
        }
        public string categories { get; set; }
        public List<ClickRateSeries> series { get; set; }
    }
    public class ClickRateChart2
    {
        public ClickRateChart2()
        {
            series = new List<Beans.ClickRateSeries>();
        }
        public string subtitle { get; set; }
        public string categories { get; set; }
        public List<ClickRateSeries> series { get; set; }
    }
    public class ClickRateSeries
    {
        public ClickRateSeries ()
        {
            data = new List<double?>();
        }
        public string name { get; set; }
        public string color { get; set; }
        public List<double?> data { get; set; }
    }
}
