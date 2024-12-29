using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sDEM2100.Beans
{
    class OverallQuality : Chart
    {
        public OverallQuality()
        {
            OverallQualitySeries = new List<Beans.OverallQualitySerial>();
        }
        public string Title { get; set; }
        public List<OverallQualitySerial> OverallQualitySeries { get; set; }
    }
    class OverallQualitySerial
    {
        public OverallQualitySerial()
        {
            data = new List<OverallQualityPoint>();
        }
        public string name { get; set; }
        public string type { get; set; }
        public List<OverallQualityPoint> data { get; set; }
    }
    class OverallQualityPoint
    {
        public double y { get; set; }
        public double? actual { get; set; }
        public string goal { get; set; }
    }
}
