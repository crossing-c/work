using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sDEM2100.Beans
{
    public class ScoreCardModels
    {
    }
    public class Chart
    {
        public Chart()
        {
            series = new List<Serial>();
        }
        public string ID { get; set; }
        public List<Serial> series { get; set; }
        public string categories { get; set; }
    }
    public class Serial
    {
        public Serial()
        {
            data = new List<Point>();
        }
        public string name { get; set; }
        public List<Point> data { get; set; }
    }
    public class Point
    {
        public string color { get; set; }
        public double y { get; set; }
    }
}
