using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sDEM2100.Beans
{
    class CrossCheckModels
    {public CrossCheckModels()
        {
            data = new List<double?>();
        }
        public string ID { get; set; }
        public string IndexName { get; set; }
        public string Goal { get; set; }
        public string Unit { get; set; }
        public string categories { get; set; }
        public List<double?> data { get; set; }
    }
}
