using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sDEM2100.Beans
{
    class DqmsBean
    {
        public int KpiId { get; set; }
        public string BuCode { get; set; }
        public string PlantCode { get; set; }
        public string WorkDate { get; set; }
        public double Actual { get; set; }
        public double Score { get; set; }
        public double LastActual { get; set; }
        public double LastScore { get; set; }
        public int Trend { get; set; }
    }
}
