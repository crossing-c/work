using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace sDEM2100.Beans
{
    class AccountModels
    {
        public AccountModels()
        {
            dt = new DataTable();
            chart = new LoginTrendChart();
        }
        public int logincount { get; set; }
        public DataTable dt { get; set; }
        public LoginTrendChart chart { get; set; }
    }
    class LoginTrendChart
    {
        public LoginTrendChart()
        {
            data = new List<double>();
        }
        public string title { get; set; }
        public string categories { get; set; }
        public List<double> data { get; set; }
    }
}
