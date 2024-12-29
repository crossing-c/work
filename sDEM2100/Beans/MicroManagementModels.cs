using System;
using System.Collections.Generic;
using System.Data;

namespace sDEM2100.Beans
{
    class MicroManagementModels
    {
        public MicroManagementModels()
        {
            data = new List<double?>();
            DtError = new DataTable();
        }
        public string ID { get; set; }
        public string IndexName { get; set; }
        public string Goal { get; set; }
        public string Unit { get; set; }
        public string categories { get; set; }
        public List<double?> data { get; set; }
        public DataTable DtError { get; set; }
    }
}
