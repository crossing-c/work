using System;
using System.Collections.Generic;
using System.Data;

namespace sDEM2100.Beans
{
    class FpyDeepDiveModels
    {
        public FpyDeepDiveModels()
        {
            data = new List<double?>();
            DtError = new DataTable();
            DtParts = new DataTable();
        }
        public string ID { get; set; }
        public DataTable DtError { get; set; }
        public DataTable DtParts { get; set; }   
        public string categories { get; set; }
        public List<double?> data { get; set; }
    }
}
