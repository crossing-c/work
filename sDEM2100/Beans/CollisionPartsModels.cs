using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sDEM2100.Beans
{
    class CollisionPartsModels
    {
        public CollisionPartsModels()
        {
            CollisionPartsData = new List<CPdata>();
            CollisionPartsChart = new CPchart();
            ftpinfo = new ftpInfo();
            CollisionPartsChart2 = new List<CPchart>();
        }
        //QMS 撞件九宮格 背景 img
        public double ImgWidth { get; set; }
        public double ImgHeight { get; set; }
        public string ImgName { get; set; }
        public Byte[] ImgBytes { get; set; }
        public ftpInfo ftpinfo { get; set; }
        public List<CPdata> CollisionPartsData { get; set; }
        public CPchart CollisionPartsChart { get; set; }
        public List<CPchart> CollisionPartsChart2 { get; set; }
    }
    class CPdata
    {
        public double x { get; set; }
        public double y { get; set; }
        public string name { get; set; }
        public string country { get; set; }
        public string color { get; set; }
    }
    class CPchart
    {
        public CPchart() { data = new List<CPchart2data>(); }
        public List<CPchart2data> data { get; set; }
        public string name { get; set; }
        public string categories { get; set; }
    }
    class CPchart2data
    {
        public double? y { get; set; }
        public string color { get; set; }
    }
    class ftpInfo
    {
        public string ftpUser { get; set; }
        public string ftpPwd { get; set; }
        public string ftpPath { get; set; }
    }
}
