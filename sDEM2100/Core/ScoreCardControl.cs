using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using sDEM2100.Beans;
using sDEM2100.DataGateway;
using System.Data;
using Compal.MESComponent;

namespace sDEM2100.Core
{
    class ScoreCardControl
    {
        private static ScoreCardOperator ScorecardOperator;
        public ScoreCardControl(object[] clientInfo,string DBName)
        {
            ScorecardOperator = new ScoreCardOperator(clientInfo, DBName);
        }
        public Chart GetScoreCardChar()
        {
            Chart scorecardchar = new Chart();
            DataTable dtScorecard = ScorecardOperator.GetScoreCardData();
            if (dtScorecard.Rows.Count > 0)
            {
                Serial serial = new Serial();
                serial.name = "ScoreCardChart";
                Point point = new Point();
                for (int i = 0; i < dtScorecard.Rows.Count; i++)
                {
                    scorecardchar.categories += dtScorecard.Rows[i]["PLANT_CODE"].ToString() + ";";
                    
                    point = new Point();
                    point.y =Math.Round(Convert.ToDouble(dtScorecard.Rows[i]["SUMSCORE"]),1);
                    if (i == 0)
                        point.color = "limegreen";
                    else if (i == dtScorecard.Rows.Count - 1)
                        point.color = "orange";
                    else
                        point.color = "Highlight";
                    serial.data.Add(point);
                }
                scorecardchar.series.Add(serial);
            }
            return scorecardchar;
        }
        
        public DataTable GetScoreCardDetail()
        {
            DataTable dtDetail = new DataTable();
            dtDetail = ScorecardOperator.GetKpi_Name();
            
            DataTable dtPlant = ScorecardOperator.GetScoreCardData();
            if (dtPlant.Rows.Count>0)
            {
                for(int i=0;i<dtPlant.Rows.Count;i++)
                {
                    string plant_code = dtPlant.Rows[i]["PLANT_CODE"].ToString();
                    dtDetail.Columns.Add(plant_code);
                    for(int j=0;j<dtDetail.Rows.Count;j++)
                    {
                        if (ScorecardOperator.CheckWeight(dtDetail.Rows[j][0].ToString(), plant_code))
                            dtDetail.Rows[j][dtPlant.Rows[i]["PLANT_CODE"].ToString()] = "-";
                        else
                            dtDetail.Rows[j][dtPlant.Rows[i]["PLANT_CODE"].ToString()] =
                                ScorecardOperator.GetACTUAL(dtDetail.Rows[j][0].ToString(), plant_code);
                    }
                }
            }

            dtDetail.Columns.Add("GOAL");
            dtDetail.Columns.Add("TREND");
            for (int g = 0; g < dtDetail.Rows.Count; g++)
            {
                DataTable dt = ScorecardOperator.GetGOAL(dtDetail.Rows[g][0].ToString());
                if (dt.Rows.Count > 0)
                {
                    dtDetail.Rows[g]["GOAL"] = dt.Rows[0]["GOAL"].ToString();
                    dtDetail.Rows[g]["TREND"] = dt.Rows[0]["TREND"].ToString();
                }
            }
            return dtDetail;
        }
        public List<Chart> GetScoreCardRadarChar()
        {
            List<Chart> charlist = new List<Chart>();
            DataTable dtplant = ScorecardOperator.GetScoreCardData();
            if (dtplant.Rows.Count > 0)
            {
                for (int i = 0; i < dtplant.Rows.Count; i++)
                {
                    string plant = dtplant.Rows[i]["PLANT_CODE"].ToString();
                    Chart radar = new Chart();
                    radar.ID = "RadarChart" + i;
                    Serial serial = new Serial();
                    serial.name = plant;
                    DataTable dtKpi_name = ScorecardOperator.GetKpi_Name();
                    if (dtKpi_name.Rows.Count > 0)
                    {
                        for (int k = 0; k < dtKpi_name.Rows.Count; k++)
                        {
                            Point point = new Point();
                            double Weight = ScorecardOperator.GetWeight(dtKpi_name.Rows[k]["Index"].ToString(), plant);
                            double radarpointy = 0;
                            double Score = 0;
                            if (Weight == 0)
                                continue;
                                Score = ScorecardOperator.GetScore(dtKpi_name.Rows[k]["Index"].ToString(), plant);
                            radarpointy = Math.Round(Score / Weight * 5, 2);//Scores/Weight*5      
                            if (radarpointy < 0)
                                radarpointy = 0;
                            point.y = radarpointy;
                            radar.categories += dtKpi_name.Rows[k]["Index"].ToString() + ";";
                            serial.data.Add(point);
                        }
                    }
                    radar.series.Add(serial);
                    charlist.Add(radar);
                }
            }
            return charlist;
        }
        public Chart GetScoreCardRadarChar(string plant)
        {
            Chart radar = new Chart();
            radar.ID = "RadarChart" + plant;
            Serial serial = new Serial();
            serial.name = plant;
            DataTable dtKpi_name = ScorecardOperator.GetKpi_Name();
            if (dtKpi_name.Rows.Count > 0)
            {
                for (int k = 0; k < dtKpi_name.Rows.Count; k++)
                {
                    Point point = new Point();
                    double Weight = ScorecardOperator.GetWeight(dtKpi_name.Rows[k]["Index"].ToString(), plant);
                    double radarpointy = 0;
                    double Score = 0;
                    if (Weight == 0)
                        continue;
                    Score = ScorecardOperator.GetScore(dtKpi_name.Rows[k]["Index"].ToString(), plant);
                    radarpointy = Math.Round(Score / Weight * 5, 2);//Scores/Weight*5      
                    if (radarpointy < 0)
                        radarpointy = 0;
                    point.y = radarpointy;
                    radar.categories += dtKpi_name.Rows[k]["Index"].ToString() + ";";
                    serial.data.Add(point);
                }
            }
            radar.series.Add(serial);
            return radar;
        }
    }
}
