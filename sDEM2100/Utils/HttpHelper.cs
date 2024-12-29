using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace Utilities
{
    public class HttpHelper
    {
        public static string PostXmlResponse(string url, Hashtable jsonString)
        {
            HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
            request.Method = "POST";
            request.ContentType = "application/x-www-form-urlencoded";
            // 凭证
            NetworkCredential c = new NetworkCredential("sfisuser", "sfisps", "gi");
            //request.Credentials = CredentialCache.DefaultCredentials;
            request.Credentials = c;
            //超时时间
            request.Timeout = 10000;
            var PostStr = HashtableToPostData(jsonString);
            byte[] data = System.Text.Encoding.UTF8.GetBytes(PostStr);
            request.ContentLength = data.Length;
            Stream writer = request.GetRequestStream();
            writer.Write(data, 0, data.Length);
            writer.Close();
            var response = request.GetResponse();
            var stream = response.GetResponseStream();
            StreamReader sr = new StreamReader(stream, Encoding.UTF8);
            String retXml = sr.ReadToEnd();
            sr.Close();
            return retXml;
        }
        private static String HashtableToPostData(Hashtable ht)
        {
            StringBuilder sb = new StringBuilder();
            foreach (string k in ht.Keys)
            {
                if (sb.Length > 0)
                {
                    sb.Append("&");
                }
                sb.Append(k + "=" +ht[k].ToString());
            }
            return sb.ToString();
        }
    }
}