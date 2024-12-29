using System.Web;
using System.Web.Mvc;

namespace DQMS
{
    public class FilterConfig
    {
        public static void RegisterGlobalFilters(GlobalFilterCollection filters)
        {
            filters.Add(new HandleErrorAttribute());
            filters.Add(new DQMS.Filters.CustomAuthorizeAttribute());
        }
    }
}
