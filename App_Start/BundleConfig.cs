using System.Web;
using System.Web.Optimization;

namespace DQMS
{
    public class BundleConfig
    {
        // 有关绑定的详细信息，请访问 http://go.microsoft.com/fwlink/?LinkId=301862
        public static void RegisterBundles(BundleCollection bundles)
        {
            bundles.Add(new ScriptBundle("~/bundles/js").Include(
                        "~/assets/js/jquery.min.js",
                        "~/assets/js/jquery.cookie.js",
                        "~/assets/js/bootstrap.min.js",
                        "~/assets/js/jquery.mCustomScrollbar.concat.min.js",
                        "~/assets/js/custom.js",
                        "~/assets/js/jquery.token.js"));

            bundles.Add(new StyleBundle("~/Content/css").Include(
                      "~/assets/css/bootstrap.min.css",
                      "~/assets/css/font-awesome.min.css",
                      "~/assets/css/jquery.mCustomScrollbar.min.css",
                      "~/assets/css/custom-white.min.css"));

            bundles.Add(new StyleBundle("~/Content/black-css").Include(
                     "~/assets/css/bootstrap.min.css",
                     "~/assets/css/font-awesome.min.css",
                     "~/assets/css/jquery.mCustomScrollbar.min.css",
                     "~/assets/css/custom-black.min.css"));
        }
    }
}
