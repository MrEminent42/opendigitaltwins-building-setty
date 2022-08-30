using System.Text.RegularExpressions;
using System.Linq;

namespace Helper
{
    public class StringHelper
    {

        public static string CamelToSpaces(string camel)
        {
            camel = Regex.Replace(camel, @"(\P{Ll})(\P{Ll}\p{Ll})", "$1 $2");
            camel = Regex.Replace(camel, @"(\p{Ll})(\P{Ll})", "$1 $2");
            camel = Regex.Replace(camel, @"\s+", " "); // get rid of extra spaces
            return camel;
        }

        public static string SpacesToCamel(string camel)
        {
            // var textInfo = CultureInfo.CurrentCulture.TextInfo;
            // Console.WriteLine("\t\t\tGot " + camel + " -> " + textInfo.ToTitleCase(camel));
            return Regex.Replace(camel, @"/\b\w/g", c => c.Value.ToUpper()).Replace(" ", "");
            // return camel.Replace(" ", "");
        }

        public static string ReplaceNonAlphanumeric(string anything, char replacement)
        {
            return Regex.Replace(anything, @"[^a-zA-Z0-9 ]", "" + replacement);
        }

        public static string FormatApostrophes(string anything)
        {
            return anything.Replace("'", @"\'").Replace("\"", "\\\"");
        }

        public static string GetNameFromJsonId(string jsonId)
        {
            return jsonId.Split(":").Last().Split(";").First();
        }
    }
}