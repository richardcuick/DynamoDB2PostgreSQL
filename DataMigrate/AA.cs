using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace DataMigrate
{
    public static class AA
    {
        public static void PrintItem(StringBuilder builder, Dictionary<string, AttributeValue> attrs)
        {
            builder.Append("{");
            int n = 0;
            foreach (KeyValuePair<string, AttributeValue> kvp in attrs)
            {
                if (n > 0)
                    builder.Append(", ");
                else
                    builder.Append(" ");

                builder.Append($" \"{kvp.Key}\" : ");
                PrintValue(builder, kvp.Value, n);
                n++;
            }
            builder.Append("}");
        }

        // Writes out just an attribute's value.
        public static void PrintValue(StringBuilder builder, AttributeValue value, int n)
        {

            // Binary attribute value.
            if (value.B != null)
            {
                builder.Append("Binary data");
            }
            // Binary set attribute value.
            else if (value.BS.Count > 0)
            {
                foreach (var bValue in value.BS)
                {
                    builder.Append("\n  Binary data");
                }
            }
            // List attribute value.
            else if (value.IsLSet)
            {
                builder.Append("[");
                int i = 0;
                foreach (AttributeValue attr in value.L)
                {
                    if (i > 0)
                        builder.Append(", ");
                    else
                        builder.Append(" ");
                    PrintValue(builder, attr, i);
                    i++;
                }
                builder.Append("]");
            }
            // Map attribute value.
            else if (value.IsMSet)
            {
                //builder.Append("{ ");
                PrintItem(builder, value.M);
                //builder.Append(" }");
            }
            // Number attribute value.
            else if (value.N != null)
            {
                builder.Append(value.N);
            }
            // Number set attribute value.
            else if (value.NS.Count > 0)
            {
                builder.AppendFormat("{0}", string.Join(",", value.NS.ToArray()));
            }
            // Null attribute value.
            else if (value.NULL)
            {
                builder.Append("null");
            }
            // String attribute value.
            else if (value.S != null)
            {
                builder.Append($"\"{HttpUtility.JavaScriptStringEncode(value.S)}\"");
            }
            // String set attribute value.
            else if (value.SS.Count > 0)
            {
                builder.AppendFormat("{0}", string.Join(" ", value.SS.ToArray()));
            }
            // Otherwise, boolean value.
            else
            {
                builder.Append(value.BOOL.ToString().ToLower());
            }
        }
    }
}
