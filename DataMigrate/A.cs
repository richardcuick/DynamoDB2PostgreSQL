using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigrate
{
    public static class A
    {
        public static void PrintItem(Dictionary<string, AttributeValue> attrs)
        {
            Console.Write("{");
            int n = 0;
            foreach (KeyValuePair<string, AttributeValue> kvp in attrs)
            {
                if (n > 0)
                    Console.Write(", ");
                else
                    Console.Write(" ");

                Console.Write($" \"{kvp.Key}\" : ");
                PrintValue(kvp.Value, n);
                n++;
            }
            Console.Write("}");
        }

        // Writes out just an attribute's value.
        public static void PrintValue(AttributeValue value, int n)
        {

            // Binary attribute value.
            if (value.B != null)
            {
                Console.Write("Binary data");
            }
            // Binary set attribute value.
            else if (value.BS.Count > 0)
            {
                foreach (var bValue in value.BS)
                {
                    Console.Write("\n  Binary data");
                }
            }
            // List attribute value.
            else if (value.L.Count > 0)
            {
                int i = 0;
                foreach (AttributeValue attr in value.L)
                {
                    PrintValue(attr, i);
                    i++;
                }
            }
            // Map attribute value.
            else if (value.IsMSet)
            {
                //Console.Write("{ ");
                PrintItem(value.M);
                //Console.Write(" }");
            }
            // Number attribute value.
            else if (value.N != null)
            {
                Console.Write(value.N);
            }
            // Number set attribute value.
            else if (value.NS.Count > 0)
            {
                Console.Write("{0}", string.Join(",", value.NS.ToArray()));
            }
            // Null attribute value.
            else if (value.NULL)
            {
                Console.Write("Null");
            }
            // String attribute value.
            else if (value.S != null)
            {
                Console.Write($"\"{value.S}\"");
            }
            // String set attribute value.
            else if (value.SS.Count > 0)
            {
                Console.Write("{0}", string.Join(" ", value.SS.ToArray()));
            }
            // Otherwise, boolean value.
            else
            {
                Console.Write(value.BOOL.ToString().ToLower());
            }
        }
    }
}
