// See https://aka.ms/new-console-template for more information
//Console.WriteLine("Hello, World!");

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DataMigrate;
using Npgsql;
using System.Text;
using Dapper;
using System.Text.Json;

namespace DataMgrate
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var cs = "Host=localhost;Username=postgres;Password=hamasaki1230~;Database=20220622-2";

            using (NpgsqlConnection con = new NpgsqlConnection(cs))
            {
                con.Open();

                var sql = "SELECT version()";

                //using var cmd = new NpgsqlCommand(sql, con);

                //var version = cmd.ExecuteScalar().ToString();
                //Console.WriteLine($"PostgreSQL version: {version}");

                ExtractJSON(con, "110556-shared_b2c_authority_roles");
                ExtractJSON(con, "110556-shared_b2c_buyer_roles");
                ExtractJSON(con, "110556-shared_b2c_supplier_roles");
                ExtractJSON(con, "110556-shared_countries");
                ExtractJSON(con, "110556-sph_authority_task");
                ExtractJSON(con, "110556-sph_crontab_data");
                ExtractJSON(con, "110556-sph_email_templates");
                ExtractJSON(con, "110556-sph_email_types");
                ExtractJSON(con, "110556-sph_esg_credentials");
                ExtractJSON(con, "110556-sph_esg_dictionary");
                ExtractJSON(con, "110556-sph_esg_master_questionnaire_answers");
                ExtractJSON(con, "110556-sph_esg_report_data");
                ExtractJSON(con, "110556-sph_esg_report_download_metadata");
                ExtractJSON(con, "110556-sph_esg_report_market_unit_mapping");
                ExtractJSON(con, "110556-sph_esg_review_chat");
                ExtractJSON(con, "110556-sph_esg_review_data");
                ExtractJSON(con, "110556-sph_esg_snapshot_questionnaire_answers");
                ExtractJSON(con, "110556-sph_form");
                ExtractJSON(con, "110556-sph_form_metadata", false);
                ExtractJSON(con, "110556-sph_form_metadata_review_rules", false);
                ExtractJSON(con, "110556-sph_form_metadata_scoring_weights");
                ExtractJSON(con, "110556-sph_invite_event");
                ExtractJSON(con, "110556-sph_tasks");
                ExtractJSON(con, "110556-sph_task_types");
                ExtractJSON(con, "110556-tsm_authority_general_profile");
                ExtractJSON(con, "110556-tsm_buyer_general_profile");
                ExtractJSON(con, "110556-tsm_country_served_operation_history");
                ExtractJSON(con, "110556-tsm_email_templates");
                ExtractJSON(con, "110556-tsm_email_types");
                ExtractJSON(con, "110556-tsm_form");
                ExtractJSON(con, "110556-tsm_form_metadata", false);
                ExtractJSON(con, "110556-tsm_help_and_support");
                ExtractJSON(con, "110556-tsm_image");
                ExtractJSON(con, "110556-tsm_keep_mind_show");
                ExtractJSON(con, "110556-tsm_notifications");
                ExtractJSON(con, "110556-tsm_parent_child_relation");
                ExtractJSON(con, "110556-tsm_parent_corp_duns_info");
                ExtractJSON(con, "110556-tsm_privacy_statement");
                ExtractJSON(con, "110556-tsm_shared_b2c_profile_user_mapping");
                ExtractJSON(con, "110556-tsm_shared_b2c_user_profile_mapping");   //wrong
                ExtractJSON(con, "110556-tsm_sph_tasks_termination");
                ExtractJSON(con, "110556-tsm_supplier_general_profile");
                ExtractJSON(con, "110556-tsm_terms_of_use");
                ExtractJSON(con, "110556-tsm_user_operation_log");
                ExtractJSON(con, "110556-tsm_workflow");
                ExtractJSON(con, "110556-tsm_workflow_history");


            }

            Console.WriteLine("Completed!");
            Console.ReadLine();
        }

        public static void ExtractJSON(NpgsqlConnection conn, string tableName, bool hasPK=true)
        {
            AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
            // This client will access the US East 1 region.
            clientConfig.RegionEndpoint = RegionEndpoint.USEast1;
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(clientConfig);

            //Dictionary<string, AttributeValue> key = new Dictionary<string, AttributeValue>
            //{
            //    //{ "roleId", new AttributeValue { S = "1" } }
            //};

            //// Create GetItem request
            //GetItemRequest request = new GetItemRequest
            //{
            //    TableName = "110556-shared_b2c_authority_roles",
            //    Key = key,
            //};

            ScanRequest request2 = new ScanRequest
            {
                TableName = tableName //"110556-shared_b2c_authority_roles"
            };

            // Issue request
            //GetItemResponse result = client.GetItemAsync(request).Result;
            ScanResponse result = client.ScanAsync(request2).Result;

            // View response
            Console.WriteLine("Item:");
            List<Dictionary<string, AttributeValue>> items = result.Items;

            DescribeTableRequest req3 = new DescribeTableRequest(tableName);

            DescribeTableResponse res3 = client.DescribeTableAsync(req3).Result;

            string partitionKeyName = string.Empty;

            foreach(KeySchemaElement ks in res3.Table.KeySchema)
            {
                if(ks.KeyType==KeyType.HASH)
                {
                    partitionKeyName = ks.AttributeName;
                }
            }

            try { 
            var cmd3 = conn.CreateCommand();
            cmd3.CommandText = $"DROP TABLE \"{tableName}\" CASCADE;";
            cmd3.ExecuteNonQuery();
            }
            catch
            {

            }

            StringBuilder sb = new StringBuilder();
            //sb.Append($"ALTER TABLE IF EXISTS public.\"{tableName }\" DROP CONSTRAINT \"{tableName}_cs\";");
            sb.Append($"CREATE TABLE IF NOT EXISTS public.\"{tableName}\"");
            sb.Append($"(\"{partitionKeyName}\" character varying COLLATE pg_catalog.\"default\" NOT NULL, ");
            sb.Append($"json jsonb, json_ddb jsonb");
            if(hasPK)sb.Append($",CONSTRAINT \"{tableName}_cs\" PRIMARY KEY (\"{partitionKeyName}\")");
            sb.Append(");");

            Console.WriteLine("----------------------------------Create Table -------------------");
            Console.WriteLine(tableName);
            Console.WriteLine("----------------------------------Create Table -------------------");

            var cmd4 = conn.CreateCommand();
            cmd4.CommandText = sb.ToString() ;
            cmd4.ExecuteNonQuery();

            //Console.ReadLine();

            //var cmd2 = conn.CreateCommand();
            //cmd2.CommandText = $"DELETE FROM \"{tableName}\";";
            //cmd2.ExecuteNonQuery();

            foreach (var item in items)
            {

                //string json = Newtonsoft.Json.JsonConvert.SerializeObject(item);

                //Console.WriteLine(json);
                Console.WriteLine("-----------------------JSON-------------------------------");
                StringBuilder json = new StringBuilder();
                AA.PrintItem(json, item);
                Console.WriteLine(json.ToString());

                Console.WriteLine("-----------------------JSON DynamoDB----------------------");

                StringBuilder json_ddb = new StringBuilder();
                AA.PrintItem(json_ddb, item);
                Console.WriteLine(json_ddb.ToString());


                string strJson = json.ToString();
                string strJson_ddb = json_ddb.ToString();
                string pk = item[partitionKeyName].S;

                JsonDocument jdoc1 = JsonDocument.Parse(strJson);
                JsonDocument jdoc2 = JsonDocument.Parse(strJson_ddb);

                var cmd = conn.CreateCommand();
                cmd.CommandText = $"INSERT INTO \"{tableName}\" VALUES(@pk,@json,@json_ddb);";
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "pk", Value = pk });
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "json", Value = jdoc1 });
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "json_ddb", Value = jdoc2 });
                cmd.ExecuteNonQuery();

                //Console.ReadLine();

                //foreach (var keyValuePair in item)
                //{
                //    Console.WriteLine("{0} : S={1}, N={2}, M={3} , SS=[{4}], NS=[{5}]",
                //        keyValuePair.Key,
                //        keyValuePair.Value.S,
                //        keyValuePair.Value.N,
                //        keyValuePair.Value.M,
                //        string.Join(", ", keyValuePair.Value.SS ?? new List<string>()),
                //        string.Join(", ", keyValuePair.Value.NS ?? new List<string>()));
                //}
            }
        }
    }
}
