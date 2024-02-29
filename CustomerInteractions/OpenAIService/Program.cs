using System;
using Azure;
using static System.Environment;
using Azure.AI.OpenAI;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System.Collections.Generic;
using System.Linq;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using Azure.Identity;
using Azure.Core;
using Razorvine.Pyro;
using System.ComponentModel;
using Newtonsoft.Json;
using Azure.Messaging.EventHubs;
using System.Threading.Tasks;
using System.Text;

namespace OpenAIService
{
    internal class Program
    {
        // kcnfyxkuzcru5mc5cy4iwfgvwm-4q7d2peuxrnu5grermuin75pbm.datawarehouse.fabric.microsoft.com
       
        private readonly System.Data.SqlClient.SqlConnection conn;

        static async Task Main(string[] args)
        {
            string endpoint = "https://polite-ground-030dc3103.4.azurestaticapps.net/api/v1";
            string key = "23d929bc-7d8d-4e9e-9ba3-87def775b15a";
            var client = new OpenAIClient(new Uri(endpoint), new AzureKeyCredential(key));

            var schema = new StructType(new[]
{
                new StructField("customerId", new StringType(), true),
                new StructField("regarding", new StringType(), true),               
                new StructField("text", new StringType(), true),
});

            List<(string,string, string)> outputRows = new List<(string,string,string)>();

            var DefaultAzureCredentialOptions = new DefaultAzureCredentialOptions
            {
                ExcludeAzureCliCredential = true,
                ExcludeManagedIdentityCredential = true,
                ExcludeSharedTokenCacheCredential = true,
                ExcludeVisualStudioCredential = false,
                ExcludeAzurePowerShellCredential = true,
                ExcludeEnvironmentCredential = true,
                ExcludeVisualStudioCodeCredential = true,
                ExcludeInteractiveBrowserCredential = true
            };
            var accessToken = new DefaultAzureCredential(DefaultAzureCredentialOptions).GetToken(new TokenRequestContext(new string[] { "https://database.windows.net//.default" }));

            var connectionString =
                "Server=kcnfyxkuzcru5mc5cy4iwfgvwm-4q7d2peuxrnu5grermuin75pbm.datawarehouse.fabric.microsoft.com,1433; Database=ActivityTouchPoints;";
            //Set AAD Access Token, Open Conneciton, Run Queries and Disconnect
            using var con = new SqlConnection(connectionString);
            con.AccessToken = accessToken.Token;
            con.Open();
           


            SqlCommand command = new SqlCommand(@"SELECT regarding,MAX(ActivityTouchPoints.description) AS description,MAX(ActivityTouchPoints.customerId) AS customerId
                                      FROM ActivityTouchPoints GROUP BY regarding 
                                     ", con);

            using (SqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    string regarding = reader["regarding"] != null ? reader["regarding"].ToString() : string.Empty;
                    string description = reader["description"] != null ? reader["description"].ToString() : string.Empty;
                    string customerId = reader["customerId"] != null ? reader["customerId"].ToString() : string.Empty;

                    string prompt = "This is the email which the customer " + regarding + "has sent:\n\".\n Please very briefly summarize this customer emails in a paragraph of 30 words or less.\n";

                    using var conDescription = new SqlConnection(connectionString);
                    conDescription.AccessToken = accessToken.Token;
                    conDescription.Open();
                    SqlCommand commandDescription = new SqlCommand(@"SELECT description
                                      FROM ActivityTouchPoints where regarding = '"+regarding +"'", conDescription);
                    using (SqlDataReader readerDescription = commandDescription.ExecuteReader())
                    {
                        while (readerDescription.Read())
                        {
                            prompt += "Email:\n" + reader["description"] + "\n";
                        }
                    }

                        // Call to OpenAI API to get the response (method not provided)
                        var chatresponseOptions = new ChatCompletionsOptions()
                    {
                        DeploymentName = "gpt-35-turbo", //This must match the custom deployment name you chose for your model
                        Messages =
    {
        new ChatRequestSystemMessage(prompt)
    },
                        MaxTokens = 100
                    };
                    Response<ChatCompletions> openAIresponse = client.GetChatCompletions(chatresponseOptions);
                    // Assuming response.choices[0].message.content is the way to get the content from the response
                    outputRows.Add((customerId, regarding,openAIresponse.Value.Choices[0].Message.Content));
                    Console.WriteLine("Customer: \n" + regarding.ToString() +"\n");
                    Console.WriteLine(openAIresponse.Value.Choices[0].Message.Content);
                    conDescription.Close();

                }
            }
            con.Close();

            // DataTable generatedTable = GenericToDataTable(outputRows);

            var json = JsonConvert.SerializeObject(outputRows);
            DataTable dt = (DataTable)JsonConvert.DeserializeObject(json, (typeof(DataTable)));
            dt.Columns["Item1"].ColumnName = "CustomerId";
            dt.Columns["Item2"].ColumnName = "Customer";
            dt.Columns["Item3"].ColumnName = "Generated_Feedback";         


            // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when events are being published or read regularly.
            // TODO: Replace the <EVENT_HUB_NAMESPACE> and <HUB_NAME> placeholder values
            Azure.Messaging.EventHubs.Producer.EventHubProducerClient producerClient = new Azure.Messaging.EventHubs.Producer.EventHubProducerClient(
                "Endpoint=sb://esehwestuuj9zwr7ykcpvt84.servicebus.windows.net/;SharedAccessKeyName=key_1298d3d7-8cd9-5127-f1b9-a70bcc338205;SharedAccessKey=N1EGRbIYRWX/6yufmG1HBtThENd87QSFY+AEhFgsd10=;EntityPath=es_c6e4a019-f5a2-4128-9f68-52f6ce7aec51");



            // Create a batch of events 
            Azure.Messaging.EventHubs.Producer.EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
            foreach (DataRow row in dt.Rows)
            {

                Response<ImageGenerations> imageGenerations = await client.GetImageGenerationsAsync(
                   new ImageGenerationOptions()
                   {
                       Prompt = row["Generated_Feedback"].ToString(),
                       Size = new ImageSize("1024x1024"),
                       DeploymentName = "dall-e-3"
                   });

                // Image Generations responses provide URLs you can use to retrieve requested images
                Uri imageUri = imageGenerations.Value.Data[0].Url;

                // Print the image URI to console:
                Console.WriteLine(imageUri);

                if (!eventBatch.TryAdd(AddEventJob(row,imageUri)))
                {
                    // if it is too large for the batch

                }
            }
            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of "+dt.Rows.Count+ " events has been published.");
                Console.ReadLine();
            }
            finally
            {
                await producerClient.DisposeAsync();
            }


        }
        private static EventData AddEventJob(DataRow entity, Uri imageUri)
        {
            try
            {
                GeneratedActivities obj = new GeneratedActivities();
                if (entity["customer"] != null)
                {
                    obj.customerId = entity["customerId"].ToString();
                    obj.Customer = entity["Customer"].ToString();
                    obj.GeneratedText = entity["Generated_Feedback"].ToString();
                    obj.imageUrl = imageUri;

                    string currentActivityRec = System.Text.Json.JsonSerializer.Serialize<GeneratedActivities>(obj);

                    // Create the event data
                    var eventData = new EventData(Encoding.UTF8.GetBytes(currentActivityRec));

                    return eventData;
                }
                else
                {
                    return null;
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }


        }

    }
    public class GeneratedActivities
    {
        public string Customer { get; set; }
        public string customerId { get; set; }
        public string GeneratedText { get; set; }
        public Uri imageUrl { get; set; }
        
    }
}
