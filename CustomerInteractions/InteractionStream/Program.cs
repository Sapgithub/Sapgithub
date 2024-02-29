using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Windows.Navigation;
using System.Text.Json;
using System.Text.RegularExpressions;


namespace InteractionStream
{
    internal class Program

    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Console Application Started!");
            try
            {
                //Step 1 - Retrieving CRM Essential Information.
                string sEnvironment = System.Configuration.ConfigurationManager.AppSettings["Environment"].ToString();
                string sUserKey = System.Configuration.ConfigurationManager.AppSettings["UserKey"].ToString();
                string sUserPassword = System.Configuration.ConfigurationManager.AppSettings["UserPassword"].ToString();
                //Step 2- Creating A Connection String.
                string conn = $@" Url = {sEnvironment};AuthType = OAuth;UserName = {sUserKey}; Password = {sUserPassword};AppId = 51f81489-12ee-4a9e-aaae-a2591f45987d;RedirectUri = app://58145B91-0C36-4500-8554-080854F2AC97;LoginPrompt=Auto; RequireNewInstance = True";
                Console.WriteLine("Operating Environment : " + sEnvironment);
                int numOfEvents = 0;
                //Step 3 - Obtaining CRM Service.
                EntityCollection addFinalCol = new EntityCollection();
                using (var service = new CrmServiceClient(conn))
                {
                    if (service != null)
                    {
                        //Executing YOUR CRM OPERATIONS.
                        EntityCollection allEmails = RetrieveActivities(service, "email");
                        numOfEvents += allEmails.Entities.Count;
                        EntityCollection allTasks = RetrieveActivities(service, "task");
                        numOfEvents += allTasks.Entities.Count;
                        EntityCollection allPhoneCalls = RetrieveActivities(service, "phonecall");
                        numOfEvents += allPhoneCalls.Entities.Count;


                        foreach (Entity email in allEmails.Entities)
                        {
                            addFinalCol.Entities.Add(email);
                        }
                        foreach (Entity tasks in allTasks.Entities)
                        {
                            addFinalCol.Entities.Add(tasks);
                        }
                        foreach (Entity phoneCalls in allPhoneCalls.Entities)
                        {
                            addFinalCol.Entities.Add(phoneCalls);
                        }
                    }
                }
                // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
                // of the application, which is best practice when events are being published or read regularly.
                // TODO: Replace the <EVENT_HUB_NAMESPACE> and <HUB_NAME> placeholder values
                EventHubProducerClient producerClient = new EventHubProducerClient(
                    "Endpoint=sb://esehwestu962qiwyp3m4sazr.servicebus.windows.net/;SharedAccessKeyName=key_303e2e87-00e0-e401-98e7-e73784073866;SharedAccessKey=FUdqY/zhCDlMwXSnohae0ni0IkosW0Syh+AEhOjqOJI=;EntityPath=es_f0c9a3ec-a258-4b43-8550-84267097964c");



                // Create a batch of events 
                EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                foreach (Entity entity in addFinalCol.Entities)
                {

                    if (!eventBatch.TryAdd(AddEventJob(entity)))
                    {
                        // if it is too large for the batch

                    }
                }
                try
                {
                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);
                    Console.WriteLine($"A batch of {numOfEvents} events has been published.");
                    Console.ReadLine();
                }
                finally
                {
                    await producerClient.DisposeAsync();
                }


            }
            catch (Exception ex)
            {
                Console.WriteLine("Error Occured : " + ex.Message);
            }
        }

        private static EventData AddEventJob(Entity entity)
        {
            try
            {
                Activities obj = new Activities();
                if (entity.Attributes.Contains("regardingobjectid"))
                {
                    obj.entryTime = DateTime.Now.ToString();
                    obj.subject = entity.GetAttributeValue<string>("subject");
                    obj.customerId = entity.GetAttributeValue<EntityReference>("regardingobjectid").Id.ToString();
                    obj.regarding = entity.GetAttributeValue<EntityReference>("regardingobjectid").Name;
                    obj.description = entity.GetAttributeValue<string>("description") != null ? StripHTML(entity.GetAttributeValue<string>("description")) : string.Empty;

                    string currentActivityRec = JsonSerializer.Serialize<Activities>(obj);

                    // Create the event data
                    var eventData = new EventData(Encoding.UTF8.GetBytes(currentActivityRec));

                    return eventData;
                }
                else
                {
                    return null;
                }

            } 
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }
          

        }

        public static string StripHTML(string input)
        {
            return Regex.Replace(input, "<.*?>", String.Empty);
        }


        private static EntityCollection RetrieveActivities(CrmServiceClient service, string entityName)
        {
            QueryExpression query = new QueryExpression()
            {
                EntityName = entityName,
                ColumnSet = new ColumnSet(true)
            };
            return service.RetrieveMultiple(query);
        }
    }

    public class Activities
    {
        public string entryTime { get; set; }
        public string customerId { get; set; }
        public string regarding { get; set; }
        public object subject { get; set; }
        public string description { get; set; }
    }

}
