using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Data;
//using Microsoft.Data.SqlClient;
using Azure.Identity;
using Azure.Core;
using System.Configuration;
using System.Data.SqlClient;

namespace RestAPI
{
    public static class CustomerFeedBacks
    {
        [FunctionName("GenerateFeedback")]
        public static async Task<IActionResult> GenerateFeedback(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "feedback")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            List<TaskModel> taskList = new List<TaskModel>();
            var DefaultAzureCredentialOptions = new DefaultAzureCredentialOptions
            {
                ExcludeAzureCliCredential = true,
                ExcludeManagedIdentityCredential = true,
                ExcludeSharedTokenCacheCredential = true,
                ExcludeVisualStudioCredential = false,
                ExcludeAzurePowerShellCredential = true,
                ExcludeEnvironmentCredential = true,
                ExcludeVisualStudioCodeCredential = true,
                ExcludeInteractiveBrowserCredential = true,
               
            };
            var accessToken = new DefaultAzureCredential(DefaultAzureCredentialOptions).GetToken(new TokenRequestContext(new string[] { "https://database.windows.net//.default" }));
            try
            {
                using (SqlConnection con = new SqlConnection(Environment.GetEnvironmentVariable("SqlConnectionString",EnvironmentVariableTarget.Process)))
                {

                    con.AccessToken = accessToken.Token;
                    con.Open();
                    var query = @"Select * from GeneratedText";
                    SqlCommand command = new SqlCommand(query, con);
                    var reader =await command.ExecuteReaderAsync();
                    while (reader.Read())
                    {
                        TaskModel task = new TaskModel()
                        {
                            customerId = (string)reader["customerId"],
                            GeneratedText = reader["GeneratedText"].ToString(),
                            Customer = reader["Customer"].ToString(),
                            imageUrl = (string)reader["imageUrl"]
                        };
                        taskList.Add(task);
                    }
                }
            }
            catch (Exception e)
            {
                log.LogError(e.ToString());
            }
            if (taskList.Count > 0)
            {
                return new OkObjectResult(taskList);
            }
            else
            {
                return new NotFoundResult();
            }
        }


        [FunctionName("GenerateFeedbackById")]
        public static IActionResult GetTaskById(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "feedback/{id}")] HttpRequest req, ILogger log, string id)
        {
            List<TaskModel> taskList = new List<TaskModel>();
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
            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection con = new SqlConnection(Environment.GetEnvironmentVariable("SqlConnectionString", EnvironmentVariableTarget.Process)))
                {
                    con.AccessToken = accessToken.Token;
                    con.Open();
                    var query = @"Select top 1 * from GeneratedText Where customerId = @Id";
                    SqlCommand command = new SqlCommand(query, con);
                    command.Parameters.AddWithValue("@Id", id);
                    SqlDataAdapter da = new SqlDataAdapter(command);
                    da.Fill(dt);
                }
            }
            catch (Exception e)
            {
                log.LogError(e.ToString());
            }
            if (dt.Rows.Count == 0)
            {
                return new NotFoundResult();
            }
            return new OkObjectResult(dt);
        }
    }
}
