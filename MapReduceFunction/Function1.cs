using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

namespace MapReduceFunction
{
    public class WordCount
    {
        public int count { get; set; }
        public string word { get; set; }

    }
    public static class Function1
    {
        private static readonly HttpClient _httpClient = HttpClientFactory.Create();
        /// <summary>
        /// get URL of Azure storage blob and prefix string of blob files
        /// </summary>
        /// <param name="req"></param>
        /// <param name="starter"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        [FunctionName(nameof(StartAsync))]
        public static async Task<HttpResponseMessage> StartAsync([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableClient starter,
            ILogger log)
        {
            // retrieve storage blobs URI of the taxi dataset
            var pathString = req.RequestUri.ParseQueryString()[@"path"] ?? throw new ArgumentNullException(@"required query string parameter 'path' not found");
            var path = new Uri(pathString);

            var containerUrl = path.GetLeftPart(UriPartial.Authority) + "/" + path.Segments[1];
            var prefix = string.Join(string.Empty, path.Segments.Skip(2));
            log.LogInformation(prefix);
            log.LogInformation(containerUrl);
            log.LogInformation(@"Starting orchestrator...");
            var newInstanceId = await starter.StartNewAsync(nameof(BeginMapReduce), new { containerUrl, prefix });
            log.LogInformation($@"- Instance id: {newInstanceId}");

            return starter.CreateCheckStatusResponse(req, newInstanceId);
        }

        [FunctionName(nameof(BeginMapReduce))]
        public static async Task<string> BeginMapReduce([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var input = context.GetInput<JObject>();

            //container URL of Azure storage blob
            var containerUrl = input["containerUrl"].ToString();
            //prefix string of Azure storage blob
            var prefix = input["prefix"].ToString();

            if (!context.IsReplaying)
            {
                log.LogInformation($@"Beginning MapReduce. Container: {containerUrl} | Prefix: {prefix}");
                context.SetCustomStatus(new { status = @"Getting files to reduce", containerUrl, prefix });
            }
            //retrieve storage Uri of each file via an Activity function
            var files = await context.CallActivityAsync<string[]>(
                nameof(GetFileListAsync),
                new string[] { containerUrl, prefix });

            if (!context.IsReplaying)
            {
                log.LogInformation($@"{files.Length} file(s) found: {JsonConvert.SerializeObject(files)}");
                log.LogInformation(@"Creating mappers...");
                context.SetCustomStatus(new { status = @"Creating mappers", files });
            }
            var file = files.Where(t => t.Contains("contentDataPrime")).First();

            //create mapper tasks which download and calculate word counts from each csv file
            var tasks = new Task<WordCount[]>[6];
            for (var i = 1; i <= tasks.Length; i++)
            {
                var Mapperinput = new string[] { file, i.ToString() };
                tasks[i] = context.CallActivityAsync<WordCount[]>(
                    nameof(MapperAsync),
                    Mapperinput);
            }

            if (!context.IsReplaying)
            {
                log.LogInformation($@"Waiting for all {files.Length} mappers to complete...");
                context.SetCustomStatus(@"Waiting for mappers");
            }
            //wait all tasks done
            await Task.WhenAll(tasks);

            if (!context.IsReplaying)
            {
                log.LogInformation(@"Executing reducer...");
                context.SetCustomStatus(@"Executing reducer");
            }
            //create reducer activity function for aggregating result 
            var result = await context.CallActivityAsync<WordCount[]>(
                nameof(Reducer),
                tasks.Select(task => task.Result).ToArray());

            log.LogInformation($@"**FINISHED** Result: {result}");
            context.SetCustomStatus(null);
            var CSVRows = result.Select(t => $"{t.word},{t.count}").ToArray();
           var csv  = String.Join("\n", CSVRows);
            //log.LogInformationNoReplay(context,@"Writing result to blob...");
            await context.CallActivityAsync(
               nameof(WriteToBlob),
               csv);
            //output result
            return csv;
        }

        /// <summary>
        /// GetFileList Activity function for retrieving all storage blob files Uri
        /// </summary>
        [FunctionName(nameof(GetFileListAsync))]
        public static async Task<string[]> GetFileListAsync(
            [ActivityTrigger] string[] paras)
        {
            var cloudBlobContainer = new CloudBlobContainer(new Uri(paras[0]));

            var blobs = Enumerable.Empty<IListBlobItem>();
            var continuationToken = default(BlobContinuationToken);
            do
            {
                var segmentBlobs = await cloudBlobContainer.ListBlobsSegmentedAsync(paras[1], continuationToken);
                blobs = blobs.Concat(segmentBlobs.Results);

                continuationToken = segmentBlobs.ContinuationToken;
            } while (continuationToken != null);

            return blobs.Select(i => i.Uri.ToString()).ToArray();
        }

        /// <summary>
        /// Mapper Activity function to download and parse a CSV file
        /// </summary>
        [FunctionName(nameof(MapperAsync))]
        public static async Task<IEnumerable<WordCount>> MapperAsync(
            [ActivityTrigger] string[] Inputs,
            ILogger log)
        {
            //container URL of Azure storage blob
            var fileUri = Inputs[0];
            //prefix string of Azure storage blob
            var product = int.Parse(Inputs[1]);

            log.LogInformation($@"Executing mapper for {Inputs[0]} ,{Inputs[1]}...");

            // download blob file
#pragma warning disable IDE0067 // Dispose objects before losing scope
            // Don't wrap in a Using because this was causing occasional ObjectDisposedException errors in v2 executions
            var reader = new StreamReader(await _httpClient.GetStreamAsync(fileUri));
#pragma warning restore IDE0067 // Dispose objects before losing scope

            var lineText = string.Empty;
            var results = new Dictionary<string, int>();
            // read a line from NetworkStream
            var counter = 1;
            var End = product * 20000;
            var Start = End - 20000;
            while (!reader.EndOfStream && (lineText = await reader.ReadLineAsync()) != null)
            {
                // parse CSV format line
                var segdata = lineText.Split(',');

                // If it is header line or blank line, then continue
                counter++;
                if (counter < Start || segdata.Length != 11 || !int.TryParse(segdata[0], out var n))
                    continue;
                if (counter > End) 
                    break; 

                var description = segdata[10];
                if (description == null)
                {
                    log.LogInformation($"Empty description {description} in segdata equals {segdata}");
                    continue;
                }
                description.Split(' ')
                    .Select(t => Regex.Replace(t.ToLower().Trim(), @"[^0-9a-zA-Z:,']+", ""))
                    .GroupBy(t => t)
                    .Select(t => new { word = t.Key, count = t.Count() })
                    .ToList().ForEach(t =>
                    {
                        var IsExisted = results.TryGetValue(t.word, out var count);
                        if (IsExisted)
                        {
                            results[t.word] += 1;
                        }
                        else
                        {
                            results.Add(t.word, 1);
                        }
                    });

            }

            log.LogInformation($@"{fileUri} mapper complete. Returning {results.Count} result(s)");
            return results.Select(t=>new WordCount { word = t.Key, count = t.Value });
        }

        /// <summary>
        /// Reducer Activity function for results aggregation
        /// </summary>
        [FunctionName(nameof(Reducer))]
        public static IEnumerable<WordCount> Reducer(
            [ActivityTrigger] WordCount[][] mapresults,
            ILogger log)
        {
            log.LogInformation(@"Reducing results...");
            var results = mapresults.Cast<WordCount>().GroupBy(t => t.word).Select(t => new WordCount
            {
                word = t.Key,
                count = t.Sum(t => t.count)
            });
            return results;


        }

        /// <summary>
        /// WriteToBlob Activity function for storing output result to blob storage. 
        /// </summary>
        [FunctionName(nameof(WriteToBlob))]
        public static async Task WriteToBlob(
            [ActivityTrigger] string content)
        {
            var storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            var cloudBlobClient = storageAccount.CreateCloudBlobClient();

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            var cloudBlobContainer = cloudBlobClient.GetContainerReference("result");
            await cloudBlobContainer.CreateAsync();

            var fileName = string.Format("mapreduce_sample_result_{0}.csv", DateTime.Now.ToString("yyyy_dd_M_HH_mm_ss"));
            var cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(fileName);
            await cloudBlockBlob.UploadTextAsync(content);
        }
    }
}