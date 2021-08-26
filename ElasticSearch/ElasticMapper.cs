using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Elasticsearch.Net;

namespace ElasticSearch
{
    public class ElasticMapper
    {
        public async Task InsertData()
        {
            var elasticConnection = GetElasticConnection();
            await CreateMapping(elasticConnection);
            await InsertBulkData(elasticConnection);
        }
        public ElasticLowLevelClient GetElasticConnection()
        {
            var settings = new ConnectionConfiguration(new Uri("http://127.0.0.1:9200"))
                .RequestTimeout(TimeSpan.FromMinutes(2));
            return new ElasticLowLevelClient(settings);
        }
        private async Task CreateMapping(ElasticLowLevelClient elasticConnection)
        {
            PostData mappingBody = PostData.String(await LoadMappingJson());
            var createMappingResponse = await elasticConnection.PutScriptAsync
                <StringResponse>("website_information", mappingBody);
            if (!createMappingResponse.Success)
                throw new IOException(createMappingResponse.Body);
        }
        private async Task InsertBulkData(ElasticLowLevelClient elasticConnection)
        {
            PostData bulkPostBody = await GetBulkPostBody();
            var bulkResponse = await elasticConnection.BulkAsync
                <StringResponse>(bulkPostBody);
            if (!bulkResponse.Success)
                throw new IOException(bulkResponse.Body);
        }
        private async Task<string> LoadMappingJson()
        {
            string mappingFileAddress = $"{Directory.GetCurrentDirectory()}\\JsonFiles\\Mapping.json";
            if (!File.Exists(mappingFileAddress))
                throw new FileNotFoundException();
            return await File.ReadAllTextAsync(mappingFileAddress);
        }
        
        private async Task<PostData> GetBulkPostBody()
        {
            string[] websiteInfoLines = await LoadDataJson();
            StringBuilder bulkPostBodyBuilder = new StringBuilder();
            for (var i = 0; i < websiteInfoLines.Length; i++)
            {
                bulkPostBodyBuilder.AppendLine($"{{\"index\": {{\"_id\": {i+1}, \"_index\": \"website_information\", \"_type\": \"info\"}}");
                bulkPostBodyBuilder.AppendLine(websiteInfoLines[i]);
            }

            bulkPostBodyBuilder.AppendLine();
            return PostData.String(bulkPostBodyBuilder.ToString());
        }

        private async Task<string[]> LoadDataJson()
        {
            string dataFileAddress = $"{Directory.GetCurrentDirectory()}\\JsonFiles\\data.jsonl";
            if (!File.Exists(dataFileAddress))
                throw new FileNotFoundException();
            return await File.ReadAllLinesAsync(dataFileAddress);
        }

       
        
    }
}