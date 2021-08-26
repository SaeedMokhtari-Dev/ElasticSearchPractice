using System;
using System.Threading.Tasks;
using ElasticSearch;
using Elasticsearch.Net;
using FluentAssertions;
using Xunit;

namespace ElasticSearchPractice.Tests
{
    public class ElasticMapperTests
    {
        [Fact]
        public async Task CreateConnection_should_return_Success()
        {
            var elasticMapper = new ElasticMapper();
            
            await elasticMapper.InsertData();
        }
    }
}