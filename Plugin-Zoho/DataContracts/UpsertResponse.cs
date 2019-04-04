using System.Collections.Generic;
using Newtonsoft.Json;

namespace Plugin_Zoho.DataContracts
{
    public class UpsertResponse
    {
        [JsonProperty("data")]
        public List<UpsertObject> Data { get; set; }
    }

    public class UpsertObject
    {
        [JsonProperty("code")]
        public string Code { get; set; }
        
        [JsonProperty("message")]
        public string Message { get; set; }
    }
}