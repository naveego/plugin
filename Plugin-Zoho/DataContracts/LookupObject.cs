using Newtonsoft.Json;

namespace Plugin_Zoho.DataContracts
{
    public class LookupObject
    {
        [JsonProperty("id")]
        public string Id { get; set; }
    }
}