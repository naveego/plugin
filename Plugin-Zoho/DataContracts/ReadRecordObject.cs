using Newtonsoft.Json;

namespace Plugin_Zoho.DataContracts
{
    public class ReadRecordObject
    {
        [JsonProperty("data")]
        public object Data { get; set; }
    }
}