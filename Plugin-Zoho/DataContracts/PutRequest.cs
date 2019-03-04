using Pub;

namespace Plugin_Zoho.DataContracts
{
    public class PutRequest
    {
        public object[] data { get; set; }
        public string[] trigger { get; set; }
    }
}