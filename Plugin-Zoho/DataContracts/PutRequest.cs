using System.Collections.Generic;
using Pub;

namespace Plugin_Zoho.DataContracts
{
    public class PutRequest
    {
        public object[] data { get; set; }
        public List<string> trigger { get; set; }
    }
}