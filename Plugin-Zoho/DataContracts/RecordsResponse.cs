using System;

namespace Plugin_Zoho.DataContracts
{
    public class Info
    {
        public int  count        { get; set; }
        public bool more_records { get; set; }
        public int  page         { get; set; }
        public int  per_page     { get; set; }
    }
    
    public class RecordsResponse
    {
        public Object[] data { get; set; }
        public Info info { get; set; }
    }
}