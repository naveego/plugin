namespace Plugin_Zoho.DataContracts
{
    public class TokenResponse
    {
        public string access_token   { get; set; }
        public string refresh_token  { get; set; }
        public int    expires_in_sec { get; set; }
        public string api_domain     { get; set; }
        public string token_type     { get; set; }
        public int    expires_in     { get; set; }
    }
}