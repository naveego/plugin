using System;
using System.Net.Http;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Newtonsoft.Json;
using Plugin_Zoho.DataContracts;

namespace Plugin_Zoho.Helper
{
    public class Authenticator
    {
        private readonly HttpClient _client;
        private readonly string     _clientId;
        private readonly string     _clientSecret;
        private DateTime            _expires;
        private readonly string     _refreshToken;
        private string              _token;

        public Authenticator(Settings settings, HttpClient client)
        {
            _client = client;
            _clientId = settings.ClientId;
            _clientSecret = settings.ClientSecret;
            _expires = DateTime.Now;
            _refreshToken = settings.RefreshToken;
            _token = String.Empty;
        }

        /// <summary>
        /// Get a token for the Zoho API
        /// </summary>
        /// <returns></returns>
        public async Task<string> GetToken()
        {
            // check if token is expired or will expire in 5 minutes or less
            if (DateTime.Compare(DateTime.Now.AddMinutes(5), _expires) >= 0)
            {
                try
                {
                    // get a token
                    var requestUri = String.Format(
                        "https://accounts.zoho.com/oauth/v2/token?refresh_token={0}&client_id={1}&client_secret={2}&grant_type=refresh_token",
                        _refreshToken,
                        _clientId,
                        _clientSecret);
            
                    var response = await _client.PostAsync(requestUri, null);
                    response.EnsureSuccessStatusCode();

                    var content = JsonConvert.DeserializeObject<TokenResponse>(await response.Content.ReadAsStringAsync());
                    
                    // update expiration and saved token
                    _expires = DateTime.Now.AddSeconds(content.expires_in_sec);
                    _token = content.access_token;

                    return _token;
                }
                catch (Exception e)
                {
                    Logger.Error(e, e.Message);
                    throw;
                }
            }
            // return saved token
            else
            {
                return _token;
            }
        }
    }
}