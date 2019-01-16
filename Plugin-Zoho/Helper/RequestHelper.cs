using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace Plugin_Zoho.Helper
{
    public class RequestHelper
    {
        private readonly Authenticator _authenticator;
        private readonly HttpClient _client;
        
        public RequestHelper(Settings settings, HttpClient client)
        {
            _authenticator = new Authenticator(settings, client);
            _client = client;
        }

        /// <summary>
        /// Get Async request wrapper for making authenticated requests
        /// </summary>
        /// <param name="uri"></param>
        /// <returns></returns>
        public async Task<HttpResponseMessage> GetAsync(string uri)
        {
            string token;

            // get the token
            try
            {
                token = await _authenticator.GetToken();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            // add token to the request and execute the request
            try
            {
                _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

                var response = await _client.GetAsync(uri);

                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }
    }
}