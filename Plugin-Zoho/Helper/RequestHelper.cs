using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Plugin_Zoho.DataContracts;

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
                var client = _client;
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

                var response = await client.GetAsync(uri);

                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }

        public async Task<HttpResponseMessage> PutAsync(string uri, string json)
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
                var client = _client;
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var putRequestObj = new PutRequest
                {
                    data = new [] {JsonConvert.DeserializeObject(json)},
                    trigger = new string[0]
                };

                var content = new StringContent(JsonConvert.SerializeObject(putRequestObj), Encoding.UTF8, "application/json");
                var response = await client.PutAsync(uri, content);

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