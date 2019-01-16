using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;
using Plugin_Zoho.Helper;
using RichardSzalay.MockHttp;

namespace Plugin_Zoho_Test.Helper
{
    public class RequestHelperTest
    {
        [Fact]
        public async Task GetAsyncTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();
            
            mockHttp.When("https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json", "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            mockHttp.When("https://mockrequest.net")
                .Respond("application/json", "success");

            var requestHelper = new RequestHelper(new Settings{ ClientId = "client", ClientSecret = "secret", RefreshToken = "refresh"}, mockHttp.ToHttpClient());
            
            // act
            var response = await requestHelper.GetAsync("https://mockrequest.net");

            // assert
            Assert.Equal("success", await response.Content.ReadAsStringAsync());
        }
        
        [Fact]
        public async Task GetAsyncWithTokenExceptionTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();
            
            mockHttp.When("https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond(HttpStatusCode.Forbidden);

            mockHttp.When("https://mockrequest.net")
                .Respond(HttpStatusCode.Unauthorized);

            var requestHelper = new RequestHelper(new Settings{ ClientId = "client", ClientSecret = "secret", RefreshToken = "refresh"}, mockHttp.ToHttpClient());
            
            // act
            Exception e  = await Assert.ThrowsAsync<HttpRequestException>(async () => await requestHelper.GetAsync("https://mockrequest.net"));

            // assert
            Assert.Contains("403", e.Message);
        }
        
        [Fact]
        public async Task GetAsyncWithRequestExceptionTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();
            
            mockHttp.When("https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json", "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            mockHttp.When("https://mockrequest.net")
                .Throw(new Exception("bad stuff"));

            var requestHelper = new RequestHelper(new Settings{ ClientId = "client", ClientSecret = "secret", RefreshToken = "refresh"}, mockHttp.ToHttpClient());
            
            // act
            Exception e  = await Assert.ThrowsAsync<Exception>(async () => await requestHelper.GetAsync("https://mockrequest.net"));

            // assert
            Assert.Contains("bad stuff", e.Message);
        }
    }
}