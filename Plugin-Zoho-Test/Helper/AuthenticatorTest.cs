using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;
using Plugin_Zoho.Helper;
using RichardSzalay.MockHttp;

namespace Plugin_Zoho_Test.Helper
{
    public class AuthenticatorTest
    {
        [Fact]
        public async Task GetTokenTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When("https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json", "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            var auth = new Authenticator(new Settings{ ClientId = "client", ClientSecret = "secret", RefreshToken = "refresh"}, mockHttp.ToHttpClient());
            
            // act
            var token = await auth.GetToken();
            var token2 = await auth.GetToken();

            // assert
            // first token is fetched
            Assert.Equal("mocktoken", token);
            // second token should be the same but not be fetched
            Assert.Equal("mocktoken", token2);
        }
        
        [Fact]
        public async Task GetTokenWithExceptionTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When("https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond(HttpStatusCode.Forbidden);

            var auth = new Authenticator(new Settings{ ClientId = "client", ClientSecret = "secret", RefreshToken = "refresh"}, mockHttp.ToHttpClient());
            
            // act
            Exception e  = await Assert.ThrowsAsync<HttpRequestException>(async () => await auth.GetToken());

            // assert
            Assert.Contains("403", e.Message);
        }
    }
}