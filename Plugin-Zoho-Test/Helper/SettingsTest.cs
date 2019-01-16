using System;
using Xunit;
using Plugin_Zoho.Helper;

namespace Plugin_Zoho_Test.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateTest()
        {
            // setup
            var settings = new Settings {ClientId = "client", ClientSecret = "secret", RefreshToken = "refresh"};
            
            // act
            settings.Validate();

            // assert
        }
        
        [Fact]
        public void ValidateNullClientIdTest()
        {
            // setup
            var settings = new Settings {ClientId = null, ClientSecret = "secret", RefreshToken = "refresh"};
            
            // act
            Exception e  = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("the clientId property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNullClientSecretTest()
        {
            // setup
            var settings = new Settings {ClientId = "client", ClientSecret = null, RefreshToken = "refresh"};
            
            // act
            Exception e  = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("the clientSecret property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNullRefreshTokenTest()
        {
            // setup
            var settings = new Settings {ClientId = "client", ClientSecret = "secret", RefreshToken = null};
            
            // act
            Exception e  = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("the refreshToken property must be set", e.Message);
        }
    }
}