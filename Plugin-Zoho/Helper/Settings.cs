using System;

namespace Plugin_Zoho.Helper
{
    public class Settings
    {
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
        public string RefreshToken{ get; set; }
        public bool InsertOnly { get; set; }
        public bool WorkflowTrigger { get; set; }

        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(ClientId))
            {
                throw new Exception("the clientId property must be set");
            }
            
            if (String.IsNullOrEmpty(ClientSecret))
            {
                throw new Exception("the clientSecret property must be set");
            }
            
            if (String.IsNullOrEmpty(RefreshToken))
            {
                throw new Exception("the refreshToken property must be set");
            }
        }
    }
}