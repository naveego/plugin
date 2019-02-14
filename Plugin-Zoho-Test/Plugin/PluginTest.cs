using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Grpc.Core;
using Pub;
using Xunit;
using RichardSzalay.MockHttp;
using Record = Pub.Record;

namespace Plugin_Zoho_Test.Plugin
{
    public class PluginTest
    {
        private ConnectRequest GetConnectSettings()
        {
            return new ConnectRequest
            {
                SettingsJson = "",
                OauthConfiguration = new OAuthConfiguration
                {
                    ClientId = "client",
                    ClientSecret = "secret",
                    ConfigurationJson = "{}"
                },
                OauthStateJson = "{\"RefreshToken\":\"refresh\",\"AuthToken\":\"\"}"
            };
        }

        [Fact]
        public async Task BeginOAuthFlowTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = new BeginOAuthFlowRequest()
            {
                Configuration = new OAuthConfiguration
                {
                    ClientId = "client",
                    ClientSecret = "secret",
                    ConfigurationJson = "{}"
                },
                RedirectUrl = "http://test.com"
            };

            var scope = "ZohoCRM.users.all,ZohoCRM.org.all,ZohoCRM.settings.all,ZohoCRM.modules.all";
            var clientId = request.Configuration.ClientId;
            var responseType = "code";
            var accessType = "offline";
            var redirectUrl = request.RedirectUrl;
            var prompt = "consent";

            var authUrl = String.Format(
                "https://accounts.zoho.com/oauth/v2/auth?scope={0}&client_id={1}&response_type={2}&access_type={3}&redirect_uri={4}&prompt={5}",
                scope,
                clientId,
                responseType,
                accessType,
                redirectUrl,
                prompt);

            // act
            var response = client.BeginOAuthFlow(request);

            // assert
            Assert.IsType<BeginOAuthFlowResponse>(response);
            Assert.Equal(authUrl, response.AuthorizationUrl);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task CompleteOAuthFlowTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When(
                    "https://accounts.zoho.com/oauth/v2/token?code=authcode&redirect_uri=http://test.com/&client_id=client&client_secret=secret&grant_type=authorization_code")
                .Respond("application/json",
                    "{\"access_token\":\"authtoken\",\"refresh_token\":\"refreshtoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var completeRequest = new CompleteOAuthFlowRequest
            {
                Configuration = new OAuthConfiguration
                {
                    ClientId = "client",
                    ClientSecret = "secret",
                    ConfigurationJson = "{}"
                },
                RedirectUrl = "http://test.com?code=authcode",
                RedirectBody = ""
            };

            // act
            var response = client.CompleteOAuthFlow(completeRequest);

            // assert
            Assert.IsType<CompleteOAuthFlowResponse>(response);
            Assert.Contains("authtoken", response.OauthStateJson);
            Assert.Contains("refreshtoken", response.OauthStateJson);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConnectSessionTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When(
                    "https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json",
                    "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/modules")
                .Respond("application/json",
                    "{\"modules\":[{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Leads\",\"presence_sub_menu\":true,\"id\":\"3656031000000002175\",\"visibility\":1,\"convertable\":true,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":2,\"singular_label\":\"Lead\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Leads\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Leads\",\"business_card_field_limit\":5,\"parent_module\":{}},]}");

            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();
            var disconnectRequest = new DisconnectRequest();

            // act
            var response = client.ConnectSession(request);
            var responseStream = response.ResponseStream;
            var records = new List<ConnectResponse>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
                client.Disconnect(disconnectRequest);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConnectTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When(
                    "https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json",
                    "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/modules")
                .Respond("application/json",
                    "{\"modules\":[{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Leads\",\"presence_sub_menu\":true,\"id\":\"3656031000000002175\",\"visibility\":1,\"convertable\":true,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":2,\"singular_label\":\"Lead\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Leads\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Leads\",\"business_card_field_limit\":5,\"parent_module\":{}},]}");

            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();

            // act
            var response = client.Connect(request);

            // assert
            Assert.IsType<ConnectResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasAllTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When(
                    "https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json",
                    "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/modules")
                .Respond("application/json",
                    "{\"modules\":[{\"global_search_supported\":false,\"deletable\":false,\"creatable\":false,\"modified_time\":null,\"plural_label\":\"Home\",\"presence_sub_menu\":false,\"id\":\"3656031000000002173\",\"visibility\":1,\"convertable\":false,\"editable\":false,\"emailTemplate_support\":false,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":false,\"web_link\":null,\"sequence_number\":1,\"singular_label\":\"Home\",\"viewable\":true,\"api_supported\":false,\"api_name\":\"Home\",\"quick_create\":false,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":false,\"arguments\":[],\"module_name\":\"Home\",\"business_card_field_limit\":0,\"parent_module\":{}},{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Leads\",\"presence_sub_menu\":true,\"id\":\"3656031000000002175\",\"visibility\":1,\"convertable\":true,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":2,\"singular_label\":\"Lead\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Leads\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Leads\",\"business_card_field_limit\":5,\"parent_module\":{}},{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Contacts\",\"presence_sub_menu\":true,\"id\":\"3656031000000002179\",\"visibility\":1,\"convertable\":false,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":3,\"singular_label\":\"Contact\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Contacts\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Contacts\",\"business_card_field_limit\":5,\"parent_module\":{}}]}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/fields?module=Leads")
                .Respond("application/json",
                    "{\"fields\":[{\"webhook\":true,\"json_type\":\"integer\",\"crypt\":null,\"field_label\":\"Prediction\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":true,\"section_id\":1,\"read_only\":true,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000178003\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":9,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Prediction_Score\",\"unique\":{},\"data_type\":\"integer\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"jsonobject\",\"crypt\":null,\"field_label\":\"LeadOwner\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":true,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000002589\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":120,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Owner\",\"unique\":{},\"data_type\":\"ownerlookup\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"string\",\"crypt\":null,\"field_label\":\"Company\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":false,\"currency\":{},\"id\":\"3656031000000002591\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":100,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":true,\"create\":true},\"subform\":null,\"api_name\":\"Company\",\"unique\":{},\"data_type\":\"text\",\"formula\":{},\"decimal_place\":null,\"mass_update\":true,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"boolean\",\"crypt\":null,\"field_label\":\"EmailOptOut\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000014177\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":5,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Email_Opt_Out\",\"unique\":{},\"data_type\":\"boolean\",\"formula\":{},\"decimal_place\":null,\"mass_update\":true,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"double\",\"crypt\":null,\"field_label\":\"AnnualRevenue\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{\"rounding_option\":\"normal\",\"precision\":2},\"id\":\"3656031000000002617\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":16,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Annual_Revenue\",\"unique\":{},\"data_type\":\"currency\",\"formula\":{},\"decimal_place\":2,\"mass_update\":true,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"jsonarray\",\"crypt\":null,\"field_label\":\"Tag\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000125055\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":2000,\"view_type\":{\"view\":true,\"edit\":false,\"quick_create\":false,\"create\":false},\"subform\":null,\"api_name\":\"Tag\",\"unique\":{},\"data_type\":\"text\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"string\",\"crypt\":null,\"field_label\":\"CreatedTime\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":false,\"currency\":{},\"id\":\"3656031000000002627\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":120,\"view_type\":{\"view\":true,\"edit\":false,\"quick_create\":false,\"create\":false},\"subform\":null,\"api_name\":\"Created_Time\",\"unique\":{},\"data_type\":\"datetime\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"string\",\"crypt\":null,\"field_label\":\"ModifiedTime\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":false,\"currency\":{},\"id\":\"3656031000000002629\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":120,\"view_type\":{\"view\":true,\"edit\":false,\"quick_create\":false,\"create\":false},\"subform\":null,\"api_name\":\"Modified_Time\",\"unique\":{},\"data_type\":\"datetime\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"string\",\"crypt\":null,\"field_label\":\"Description\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":3,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000002645\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":32000,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Description\",\"unique\":{},\"data_type\":\"textarea\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"crypt\":null,\"field_label\":\"LeadImage\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":5,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000152001\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":255,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Record_Image\",\"unique\":{},\"data_type\":\"profileimage\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}}]}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/fields?module=Contacts")
                .Respond(HttpStatusCode.NoContent);

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/fields?module=Home")
                .Respond(HttpStatusCode.BadRequest);

            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.All,
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When(
                    "https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json",
                    "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/modules")
                .Respond("application/json",
                    "{\"modules\":[{\"global_search_supported\":false,\"deletable\":false,\"creatable\":false,\"modified_time\":null,\"plural_label\":\"Home\",\"presence_sub_menu\":false,\"id\":\"3656031000000002173\",\"visibility\":1,\"convertable\":false,\"editable\":false,\"emailTemplate_support\":false,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":false,\"web_link\":null,\"sequence_number\":1,\"singular_label\":\"Home\",\"viewable\":true,\"api_supported\":false,\"api_name\":\"Home\",\"quick_create\":false,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":false,\"arguments\":[],\"module_name\":\"Home\",\"business_card_field_limit\":0,\"parent_module\":{}},{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Leads\",\"presence_sub_menu\":true,\"id\":\"3656031000000002175\",\"visibility\":1,\"convertable\":true,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":2,\"singular_label\":\"Lead\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Leads\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Leads\",\"business_card_field_limit\":5,\"parent_module\":{}},{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Contacts\",\"presence_sub_menu\":true,\"id\":\"3656031000000002179\",\"visibility\":1,\"convertable\":false,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":3,\"singular_label\":\"Contact\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Contacts\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Contacts\",\"business_card_field_limit\":5,\"parent_module\":{}}]}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/fields?module=Leads")
                .Respond("application/json",
                    "{\"fields\":[{\"webhook\":true,\"json_type\":\"integer\",\"crypt\":null,\"field_label\":\"Prediction\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":true,\"section_id\":1,\"read_only\":true,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000178003\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":9,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Prediction_Score\",\"unique\":{},\"data_type\":\"integer\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"jsonobject\",\"crypt\":null,\"field_label\":\"LeadOwner\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":true,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000002589\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":120,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Owner\",\"unique\":{},\"data_type\":\"ownerlookup\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"string\",\"crypt\":null,\"field_label\":\"Company\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":false,\"currency\":{},\"id\":\"3656031000000002591\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":100,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":true,\"create\":true},\"subform\":null,\"api_name\":\"Company\",\"unique\":{},\"data_type\":\"text\",\"formula\":{},\"decimal_place\":null,\"mass_update\":true,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"boolean\",\"crypt\":null,\"field_label\":\"EmailOptOut\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000014177\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":5,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Email_Opt_Out\",\"unique\":{},\"data_type\":\"boolean\",\"formula\":{},\"decimal_place\":null,\"mass_update\":true,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"double\",\"crypt\":null,\"field_label\":\"AnnualRevenue\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{\"rounding_option\":\"normal\",\"precision\":2},\"id\":\"3656031000000002617\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":16,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Annual_Revenue\",\"unique\":{},\"data_type\":\"currency\",\"formula\":{},\"decimal_place\":2,\"mass_update\":true,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"jsonarray\",\"crypt\":null,\"field_label\":\"Tag\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000125055\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":2000,\"view_type\":{\"view\":true,\"edit\":false,\"quick_create\":false,\"create\":false},\"subform\":null,\"api_name\":\"Tag\",\"unique\":{},\"data_type\":\"text\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"string\",\"crypt\":null,\"field_label\":\"CreatedTime\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":false,\"currency\":{},\"id\":\"3656031000000002627\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":120,\"view_type\":{\"view\":true,\"edit\":false,\"quick_create\":false,\"create\":false},\"subform\":null,\"api_name\":\"Created_Time\",\"unique\":{},\"data_type\":\"datetime\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"string\",\"crypt\":null,\"field_label\":\"ModifiedTime\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":1,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":false,\"currency\":{},\"id\":\"3656031000000002629\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":120,\"view_type\":{\"view\":true,\"edit\":false,\"quick_create\":false,\"create\":false},\"subform\":null,\"api_name\":\"Modified_Time\",\"unique\":{},\"data_type\":\"datetime\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"json_type\":\"string\",\"crypt\":null,\"field_label\":\"Description\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":3,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000002645\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":32000,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Description\",\"unique\":{},\"data_type\":\"textarea\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}},{\"webhook\":true,\"crypt\":null,\"field_label\":\"LeadImage\",\"tooltip\":null,\"created_source\":\"default\",\"field_read_only\":false,\"section_id\":5,\"read_only\":false,\"association_details\":null,\"businesscard_supported\":true,\"currency\":{},\"id\":\"3656031000000152001\",\"custom_field\":false,\"lookup\":{},\"visible\":true,\"length\":255,\"view_type\":{\"view\":true,\"edit\":true,\"quick_create\":false,\"create\":true},\"subform\":null,\"api_name\":\"Record_Image\",\"unique\":{},\"data_type\":\"profileimage\",\"formula\":{},\"decimal_place\":null,\"mass_update\":false,\"pick_list_values\":[],\"multiselectlookup\":{},\"auto_number\":{}}]}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/fields?module=Contacts")
                .Respond(HttpStatusCode.NoContent);

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/fields?module=Home")
                .Respond(HttpStatusCode.BadRequest);

            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {new Schema {Id = "2"}}
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Empty(response.Schemas);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When(
                    "https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json",
                    "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/modules")
                .Respond("application/json",
                    "{\"modules\":[{\"global_search_supported\":false,\"deletable\":false,\"creatable\":false,\"modified_time\":null,\"plural_label\":\"Home\",\"presence_sub_menu\":false,\"id\":\"3656031000000002173\",\"visibility\":1,\"convertable\":false,\"editable\":false,\"emailTemplate_support\":false,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":false,\"web_link\":null,\"sequence_number\":1,\"singular_label\":\"Home\",\"viewable\":true,\"api_supported\":false,\"api_name\":\"Home\",\"quick_create\":false,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":false,\"arguments\":[],\"module_name\":\"Home\",\"business_card_field_limit\":0,\"parent_module\":{}},{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Leads\",\"presence_sub_menu\":true,\"id\":\"3656031000000002175\",\"visibility\":1,\"convertable\":true,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":2,\"singular_label\":\"Lead\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Leads\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Leads\",\"business_card_field_limit\":5,\"parent_module\":{}},{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Contacts\",\"presence_sub_menu\":true,\"id\":\"3656031000000002179\",\"visibility\":1,\"convertable\":false,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":3,\"singular_label\":\"Contact\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Contacts\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Contacts\",\"business_card_field_limit\":5,\"parent_module\":{}}]}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/Leads?page=1")
                .Respond("application/json",
                    "{\"data\":[{\"Owner\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Company\":\"Redeker\",\"Email\":\"Lezlie-craghead@craghead.org\",\"Description\":null,\"$currency_symbol\":\"$\",\"Rating\":null,\"Website\":\"www.kingchristopheraesq.com\",\"Twitter\":\"kris\",\"Salutation\":\"Mr.\",\"Last_Activity_Time\":null,\"First_Name\":\"Lezlie\",\"Lead_Status\":\"AttemptedtoContact\",\"Industry\":\"StorageEquipment\",\"Full_Name\":\"Mr.LezlieCraghead\",\"Record_Image\":null,\"Modified_By\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Skype_ID\":\"Lezlie-craghead\",\"$converted\":false,\"$process_flow\":false,\"Phone\":\"555-555-5555\",\"Street\":\"228RunamuckPl#2808\",\"Zip_Code\":\"21224\",\"id\":\"3656031000000193851\",\"Email_Opt_Out\":false,\"$approved\":true,\"Designation\":\"VPAccounting\",\"$approval\":{\"delegate\":false,\"approve\":false,\"reject\":false,\"resubmit\":false},\"Modified_Time\":\"2018-12-10T13:15:05-08:00\",\"Created_Time\":\"2018-12-10T13:15:05-08:00\",\"$converted_detail\":{},\"$editable\":true,\"City\":\"Baltimore\",\"No_of_Employees\":0,\"Mobile\":\"555-555-5555\",\"Prediction_Score\":null,\"Last_Name\":\"Craghead\",\"State\":\"MD\",\"Lead_Source\":\"TradeShow\",\"Country\":\"BaltimoreCity\",\"Tag\":[],\"Created_By\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Fax\":null,\"Annual_Revenue\":850000,\"Secondary_Email\":null},{\"Owner\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Company\":\"Redeker\",\"Email\":\"Lezlie-craghead@craghead.org\",\"Description\":null,\"$currency_symbol\":\"$\",\"Rating\":null,\"Website\":\"www.kingchristopheraesq.com\",\"Twitter\":\"kris\",\"Salutation\":\"Mr.\",\"Last_Activity_Time\":null,\"First_Name\":\"Lezlie\",\"Lead_Status\":\"AttemptedtoContact\",\"Industry\":\"StorageEquipment\",\"Full_Name\":\"Mr.LezlieCraghead\",\"Record_Image\":null,\"Modified_By\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Skype_ID\":\"Lezlie-craghead\",\"$converted\":false,\"$process_flow\":false,\"Phone\":\"555-555-5555\",\"Street\":\"228RunamuckPl#2808\",\"Zip_Code\":\"21224\",\"id\":\"3656031000000193851\",\"Email_Opt_Out\":false,\"$approved\":true,\"Designation\":\"VPAccounting\",\"$approval\":{\"delegate\":false,\"approve\":false,\"reject\":false,\"resubmit\":false},\"Modified_Time\":\"2018-12-10T13:15:05-08:00\",\"Created_Time\":\"2018-12-10T13:15:05-08:00\",\"$converted_detail\":{},\"$editable\":true,\"City\":\"Baltimore\",\"No_of_Employees\":0,\"Mobile\":\"555-555-5555\",\"Prediction_Score\":null,\"Last_Name\":\"Craghead\",\"State\":\"MD\",\"Lead_Source\":\"TradeShow\",\"Country\":\"BaltimoreCity\",\"Tag\":[],\"Created_By\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Fax\":null,\"Annual_Revenue\":850000,\"Secondary_Email\":null}],\"info\":{\"per_page\":200,\"count\":2,\"page\":1,\"more_records\":false}}");
            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new ReadRequest()
            {
                Schema = new Schema {Name = "Leads"}
            };

            // act
            client.Connect(connectRequest);
            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(2, records.Count);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamLimitTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When(
                    "https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json",
                    "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/modules")
                .Respond("application/json",
                    "{\"modules\":[{\"global_search_supported\":false,\"deletable\":false,\"creatable\":false,\"modified_time\":null,\"plural_label\":\"Home\",\"presence_sub_menu\":false,\"id\":\"3656031000000002173\",\"visibility\":1,\"convertable\":false,\"editable\":false,\"emailTemplate_support\":false,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":false,\"web_link\":null,\"sequence_number\":1,\"singular_label\":\"Home\",\"viewable\":true,\"api_supported\":false,\"api_name\":\"Home\",\"quick_create\":false,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":false,\"arguments\":[],\"module_name\":\"Home\",\"business_card_field_limit\":0,\"parent_module\":{}},{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Leads\",\"presence_sub_menu\":true,\"id\":\"3656031000000002175\",\"visibility\":1,\"convertable\":true,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":2,\"singular_label\":\"Lead\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Leads\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Leads\",\"business_card_field_limit\":5,\"parent_module\":{}},{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Contacts\",\"presence_sub_menu\":true,\"id\":\"3656031000000002179\",\"visibility\":1,\"convertable\":false,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":3,\"singular_label\":\"Contact\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Contacts\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Contacts\",\"business_card_field_limit\":5,\"parent_module\":{}}]}");

            mockHttp.When("https://www.zohoapis.com/crm/v2/Leads?page=1")
                .Respond("application/json",
                    "{\"data\":[{\"Owner\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Company\":\"Redeker\",\"Email\":\"Lezlie-craghead@craghead.org\",\"Description\":null,\"$currency_symbol\":\"$\",\"Rating\":null,\"Website\":\"www.kingchristopheraesq.com\",\"Twitter\":\"kris\",\"Salutation\":\"Mr.\",\"Last_Activity_Time\":null,\"First_Name\":\"Lezlie\",\"Lead_Status\":\"AttemptedtoContact\",\"Industry\":\"StorageEquipment\",\"Full_Name\":\"Mr.LezlieCraghead\",\"Record_Image\":null,\"Modified_By\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Skype_ID\":\"Lezlie-craghead\",\"$converted\":false,\"$process_flow\":false,\"Phone\":\"555-555-5555\",\"Street\":\"228RunamuckPl#2808\",\"Zip_Code\":\"21224\",\"id\":\"3656031000000193851\",\"Email_Opt_Out\":false,\"$approved\":true,\"Designation\":\"VPAccounting\",\"$approval\":{\"delegate\":false,\"approve\":false,\"reject\":false,\"resubmit\":false},\"Modified_Time\":\"2018-12-10T13:15:05-08:00\",\"Created_Time\":\"2018-12-10T13:15:05-08:00\",\"$converted_detail\":{},\"$editable\":true,\"City\":\"Baltimore\",\"No_of_Employees\":0,\"Mobile\":\"555-555-5555\",\"Prediction_Score\":null,\"Last_Name\":\"Craghead\",\"State\":\"MD\",\"Lead_Source\":\"TradeShow\",\"Country\":\"BaltimoreCity\",\"Tag\":[],\"Created_By\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Fax\":null,\"Annual_Revenue\":850000,\"Secondary_Email\":null},{\"Owner\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Company\":\"Redeker\",\"Email\":\"Lezlie-craghead@craghead.org\",\"Description\":null,\"$currency_symbol\":\"$\",\"Rating\":null,\"Website\":\"www.kingchristopheraesq.com\",\"Twitter\":\"kris\",\"Salutation\":\"Mr.\",\"Last_Activity_Time\":null,\"First_Name\":\"Lezlie\",\"Lead_Status\":\"AttemptedtoContact\",\"Industry\":\"StorageEquipment\",\"Full_Name\":\"Mr.LezlieCraghead\",\"Record_Image\":null,\"Modified_By\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Skype_ID\":\"Lezlie-craghead\",\"$converted\":false,\"$process_flow\":false,\"Phone\":\"555-555-5555\",\"Street\":\"228RunamuckPl#2808\",\"Zip_Code\":\"21224\",\"id\":\"3656031000000193851\",\"Email_Opt_Out\":false,\"$approved\":true,\"Designation\":\"VPAccounting\",\"$approval\":{\"delegate\":false,\"approve\":false,\"reject\":false,\"resubmit\":false},\"Modified_Time\":\"2018-12-10T13:15:05-08:00\",\"Created_Time\":\"2018-12-10T13:15:05-08:00\",\"$converted_detail\":{},\"$editable\":true,\"City\":\"Baltimore\",\"No_of_Employees\":0,\"Mobile\":\"555-555-5555\",\"Prediction_Score\":null,\"Last_Name\":\"Craghead\",\"State\":\"MD\",\"Lead_Source\":\"TradeShow\",\"Country\":\"BaltimoreCity\",\"Tag\":[],\"Created_By\":{\"name\":\"Name\",\"id\":\"3656031000000191017\"},\"Fax\":null,\"Annual_Revenue\":850000,\"Secondary_Email\":null}],\"info\":{\"per_page\":200,\"count\":2,\"page\":1,\"more_records\":false}}");

            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new ReadRequest()
            {
                Schema = new Schema {Name = "Leads"},
                Limit = 1
            };

            // act
            client.Connect(connectRequest);
            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task PrepareWriteTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When(
                    "https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json",
                    "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");
            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/modules")
                .Respond("application/json",
                    "{\"modules\":[{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Leads\",\"presence_sub_menu\":true,\"id\":\"3656031000000002175\",\"visibility\":1,\"convertable\":true,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":2,\"singular_label\":\"Lead\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Leads\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Leads\",\"business_card_field_limit\":5,\"parent_module\":{}},]}");


            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new PrepareWriteRequest()
            {
                Schema = new Schema {Name = "Leads"},
                CommitSlaSeconds = 1
            };

            // act
            client.Connect(connectRequest);
            var response = client.PrepareWrite(request);

            // assert
            Assert.IsType<PrepareWriteResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task WriteStreamTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            mockHttp.When(
                    "https://accounts.zoho.com/oauth/v2/token?refresh_token=refresh&client_id=client&client_secret=secret&grant_type=refresh_token")
                .Respond("application/json",
                    "{\"access_token\":\"mocktoken\",\"expires_in_sec\":3600,\"api_domain\":\"testdomain\",\"token_type\":\"Bearer\",\"expires_in\":3600000}");
            mockHttp.When("https://www.zohoapis.com/crm/v2/settings/modules")
                .Respond("application/json",
                    "{\"modules\":[{\"global_search_supported\":true,\"deletable\":true,\"creatable\":true,\"modified_time\":null,\"plural_label\":\"Leads\",\"presence_sub_menu\":true,\"id\":\"3656031000000002175\",\"visibility\":1,\"convertable\":true,\"editable\":true,\"emailTemplate_support\":true,\"profiles\":[{\"name\":\"Administrator\",\"id\":\"3656031000000026011\"},{\"name\":\"Standard\",\"id\":\"3656031000000026014\"}],\"filter_supported\":true,\"web_link\":null,\"sequence_number\":2,\"singular_label\":\"Lead\",\"viewable\":true,\"api_supported\":true,\"api_name\":\"Leads\",\"quick_create\":true,\"modified_by\":null,\"generated_type\":\"default\",\"feeds_required\":false,\"scoring_supported\":true,\"arguments\":[],\"module_name\":\"Leads\",\"business_card_field_limit\":5,\"parent_module\":{}},]}");
            mockHttp.When("https://www.zohoapis.com/crm/v2/Leads/1")
                .Respond("application/json",
                    "{\"data\": [{\"id\": 1,\"Modified_Time\": \"2/13/2019\"}]}");
            mockHttp.When("https://www.zohoapis.com/crm/v2/Leads")
                .Respond(HttpStatusCode.OK);
            
            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var prepareRequest = new PrepareWriteRequest()
            {
                Schema = new Schema
                {
                    Name = "Leads",
                    Properties =
                    {
                        new Property
                        {
                            Id = "Modified_Time",
                            IsUpdateCounter = true
                        }
                    }
                },
                CommitSlaSeconds = 1
            };
            
            var records = new List<Record>()
            {
                {new Record
                {
                    Action = Record.Types.Action.Upsert,
                    CorrelationId = "test",
                    DataJson = "{\"id\":1,\"Modified_Time\":\"2/14/2019\"}"
                }}
            };
            
            var recordAcks = new List<RecordAck>();

            // act
            client.Connect(connectRequest);
            client.PrepareWrite(prepareRequest);
            
            using (var call = client.WriteStream())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var ack = call.ResponseStream.Current;
                        recordAcks.Add(ack);
                    }
                });

                foreach (Record record in records)
                {
                    await call.RequestStream.WriteAsync(record);
                }
                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }

            // assert
            Assert.Single(recordAcks);
            Assert.Equal("",recordAcks[0].Error);
            Assert.Equal("test",recordAcks[0].CorrelationId);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DisconnectTest()
        {
            // setup
            var mockHttp = new MockHttpMessageHandler();

            Server server = new Server
            {
                Services = {Publisher.BindService(new Plugin_Zoho.Plugin.Plugin(mockHttp.ToHttpClient()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = new DisconnectRequest();

            // act
            var response = client.Disconnect(request);

            // assert
            Assert.IsType<DisconnectResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
    }
}