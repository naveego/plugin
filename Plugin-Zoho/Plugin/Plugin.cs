using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Google.Protobuf.Collections;
using Grpc.Core;
using Newtonsoft.Json;
using Plugin_Zoho.DataContracts;
using Plugin_Zoho.Helper;
using Pub;

namespace Plugin_Zoho.Plugin
{
    public class Plugin : Publisher.PublisherBase
    {
        private RequestHelper _client;
        private readonly HttpClient _injectedClient;
        private readonly ServerStatus _server;
        private TaskCompletionSource<bool> _tcs;

        public Plugin(HttpClient client = null)
        {
            _injectedClient = client != null ? client : new HttpClient();
            _server = new ServerStatus();
        }

        /// <summary>
        /// Creates an authorization url for oauth requests
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BeginOAuthFlowResponse> BeginOAuthFlow(BeginOAuthFlowRequest request,
            ServerCallContext context)
        {
            Logger.Info("Getting Auth URL...");
            
            // params for auth url
            var scope = "ZohoCRM.users.all,ZohoCRM.org.all,ZohoCRM.settings.all,ZohoCRM.modules.all";
            var clientId = request.Configuration.ClientId;
            var responseType = "code";
            var accessType = "offline";
            var redirectUrl = request.RedirectUrl;
            var prompt = "consent";
            
            // build auth url
            var authUrl = String.Format("https://accounts.zoho.com/oauth/v2/auth?scope={0}&client_id={1}&response_type={2}&access_type={3}&redirect_uri={4}&prompt={5}",
                scope,
                clientId,
                responseType,
                accessType,
                redirectUrl,
                prompt);
            
            // return auth url
            var oAuthResponse = new BeginOAuthFlowResponse
            {
                AuthorizationUrl = authUrl
            };
            
            Logger.Info($"Created Auth URL: {authUrl}");
            
            return Task.FromResult(oAuthResponse);
        }

        /// <summary>
        /// Gets auth token and refresh tokens from auth code
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task<CompleteOAuthFlowResponse> CompleteOAuthFlow(CompleteOAuthFlowRequest request, ServerCallContext context)
        {
            Logger.Info("Getting Auth and Refresh Token...");
            
            // get code from redirect url
            string code;
            var uri = new Uri(request.RedirectUrl);
            
            try
            {
                code = HttpUtility.ParseQueryString(uri.Query).Get("code");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            // token url parameters
            var redirectUrl = String.Format("{0}{1}{2}{3}", uri.Scheme, Uri.SchemeDelimiter, uri.Authority, uri.AbsolutePath);
            var clientId = request.Configuration.ClientId;
            var clientSecret = request.Configuration.ClientSecret;
            var grantType = "authorization_code";
            
            // build token url
            var tokenUrl = String.Format("https://accounts.zoho.com/oauth/v2/token?code={0}&redirect_uri={1}&client_id={2}&client_secret={3}&grant_type={4}",
                code,
                redirectUrl,
                clientId,
                clientSecret,
                grantType
                );

            // get tokens
            var oAuthState = new OAuthState();
            try
            {
                var response = await _injectedClient.PostAsync(tokenUrl, null);
                response.EnsureSuccessStatusCode();

                var content = JsonConvert.DeserializeObject<TokenResponse>(await response.Content.ReadAsStringAsync());

                oAuthState.AuthToken = content.access_token;
                oAuthState.RefreshToken = content.refresh_token;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }

            // return oauth state json
            var oAuthResponse = new CompleteOAuthFlowResponse
            {
                OauthStateJson = JsonConvert.SerializeObject(oAuthState)
            };
            
            Logger.Info("Got Auth Token and Refresh Token");

            return oAuthResponse;
        }

        /// <summary>
        /// Establishes a connection with Zoho CRM. Creates an authenticated http client and tests it.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>A message indicating connection success</returns>
        public override async Task<ConnectResponse> Connect(ConnectRequest request, ServerCallContext context)
        {        
            _server.Connected = false;
            
            Logger.Info("Connecting...");
            Logger.Info("Got OAuth State: " + !String.IsNullOrEmpty(request.OauthStateJson));
            Logger.Info("Got OAuthConfig " + !String.IsNullOrEmpty(JsonConvert.SerializeObject(request.OauthConfiguration)));

            OAuthState oAuthState;
            try
            {
                oAuthState = JsonConvert.DeserializeObject<OAuthState>(request.OauthStateJson);
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return new ConnectResponse
                {
                    OauthStateJson = request.OauthStateJson,
                    ConnectionError = "",
                    OauthError = e.Message,
                    SettingsError = ""
                };
            }

            var settings = new Settings
            {
                ClientId = request.OauthConfiguration.ClientId,
                ClientSecret = request.OauthConfiguration.ClientSecret,
                RefreshToken = oAuthState.RefreshToken
            };
            
            // validate settings passed in
            try
            {
                _server.Settings = settings;
                _server.Settings.Validate();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return new ConnectResponse
                {
                    OauthStateJson = request.OauthStateJson,
                    ConnectionError = "",
                    OauthError = "",
                    SettingsError = e.Message
                };
            }
            
            // create new authenticated request helper with validated settings
            try
            {
                _client = new RequestHelper(_server.Settings, _injectedClient);
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            // attempt to call the Zoho api
            try
            {
                var response = await _client.GetAsync("https://www.zohoapis.com/crm/v2/settings/modules");
                response.EnsureSuccessStatusCode();

                _server.Connected = true;
                
                Logger.Info("Connected to Zoho");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                
                return new ConnectResponse
                {
                    OauthStateJson = request.OauthStateJson,
                    ConnectionError = e.Message,
                    OauthError = "",
                    SettingsError = ""
                };
            }
            
            return new ConnectResponse
            {
                OauthStateJson = request.OauthStateJson,
                ConnectionError = "",
                OauthError = "",
                SettingsError = ""
            };
        }

        public override async Task ConnectSession(ConnectRequest request, IServerStreamWriter<ConnectResponse> responseStream, ServerCallContext context)
        {
            Logger.Info("Connecting session...");
            
            // create task to wait for disconnect to be called
            _tcs?.SetResult(true);
            _tcs = new TaskCompletionSource<bool>();
            
            // call connect method
            var response = await Connect(request, context);

            await responseStream.WriteAsync(response);

            Logger.Info("Session connected.");

            // wait for disconnect to be called
            await _tcs.Task;
        }

        
        /// <summary>
        /// Discovers shapes located in the users Zoho CRM instance
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>Discovered shapes</returns>
        public override async Task<DiscoverShapesResponse> DiscoverShapes(DiscoverShapesRequest request, ServerCallContext context)
        {
            Logger.Info("Discovering Shapes...");
            
            DiscoverShapesResponse discoverShapesResponse = new DiscoverShapesResponse();
            ModuleResponse modulesResponse;
            
            // get the modules present in Zoho
            try
            {
                Logger.Debug("Getting modules...");
                var response = await _client.GetAsync("https://www.zohoapis.com/crm/v2/settings/modules");
                response.EnsureSuccessStatusCode();
                
                Logger.Debug(await response.Content.ReadAsStringAsync());
    
                modulesResponse = JsonConvert.DeserializeObject<ModuleResponse>(await response.Content.ReadAsStringAsync());
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            // attempt to get a shape for each module found
            try
            {
                Logger.Info($"Shapes attempted: {modulesResponse.modules.Length}");

                var tasks = modulesResponse.modules.Select((x, i) => GetShapeForModule(x, i.ToString()))
                    .ToArray();

                await Task.WhenAll(tasks);
                    
                discoverShapesResponse.Shapes.AddRange(tasks.Where(x => x.Result != null).Select(x => x.Result));
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            
            Logger.Info($"Shapes found: {discoverShapesResponse.Shapes.Count}");
            
            // only return requested shapes if refresh mode selected
            if (request.Mode == DiscoverShapesRequest.Types.Mode.Refresh)
            {
                var refreshShapes = request.ToRefresh;
                var shapes = discoverShapesResponse.Shapes;
                discoverShapesResponse.Shapes.Clear(); 
                discoverShapesResponse.Shapes.AddRange(shapes.Join(refreshShapes, shape => shape.Id, refresh => refresh.Id, (shape, refresh) => shape));

                return discoverShapesResponse;
            }
            // return all shapes otherwise
            else
            {
                return discoverShapesResponse;
            }
        }
        
        /// <summary>
        /// Publishes a stream of data for a given shape
        /// </summary>
        /// <param name="request"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task PublishStream(PublishRequest request, IServerStreamWriter<Record> responseStream, ServerCallContext context)
        {
            var shape = request.Shape;
            var limit = request.Limit;
            var limitFlag = request.Limit != 0;

            Logger.Info($"Publishing records for shape: {shape.Name}");
            
            try
            {
                RecordsResponse recordsResponse;
                int page = 1;
                int recordsCount = 0;
                
                // Publish records for the given shape
                do
                {
                    // get records for shape page by page
                    var response = await _client.GetAsync(String.Format("https://www.zohoapis.com/crm/v2/{0}?page={1}", shape.Name, page));
                    response.EnsureSuccessStatusCode();
    
                    recordsResponse = JsonConvert.DeserializeObject<RecordsResponse>(await response.Content.ReadAsStringAsync());
                    
                    Logger.Debug($"data: {JsonConvert.SerializeObject(recordsResponse.data)}");
                    
                    // publish each record in the page
                    foreach (var record in recordsResponse.data)
                    {
                        var recordOutput = new Record
                        {
                            Action = Record.Types.Action.Upsert,
                            DataJson = JsonConvert.SerializeObject(record)
                        };

                        // stop publishing if the limit flag is enabled and the limit has been reached
                        if (limitFlag && recordsCount == limit)
                        {
                            break;
                        }
                        
                        // publish record
                        await responseStream.WriteAsync(recordOutput);
                        recordsCount++;
                    }

                    // get next page
                    page++;
                }
                // keep fetching while there are more pages and the plugin is still connected
                while (recordsResponse.info.more_records && _server.Connected);
                
                Logger.Info($"Published {recordsCount} records");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }

        /// <summary>
        /// Handles disconnect requests from the agent
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<DisconnectResponse> Disconnect(DisconnectRequest request, ServerCallContext context)
        {
            // clear connection
            _server.Connected = false;
            _server.Settings = null;

            // alert connection session to close
            if (_tcs != null)
            {
                _tcs.SetResult(true);
                _tcs = null;
            }
            
            Logger.Info("Disconnected");
            return Task.FromResult(new DisconnectResponse());
        }

        /// <summary>
        /// Gets a shape for a given module
        /// </summary>
        /// <param name="module"></param>
        /// <param name="id"></param>
        /// <returns>returns a shape or null if unavailable</returns>
        private async Task<Shape> GetShapeForModule(Module module, string id)
        {
            // base shape to be added to
            var shape = new Shape
            {
                Id = id,
                Name = module.api_name,
                Description = module.module_name
            };
            
            try
            {
                Logger.Debug($"Getting fields for: {module.module_name}");
            
                // get fields for module
                var response = await _client.GetAsync(String.Format("https://www.zohoapis.com/crm/v2/settings/fields?module={0}", module.api_name));
                
                // if response is empty or call did not succeed return null
                if (response.StatusCode == HttpStatusCode.NoContent || !response.IsSuccessStatusCode)
                {
                    Logger.Debug($"No fields for: {module.module_name}");
                    return null;
                }
                
                Logger.Debug($"Got fields for: {module.module_name}");
                
                // for each field in the shape add a new property
                var fieldsResponse = JsonConvert.DeserializeObject<FieldsResponse>(await response.Content.ReadAsStringAsync());

                for (var k = 0; k < fieldsResponse.fields.Length; k++)
                {
                    var field = fieldsResponse.fields[k];
                    var property = new Property
                    {
                        Id = k.ToString(),
                        Name = field.field_label,
                        Type = GetPropertyType(field),
                        IsKey = false,
                        IsCreateCounter = field.api_label == "Created_Time",
                        IsUpdateCounter = field.api_label == "Modified_Time",
                        TypeAtSource = field.data_type,
                        IsNullable = field.api_label == "Created_Time" || field.api_label == "Modified_Time"
                    };
                
                    shape.Properties.Add(property);
                }
                
                Logger.Debug($"Added shape for: {module.module_name}");
                return shape;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return null;
            }
        }

        /// <summary>
        /// Gets the Naveego type from the provided Zoho information
        /// </summary>
        /// <param name="field"></param>
        /// <returns>The property type</returns>
        private PropertyType GetPropertyType(Field field)
        {
            switch (field.json_type)
            {
                case "boolean":
                    return PropertyType.Bool;
                case "double":
                    return PropertyType.Float;
                case "integer":
                    return PropertyType.Integer;
                case "jsonarray":
                    return PropertyType.Json;
                case "jsonobject":
                    return PropertyType.Json;
                case "string":
                    if (field.data_type == "datetime")
                    {
                        return PropertyType.Datetime;
                    }
                    else if (field.length > 1024)
                    {
                        return PropertyType.Text;
                    }
                    else
                    {
                        return PropertyType.String;
                    }
                default:
                    return PropertyType.String;
            }
        }
    }
}