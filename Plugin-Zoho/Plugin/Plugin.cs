using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
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
            _server = new ServerStatus
            {
                Connected = false,
                WriteConfigured = false
            };
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
            var authUrl = String.Format(
                "https://accounts.zoho.com/oauth/v2/auth?scope={0}&client_id={1}&response_type={2}&access_type={3}&redirect_uri={4}&prompt={5}",
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
        public override async Task<CompleteOAuthFlowResponse> CompleteOAuthFlow(CompleteOAuthFlowRequest request,
            ServerCallContext context)
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
            var redirectUrl = String.Format("{0}{1}{2}{3}", uri.Scheme, Uri.SchemeDelimiter, uri.Authority,
                uri.AbsolutePath);
            var clientId = request.Configuration.ClientId;
            var clientSecret = request.Configuration.ClientSecret;
            var grantType = "authorization_code";

            // build token url
            var tokenUrl = String.Format(
                "https://accounts.zoho.com/oauth/v2/token?code={0}&redirect_uri={1}&client_id={2}&client_secret={3}&grant_type={4}",
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

                if (String.IsNullOrEmpty(oAuthState.RefreshToken))
                {
                    throw new Exception("Response did not contain a refresh token");
                }
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
            Logger.Info("Got OAuthConfig " +
                        !String.IsNullOrEmpty(JsonConvert.SerializeObject(request.OauthConfiguration)));

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

        public override async Task ConnectSession(ConnectRequest request,
            IServerStreamWriter<ConnectResponse> responseStream, ServerCallContext context)
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
        /// Discovers schemas located in the users Zoho CRM instance
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>Discovered schemas</returns>
        public override async Task<DiscoverSchemasResponse> DiscoverSchemas(DiscoverSchemasRequest request,
            ServerCallContext context)
        {
            Logger.Info("Discovering Schemas...");

            DiscoverSchemasResponse discoverSchemasResponse = new DiscoverSchemasResponse();
            ModuleResponse modulesResponse;

            // get the modules present in Zoho
            try
            {
                Logger.Debug("Getting modules...");
                var response = await _client.GetAsync("https://www.zohoapis.com/crm/v2/settings/modules");
                response.EnsureSuccessStatusCode();

                Logger.Debug(await response.Content.ReadAsStringAsync());

                modulesResponse =
                    JsonConvert.DeserializeObject<ModuleResponse>(await response.Content.ReadAsStringAsync());
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }

            // attempt to get a schema for each module found
            try
            {
                Logger.Info($"Schemas attempted: {modulesResponse.modules.Length}");

                var tasks = modulesResponse.modules.Select(GetSchemaForModule)
                    .ToArray();

                await Task.WhenAll(tasks);

                discoverSchemasResponse.Schemas.AddRange(tasks.Where(x => x.Result != null).Select(x => x.Result));
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }

            Logger.Info($"Schemas found: {discoverSchemasResponse.Schemas.Count}");

            // only return requested schemas if refresh mode selected
            if (request.Mode == DiscoverSchemasRequest.Types.Mode.Refresh)
            {
                var refreshSchemas = request.ToRefresh;
                var schemas =
                    JsonConvert.DeserializeObject<Schema[]>(
                        JsonConvert.SerializeObject(discoverSchemasResponse.Schemas));
                discoverSchemasResponse.Schemas.Clear();
                discoverSchemasResponse.Schemas.AddRange(schemas.Join(refreshSchemas, GetModuleName, GetModuleName, (shape, refresh) => shape));

                Logger.Debug($"Schemas found: {JsonConvert.SerializeObject(schemas)}");
                Logger.Debug($"Refresh requested on schemas: {JsonConvert.SerializeObject(refreshSchemas)}");

                Logger.Info($"Schemas returned: {discoverSchemasResponse.Schemas.Count}");
                return discoverSchemasResponse;
            }

            // return all schemas otherwise
            Logger.Info($"Schemas returned: {discoverSchemasResponse.Schemas.Count}");
            return discoverSchemasResponse;
        }

        /// <summary>
        /// Publishes a stream of data for a given schema
        /// </summary>
        /// <param name="request"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task ReadStream(ReadRequest request, IServerStreamWriter<Record> responseStream,
            ServerCallContext context)
        {
            var schema = request.Schema;
            var limit = request.Limit;
            var limitFlag = request.Limit != 0;

            Logger.Info($"Publishing records for schema: {schema.Name}");
            
            // get information from schema
            var moduleName = GetModuleName(schema);

            try
            {
                RecordsResponse recordsResponse;
                int page = 1;
                int recordsCount = 0;
                // Publish records for the given schema
                do
                {                
                    // get records for schema page by page
                    var response = await _client.GetAsync(String.Format("https://www.zohoapis.com/crm/v2/{0}?page={1}",
                        moduleName, page));
                    response.EnsureSuccessStatusCode();

                    // if response is empty or call did not succeed return no records
                    if (!IsSuccessAndNotEmpty(response))
                    {
                        Logger.Info($"No records for: {schema.Name}");
                        return;
                    }
                    
                    recordsResponse = JsonConvert.DeserializeObject<RecordsResponse>(await response.Content.ReadAsStringAsync());
                    
                    Logger.Debug($"data: {JsonConvert.SerializeObject(recordsResponse.data)}");

                    // publish each record in the page
                    foreach (var record in recordsResponse.data)
                    {
                        var outRecord = new Dictionary<string, object>();
                        
                        foreach (var property in schema.Properties)
                        {
                            object value;
                            switch (property.Type)
                            {
                                case PropertyType.String:
                                    value = record[property.Id];
                                    if (!(value is string))
                                    {
                                        record[property.Id] = JsonConvert.SerializeObject(value);
                                    }
                                    break;
                                case PropertyType.Json:
                                    value = record[property.Id];
                                    record[property.Id] = new ReadRecordObject
                                    {
                                        Data = value
                                    };
                                    break;
                            }

                            outRecord.Add(property.Id, record.ContainsKey(property.Id) ? record[property.Id] : null);
                        }
                        
                        var recordOutput = new Record
                        {
                            Action = Record.Types.Action.Upsert,
                            DataJson = JsonConvert.SerializeObject(outRecord)
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

                    // stop publishing if the limit flag is enabled and the limit has been reached
                    if (limitFlag && recordsCount == limit)
                    {
                        break;
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
        /// Prepares the plugin to handle a write request
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<PrepareWriteResponse> PrepareWrite(PrepareWriteRequest request, ServerCallContext context)
        {
            Logger.Info("Preparing write...");
            _server.WriteConfigured = false;

            var writeSettings = new WriteSettings
            {
                CommitSLA = request.CommitSlaSeconds,
                Schema = request.Schema
            };

            _server.WriteSettings = writeSettings;
            _server.WriteConfigured = true;

            Logger.Info("Write prepared.");
            return Task.FromResult(new PrepareWriteResponse());
        }

        /// <summary>
        /// Takes in records and writes them out to the Zoho instance then sends acks back to the client
        /// </summary>
        /// <param name="requestStream"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task WriteStream(IAsyncStreamReader<Record> requestStream,
            IServerStreamWriter<RecordAck> responseStream, ServerCallContext context)
        {
            try
            {
                Logger.Info("Writing records to Zoho...");
                var schema = _server.WriteSettings.Schema;
                var sla = _server.WriteSettings.CommitSLA;
                var inCount = 0;
                var outCount = 0;
                
                // get next record to publish while connected and configured
                while (await requestStream.MoveNext(context.CancellationToken) && _server.Connected && _server.WriteConfigured)
                {
                    var record = requestStream.Current;
                    inCount++;
                    
                    Logger.Debug($"Got record: {record.DataJson}");
                    
                    // send record to source system
                    // timeout if it takes longer than the sla
                    var task = Task.Run(() => PutRecord(schema,record));
                    if (task.Wait(TimeSpan.FromSeconds(sla)))
                    {
                        // send ack
                        var ack = new RecordAck
                        {
                            CorrelationId = record.CorrelationId,
                            Error = task.Result
                        };
                        await responseStream.WriteAsync(ack);
                        
                        if (String.IsNullOrEmpty(task.Result))
                        {
                            outCount++;
                        }
                    }
                    else
                    {
                        // send timeout ack
                        var ack = new RecordAck
                        {
                            CorrelationId = record.CorrelationId,
                            Error = "timed out"
                        };
                        await responseStream.WriteAsync(ack);
                    }
                }
                
                Logger.Info($"Wrote {outCount} of {inCount} records to Zoho.");
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
        /// Gets a schema for a given module
        /// </summary>
        /// <param name="module"></param>
        /// <returns>returns a schema or null if unavailable</returns>
        private async Task<Schema> GetSchemaForModule(Module module)
        {
            // base schema to be added to
            var schema = new Schema
            {
                Id = module.api_name,
                Name = module.api_name,
                Description = module.module_name,
                PublisherMetaJson = JsonConvert.SerializeObject(new PublisherMetaJson
                {
                    Module = module.api_name
                }),
                DataFlowDirection = Schema.Types.DataFlowDirection.ReadWrite
            };

            try
            {
                Logger.Debug($"Getting fields for: {module.module_name}");

                // get fields for module
                var response = await _client.GetAsync(
                    String.Format("https://www.zohoapis.com/crm/v2/settings/fields?module={0}", module.api_name));

                // if response is empty or call did not succeed return null
                if (!IsSuccessAndNotEmpty(response))
                {
                    Logger.Debug($"No fields for: {module.module_name}");
                    return null;
                }

                Logger.Debug($"Got fields for: {module.module_name}");

                // for each field in the schema add a new property
                var fieldsResponse =
                    JsonConvert.DeserializeObject<FieldsResponse>(await response.Content.ReadAsStringAsync());

                var key = new Property
                {
                    Id = "id",
                    Name = "id",
                    Type = PropertyType.String,
                    IsKey = true,
                    IsCreateCounter = false,
                    IsUpdateCounter = false,
                    TypeAtSource = "id",
                    IsNullable = false
                };

                schema.Properties.Add(key);

                foreach (var field in fieldsResponse.fields)
                {
                    var property = new Property
                    {
                        Id = field.api_name,
                        Name = field.field_label,
                        Type = GetPropertyType(field),
                        IsKey = false,
                        IsCreateCounter = field.api_name == "Created_Time",
                        IsUpdateCounter = field.api_name == "Modified_Time",
                        TypeAtSource = field.data_type,
                        IsNullable = true
                    };

                    schema.Properties.Add(property);
                }

                Logger.Debug($"Added schema for: {module.module_name}");
                return schema;
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
                    if (field.data_type == "userlookup")
                    {
                        return PropertyType.Json;
                    }
                    return PropertyType.String;
            }
        }

        /// <summary>
        /// Writes a record out to Zoho
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="record"></param>
        /// <returns></returns>
        private async Task<string> PutRecord(Schema schema, Record record)
        {
            Dictionary<string, object> recObj;
            
            // get information from schema
            var moduleName = GetModuleName(schema);
            
            try
            {
                // check if source has newer record than write back record
                recObj = JsonConvert.DeserializeObject<Dictionary<string, object>>(record.DataJson);
                
                Logger.Info(record.DataJson);

                if (recObj.ContainsKey("id"))
                {
                    var id = recObj["id"];
                
                    // build and send request
                    var uri = String.Format("https://www.zohoapis.com/crm/v2/{0}/{1}", moduleName, id ?? "null");

                    var response = await _client.GetAsync(uri);
                    if (IsSuccessAndNotEmpty(response))
                    {
                        var recordsResponse = JsonConvert.DeserializeObject<RecordsResponse>(await response.Content.ReadAsStringAsync());
                        var srcObj = recordsResponse.data[0];
                
                        // get modified key from schema
                        var modifiedKey = schema.Properties.First(x => x.IsUpdateCounter);

                        if (recObj.ContainsKey(modifiedKey.Id) && srcObj.ContainsKey(modifiedKey.Id))
                        {
                            if (recObj[modifiedKey.Id] != null && srcObj[modifiedKey.Id] != null)
                            {
                                // if source is newer than request then exit
                                if (DateTime.Parse(recObj[modifiedKey.Id].ToString()) <=
                                    DateTime.Parse(srcObj[modifiedKey.Id].ToString()))
                                {
                                    Logger.Info($"Source is newer for record {record.DataJson}");
                                    return "source system is newer than requested write back";
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return e.Message;
            }
            try
            {   
                // build and send request
                var uri = String.Format("https://www.zohoapis.com/crm/v2/{0}/upsert", moduleName);
                
                var putRequestObj = new PutRequest
                {
                    data = new [] {recObj},
                    trigger = new string[0]
                };

                var json = new StringContent(JsonConvert.SerializeObject(putRequestObj), Encoding.UTF8, "application/json");
                
                var response = await _client.PostAsync(uri, json);
                
                response.EnsureSuccessStatusCode();

                var upsertResponse = JsonConvert.DeserializeObject<UpsertResponse>(await response.Content.ReadAsStringAsync());
                var upsertObj = upsertResponse.Data.FirstOrDefault();


                if (upsertObj != null)
                {
                    if (upsertObj.Code == "error")
                    {
                        return upsertObj.Code;
                    }
                }
                
                Logger.Info(await response.Content.ReadAsStringAsync());
                
                Logger.Info("Modified 1 record.");
                return "";
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return e.Message;
            }
        }
        
        /// <summary>
        /// Conversion function that has legacy support for old schemas and correct support for new schemas
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        private string GetModuleName(Schema schema){
            var moduleName = schema.Name;
            var metaJson = JsonConvert.DeserializeObject<PublisherMetaJson>(schema.PublisherMetaJson);
            if (metaJson != null && !string.IsNullOrEmpty(metaJson.Module))
            {
                moduleName = metaJson.Module;
            }

            return moduleName;
        }

        /// <summary>
        /// Checks if a http response message is not empty and did not fail
        /// </summary>
        /// <param name="response"></param>
        /// <returns></returns>
        private bool IsSuccessAndNotEmpty(HttpResponseMessage response)
        {
            return response.StatusCode != HttpStatusCode.NoContent && response.IsSuccessStatusCode;
        }
    }
}