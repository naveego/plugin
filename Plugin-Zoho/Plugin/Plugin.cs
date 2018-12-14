using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
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

        public Plugin(HttpClient client = null)
        {
            _injectedClient = client != null ? client : new HttpClient();
            _server = new ServerStatus();
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
            
            // validate settings passed in
            try
            {
                _server.Settings = JsonConvert.DeserializeObject<Settings>(request.SettingsJson);
                _server.Settings.Validate();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
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
                throw;
            }
            
            return new ConnectResponse();
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
                var response = await _client.GetAsync("https://www.zohoapis.com/crm/v2/settings/modules");
                response.EnsureSuccessStatusCode();
    
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