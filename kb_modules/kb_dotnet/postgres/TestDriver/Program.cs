using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TestDriver
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.Write("Enter PostgreSQL password: ");
            var password = ReadPassword();
            
            KBDataStructures kb = null;
            
            try
            {
                Console.WriteLine("Creating KBDataStructures...");
                
                kb = new KBDataStructures(
                    "localhost",
                    5432,
                    "knowledge_base",
                    "gedgar",
                    password,
                    "knowledge_base"
                );

                Console.WriteLine("Connecting to database...");
              Console.WriteLine("Initializing database connections and components...");

                 // Initialize all components with database connection
                await kb.InitializeAsync();

                Console.WriteLine("KBDataStructures fully initialized successfully!");
                
                
                Console.WriteLine("Database connection established successfully!");

                // === STATUS DATA TEST ===
                Console.WriteLine("\n\n\n***************************  status data ***************************");
                
                // Check if components are initialized
                if (kb.FindStatusNodeIds == null)
                {
                    throw new InvalidOperationException("Status data components not initialized. Check KBDataStructures constructor.");
                }
                
                var statusNodeIds = await kb.FindStatusNodeIds(null, null, null, null);
                Console.WriteLine($"node_ids: {statusNodeIds.Count} found");
                var statusPaths = kb.FindPathValues(statusNodeIds);
                Console.WriteLine($"path_values: {statusPaths.Count} found");

            Console.WriteLine("\n=== specific status node ===");
                var specificNode = await kb.FindStatusNodeId(
                    "kb1", 
                    "info2_status", 
                    new Dictionary<string, object> { ["prop3"] = "val3" },
                    "*.header1_link.header1_name.KB_STATUS_FIELD.info2_status"
                );
                Console.WriteLine($"node_id: found");
                var spPath = kb.FindPathValues(new List<dynamic> { specificNode })[0];
                Console.WriteLine($"path: {spPath}");
                var descriptions = kb.FindDescription(specificNode);
                Console.WriteLine($"description: {descriptions}");
                
                // Fix: GetStatusData returns a tuple (object, string)
                var statusResult = await kb.GetStatusData(spPath);
                Console.WriteLine($"initial data: {statusResult.Item1}");
                
                // Fix: SetStatusData method signature matches the interface
                var setResult = await kb.SetStatusData(spPath, new Dictionary<string, object> 
                { 
                    ["prop1"] = "val1", 
                    ["prop2"] = "val2" 
                }, 3, 1000);
                Console.WriteLine($"set result: success={setResult.Item1}, message={setResult.Item2}");
                
                statusResult = await kb.GetStatusData(spPath);
                Console.WriteLine($"after set: {statusResult.Item1}");

                // === JOB QUEUE TEST ===
                Console.WriteLine("***************************  job queue data ***************************");
                var jobNodeIds = await kb.FindJobIds(null, null, null, null);
                
                // Fix: Use FindPathValues for job paths
                var jobPaths = kb.FindPathValues(jobNodeIds);
                var jobPath = jobPaths[0];
                Console.WriteLine($"first job path: {jobPath}");
                
                var clearResult = await kb.ClearJobQueue(jobPath);
                Console.WriteLine($"clear result: {clearResult}");
                Console.WriteLine($"queued_number: {await kb.GetQueuedNumber(jobPath)}");
                Console.WriteLine($"free_number: {await kb.GetFreeNumber(jobPath)}");
                
                var emptyJob = await kb.PeakJobData(jobPath, 3, 1000);
                Console.WriteLine($"peak empty: {emptyJob}");
                
                var pushResult = await kb.PushJobData(jobPath, new Dictionary<string, object> 
                { 
                    ["prop1"] = "val1", 
                    ["prop2"] = "val2" 
                }, 3, 1000);
                Console.WriteLine($"push result: {pushResult}");
                
                Console.WriteLine($"queued: {await kb.GetQueuedNumber(jobPath)}");
                Console.WriteLine($"free: {await kb.GetFreeNumber(jobPath)}");
                Console.WriteLine($"pending: {(await kb.ListPendingJobs(jobPath, null, 0)).Count} jobs");
                Console.WriteLine($"active: {(await kb.ListActiveJobs(jobPath, null, 0)).Count} jobs");
                
                var job = await kb.PeakJobData(jobPath, 3, 1000);
                if (job != null)
                {
                    var jobDict = (Dictionary<string, object>)job;
                    Console.WriteLine($"job_id: {jobDict["id"]}, data: {jobDict["data"]}");
                    var completionResult = await kb.MarkJobCompleted(Convert.ToInt32(jobDict["id"]), 3, 1000);
                    Console.WriteLine($"completion result: {completionResult}");
                }
                
                Console.WriteLine($"post-complete free: {await kb.GetFreeNumber(jobPath)}");

                // === STREAM TABLES TEST ===
                Console.WriteLine("***************************  stream data ***************************");
                var streamNodeIds = await kb.FindStreamIds("kb1", "info1_stream", null, null);
                var streamKeys = kb.FindStreamTableKeys(streamNodeIds);
                Console.WriteLine($"stream_table_keys: {streamKeys.Count} found");
                
                var descriptionPaths = await kb.FindDescriptionPaths(streamKeys);
                Console.WriteLine($"descriptions: {descriptionPaths.Count} found");
                
                if (streamKeys.Count > 0)
                {
                    var clearStreamResult = await kb.ClearStreamData(streamKeys[0], null);
                    Console.WriteLine($"clear stream result: {clearStreamResult}");
                    
                    var pushStreamResult = await kb.PushStreamData(streamKeys[0], new Dictionary<string, object> 
                    { 
                        ["prop1"] = "val1", 
                        ["prop2"] = "val2" 
                    }, 3, 1000);
                    Console.WriteLine($"push stream result: {pushStreamResult}");
                    
                    var streamData = await kb.ListStreamData(streamKeys[0], null, 0, null, null, "ASC");
                    Console.WriteLine($"list_stream_data: {streamData.Count} records");
                    
                    // Use DateTime for filtering
                    var fifteenMinutesAgo = DateTime.Now.AddMinutes(-15);
                    var now = DateTime.Now;
                    var filtered = await kb.ListStreamData(streamKeys[0], null, 0, fifteenMinutesAgo, now, "ASC");
                    Console.WriteLine($"filtered: {filtered.Count} records");
                }

                // === RPC CLIENT/SERVER TEST ===
                Console.WriteLine("***************************  RPC Client Functions ***************************");
                var clientNodeIds = await kb.RpcClientFindRpcClientIds(null, null, null, null);
                Console.WriteLine($"rpc_client_node_ids: {clientNodeIds.Count} found");
                
                if (clientNodeIds.Count > 0)
                {
                    var clientKeys = kb.RpcClientFindRpcClientKeys(clientNodeIds);
                    if (clientKeys.Count > 0)
                    {
                        await TestClientQueue(kb, clientKeys[0]);
                    }
                }

                Console.WriteLine("***************************  RPC Server Functions ***************************");
                var serverNodeIds = await kb.RpcServerFindIds(null, null, null, null);
                Console.WriteLine($"rpc_server_node_ids: {serverNodeIds.Count} found");
                
                if (serverNodeIds.Count > 0)
                {
                    var serverKeys = kb.RpcServerFindTableKeys(serverNodeIds);
                    if (serverKeys.Count > 0)
                    {
                        await TestServerFunctions(kb, serverKeys[0]);
                    }
                }

                // === LINK TABLES TEST ===
                Console.WriteLine("***************************  Link Tables ***************************");
                kb.ClearFilters();
                kb.SearchStartingPath("kb1.header1_link.header1_name");
                var startingPathResults = await kb.ExecuteKbSearch();
                Console.WriteLine($"starting_path results: {startingPathResults.Count} found");
                
                kb.ClearFilters();
                kb.SearchHasLink();
                var hasLinkResults = await kb.ExecuteKbSearch();
                Console.WriteLine($"has_link results: {hasLinkResults.Count} found");
                
                var linkNames = await kb.LinkTableFindAllLinkNames();
                Console.WriteLine($"link_names: {linkNames.Count} found");
                
                var nodeNames = await kb.LinkTableFindAllNodeNames();
                Console.WriteLine($"node_names: {nodeNames.Count} found");

                // === LINK MOUNT TABLE TEST ===
                kb.ClearFilters();
                kb.SearchHasLinkMount();
                var hasLinkMountResults = await kb.ExecuteKbSearch();
                Console.WriteLine($"has_link_mount: {hasLinkMountResults.Count} found");
                
                var linkMountNames = await kb.LinkMountTableFindAllLinkNames();
                Console.WriteLine($"link_mount_names: {linkMountNames.Count} found");
                
                var mountPaths = await kb.LinkMountTableFindAllMountPaths();
                Console.WriteLine($"mount_paths: {mountPaths.Count} found");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
            finally
            {
                if (kb != null)
                {
                    await kb.DisconnectAsync();
                }
            }
        }

        static async Task TestServerFunctions(KBDataStructures kb, string serverPath)
        {
            Console.WriteLine($"rpc_server_path: {serverPath}");
            Console.WriteLine("initial state");

            var clearResult = await kb.RpcServerClearServerQueue(serverPath, 3, 1000);
            Console.WriteLine($"clear server queue result: {clearResult}");
            
            var initialJobs = await kb.RpcServerListJobsJobTypes(serverPath, "new_job");
            Console.WriteLine($"list_jobs_job_types: {initialJobs.Count} jobs");

            var requestIds = new[] { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
            
            for (int i = 0; i < requestIds.Length; i++)
            {
                var pushResult = await kb.RpcServerPushRpcQueue(
                    serverPath,
                    requestIds[i],
                    $"rpc_action{i + 1}",
                    new Dictionary<string, object> { [$"data{i + 1}"] = $"data{i + 1}" },
                    $"transaction_tag_{i + 1}",
                    i + 1,
                    "rpc_client_queue",
                    5,
                    500
                );
                Console.WriteLine($"push rpc queue result {i + 1}: {pushResult}");
            }
            
            var queuedJobs = await kb.RpcServerListJobsJobTypes(serverPath, "new_job");
            Console.WriteLine($"queued after pushes: {queuedJobs.Count} jobs");

            var jobs = new[]
            {
                await kb.RpcServerPeakServerQueue(serverPath, 5, 1000),
                await kb.RpcServerPeakServerQueue(serverPath, 5, 1000),
                await kb.RpcServerPeakServerQueue(serverPath, 5, 1000)
            };

            for (int i = 0; i < jobs.Length; i++)
            {
                Console.WriteLine($"job_data_{i + 1}: {jobs[i] != null}");
            }

            foreach (var job in jobs)
            {
                if (job != null)
                {
                    var jobDict = (Dictionary<string, object>)job;
                    if (jobDict.ContainsKey("id"))
                    {
                        var completionResult = await kb.RpcServerMarkJobCompletion(serverPath, Convert.ToInt32(jobDict["id"]), 5, 1000);
                        Console.WriteLine($"mark job completion result: {completionResult}");
                        
                        var allJobs = await kb.RpcServerCountAllJobs(serverPath);
                        Console.WriteLine($"count_all_jobs: empty={allJobs.EmptyJobs}, new={allJobs.NewJobs}, processing={allJobs.ProcessingJobs}");
                    }
                }
            }
        }

        static async Task TestClientQueue(KBDataStructures kb, string clientPath)
        {
            Console.WriteLine("=== Initial State ===");
            Console.WriteLine($"free_slots: {await kb.RpcClientFindFreeSlots(clientPath)}");
            Console.WriteLine($"queued_slots: {await kb.RpcClientFindQueuedSlots(clientPath)}");
            var waitingJobs = await kb.RpcClientListWaitingJobs(clientPath);
            Console.WriteLine($"waiting_jobs: {waitingJobs.Count}");

            var clearResult = await kb.RpcClientClearReplyQueue(clientPath, 3, 1000);
            Console.WriteLine($"cleared, result: {clearResult}, free_slots: {await kb.RpcClientFindFreeSlots(clientPath)}");

            Console.WriteLine("\n=== Pushing First Set of Reply Data ===");
            var actions = new[] { "Action1", "Action2" };
            
            foreach (var action in actions)
            {
                var rid = Guid.NewGuid().ToString();
                await kb.RpcClientPushAndClaimReplyData(
                    clientPath, 
                    rid, 
                    "xxx", 
                    action, 
                    "tag", 
                    new Dictionary<string, object> { ["payload"] = action },
                    3,
                    1000
                );
                Console.WriteLine($"Pushed reply data ID: {rid}");
            }

            waitingJobs = await kb.RpcClientListWaitingJobs(clientPath);
            Console.WriteLine($"waiting_jobs: {waitingJobs.Count}");

            Console.WriteLine("\n=== Peek and Release ===");
            var peeked = await kb.RpcClientPeakAndClaimReplyData(clientPath, 3, 1000);
            Console.WriteLine($"peeked: {peeked != null}");

            var finalClearResult = await kb.RpcClientClearReplyQueue(clientPath, 3, 1000);
            Console.WriteLine($"final clear result: {finalClearResult}, free_slots: {await kb.RpcClientFindFreeSlots(clientPath)}");
        }

        static string ReadPassword()
        {
            string password = "";
            ConsoleKeyInfo key;
            
            do
            {
                key = Console.ReadKey(true);
                
                if (key.Key != ConsoleKey.Backspace && key.Key != ConsoleKey.Enter)
                {
                    password += key.KeyChar;
                    Console.Write("*");
                }
                else if (key.Key == ConsoleKey.Backspace && password.Length > 0)
                {
                    password = password.Substring(0, password.Length - 1);
                    Console.Write("\b \b");
                }
            }
            while (key.Key != ConsoleKey.Enter);
            
            Console.WriteLine();
            return password;
        }
    }
}