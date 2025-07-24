using System;
using System.Collections.Generic;
using System.Threading.Tasks;

public class KBDataStructures
{
    public KBSearch QuerySupport { get; private set; }

    // Delegated KB_Search methods
    public Action ClearFilters { get; private set; }
    public Action<string> SearchLabel { get; private set; }
    public Action<string> SearchName { get; private set; }
    public Action<string> SearchPropertyKey { get; private set; }
    public Action<string, object> SearchPropertyValue { get; private set; }
    public Action SearchHasLink { get; private set; }
    public Action SearchHasLinkMount { get; private set; }
    public Action<string> SearchPath { get; private set; }
    public Action<string> SearchStartingPath { get; private set; }
    public Func<Task<List<dynamic>>> ExecuteKbSearch { get; private set; }
    public Func<object, List<Dictionary<string, string>>> FindDescription { get; private set; }
    public Func<object, Task<Dictionary<string, object>>> FindDescriptionPaths { get; private set; }
    public Func<object, List<string>> FindPathValues { get; private set; }
    public Func<string, (string, List<(string, string)>)> DecodeLinkNodes { get; private set; }

    // Status data
    public KBStatusData StatusData { get; private set; }
    public Func<string?, string?, Dictionary<string, object>?, string?, Task<List<dynamic>>> FindStatusNodeIds { get; private set; }
    public Func<string, string, Dictionary<string, object>?, string?, Task<dynamic>> FindStatusNodeId { get; private set; }
    public Func<string, Task<(object, string)>> GetStatusData { get; private set; }
    public Func<string, Dictionary<string, object>, int, int, Task<(bool, string)>> SetStatusData { get; private set; }

    // Job queue
    public KBJobQueue JobQueue { get; private set; }
    public Func<string?, string?, Dictionary<string, object>?, string?, Task<List<dynamic>>> FindJobIds { get; private set; }
    public Func<string?, string?, Dictionary<string, object>?, string?, Task<dynamic>> FindJobId { get; private set; }
    public Func<string, Task<int>> GetQueuedNumber { get; private set; }
    public Func<string, Task<int>> GetFreeNumber { get; private set; }
    public Func<string, int, int, Task<dynamic?>> PeakJobData { get; private set; }
    public Func<int, int, int, Task<JobCompletionResult>> MarkJobCompleted { get; private set; }
    public Func<string, Dictionary<string, object>, int, int, Task<JobPushResult>> PushJobData { get; private set; }
    public Func<string, int?, int, Task<List<dynamic>>> ListPendingJobs { get; private set; }
    public Func<string, int?, int, Task<List<dynamic>>> ListActiveJobs { get; private set; }
    public Func<string, Task<JobQueueClearResult>> ClearJobQueue { get; private set; }

    // Stream
    public KBStream Stream { get; private set; }
    public Func<string?, string?, Dictionary<string, object>?, string?, Task<List<dynamic>>> FindStreamIds { get; private set; }
    public Func<string?, string?, Dictionary<string, object>?, string?, Task<dynamic>> FindStreamId { get; private set; }
    public Func<List<dynamic>, List<string>> FindStreamTableKeys { get; private set; }
    public Func<string, Dictionary<string, object>, int, int, Task<Dictionary<string, object>>> PushStreamData { get; private set; }
    public Func<string, int?, int, DateTime?, DateTime?, string, Task<List<dynamic>>> ListStreamData { get; private set; }
    public Func<string, DateTime?, Task<ClearStreamDataResult>> ClearStreamData { get; private set; }
    public Func<string, bool, Task<int>> GetStreamDataCount { get; private set; }
    public Func<string, DateTime, DateTime, Task<List<dynamic>>> GetStreamDataRange { get; private set; }
    public Func<string, bool, Task<Dictionary<string, object>>> GetStreamStatistics { get; private set; }
    public Func<int, Task<Dictionary<string, object>?>> GetStreamDataById { get; private set; }

    // RPC client
    public KBRpcClient RpcClient { get; private set; }
    public Func<string?, string?, Dictionary<string, object>?, string?, Task<dynamic>> RpcClientFindRpcClientId { get; private set; }
    public Func<string?, string?, Dictionary<string, object>?, string?, Task<List<dynamic>>> RpcClientFindRpcClientIds { get; private set; }
    public Func<List<dynamic>, List<string>> RpcClientFindRpcClientKeys { get; private set; }
    public Func<string, Task<int>> RpcClientFindFreeSlots { get; private set; }
    public Func<string, Task<int>> RpcClientFindQueuedSlots { get; private set; }
    public Func<string, int, int, Task<Dictionary<string, object>?>> RpcClientPeakAndClaimReplyData { get; private set; }
    public Func<string, int, int, Task<int>> RpcClientClearReplyQueue { get; private set; }
    public Func<string, string, string, string, string, object, int, int, Task> RpcClientPushAndClaimReplyData { get; private set; }
    public Func<string?, Task<List<Dictionary<string, object>>>> RpcClientListWaitingJobs { get; private set; }

    // RPC server
    public KBRpcServer RpcServer { get; private set; }
    public Func<string?, string?, Dictionary<string, object>?, string?, Task<dynamic>> RpcServerFindId { get; private set; }
    public Func<string?, string?, Dictionary<string, object>?, string?, Task<List<dynamic>>> RpcServerFindIds { get; private set; }
    public Func<List<dynamic>, List<string>> RpcServerFindTableKeys { get; private set; }
    public Func<string, string, Task<List<dynamic>>> RpcServerListJobsJobTypes { get; private set; }
    public Func<string, Task<JobCountResult>> RpcServerCountAllJobs { get; private set; }
    public Func<string, Task<int>> RpcServerCountEmptyJobs { get; private set; }
    public Func<string, Task<int>> RpcServerCountNewJobs { get; private set; }
    public Func<string, Task<int>> RpcServerCountProcessingJobs { get; private set; }
    public Func<string, string, Task<int>> RpcServerCountJobsJobTypes { get; private set; }
    public Func<string, string?, string, object, string, int, string?, int, int, Task<dynamic>> RpcServerPushRpcQueue { get; private set; }
    public Func<string, int, int, Task<dynamic?>> RpcServerPeakServerQueue { get; private set; }
    public Func<string, int, int, int, Task<bool>> RpcServerMarkJobCompletion { get; private set; }
    public Func<string, int, int, Task<int>> RpcServerClearServerQueue { get; private set; }

    // Link tables
    public KBLinkTable LinkTable { get; private set; }
    public Func<string, string?, Task<List<Dictionary<string, object>>>> LinkTableFindRecordsByLinkName { get; private set; }
    public Func<string, string?, Task<List<Dictionary<string, object>>>> LinkTableFindRecordsByNodePath { get; private set; }
    public Func<Task<List<string>>> LinkTableFindAllLinkNames { get; private set; }
    public Func<Task<List<string>>> LinkTableFindAllNodeNames { get; private set; }

    public KBLinkMountTable LinkMountTable { get; private set; }
    public Func<string, string?, Task<List<Dictionary<string, object>>>> LinkMountTableFindRecordsByLinkName { get; private set; }
    public Func<string, string?, Task<List<Dictionary<string, object>>>> LinkMountTableFindRecordsByMountPath { get; private set; }
    public Func<Task<List<string>>> LinkMountTableFindAllLinkNames { get; private set; }
    public Func<Task<List<string>>> LinkMountTableFindAllMountPaths { get; private set; }

    public KBDataStructures(
        string host,
        int port,
        string dbName,
        string user,
        string password,
        string database)
    {
        // Core search
        QuerySupport = new KBSearch(host, port, dbName, user, password, database);
        ClearFilters = QuerySupport.ClearFilters;
        SearchLabel = QuerySupport.SearchLabel;
        SearchName = QuerySupport.SearchName;
        SearchPropertyKey = QuerySupport.SearchPropertyKey;
        SearchPropertyValue = QuerySupport.SearchPropertyValue;
        SearchHasLink = QuerySupport.SearchHasLink;
        SearchHasLinkMount = QuerySupport.SearchHasLinkMount;
        SearchPath = QuerySupport.SearchPath;
        SearchStartingPath = QuerySupport.SearchStartingPath;
        ExecuteKbSearch = QuerySupport.ExecuteQueryAsync;
        FindDescription = QuerySupport.FindDescription;
        FindDescriptionPaths = QuerySupport.FindDescriptionPathsAsync;
        FindPathValues = QuerySupport.FindPathValues;
        DecodeLinkNodes = QuerySupport.DecodeLinkNodes;

        // Status Data
        StatusData = new KBStatusData(QuerySupport, database);
        FindStatusNodeIds = StatusData.FindNodeIdsAsync;
        FindStatusNodeId = StatusData.FindNodeIdAsync;
        GetStatusData = StatusData.GetStatusDataAsync;
        SetStatusData = StatusData.SetStatusDataAsync;

        // Job Queue
        JobQueue = new KBJobQueue(QuerySupport, database);
        FindJobIds = JobQueue.FindJobIdsAsync;
        FindJobId = JobQueue.FindJobIdAsync;
        GetQueuedNumber = JobQueue.GetQueuedNumberAsync;
        GetFreeNumber = JobQueue.GetFreeNumberAsync;
        PeakJobData = JobQueue.PeakJobDataAsync;
        MarkJobCompleted = JobQueue.MarkJobCompletedAsync;
        PushJobData = JobQueue.PushJobDataAsync;
        ListPendingJobs = JobQueue.ListPendingJobsAsync;
        ListActiveJobs = JobQueue.ListActiveJobsAsync;
        ClearJobQueue = JobQueue.ClearJobQueueAsync;

        // Stream
        Stream = new KBStream(QuerySupport, database);
        FindStreamIds = Stream.FindStreamIdsAsync;
        FindStreamId = Stream.FindStreamIdAsync;
        FindStreamTableKeys = Stream.FindStreamTableKeys;
        PushStreamData = Stream.PushStreamDataAsync;
        ListStreamData = Stream.ListStreamDataAsync;
        ClearStreamData = Stream.ClearStreamDataAsync;
        GetStreamDataCount = Stream.GetStreamDataCountAsync;
        GetStreamDataRange = Stream.GetStreamDataRangeAsync;
        GetStreamStatistics = Stream.GetStreamStatisticsAsync;
        GetStreamDataById = Stream.GetStreamDataByIdAsync;

        // RPC Client
        RpcClient = new KBRpcClient(QuerySupport, database);
        RpcClientFindRpcClientId = RpcClient.FindRpcClientIdAsync;
        RpcClientFindRpcClientIds = RpcClient.FindRpcClientIdsAsync;
        RpcClientFindRpcClientKeys = RpcClient.FindRpcClientKeys;
        RpcClientFindFreeSlots = RpcClient.FindFreeSlotsAsync;
        RpcClientFindQueuedSlots = RpcClient.FindQueuedSlotsAsync;
        RpcClientPeakAndClaimReplyData = RpcClient.PeakAndClaimReplyDataAsync;
        RpcClientClearReplyQueue = RpcClient.ClearReplyQueueAsync;
        RpcClientPushAndClaimReplyData = RpcClient.PushAndClaimReplyDataAsync;
        RpcClientListWaitingJobs = RpcClient.ListWaitingJobsAsync;

        // RPC Server
        RpcServer = new KBRpcServer(QuerySupport, database);
        RpcServerFindId = RpcServer.FindRpcServerIdAsync;
        RpcServerFindIds = RpcServer.FindRpcServerIdsAsync;
        RpcServerFindTableKeys = RpcServer.FindRpcServerTableKeys;
        RpcServerListJobsJobTypes = RpcServer.ListJobsJobTypesAsync;
        RpcServerCountAllJobs = RpcServer.CountAllJobsAsync;
        RpcServerCountEmptyJobs = RpcServer.CountEmptyJobsAsync;
        RpcServerCountNewJobs = RpcServer.CountNewJobsAsync;
        RpcServerCountProcessingJobs = RpcServer.CountProcessingJobsAsync;
        RpcServerCountJobsJobTypes = RpcServer.CountJobsJobTypesAsync;
        RpcServerPushRpcQueue = RpcServer.PushRpcQueueAsync;
        RpcServerPeakServerQueue = RpcServer.PeakServerQueueAsync;
        RpcServerMarkJobCompletion = RpcServer.MarkJobCompletionAsync;
        RpcServerClearServerQueue = RpcServer.ClearServerQueueAsync;

        // Link Table
        LinkTable = new KBLinkTable(QuerySupport.GetConnection(), database);
        LinkTableFindRecordsByLinkName = LinkTable.FindRecordsByLinkNameAsync;
        LinkTableFindRecordsByNodePath = LinkTable.FindRecordsByNodePathAsync;
        LinkTableFindAllLinkNames = LinkTable.FindAllLinkNamesAsync;
        LinkTableFindAllNodeNames = LinkTable.FindAllNodeNamesAsync;

        // Link Mount Table
        LinkMountTable = new KBLinkMountTable(QuerySupport.GetConnection(), database);
        LinkMountTableFindRecordsByLinkName = LinkMountTable.FindRecordsByLinkNameAsync;
        LinkMountTableFindRecordsByMountPath = LinkMountTable.FindRecordsByMountPathAsync;
        LinkMountTableFindAllLinkNames = LinkMountTable.FindAllLinkNamesAsync;
        LinkMountTableFindAllMountPaths = LinkMountTable.FindAllMountPathsAsync;
    }

    /// <summary>
    /// Disconnects all database connections
    /// </summary>
    public async Task DisconnectAsync()
    {
        await QuerySupport.DisconnectAsync();
        await Stream.DisconnectAsync();
    }

    /// <summary>
    /// Gets the underlying database connection for advanced operations
    /// </summary>
    public Npgsql.NpgsqlConnection GetConnection()
    {
        return QuerySupport.GetConnection();
    }

    /// <summary>
    /// Provides access to multiple status data operations
    /// </summary>
    public async Task<(bool Success, string Message, Dictionary<string, string> Results)> SetMultipleStatusDataAsync(
        object pathDataPairs,
        int retryCount = 3,
        int retryDelay = 1000)
    {
        return await StatusData.SetMultipleStatusDataAsync(pathDataPairs, retryCount, retryDelay);
    }

    /// <summary>
    /// Gets multiple status data entries
    /// </summary>
    public async Task<Dictionary<string, object>> GetMultipleStatusDataAsync(object paths)
    {
        return await StatusData.GetMultipleStatusDataAsync(paths);
    }

    /// <summary>
    /// Gets job statistics for a specific path
    /// </summary>
    public async Task<Dictionary<string, object>> GetJobStatisticsAsync(string path)
    {
        return await JobQueue.GetJobStatisticsAsync(path);
    }

    /// <summary>
    /// Gets a job by its ID
    /// </summary>
    public async Task<dynamic?> GetJobByIdAsync(int jobId)
    {
        return await JobQueue.GetJobByIdAsync(jobId);
    }

    /// <summary>
    /// Gets the latest stream data for a path
    /// </summary>
    public async Task<Dictionary<string, object>?> GetLatestStreamDataAsync(string path)
    {
        return await Stream.GetLatestStreamDataAsync(path);
    }
}

