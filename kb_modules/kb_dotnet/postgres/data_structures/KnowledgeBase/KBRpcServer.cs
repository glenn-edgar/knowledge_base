using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

public class NoMatchingRecordException : Exception
{
    public NoMatchingRecordException(string message) : base(message) { }
}

public class JobCountResult
{
    public int EmptyJobs { get; set; }
    public int NewJobs { get; set; }
    public int ProcessingJobs { get; set; }
}

public class KBRpcServer
{
    private readonly KBSearch _kbSearch;
    private readonly string _baseTable;
    private readonly int _maxBackoff = 8000;

    public KBRpcServer(KBSearch kbSearch, string database)
    {
        _kbSearch = kbSearch;
        _baseTable = $"{database}_rpc_server";
    }

    private NpgsqlConnection GetConnection()
    {
        return _kbSearch.GetConnection();
    }

    private async Task SleepAsync(int milliseconds)
    {
        await Task.Delay(milliseconds);
    }

    public async Task<dynamic> FindRpcServerIdAsync(
        string? kb = null,
        string? nodeName = null,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        var results = await FindRpcServerIdsAsync(kb, nodeName, properties, nodePath);
        
        if (results.Count == 0)
        {
            throw new InvalidOperationException(
                $"No node found matching path parameters: {nodeName}, {JsonConvert.SerializeObject(properties)}, {nodePath}");
        }
        
        if (results.Count > 1)
        {
            throw new InvalidOperationException(
                $"Multiple nodes found matching path parameters: {nodeName}, {JsonConvert.SerializeObject(properties)}, {nodePath}");
        }
        
        return results[0];
    }

    public async Task<List<dynamic>> FindRpcServerIdsAsync(
        string? kb = null,
        string? nodeName = null,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        _kbSearch.ClearFilters();
        _kbSearch.SearchLabel("KB_RPC_SERVER_FIELD");
        
        if (!string.IsNullOrEmpty(kb)) _kbSearch.SearchKb(kb);
        if (!string.IsNullOrEmpty(nodeName)) _kbSearch.SearchName(nodeName);
        
        if (properties != null)
        {
            foreach (var key in properties.Keys)
            {
                _kbSearch.SearchPropertyValue(key, properties[key]);
            }
        }
        
        if (!string.IsNullOrEmpty(nodePath)) _kbSearch.SearchPath(nodePath);

        var nodeIds = await _kbSearch.ExecuteQueryAsync();
        
        if (nodeIds == null || nodeIds.Count == 0)
        {
            throw new InvalidOperationException(
                $"No node found matching path parameters: {nodeName}, {JsonConvert.SerializeObject(properties)}, {nodePath}");
        }
        
        return nodeIds;
    }

    public List<string> FindRpcServerTableKeys(List<dynamic> keyData)
    {
        return keyData.Select(k => ((Dictionary<string, object>)k)["path"]?.ToString() ?? string.Empty).ToList();
    }

    public async Task<List<dynamic>> ListJobsJobTypesAsync(string serverPath, string state)
    {
        if (!IsValidLtree(serverPath))
        {
            throw new ArgumentException("serverPath must be a non-empty valid ltree string");
        }
        
        var allowed = new HashSet<string> { "empty", "new_job", "processing" };
        if (!allowed.Contains(state))
        {
            throw new ArgumentException($"state must be one of {string.Join(",", allowed)}");
        }

        var client = GetConnection();
        var query = $@"
            SELECT *
            FROM {_baseTable}
            WHERE server_path = $1::ltree AND state = $2
            ORDER BY priority DESC, request_timestamp ASC
        ";

        try
        {
            using var cmd = new NpgsqlCommand(query, client);
            cmd.Parameters.AddWithValue(serverPath);
            cmd.Parameters.AddWithValue(state);
            
            using var reader = await cmd.ExecuteReaderAsync();
            var results = new List<dynamic>();
            
            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
                }
                results.Add(row);
            }
            
            return results;
        }
        catch (Exception ex)
        {
            throw new Exception($"Database error in listJobsJobTypes: {ex.Message}", ex);
        }
    }

    public async Task<JobCountResult> CountAllJobsAsync(string serverPath)
    {
        return new JobCountResult
        {
            EmptyJobs = await CountEmptyJobsAsync(serverPath),
            NewJobs = await CountNewJobsAsync(serverPath),
            ProcessingJobs = await CountProcessingJobsAsync(serverPath)
        };
    }

    public Task<int> CountProcessingJobsAsync(string serverPath)
    {
        return CountJobsJobTypesAsync(serverPath, "processing");
    }

    public Task<int> CountNewJobsAsync(string serverPath)
    {
        return CountJobsJobTypesAsync(serverPath, "new_job");
    }

    public Task<int> CountEmptyJobsAsync(string serverPath)
    {
        return CountJobsJobTypesAsync(serverPath, "empty");
    }

    public async Task<int> CountJobsJobTypesAsync(string serverPath, string state)
    {
        if (!IsValidLtree(serverPath))
        {
            throw new ArgumentException("serverPath must be valid ltree");
        }
        
        var valid = new HashSet<string> { "empty", "new_job", "processing", "completed_job" };
        if (!valid.Contains(state))
        {
            throw new ArgumentException($"state must be one of {string.Join(",", valid)}");
        }
        
        var client = GetConnection();
        var query = $@"
            SELECT COUNT(*) AS job_count
            FROM {_baseTable}
            WHERE server_path = $1::ltree AND state = $2
        ";

        try
        {
            using var cmd = new NpgsqlCommand(query, client);
            cmd.Parameters.AddWithValue(serverPath);
            cmd.Parameters.AddWithValue(state);
            
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
        catch (Exception ex)
        {
            throw new Exception($"Database error in countJobsJobTypes: {ex.Message}", ex);
        }
    }

    public async Task<dynamic> PushRpcQueueAsync(
        string serverPath,
        string? requestId,
        string rpcAction,
        object requestPayload,
        string transactionTag,
        int priority = 0,
        string? rpcClientQueue = null,
        int maxRetries = 5,
        int waitTime = 500)
    {
        if (!IsValidLtree(serverPath)) 
            throw new ArgumentException("invalid serverPath");
        
        var reqId = requestId ?? Guid.NewGuid().ToString();
        try 
        { 
            reqId = Guid.NewGuid().ToString().Replace("-", "");
        } 
        catch { }
        
        if (string.IsNullOrEmpty(rpcAction)) 
            throw new ArgumentException("rpc_action must be non-empty");
        
        // Test JSON serialization
        JsonConvert.SerializeObject(requestPayload);
        
        if (string.IsNullOrEmpty(transactionTag)) 
            throw new ArgumentException("transaction_tag must be non-empty");
        
        if (!string.IsNullOrEmpty(rpcClientQueue) && !IsValidLtree(rpcClientQueue))
        {
            throw new ArgumentException("rpc_client_queue must be valid ltree or null");
        }

        var tableRef = _baseTable;
        int attempt = 0;
        int delay = waitTime;

        while (attempt < maxRetries)
        {
            try
            {
                // Use simpler approach without complex transactions
                var client = GetConnection();
                
                // First, find an empty slot
                var selectQuery = $@"
                    SELECT id FROM {tableRef}
                    WHERE server_path = $1::ltree AND state='empty'
                    ORDER BY priority DESC, request_timestamp ASC
                    LIMIT 1
                ";

                int? recordId = null;
                using (var selectCmd = new NpgsqlCommand(selectQuery, client))
                {
                    selectCmd.Parameters.AddWithValue(serverPath);
                    using var reader = await selectCmd.ExecuteReaderAsync();
                    
                    if (await reader.ReadAsync())
                    {
                        recordId = Convert.ToInt32(reader["id"]);
                    }
                }

                if (!recordId.HasValue)
                {
                    throw new NoMatchingRecordException("No matching record");
                }

                // Update the record
                var updateQuery = $@"
                    UPDATE {tableRef} SET
                        server_path=$1::ltree, request_id=$2::uuid, rpc_action=$3,
                        request_payload=$4::json, transaction_tag=$5, priority=$6,
                        rpc_client_queue=$7::ltree, state='new_job',
                        request_timestamp=NOW() AT TIME ZONE 'UTC', completed_timestamp=NULL
                    WHERE id=$8 AND state='empty'
                    RETURNING *;
                ";
                
                using var updateCmd = new NpgsqlCommand(updateQuery, client);
                updateCmd.Parameters.AddWithValue(serverPath);
                updateCmd.Parameters.AddWithValue(reqId);
                updateCmd.Parameters.AddWithValue(rpcAction);
                updateCmd.Parameters.AddWithValue(JsonConvert.SerializeObject(requestPayload));
                updateCmd.Parameters.AddWithValue(transactionTag);
                updateCmd.Parameters.AddWithValue(priority);
                updateCmd.Parameters.AddWithValue((object?)rpcClientQueue ?? DBNull.Value);
                updateCmd.Parameters.AddWithValue(recordId.Value);

                using var updateReader = await updateCmd.ExecuteReaderAsync();
                
                if (await updateReader.ReadAsync())
                {
                    var result = new Dictionary<string, object>();
                    for (int i = 0; i < updateReader.FieldCount; i++)
                    {
                        result[updateReader.GetName(i)] = updateReader.IsDBNull(i) ? null : updateReader.GetValue(i);
                    }
                    return result;
                }
                else
                {
                    // Record was taken by another process, retry
                    attempt++;
                    if (attempt < maxRetries)
                    {
                        await SleepAsync(delay);
                        delay = Math.Min(delay * 2, _maxBackoff);
                        continue;
                    }
                    throw new Exception("Record was taken by another process");
                }
            }
            catch (Exception ex)
            {
                attempt++;
                if (attempt < maxRetries)
                {
                    await SleepAsync(delay);
                    delay = Math.Min(delay * 2, _maxBackoff);
                    continue;
                }
                
                if (ex is NoMatchingRecordException) throw;
                throw new Exception($"Error in pushRpcQueue: {ex.Message}", ex);
            }
        }
        
        throw new Exception($"Failed after {maxRetries} retries");
    }

    private bool IsValidLtree(string path)
    {
        if (string.IsNullOrEmpty(path)) return false;
        var parts = path.Split('.');
        return parts.All(p => Regex.IsMatch(p, @"^[A-Za-z_][A-Za-z0-9_]*$"));
    }

    public async Task<dynamic?> PeakServerQueueAsync(
        string serverPath,
        int retries = 5,
        int waitTime = 1000)
    {
        var client = GetConnection();
        int attempt = 0;
        
        while (attempt < retries)
        {
            using var transaction = await client.BeginTransactionAsync();
            try
            {
                await using var isolationCmd = new NpgsqlCommand("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", client, transaction);
                await isolationCmd.ExecuteNonQueryAsync();
                
                var selectQuery = $@"
                    SELECT * FROM {_baseTable}
                    WHERE server_path=$1::ltree AND state='new_job'
                    ORDER BY priority DESC, request_timestamp ASC
                    LIMIT 1 FOR UPDATE SKIP LOCKED
                ";
                
                await using var selectCmd = new NpgsqlCommand(selectQuery, client, transaction);
                selectCmd.Parameters.AddWithValue(serverPath);
                
                await using var reader = await selectCmd.ExecuteReaderAsync();
                
                if (!await reader.ReadAsync())
                {
                    await transaction.RollbackAsync();
                    return null;
                }
                
                var row = new Dictionary<string, object>();
                var idValue = reader["id"];
                var id = Convert.ToInt32(idValue);
                
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
                }
                
                await reader.CloseAsync();

                var updateQuery = $@"
                    UPDATE {_baseTable} SET state='processing', processing_timestamp=NOW() AT TIME ZONE 'UTC'
                    WHERE id=$1 RETURNING id;
                ";
                
                await using var updateCmd = new NpgsqlCommand(updateQuery, client, transaction);
                updateCmd.Parameters.AddWithValue(id);
                
                var updateResult = await updateCmd.ExecuteScalarAsync();
                
                if (updateResult == null)
                {
                    await transaction.RollbackAsync();
                    throw new Exception($"Failed to update processing for id {id}");
                }
                
                await transaction.CommitAsync();
                return row;
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                attempt++;
                
                if (attempt < retries)
                {
                    await SleepAsync(waitTime * (int)Math.Pow(2, attempt));
                    continue;
                }
                
                throw new Exception($"Error in peakServerQueue: {ex.Message}", ex);
            }
        }
        
        return null;
    }

    public async Task<bool> MarkJobCompletionAsync(
        string serverPath,
        int id,
        int retries = 5,
        int waitTime = 1000)
    {
        var client = GetConnection();
        int attempt = 0;
        
        while (attempt < retries)
        {
            using var transaction = await client.BeginTransactionAsync();
            try
            {
                await using var isolationCmd = new NpgsqlCommand("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", client, transaction);
                await isolationCmd.ExecuteNonQueryAsync();
                
                var verifyQuery = $@"
                    SELECT id FROM {_baseTable}
                    WHERE id=$1 AND server_path=$2::ltree AND state='processing' FOR UPDATE
                ";
                
                await using var verifyCmd = new NpgsqlCommand(verifyQuery, client, transaction);
                verifyCmd.Parameters.AddWithValue(id);
                verifyCmd.Parameters.AddWithValue(serverPath);
                
                await using var verifyReader = await verifyCmd.ExecuteReaderAsync();
                
                if (!await verifyReader.ReadAsync())
                {
                    await transaction.RollbackAsync();
                    return false;
                }
                
                await verifyReader.CloseAsync();

                var updateQuery = $@"
                    UPDATE {_baseTable} SET state='empty', completed_timestamp=NOW() AT TIME ZONE 'UTC'
                    WHERE id=$1 RETURNING id;
                ";
                
                await using var updateCmd = new NpgsqlCommand(updateQuery, client, transaction);
                updateCmd.Parameters.AddWithValue(id);
                await updateCmd.ExecuteNonQueryAsync();
                
                await transaction.CommitAsync();
                return true;
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                attempt++;
                
                if (attempt < retries)
                {
                    await SleepAsync(waitTime * (int)Math.Pow(2, attempt));
                    continue;
                }
                
                throw new Exception($"Error in markJobCompletion: {ex.Message}", ex);
            }
        }
        
        return false;
    }

    public async Task<int> ClearServerQueueAsync(
        string serverPath,
        int maxRetries = 3,
        int retryDelay = 1000)
    {
        var client = GetConnection();
        int attempt = 0;
        var updateQuery = $@"
            UPDATE {_baseTable}
            SET request_id = gen_random_uuid(),
                request_payload = '{{}}'::json,
                completed_timestamp = CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
                state = 'empty',
                rpc_client_queue = NULL
            WHERE server_path = $1::ltree;
        ";
        
        while (attempt < maxRetries)
        {
            using var transaction = await client.BeginTransactionAsync();
            try
            {
                await using var cmd = new NpgsqlCommand(updateQuery, client, transaction);
                cmd.Parameters.AddWithValue(serverPath);
                
                var rowCount = await cmd.ExecuteNonQueryAsync();
                await transaction.CommitAsync();
                return rowCount;
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                attempt++;
                
                if (attempt < maxRetries)
                {
                    await SleepAsync(retryDelay);
                    continue;
                }
                
                throw new Exception($"Failed to clear server queue: {ex.Message}", ex);
            }
        }
        
        return 0;
    }

    // Simple string hash to 32-bit int
    private int Hash(string s)
    {
        int h = 0;
        foreach (char ch in s)
        {
            h = ((h << 5) - h) + ch;
            h |= 0; // Convert to 32-bit integer
        }
        return h;
    }
}