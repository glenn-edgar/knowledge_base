using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Newtonsoft.Json;

public class KBRpcClient
{
    private readonly KBSearch _kbSearch;
    private readonly NpgsqlConnection _client;
    private readonly string _baseTable;

    public KBRpcClient(KBSearch kbSearch, string database)
    {
        _kbSearch = kbSearch;
        _client = kbSearch.GetConnection();
        _baseTable = $"{database}_rpc_client";
    }

    private async Task SleepAsync(int milliseconds)
    {
        await Task.Delay(milliseconds);
    }

    public async Task<dynamic> FindRpcClientIdAsync(
        string? kb = null,
        string? nodeName = null,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        var ids = await FindRpcClientIdsAsync(kb, nodeName, properties, nodePath);
        
        if (ids.Count == 0)
        {
            throw new InvalidOperationException(
                $"No node found matching parameters: {nodeName}, {JsonConvert.SerializeObject(properties)}, {nodePath}");
        }
        
        if (ids.Count > 1)
        {
            throw new InvalidOperationException(
                $"Multiple nodes found matching parameters: {nodeName}, {JsonConvert.SerializeObject(properties)}, {nodePath}");
        }
        
        return ids[0];
    }

    public async Task<List<dynamic>> FindRpcClientIdsAsync(
        string? kb = null,
        string? nodeName = null,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        _kbSearch.ClearFilters();
        _kbSearch.SearchLabel("KB_RPC_CLIENT_FIELD");
        
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

        var results = await _kbSearch.ExecuteQueryAsync();
        
        if (results == null || results.Count == 0)
        {
            throw new InvalidOperationException(
                $"No node found matching parameters: {nodeName}, {JsonConvert.SerializeObject(properties)}, {nodePath}");
        }
        
        return results;
    }

    public List<string> FindRpcClientKeys(List<dynamic> keyData)
    {
        return keyData.Select(r => ((Dictionary<string, object>)r)["path"]?.ToString() ?? string.Empty).ToList();
    }

    public async Task<int> FindFreeSlotsAsync(string clientPath)
    {
        var query = $@"
            SELECT COUNT(*) AS total_records,
                   COUNT(*) FILTER (WHERE is_new_result = FALSE) AS free_slots
            FROM {_baseTable}
            WHERE client_path = $1
        ";
        
        try
        {
            using var cmd = new NpgsqlCommand(query, _client);
            cmd.Parameters.AddWithValue("$1", clientPath);
            
            using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            
            var total = Convert.ToInt32(reader["total_records"]);
            var free = Convert.ToInt32(reader["free_slots"]);
            
            if (total == 0)
            {
                throw new InvalidOperationException($"No records found for client_path: {clientPath}");
            }
            
            return free;
        }
        catch (Exception ex) when (!(ex is InvalidOperationException))
        {
            throw new Exception($"Database error when finding free slots: {ex.Message}", ex);
        }
    }

    public async Task<int> FindQueuedSlotsAsync(string clientPath)
    {
        var query = $@"
            SELECT COUNT(*) AS total_records,
                   COUNT(*) FILTER (WHERE is_new_result = TRUE) AS queued_slots
            FROM {_baseTable}
            WHERE client_path = $1
        ";
        
        try
        {
            using var cmd = new NpgsqlCommand(query, _client);
            cmd.Parameters.AddWithValue("$1", clientPath);
            
            using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            
            var total = Convert.ToInt32(reader["total_records"]);
            var queued = Convert.ToInt32(reader["queued_slots"]);
            
            if (total == 0)
            {
                throw new InvalidOperationException($"No records found for client_path: {clientPath}");
            }
            
            return queued;
        }
        catch (Exception ex) when (!(ex is InvalidOperationException))
        {
            throw new Exception($"Database error when finding queued slots: {ex.Message}", ex);
        }
    }

    public async Task<Dictionary<string, object>?> PeakAndClaimReplyDataAsync(
        string clientPath,
        int maxRetries = 3,
        int retryDelay = 1000)
    {
        int attempt = 0;
        var table = _baseTable;
        
        while (attempt < maxRetries)
        {
            try
            {
                using var transaction = await _client.BeginTransactionAsync();
                
                try
                {
                    var updateQuery = $@"
                        UPDATE {table}
                        SET is_new_result = FALSE
                        WHERE id = (
                            SELECT id FROM {table}
                            WHERE client_path = $1 AND is_new_result = TRUE
                            ORDER BY response_timestamp ASC
                            FOR UPDATE SKIP LOCKED LIMIT 1
                        )
                        RETURNING *
                    ";
                    
                    using var updateCmd = new NpgsqlCommand(updateQuery, _client, transaction);
                    updateCmd.Parameters.AddWithValue("$1", clientPath);
                    
                    using var updateReader = await updateCmd.ExecuteReaderAsync();
                    
                    if (await updateReader.ReadAsync())
                    {
                        var result = new Dictionary<string, object>();
                        for (int i = 0; i < updateReader.FieldCount; i++)
                        {
                            result[updateReader.GetName(i)] = updateReader.IsDBNull(i) ? null : updateReader.GetValue(i);
                        }
                        
                        await updateReader.CloseAsync();
                        await transaction.CommitAsync();
                        return result;
                    }
                    
                    await updateReader.CloseAsync();
                    
                    // Check existence
                    var existQuery = $@"
                        SELECT EXISTS(
                            SELECT 1 FROM {table}
                            WHERE client_path = $1 AND is_new_result = TRUE
                        ) AS exists
                    ";
                    
                    using var existCmd = new NpgsqlCommand(existQuery, _client, transaction);
                    existCmd.Parameters.AddWithValue("$1", clientPath);
                    
                    var exists = (bool)await existCmd.ExecuteScalarAsync();
                    
                    if (!exists)
                    {
                        await transaction.RollbackAsync();
                        return null;
                    }
                    
                    await transaction.RollbackAsync();
                    attempt++;
                    await SleepAsync(retryDelay);
                }
                catch
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
            catch (Exception)
            {
                attempt++;
                await SleepAsync(retryDelay);
            }
        }
        
        throw new Exception($"Could not lock a new-reply row after {maxRetries} attempts");
    }

    public async Task<int> ClearReplyQueueAsync(
        string clientPath,
        int maxRetries = 3,
        int retryDelay = 1000)
    {
        int attempt = 0;
        var table = _baseTable;
        
        while (attempt < maxRetries)
        {
            try
            {
                using var transaction = await _client.BeginTransactionAsync();
                
                try
                {
                    var selectQuery = $@"
                        SELECT id FROM {table}
                        WHERE client_path = $1 FOR UPDATE NOWAIT
                    ";
                    
                    using var selectCmd = new NpgsqlCommand(selectQuery, _client, transaction);
                    selectCmd.Parameters.AddWithValue("$1", clientPath);
                    
                    using var selectReader = await selectCmd.ExecuteReaderAsync();
                    var ids = new List<int>();
                    
                    while (await selectReader.ReadAsync())
                    {
                        ids.Add(selectReader.GetInt32("id"));
                    }
                    
                    await selectReader.CloseAsync();
                    
                    if (ids.Count == 0)
                    {
                        await transaction.CommitAsync();
                        return 0;
                    }
                    
                    int updated = 0;
                    foreach (var id in ids)
                    {
                        var updateQuery = $@"
                            UPDATE {table}
                            SET request_id = $1,
                                server_path = $2,
                                response_payload = $3,
                                response_timestamp = NOW(),
                                is_new_result = FALSE
                            WHERE id = $4
                        ";
                        
                        using var updateCmd = new NpgsqlCommand(updateQuery, _client, transaction);
                        var uuid = Guid.NewGuid().ToString();
                        updateCmd.Parameters.AddWithValue("$1", uuid);
                        updateCmd.Parameters.AddWithValue("$2", clientPath);
                        updateCmd.Parameters.AddWithValue("$3", JsonConvert.SerializeObject(new { }));
                        updateCmd.Parameters.AddWithValue("$4", id);
                        
                        await updateCmd.ExecuteNonQueryAsync();
                        updated++;
                    }
                    
                    await transaction.CommitAsync();
                    return updated;
                }
                catch
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
            catch (Exception)
            {
                attempt++;
                await SleepAsync(retryDelay);
            }
        }
        
        throw new Exception($"Could not acquire lock after {maxRetries} retries");
    }

    public async Task PushAndClaimReplyDataAsync(
        string clientPath,
        string requestUuid,
        string serverPath,
        string rpcAction,
        string transactionTag,
        object replyData,
        int maxRetries = 3,
        int retryDelay = 1000)
    {
        int attempt = 0;
        var table = _baseTable;
        
        while (attempt <= maxRetries)
        {
            try
            {
                using var transaction = await _client.BeginTransactionAsync();
                
                try
                {
                    var upsertQuery = $@"
                        WITH candidate AS (
                            SELECT id FROM {table}
                            WHERE client_path = $1 AND is_new_result = FALSE
                            ORDER BY response_timestamp ASC
                            FOR UPDATE SKIP LOCKED LIMIT 1
                        )
                        UPDATE {table}
                        SET request_id = $2, server_path = $3, rpc_action = $4,
                            transaction_tag = $5, response_payload = $6,
                            is_new_result = TRUE, response_timestamp = CURRENT_TIMESTAMP
                        FROM candidate
                        WHERE {table}.id = candidate.id
                        RETURNING {table}.id;
                    ";
                    
                    using var cmd = new NpgsqlCommand(upsertQuery, _client, transaction);
                    cmd.Parameters.AddWithValue("$1", clientPath);
                    cmd.Parameters.AddWithValue("$2", requestUuid);
                    cmd.Parameters.AddWithValue("$3", serverPath);
                    cmd.Parameters.AddWithValue("$4", rpcAction);
                    cmd.Parameters.AddWithValue("$5", transactionTag);
                    cmd.Parameters.AddWithValue("$6", JsonConvert.SerializeObject(replyData));
                    
                    var result = await cmd.ExecuteScalarAsync();
                    
                    if (result == null)
                    {
                        await transaction.RollbackAsync();
                        throw new InvalidOperationException("No available record found");
                    }
                    
                    await transaction.CommitAsync();
                    return;
                }
                catch
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
            catch (Exception ex)
            {
                attempt++;
                if (attempt > maxRetries) throw;
                await SleepAsync(retryDelay);
            }
        }
    }

    public async Task<List<Dictionary<string, object>>> ListWaitingJobsAsync(string? clientPath = null)
    {
        var table = _baseTable;
        var query = $@"
            SELECT id, request_id, client_path, server_path,
                   response_payload, response_timestamp, is_new_result
            FROM {table}
            WHERE is_new_result = TRUE
        ";
        
        var parameters = new List<object>();
        
        if (!string.IsNullOrEmpty(clientPath))
        {
            query += " AND client_path = $1";
            parameters.Add(clientPath);
        }
        
        query += " ORDER BY response_timestamp ASC";

        try
        {
            using var cmd = new NpgsqlCommand(query, _client);
            for (int i = 0; i < parameters.Count; i++)
            {
                cmd.Parameters.AddWithValue($"${i + 1}", parameters[i]);
            }
            
            using var reader = await cmd.ExecuteReaderAsync();
            var results = new List<Dictionary<string, object>>();
            
            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object>();
                
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var columnName = reader.GetName(i);
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    
                    // Handle special transformations
                    if (columnName == "request_id" && value != null)
                    {
                        value = value.ToString();
                    }
                    else if (columnName == "response_timestamp" && value is DateTime dateTime)
                    {
                        value = dateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
                    }
                    
                    row[columnName] = value;
                }
                
                results.Add(row);
            }
            
            return results;
        }
        catch (Exception ex)
        {
            throw new Exception($"Database error when listing waiting jobs: {ex.Message}", ex);
        }
    }
}

