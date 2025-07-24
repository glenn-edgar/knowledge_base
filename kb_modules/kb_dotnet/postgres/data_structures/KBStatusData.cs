using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Newtonsoft.Json;

public class KBStatusData
{
    private readonly KBSearch _kbSearch;
    private readonly NpgsqlConnection _client;
    private readonly string _baseTable;

    public KBStatusData(KBSearch kbSearch, string database)
    {
        _kbSearch = kbSearch;
        _client = kbSearch.GetConnection();
        _baseTable = $"{database}_status";
    }

    private async Task SleepAsync(int milliseconds)
    {
        await Task.Delay(milliseconds);
    }

    public async Task<dynamic> FindNodeIdAsync(
        string kb,
        string nodeName,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        var results = await FindNodeIdsAsync(kb, nodeName, properties, nodePath);
        
        if (results.Count == 0)
        {
            throw new InvalidOperationException(
                $"No node found matching parameters: kb={kb}, name={nodeName}, properties={JsonConvert.SerializeObject(properties)}, path={nodePath}");
        }
        
        if (results.Count > 1)
        {
            throw new InvalidOperationException(
                $"Multiple nodes ({results.Count}) found matching parameters: kb={kb}, name={nodeName}, properties={JsonConvert.SerializeObject(properties)}, path={nodePath}");
        }
        
        return results[0];
    }

    public async Task<List<dynamic>> FindNodeIdsAsync(
        string? kb = null,
        string? nodeName = null,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        try
        {
            _kbSearch.ClearFilters();
            _kbSearch.SearchLabel("KB_STATUS_FIELD");
            
            if (!string.IsNullOrEmpty(kb)) _kbSearch.SearchKb(kb);
            if (!string.IsNullOrEmpty(nodeName)) _kbSearch.SearchName(nodeName);
            
            if (properties != null)
            {
                foreach (var kvp in properties)
                {
                    _kbSearch.SearchPropertyValue(kvp.Key, kvp.Value);
                }
            }
            
            if (!string.IsNullOrEmpty(nodePath)) _kbSearch.SearchPath(nodePath);

            var nodeIds = await _kbSearch.ExecuteQueryAsync();
            
            if (nodeIds == null || nodeIds.Count == 0)
            {
                throw new InvalidOperationException(
                    $"No nodes found matching parameters: kb={kb}, name={nodeName}, properties={JsonConvert.SerializeObject(properties)}, path={nodePath}");
            }
            
            return nodeIds;
        }
        catch (Exception e)
        {
            if (e.Message.StartsWith("No nodes found")) throw;
            throw new Exception($"Error finding node IDs: {e.Message}", e);
        }
    }

    public async Task<(object Data, string Path)> GetStatusDataAsync(string path)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");

        var query = $@"
            SELECT data, path
            FROM {_baseTable}
            WHERE path = $1
            LIMIT 1
        ";

        using var cmd = new NpgsqlCommand(query, _client);
        cmd.Parameters.AddWithValue("$1", path);
        
        using var reader = await cmd.ExecuteReaderAsync();
        
        if (!await reader.ReadAsync())
        {
            throw new InvalidOperationException($"No data found for path: {path}");
        }

        var data = reader["data"];
        var pathValue = reader["path"].ToString();

        if (data is string dataStr)
        {
            try
            {
                data = JsonConvert.DeserializeObject(dataStr);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to decode JSON data for path '{path}': {ex.Message}", ex);
            }
        }

        return (data, pathValue);
    }

    public async Task<Dictionary<string, object>> GetMultipleStatusDataAsync(object paths)
    {
        List<string> pathList;
        
        if (paths is string singlePath)
        {
            pathList = new List<string> { singlePath };
        }
        else if (paths is IEnumerable<string> pathEnumerable)
        {
            pathList = pathEnumerable.ToList();
        }
        else
        {
            throw new ArgumentException("paths must be a string or IEnumerable<string>");
        }

        if (pathList.Count == 0) return new Dictionary<string, object>();

        var placeholders = string.Join(", ", pathList.Select((_, i) => $"${i + 1}"));
        var query = $@"
            SELECT data, path
            FROM {_baseTable}
            WHERE path IN ({placeholders})
        ";

        using var cmd = new NpgsqlCommand(query, _client);
        for (int i = 0; i < pathList.Count; i++)
        {
            cmd.Parameters.AddWithValue($"${i + 1}", pathList[i]);
        }

        using var reader = await cmd.ExecuteReaderAsync();
        var output = new Dictionary<string, object>();

        while (await reader.ReadAsync())
        {
            var data = reader["data"];
            var pathValue = reader["path"].ToString();

            if (data is string dataStr)
            {
                try
                {
                    data = JsonConvert.DeserializeObject(dataStr);
                }
                catch
                {
                    Console.WriteLine($"Warning: Failed to parse JSON for path '{pathValue}'");
                }
            }

            output[pathValue] = data;
        }

        return output;
    }

    public async Task<(bool Success, string Message)> SetStatusDataAsync(
        string path,
        Dictionary<string, object> data,
        int retryCount = 3,
        int retryDelay = 1000)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");
        
        if (data == null) 
            throw new ArgumentException("Data must be a dictionary");
        
        if (retryCount < 0) 
            throw new ArgumentException("Retry count must be non-negative");
        
        if (retryDelay < 0) 
            throw new ArgumentException("Retry delay must be non-negative");

        var jsonData = JsonConvert.SerializeObject(data);
        var upsertQuery = $@"
            INSERT INTO {_baseTable} (path, data)
            VALUES ($1, $2)
            ON CONFLICT (path)
            DO UPDATE SET data = EXCLUDED.data
            RETURNING path, (xmax = 0) AS was_inserted
        ";

        Exception lastError = null;
        
        for (int attempt = 0; attempt <= retryCount; attempt++)
        {
            try
            {
                using var cmd = new NpgsqlCommand(upsertQuery, _client);
                cmd.Parameters.AddWithValue("$1", path);
                cmd.Parameters.AddWithValue("$2", jsonData);
                
                using var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                
                var returnedPath = reader["path"].ToString();
                var wasInserted = (bool)reader["was_inserted"];
                var operation = wasInserted ? "inserted" : "updated";
                
                return (true, $"Successfully {operation} data for path: {returnedPath}");
            }
            catch (Exception e)
            {
                lastError = e;
                if (attempt < retryCount)
                {
                    await SleepAsync(retryDelay);
                    continue;
                }
            }
        }

        throw new Exception(
            $"Failed to set status data for path '{path}' after {retryCount + 1} attempts: {lastError?.Message}",
            lastError);
    }

    public async Task<(bool Success, string Message, Dictionary<string, string> Results)> SetMultipleStatusDataAsync(
        object pathDataPairs,
        int retryCount = 3,
        int retryDelay = 1000)
    {
        List<(string Path, object Data)> pairs;

        if (pathDataPairs is Dictionary<string, object> dict)
        {
            pairs = dict.Select(kvp => (kvp.Key, kvp.Value)).ToList();
        }
        else if (pathDataPairs is IEnumerable<(string, object)> tupleEnumerable)
        {
            pairs = tupleEnumerable.ToList();
        }
        else if (pathDataPairs is IEnumerable<KeyValuePair<string, object>> kvpEnumerable)
        {
            pairs = kvpEnumerable.Select(kvp => (kvp.Key, kvp.Value)).ToList();
        }
        else
        {
            throw new ArgumentException("pathDataPairs must be a Dictionary<string, object> or IEnumerable of key-value pairs");
        }

        if (pairs.Count == 0) 
            throw new ArgumentException("path_data_pairs cannot be empty");
        
        if (retryCount < 0) 
            throw new ArgumentException("Retry count must be non-negative");
        
        if (retryDelay < 0) 
            throw new ArgumentException("Retry delay must be non-negative");

        var jsonPairs = pairs.Select(p => (p.Path, JsonConvert.SerializeObject(p.Data))).ToList();
        var upsertQuery = $@"
            INSERT INTO {_baseTable} (path, data)
            VALUES ($1, $2)
            ON CONFLICT (path)
            DO UPDATE SET data = EXCLUDED.data
            RETURNING path, (xmax = 0) AS was_inserted
        ";

        Exception lastError = null;

        for (int attempt = 0; attempt <= retryCount; attempt++)
        {
            try
            {
                using var transaction = await _client.BeginTransactionAsync();
                var results = new Dictionary<string, string>();

                try
                {
                    foreach (var (path, jsonData) in jsonPairs)
                    {
                        using var cmd = new NpgsqlCommand(upsertQuery, _client, transaction);
                        cmd.Parameters.AddWithValue("$1", path);
                        cmd.Parameters.AddWithValue("$2", jsonData);

                        using var reader = await cmd.ExecuteReaderAsync();
                        await reader.ReadAsync();

                        var returnedPath = reader["path"].ToString();
                        var wasInserted = (bool)reader["was_inserted"];
                        var operation = wasInserted ? "inserted" : "updated";
                        results[returnedPath] = operation;
                    }

                    await transaction.CommitAsync();
                    return (true, $"Successfully processed {jsonPairs.Count} records", results);
                }
                catch
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
            catch (Exception e)
            {
                lastError = e;
                if (attempt < retryCount)
                {
                    await SleepAsync(retryDelay);
                    continue;
                }
            }
        }

        throw new Exception(
            $"Failed to set multiple status data after {retryCount + 1} attempts: {lastError?.Message}",
            lastError);
    }
}

