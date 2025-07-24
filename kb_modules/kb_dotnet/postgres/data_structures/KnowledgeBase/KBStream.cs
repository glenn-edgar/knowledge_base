using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Newtonsoft.Json;

public class StreamRecord
{
    public int Id { get; set; }
    public string Path { get; set; } = string.Empty;
    public DateTime RecordedAt { get; set; }
    public object Data { get; set; } = new();
    public bool Valid { get; set; }
}

public class ClearStreamDataResult
{
    public bool Success { get; set; }
    public int ClearedCount { get; set; }
    public List<dynamic>? ClearedRecords { get; set; }
    public string? Error { get; set; }
}

public class KBStream
{
    private readonly KBSearch _kbSearch;
    private readonly string _baseTable;

    public KBStream(KBSearch kbSearch, string database)
    {
        _kbSearch = kbSearch;
        _baseTable = $"{database}_stream";
    }

    private NpgsqlConnection GetConnection()
    {
        return _kbSearch.GetConnection();
    }

    private async Task<List<dynamic>> ExecuteQueryAsync(string query, List<object>? parameters = null)
    {
        parameters ??= new List<object>();
        var client = GetConnection();
        
        using var cmd = new NpgsqlCommand(query, client);
        for (int i = 0; i < parameters.Count; i++)
        {
            cmd.Parameters.AddWithValue(parameters[i]);
        }

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

    private async Task<dynamic?> ExecuteSingleAsync(string query, List<object>? parameters = null)
    {
        var results = await ExecuteQueryAsync(query, parameters);
        return results.FirstOrDefault();
    }

    private async Task SleepAsync(int milliseconds)
    {
        await Task.Delay(milliseconds);
    }

    public async Task<dynamic> FindStreamIdAsync(
        string? kb = null,
        string? nodeName = null,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        var results = await FindStreamIdsAsync(kb, nodeName, properties, nodePath);
        
        if (results.Count == 0)
        {
            throw new InvalidOperationException(
                $"No stream node found matching parameters: name={nodeName}, properties={JsonConvert.SerializeObject(properties)}, path={nodePath}");
        }
        
        if (results.Count > 1)
        {
            throw new InvalidOperationException(
                $"Multiple stream nodes ({results.Count}) found matching parameters: name={nodeName}, properties={JsonConvert.SerializeObject(properties)}, path={nodePath}");
        }
        
        return results[0];
    }

    public async Task<List<dynamic>> FindStreamIdsAsync(
        string? kb = null,
        string? nodeName = null,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        try
        {
            _kbSearch.ClearFilters();
            _kbSearch.SearchLabel("KB_STREAM_FIELD");
            
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
                    $"No stream nodes found matching parameters: name={nodeName}, properties={JsonConvert.SerializeObject(properties)}, path={nodePath}");
            }
            
            return nodeIds;
        }
        catch (Exception e)
        {
            if (e.Message.StartsWith("No stream")) throw;
            throw new Exception($"Error finding stream node IDs: {e.Message}", e);
        }
    }

    public List<string> FindStreamTableKeys(List<dynamic> rows)
    {
        if (rows == null || rows.Count == 0) return new List<string>();
        
        return rows
            .Select(r => ((Dictionary<string, object>)r)["path"]?.ToString())
            .Where(p => !string.IsNullOrEmpty(p))
            .Cast<string>()
            .ToList();
    }

    public async Task<Dictionary<string, object>> PushStreamDataAsync(
        string path,
        Dictionary<string, object> data,
        int maxRetries = 3,
        int retryDelay = 1000)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty");
        
        if (data == null) 
            throw new ArgumentException("Data must be an object");

        var client = GetConnection();

        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var countRow = await ExecuteSingleAsync(
                    $"SELECT COUNT(*) AS count FROM {_baseTable} WHERE path = $1::ltree",
                    new List<object> { path });
                
                var total = Convert.ToInt32(((Dictionary<string, object>)countRow)["count"]);
                
                if (total == 0)
                {
                    throw new InvalidOperationException($"No records found for path='{path}'. Must pre-allocate.");
                }

                using var transaction = await client.BeginTransactionAsync();
                
                try
                {
                    var row = await ExecuteSingleAsync(
                        $@"
                        SELECT id, recorded_at, valid
                        FROM {_baseTable}
                        WHERE path = $1::ltree
                        ORDER BY recorded_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                        ",
                        new List<object> { path });

                    if (row == null)
                    {
                        await transaction.RollbackAsync();
                        if (attempt < maxRetries)
                        {
                            await SleepAsync(retryDelay);
                            continue;
                        }
                        else
                        {
                            throw new InvalidOperationException(
                                $"Could not lock any row for path='{path}' after {maxRetries} attempts");
                        }
                    }

                    var rowDict = (Dictionary<string, object>)row;
                    var prevRecordedAt = (DateTime)rowDict["recorded_at"];
                    var wasValid = (bool)rowDict["valid"];
                    var id = Convert.ToInt32(rowDict["id"]);

                    var upd = await ExecuteSingleAsync(
                        $@"
                        UPDATE {_baseTable}
                        SET data = $1::json, recorded_at = NOW(), valid = TRUE
                        WHERE id = $2
                        RETURNING id, path, recorded_at, data, valid
                        ",
                        new List<object> { JsonConvert.SerializeObject(data), id });

                    if (upd == null)
                    {
                        await transaction.RollbackAsync();
                        throw new InvalidOperationException($"Failed to update record id={id}");
                    }

                    await transaction.CommitAsync();

                    var updDict = (Dictionary<string, object>)upd;
                    return new Dictionary<string, object>
                    {
                        ["id"] = updDict["id"],
                        ["path"] = updDict["path"],
                        ["recorded_at"] = updDict["recorded_at"],
                        ["data"] = updDict["data"],
                        ["valid"] = updDict["valid"],
                        ["previous_recorded_at"] = prevRecordedAt,
                        ["was_previously_valid"] = wasValid,
                        ["operation"] = "circular_buffer_replace"
                    };
                }
                catch
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
            catch (Exception e)
            {
                if (e.Message.StartsWith("No records") || e.Message.Contains("Could not lock"))
                {
                    throw;
                }
                
                if (attempt < maxRetries)
                {
                    await SleepAsync(retryDelay);
                    continue;
                }
                else
                {
                    throw new Exception($"Error pushing stream data: {e.Message}", e);
                }
            }
        }
        
        throw new Exception("Unexpected error in pushStreamData");
    }

    public async Task<Dictionary<string, object>?> GetLatestStreamDataAsync(string path)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty");

        var row = await ExecuteSingleAsync(
            $@"
            SELECT id, path, recorded_at, data, valid
            FROM {_baseTable}
            WHERE path = $1::ltree AND valid = TRUE
            ORDER BY recorded_at DESC
            LIMIT 1
            ",
            new List<object> { path });

        return row as Dictionary<string, object>;
    }

    public async Task<int> GetStreamDataCountAsync(string path, bool includeInvalid = false)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty");

        var query = includeInvalid
            ? $"SELECT COUNT(*) AS count FROM {_baseTable} WHERE path = $1::ltree"
            : $"SELECT COUNT(*) AS count FROM {_baseTable} WHERE path = $1::ltree AND valid = TRUE";

        var row = await ExecuteSingleAsync(query, new List<object> { path });
        var rowDict = (Dictionary<string, object>)row;
        return Convert.ToInt32(rowDict["count"]);
    }

    public async Task<ClearStreamDataResult> ClearStreamDataAsync(string path, DateTime? olderThan = null)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty");

        try
        {
            string query;
            List<object> parameters;

            if (olderThan.HasValue)
            {
                query = $@"
                    UPDATE {_baseTable}
                    SET valid = FALSE
                    WHERE path = $1::ltree AND recorded_at < $2 AND valid = TRUE
                    RETURNING id, recorded_at
                ";
                parameters = new List<object> { path, olderThan.Value };
            }
            else
            {
                query = $@"
                    UPDATE {_baseTable}
                    SET valid = FALSE
                    WHERE path = $1::ltree AND valid = TRUE
                    RETURNING id, recorded_at
                ";
                parameters = new List<object> { path };
            }

            var records = await ExecuteQueryAsync(query, parameters);
            
            return new ClearStreamDataResult
            {
                Success = true,
                ClearedCount = records.Count,
                ClearedRecords = records
            };
        }
        catch (Exception e)
        {
            return new ClearStreamDataResult
            {
                Success = false,
                ClearedCount = 0,
                Error = e.Message
            };
        }
    }

    public async Task<List<dynamic>> ListStreamDataAsync(
        string path,
        int? limit = null,
        int offset = 0,
        DateTime? recordedAfter = null,
        DateTime? recordedBefore = null,
        string order = "ASC")
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty");
        
        if (!new[] { "ASC", "DESC" }.Contains(order))
            throw new ArgumentException("Order must be 'ASC' or 'DESC'");

        var query = $@"
            SELECT id, path, recorded_at, data, valid
            FROM {_baseTable}
            WHERE path = $1::ltree AND valid = TRUE
        ";
        
        var parameters = new List<object> { path };

        if (recordedAfter.HasValue)
        {
            parameters.Add(recordedAfter.Value);
            query += $" AND recorded_at >= ${parameters.Count}";
        }

        if (recordedBefore.HasValue)
        {
            parameters.Add(recordedBefore.Value);
            query += $" AND recorded_at <= ${parameters.Count}";
        }

        query += $" ORDER BY recorded_at {order}";

        if (limit.HasValue && limit.Value > 0)
        {
            parameters.Add(limit.Value);
            query += $" LIMIT ${parameters.Count}";
        }

        if (offset > 0)
        {
            parameters.Add(offset);
            query += $" OFFSET ${parameters.Count}";
        }

        return await ExecuteQueryAsync(query, parameters);
    }

    public async Task<List<dynamic>> GetStreamDataRangeAsync(
        string path,
        DateTime startTime,
        DateTime endTime)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty");
        
        if (startTime >= endTime) 
            throw new ArgumentException("startTime must be before endTime");

        var query = $@"
            SELECT id, path, recorded_at, data, valid
            FROM {_baseTable}
            WHERE path = $1::ltree
              AND recorded_at >= $2
              AND recorded_at <= $3
              AND valid = TRUE
            ORDER BY recorded_at ASC
        ";

        return await ExecuteQueryAsync(query, new List<object> { path, startTime, endTime });
    }

    public async Task<Dictionary<string, object>> GetStreamStatisticsAsync(
        string path,
        bool includeInvalid = false)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty");

        var statsQuery = includeInvalid
            ? $@"
                SELECT 
                  COUNT(*) AS total_records,
                  COUNT(CASE WHEN valid THEN 1 END) AS valid_records,
                  COUNT(CASE WHEN NOT valid THEN 1 END) AS invalid_records,
                  MIN(CASE WHEN valid THEN recorded_at END) AS earliest_valid_recorded,
                  MAX(CASE WHEN valid THEN recorded_at END) AS latest_valid_recorded,
                  MIN(recorded_at) AS earliest_recorded_overall,
                  MAX(recorded_at) AS latest_recorded_overall,
                  AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) AS avg_interval_seconds_all,
                  AVG(CASE WHEN valid THEN EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at))) END) AS avg_interval_seconds_valid
                FROM {_baseTable}
                WHERE path = $1::ltree
              "
            : $@"
                SELECT 
                  COUNT(*) AS valid_records,
                  MIN(recorded_at) AS earliest_recorded,
                  MAX(recorded_at) AS latest_recorded,
                  AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) AS avg_interval_seconds
                FROM {_baseTable}
                WHERE path = $1::ltree AND valid = TRUE
              ";

        var row = await ExecuteSingleAsync(statsQuery, new List<object> { path });
        return (row as Dictionary<string, object>) ?? new Dictionary<string, object>();
    }

    public async Task<Dictionary<string, object>?> GetStreamDataByIdAsync(int recordId)
    {
        if (!int.TryParse(recordId.ToString(), out _))
        {
            throw new ArgumentException("recordId must be an integer");
        }

        var query = $@"
            SELECT id, path, recorded_at, data
            FROM {_baseTable}
            WHERE id = $1
        ";

        var result = await ExecuteSingleAsync(query, new List<object> { recordId });
        return result as Dictionary<string, object>;
    }

    public async Task DisconnectAsync()
    {
        // Connection is managed by KBSearch, so this is a no-op
        await Task.CompletedTask;
    }
}