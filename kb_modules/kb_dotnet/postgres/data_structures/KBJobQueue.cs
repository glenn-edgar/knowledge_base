using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Newtonsoft.Json;

public class JobCompletionResult
{
    public bool Success { get; set; }
    public int JobId { get; set; }
    public DateTime CompletedAt { get; set; }
}

public class JobPushResult
{
    public int JobId { get; set; }
    public DateTime ScheduleAt { get; set; }
    public object Data { get; set; }
}

public class JobQueueClearResult
{
    public bool Success { get; set; } = true;
    public int ClearedCount { get; set; }
    public List<dynamic> ClearedJobs { get; set; } = new List<dynamic>();
}

public class KBJobQueue
{
    private readonly KBSearch _kbSearch;
    private readonly NpgsqlConnection _client;
    private readonly string _baseTable;

    public KBJobQueue(KBSearch kbSearch, string database)
    {
        _kbSearch = kbSearch;
        _client = kbSearch.GetConnection();
        _baseTable = $"{database}_job";
    }

    private async Task<List<dynamic>> ExecuteQueryAsync(string query, List<object>? parameters = null)
    {
        parameters ??= new List<object>();
        
        using var cmd = new NpgsqlCommand(query, _client);
        for (int i = 0; i < parameters.Count; i++)
        {
            cmd.Parameters.AddWithValue($"${i + 1}", parameters[i]);
        }

        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<dynamic>();
        
        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
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

    public async Task<dynamic> FindJobIdAsync(
        string? kb = null,
        string? nodeName = null,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        var results = await FindJobIdsAsync(kb, nodeName, properties, nodePath);
        
        if (results.Count == 0)
        {
            throw new InvalidOperationException(
                $"No job found matching parameters: name={nodeName}, properties={JsonConvert.SerializeObject(properties)}, path={nodePath}");
        }
        
        if (results.Count > 1)
        {
            throw new InvalidOperationException(
                $"Multiple jobs ({results.Count}) found matching parameters: name={nodeName}, properties={JsonConvert.SerializeObject(properties)}, path={nodePath}");
        }
        
        return results[0];
    }

    public async Task<List<dynamic>> FindJobIdsAsync(
        string? kb = null,
        string? nodeName = null,
        Dictionary<string, object>? properties = null,
        string? nodePath = null)
    {
        try
        {
            _kbSearch.ClearFilters();
            _kbSearch.SearchLabel("KB_JOB_QUEUE");
            
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

            var rows = await _kbSearch.ExecuteQueryAsync();
            
            if (rows.Count == 0)
            {
                throw new InvalidOperationException(
                    $"No jobs found matching parameters: name={nodeName}, properties={JsonConvert.SerializeObject(properties)}, path={nodePath}");
            }
            
            return rows;
        }
        catch (Exception e)
        {
            if (e.Message.StartsWith("No jobs")) throw;
            throw new Exception($"Error finding job IDs: {e.Message}", e);
        }
    }

    public List<string> FindJobPaths(List<dynamic> rows)
    {
        return rows
            .Select(r => ((Dictionary<string, object>)r)["path"]?.ToString())
            .Where(p => !string.IsNullOrEmpty(p))
            .Cast<string>()
            .ToList();
    }

    public async Task<int> GetQueuedNumberAsync(string path)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");

        var query = $@"
            SELECT COUNT(*) AS count
            FROM {_baseTable}
            WHERE path = $1 AND valid = TRUE
        ";
        
        var row = await ExecuteSingleAsync(query, new List<object> { path });
        var rowDict = (Dictionary<string, object>)row;
        return Convert.ToInt32(rowDict["count"]);
    }

    public async Task<int> GetFreeNumberAsync(string path)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");

        var query = $@"
            SELECT COUNT(*) AS count
            FROM {_baseTable}  
            WHERE path = $1 AND valid = FALSE
        ";
        
        var row = await ExecuteSingleAsync(query, new List<object> { path });
        var rowDict = (Dictionary<string, object>)row;
        return Convert.ToInt32(rowDict["count"]);
    }

    public async Task<dynamic?> PeakJobDataAsync(
        string path,
        int maxRetries = 3,
        int retryDelay = 1000)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");

        int attempt = 0;
        
        while (attempt < maxRetries)
        {
            try
            {
                using var transaction = await _client.BeginTransactionAsync();
                
                try
                {
                    var findQuery = $@"
                        SELECT id, data, schedule_at
                        FROM {_baseTable}
                        WHERE path = $1
                          AND valid = TRUE
                          AND is_active = FALSE
                          AND (schedule_at IS NULL OR schedule_at <= NOW())
                        ORDER BY schedule_at ASC NULLS FIRST
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    ";
                    
                    var job = await ExecuteSingleAsync(findQuery, new List<object> { path });
                    
                    if (job == null)
                    {
                        await transaction.RollbackAsync();
                        return null;
                    }
                    
                    var jobDict = (Dictionary<string, object>)job;
                    var jobId = Convert.ToInt32(jobDict["id"]);
                    
                    var updateQuery = $@"
                        UPDATE {_baseTable}
                        SET started_at = NOW(), is_active = TRUE
                        WHERE id = $1 AND valid = TRUE AND is_active = FALSE
                        RETURNING started_at
                    ";
                    
                    var upd = await ExecuteSingleAsync(updateQuery, new List<object> { jobId });
                    
                    if (upd == null)
                    {
                        await transaction.RollbackAsync();
                        attempt++;
                        await Task.Delay(retryDelay);
                        continue;
                    }
                    
                    await transaction.CommitAsync();
                    
                    var updDict = (Dictionary<string, object>)upd;
                    jobDict["started_at"] = updDict["started_at"];
                    
                    return jobDict;
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
                await Task.Delay(retryDelay);
            }
        }
        
        throw new Exception($"Could not lock and claim a job for path='{path}' after {maxRetries} retries");
    }

    public async Task<JobCompletionResult> MarkJobCompletedAsync(
        int jobId,
        int maxRetries = 3,
        int retryDelay = 1000)
    {
        if (!int.TryParse(jobId.ToString(), out _)) 
            throw new ArgumentException("jobId must be a valid integer");

        int attempt = 0;
        
        while (attempt < maxRetries)
        {
            try
            {
                using var transaction = await _client.BeginTransactionAsync();
                
                try
                {
                    var lockQuery = $"SELECT id FROM {_baseTable} WHERE id = $1 FOR UPDATE NOWAIT";
                    var row = await ExecuteSingleAsync(lockQuery, new List<object> { jobId });
                    
                    if (row == null)
                    {
                        await transaction.RollbackAsync();
                        throw new InvalidOperationException($"No job found with id={jobId}");
                    }
                    
                    var updateQuery = $@"
                        UPDATE {_baseTable}
                        SET completed_at = NOW(), valid = FALSE, is_active = FALSE
                        WHERE id = $1
                        RETURNING id, completed_at
                    ";
                    
                    var res = await ExecuteSingleAsync(updateQuery, new List<object> { jobId });
                    
                    if (res == null)
                    {
                        await transaction.RollbackAsync();
                        throw new InvalidOperationException($"Failed to mark job {jobId} as completed");
                    }
                    
                    await transaction.CommitAsync();
                    
                    var resDict = (Dictionary<string, object>)res;
                    return new JobCompletionResult
                    {
                        Success = true,
                        JobId = Convert.ToInt32(resDict["id"]),
                        CompletedAt = (DateTime)resDict["completed_at"]
                    };
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
                await Task.Delay(retryDelay);
            }
        }
        
        throw new Exception($"Could not lock job id={jobId} after {maxRetries} attempts");
    }

    public async Task<JobPushResult> PushJobDataAsync(
        string path,
        Dictionary<string, object> data,
        int maxRetries = 3,
        int retryDelay = 1000)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");
        
        if (data == null) 
            throw new ArgumentException("Data must be a dictionary");

        var selectQuery = $@"
            SELECT id FROM {_baseTable}
            WHERE path = $1 AND valid = FALSE
            ORDER BY completed_at ASC
            LIMIT 1 FOR UPDATE SKIP LOCKED
        ";
        
        var updateQuery = $@"
            UPDATE {_baseTable}
            SET data = $1,
                schedule_at = NOW(),
                started_at  = NOW(),
                completed_at= NOW(),
                valid      = TRUE,
                is_active  = FALSE
            WHERE id = $2
            RETURNING id, schedule_at, data
        ";

        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                using var transaction = await _client.BeginTransactionAsync();
                
                try
                {
                    var row = await ExecuteSingleAsync(selectQuery, new List<object> { path });
                    
                    if (row == null)
                    {
                        await transaction.RollbackAsync();
                        throw new InvalidOperationException($"No available job slot for path '{path}'");
                    }
                    
                    var rowDict = (Dictionary<string, object>)row;
                    var id = Convert.ToInt32(rowDict["id"]);
                    
                    var res = await ExecuteSingleAsync(updateQuery, new List<object> { JsonConvert.SerializeObject(data), id });
                    
                    if (res == null)
                    {
                        await transaction.RollbackAsync();
                        throw new InvalidOperationException($"Failed to update job slot for path '{path}'");
                    }
                    
                    await transaction.CommitAsync();
                    
                    var resDict = (Dictionary<string, object>)res;
                    return new JobPushResult
                    {
                        JobId = Convert.ToInt32(resDict["id"]),
                        ScheduleAt = (DateTime)resDict["schedule_at"],
                        Data = resDict["data"]
                    };
                }
                catch
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
            catch (Exception ex)
            {
                if (attempt < maxRetries)
                {
                    await Task.Delay(retryDelay);
                    continue;
                }
                throw new Exception($"Could not acquire lock for path '{path}' after {maxRetries} attempts: {ex.Message}", ex);
            }
        }
        
        throw new Exception("Unexpected error in PushJobDataAsync");
    }

    public async Task<List<dynamic>> ListPendingJobsAsync(
        string path,
        int? limit = null,
        int offset = 0)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");

        var query = $@"
            SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
            FROM {_baseTable}
            WHERE path = $1 AND valid = TRUE AND is_active = FALSE
            ORDER BY schedule_at ASC
        ";
        
        var parameters = new List<object> { path };
        
        if (limit.HasValue)
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

    public async Task<List<dynamic>> ListActiveJobsAsync(
        string path,
        int? limit = null,
        int offset = 0)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");

        var query = $@"
            SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
            FROM {_baseTable}
            WHERE path = $1 AND valid = TRUE AND is_active = TRUE
            ORDER BY started_at ASC
        ";
        
        var parameters = new List<object> { path };
        
        if (limit.HasValue)
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

    public async Task<JobQueueClearResult> ClearJobQueueAsync(string path)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");

        var updateQuery = $@"
            UPDATE {_baseTable}
            SET schedule_at = NOW(),
                started_at  = NOW(),
                completed_at= NOW(),
                is_active   = FALSE,
                valid       = FALSE,
                data        = $1
            WHERE path = $2
            RETURNING id, completed_at
        ";

        try
        {
            using var transaction = await _client.BeginTransactionAsync();
            
            try
            {
                // Lock table in exclusive mode
                using var lockCmd = new NpgsqlCommand($"LOCK TABLE {_baseTable} IN EXCLUSIVE MODE", _client, transaction);
                await lockCmd.ExecuteNonQueryAsync();

                // Perform the mass-update
                var rows = await ExecuteQueryAsync(updateQuery, new List<object> { JsonConvert.SerializeObject(new { }), path });

                await transaction.CommitAsync();

                return new JobQueueClearResult
                {
                    Success = true,
                    ClearedCount = rows.Count,
                    ClearedJobs = rows
                };
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"Failed to clear job queue for path '{path}': {ex.Message}", ex);
        }
    }

    public async Task<Dictionary<string, object>> GetJobStatisticsAsync(string path)
    {
        if (string.IsNullOrEmpty(path)) 
            throw new ArgumentException("Path cannot be empty or None");

        var statsQuery = $@"
            SELECT
                COUNT(*) AS total_jobs,
                COUNT(*) FILTER (WHERE valid AND NOT is_active) AS pending_jobs,
                COUNT(*) FILTER (WHERE valid AND is_active) AS active_jobs,
                COUNT(*) FILTER (WHERE NOT valid) AS completed_jobs,
                MIN(schedule_at) AS earliest_scheduled,
                MAX(completed_at) AS latest_completed,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) AS avg_processing_time_seconds
            FROM {_baseTable}
            WHERE path = $1
        ";
        
        var row = await ExecuteSingleAsync(statsQuery, new List<object> { path });
        
        return (row as Dictionary<string, object>) ?? new Dictionary<string, object>
        {
            ["total_jobs"] = 0,
            ["pending_jobs"] = 0,
            ["active_jobs"] = 0,
            ["completed_jobs"] = 0,
            ["earliest_scheduled"] = null,
            ["latest_completed"] = null,
            ["avg_processing_time_seconds"] = null
        };
    }

    public async Task<dynamic?> GetJobByIdAsync(int jobId)
    {
        if (!int.TryParse(jobId.ToString(), out _)) 
            throw new ArgumentException("jobId must be a valid integer");

        var query = $@"
            SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
            FROM {_baseTable}
            WHERE id = $1
        ";
        
        return await ExecuteSingleAsync(query, new List<object> { jobId });
    }

    public void Close()
    {
        // No-op: client remains managed by KBSearch
    }
}

