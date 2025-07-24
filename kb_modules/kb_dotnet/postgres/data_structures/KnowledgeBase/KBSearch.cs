using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Newtonsoft.Json;

public class Filter
{
    public string Condition { get; set; } = string.Empty;  // uses a single '#' placeholder for each param
    public List<object> Params { get; set; } = new List<object>();
}

public class KBSearch
{
    public List<string> Path { get; set; } = new List<string>();
    public string Host { get; set; }
    public int Port { get; set; }
    public string DbName { get; set; }
    public string User { get; set; }
    public string Password { get; set; }
    public string BaseTable { get; set; }
    public string LinkTable { get; set; }
    public string LinkMountTable { get; set; }
    public List<Filter> Filters { get; set; } = new List<Filter>();
    public List<dynamic>? Results { get; set; } = null;
    public Dictionary<string, object> PathValues { get; set; } = new Dictionary<string, object>();
    private NpgsqlConnection? _connection = null;
    private bool _isConnected = false;
    private readonly object _connectionLock = new object();

    public KBSearch(
        string host,
        int port,
        string dbName,
        string user,
        string password,
        string baseTable)
    {
        Host = host;
        Port = port;
        DbName = dbName;
        User = user;
        Password = password;
        BaseTable = baseTable;
        LinkTable = $"{baseTable}_link";
        LinkMountTable = $"{baseTable}_link_mount";
        
        // Don't auto-connect in constructor - let it be explicitly called
        // This prevents the timing issues we were seeing
    }

    /// <summary>
    /// Establishes the database connection - now public and awaitable
    /// </summary>
    public async Task ConnectAsync()
    {
        lock (_connectionLock)
        {
            if (_isConnected && _connection?.State == System.Data.ConnectionState.Open)
            {
                return; // Already connected
            }
        }

        try
        {
            var connectionString = $"Host={Host};Port={Port};Database={DbName};Username={User};Password={Password}";
            
            // Dispose existing connection if any
            if (_connection != null)
            {
                await _connection.DisposeAsync();
            }

            _connection = new NpgsqlConnection(connectionString);
            await _connection.OpenAsync();
            
            lock (_connectionLock)
            {
                _isConnected = true;
            }
            
            Console.WriteLine($"Successfully connected to database: {DbName}");
        }
        catch (Exception ex)
        {
            lock (_connectionLock)
            {
                _isConnected = false;
            }
            Console.WriteLine($"Error connecting to database: {ex.Message}");
            throw new InvalidOperationException($"Failed to connect to database {DbName}: {ex.Message}", ex);
        }
    }

    public async Task DisconnectAsync()
    {
        lock (_connectionLock)
        {
            _isConnected = false;
        }

        if (_connection != null)
        {
            try
            {
                if (_connection.State == System.Data.ConnectionState.Open)
                {
                    await _connection.CloseAsync();
                }
                await _connection.DisposeAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during disconnect: {ex.Message}");
            }
            finally
            {
                _connection = null;
            }
        }
    }

    /// <summary>
    /// Throws if not connected.
    /// </summary>
    public NpgsqlConnection GetConnection()
    {
        lock (_connectionLock)
        {
            if (!_isConnected || _connection == null || _connection.State != System.Data.ConnectionState.Open)
            {
                throw new InvalidOperationException("Not connected to database. Call ConnectAsync() first.");
            }
        }
        return _connection;
    }

    /// <summary>
    /// Checks if the database connection is active
    /// </summary>
    public bool IsConnected
    {
        get
        {
            lock (_connectionLock)
            {
                return _isConnected && _connection?.State == System.Data.ConnectionState.Open;
            }
        }
    }

    /// <summary>
    /// Ensures connection is established, reconnects if needed
    /// </summary>
    public async Task EnsureConnectedAsync()
    {
        if (!IsConnected)
        {
            await ConnectAsync();
        }
    }

    public void ClearFilters()
    {
        Filters.Clear();
        Results = null;
    }

    public void SearchKb(string kb)
    {
        Filters.Add(new Filter { Condition = "knowledge_base = #", Params = new List<object> { kb } });
    }

    public void SearchLabel(string label)
    {
        Filters.Add(new Filter { Condition = "label = #", Params = new List<object> { label } });
    }

    public void SearchName(string name)
    {
        Filters.Add(new Filter { Condition = "name = #", Params = new List<object> { name } });
    }

    public void SearchPropertyKey(string key)
    {
        // JSONB ? operator
        Filters.Add(new Filter { Condition = "properties::jsonb ? #", Params = new List<object> { key } });
    }

    public void SearchPropertyValue(string key, object value)
    {
        var jsonObject = new Dictionary<string, object> { [key] = value };
        Filters.Add(new Filter
        {
            Condition = "properties::jsonb @> #::jsonb",
            Params = new List<object> { JsonConvert.SerializeObject(jsonObject) }
        });
    }

    public void SearchStartingPath(string startingPath)
    {
        Filters.Add(new Filter { Condition = "path <@ #::ltree", Params = new List<object> { startingPath } });
    }

    public void SearchPath(string pathExpr)
    {
        // Use ltree match operator instead of regex
        Filters.Add(new Filter { Condition = "path ~ #::lquery", Params = new List<object> { pathExpr } });
    }

    public void SearchHasLink()
    {
        Filters.Add(new Filter { Condition = "has_link = TRUE", Params = new List<object>() });
    }

    public void SearchHasLinkMount()
    {
        Filters.Add(new Filter { Condition = "has_link_mount = TRUE", Params = new List<object>() });
    }

    /// <summary>
    /// Builds and runs a WITH‐CTE chain of filters, returning all columns.
    /// </summary>
    public async Task<List<dynamic>> ExecuteQueryAsync()
    {
        // Ensure we're connected before executing
        await EnsureConnectedAsync();
        var connection = GetConnection();
        const string columnStr = "*";

        // no filters → simple SELECT
        if (Filters.Count == 0)
        {
            using var cmd = new NpgsqlCommand($"SELECT {columnStr} FROM {BaseTable}", connection);
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
            Results = results;
            return Results;
        }

        // build CTEs
        var cteParts = new List<string>
        {
            $"base_data AS (SELECT {columnStr} FROM {BaseTable})"
        };
        var combinedParams = new List<object>();
        int paramCount = 0;

        for (int i = 0; i < Filters.Count; i++)
        {
            var filter = Filters[i];
            string condition = filter.Condition;
            
            // for each param, replace one '#' with $<n>
            foreach (var param in filter.Params)
            {
                paramCount++;
                string placeholder = $"${paramCount}";
                int index = condition.IndexOf('#');
                if (index >= 0)
                {
                    condition = condition.Substring(0, index) + placeholder + condition.Substring(index + 1);
                }
                combinedParams.Add(param);
            }
            
            string cteName = $"filter_{i}";
            string prev = i == 0 ? "base_data" : $"filter_{i - 1}";
            cteParts.Add($"{cteName} AS (SELECT {columnStr} FROM {prev} WHERE {condition})");
        }

        string finalQuery = $"WITH {string.Join(",\n", cteParts)}\nSELECT {columnStr} FROM filter_{Filters.Count - 1}";

        try
        {
            using var cmd = new NpgsqlCommand(finalQuery, connection);
            for (int i = 0; i < combinedParams.Count; i++)
            {
                cmd.Parameters.AddWithValue( combinedParams[i]);
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
            Results = results;
            return Results;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing query: {ex.Message}");
            Console.WriteLine($"Query: {finalQuery}");
            Console.WriteLine($"Parameters: [{string.Join(", ", combinedParams)}]");
            throw;
        }
    }

    public List<string> FindPathValues(object keyData)
    {
        if (keyData == null) return new List<string>();
        
        List<dynamic> rows;
        if (keyData is IEnumerable<dynamic> enumerable)
        {
            rows = enumerable.ToList();
        }
        else
        {
            rows = new List<dynamic> { keyData };
        }
        
        return rows.Select(r => ((Dictionary<string, object>)r)["path"]?.ToString() ?? string.Empty).ToList();
    }

    public List<dynamic> GetResults()
    {
        return Results ?? new List<dynamic>();
    }

    public List<Dictionary<string, string>> FindDescription(object keyData)
    {
        List<dynamic> rows;
        if (keyData is IEnumerable<dynamic> enumerable)
        {
            rows = enumerable.ToList();
        }
        else
        {
            rows = new List<dynamic> { keyData };
        }

        return rows.Select(r =>
        {
            var row = (Dictionary<string, object>)r;
            var path = row["path"]?.ToString() ?? string.Empty;
            var properties = row.ContainsKey("properties") ? row["properties"] : new Dictionary<string, object>();
            var propsDict = properties as Dictionary<string, object> ?? new Dictionary<string, object>();
            var description = propsDict.ContainsKey("description") ? propsDict["description"]?.ToString() ?? string.Empty : string.Empty;
            
            return new Dictionary<string, string> { [path] = description };
        }).ToList();
    }

    /// <summary>
    /// Fetches `data` for one or many paths in a single query.
    /// </summary>
    public async Task<Dictionary<string, object>> FindDescriptionPathsAsync(object pathArray)
    {
        // Ensure connection before executing
        await EnsureConnectedAsync();
        var connection = GetConnection();
        List<string> paths;
        
        if (pathArray is string singlePath)
        {
            paths = new List<string> { singlePath };
        }
        else if (pathArray is IEnumerable<string> pathList)
        {
            paths = pathList.ToList();
        }
        else
        {
            throw new ArgumentException("pathArray must be a string or IEnumerable<string>");
        }

        if (paths.Count == 0) return new Dictionary<string, object>();

        string query;
        if (paths.Count == 1)
        {
            query = $"SELECT path, data FROM {BaseTable} WHERE path = $1::ltree";
        }
        else
        {
            var placeholders = string.Join(", ", paths.Select((_, i) => $"${i + 1}::ltree"));
            query = $"SELECT path, data FROM {BaseTable} WHERE path IN ({placeholders})";
        }

        try
        {
            using var cmd = new NpgsqlCommand(query, connection);
            for (int i = 0; i < paths.Count; i++)
            {
                cmd.Parameters.AddWithValue(paths[i]);
            }

            using var reader = await cmd.ExecuteReaderAsync();
            var output = new Dictionary<string, object>();
            
            while (await reader.ReadAsync())
            {
                var path = reader["path"]?.ToString() ?? string.Empty;
                var data = reader.IsDBNull(reader.GetOrdinal("data")) ? null : reader["data"];
                output[path] = data;
            }

            // fill missing
            foreach (var path in paths)
            {
                if (!output.ContainsKey(path))
                {
                    output[path] = null;
                }
            }

            return output;
        }
        catch (Exception ex)
        {
            throw new Exception($"Error retrieving data for paths: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Splits a link‐encoded LTREE into [kbName, [[link, name], …]].
    /// </summary>
    public (string KbName, List<(string Link, string Name)> Pairs) DecodeLinkNodes(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            throw new ArgumentException("Path must be a non-empty string");
        }

        var parts = path.Split('.');
        if (parts.Length < 3)
        {
            throw new ArgumentException($"Path must have at least 3 elements (kb.link.name), got {parts.Length}");
        }

        int remaining = parts.Length - 1;
        if (remaining % 2 != 0)
        {
            throw new ArgumentException($"After kb identifier, must have even number of elements (link/name pairs), got {remaining}");
        }

        string kb = parts[0];
        var pairs = new List<(string Link, string Name)>();
        
        for (int i = 1; i < parts.Length; i += 2)
        {
            pairs.Add((parts[i], parts[i + 1]));
        }

        return (kb, pairs);
    }
}