using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;

public class KBLinkMountTable
{
    private readonly NpgsqlConnection _client;
    private readonly string _baseTable;

    /// <summary>
    /// Creates a new instance of KBLinkMountTable
    /// </summary>
    /// <param name="client">An instance of NpgsqlConnection (already connected)</param>
    /// <param name="baseTableName">The base name of your table (without "_link_mount" suffix)</param>
    public KBLinkMountTable(NpgsqlConnection client, string baseTableName)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _baseTable = $"{baseTableName}_link_mount";
    }

    /// <summary>
    /// Find all rows where link_name = linkName,
    /// optionally filtered by knowledge_base.
    /// </summary>
    /// <param name="linkName">The link name to search for</param>
    /// <param name="kb">Optional knowledge base filter</param>
    /// <returns>List of matching records</returns>
    public async Task<List<Dictionary<string, object>>> FindRecordsByLinkNameAsync(
        string linkName,
        string? kb = null)
    {
        if (string.IsNullOrEmpty(linkName))
            throw new ArgumentException("Link name cannot be null or empty", nameof(linkName));

        string query;
        List<object> parameters;

        if (kb == null)
        {
            query = $"SELECT * FROM {_baseTable} WHERE link_name = $1";
            parameters = new List<object> { linkName };
        }
        else
        {
            query = $@"
                SELECT *
                FROM {_baseTable}
                WHERE link_name = $1 AND knowledge_base = $2
            ";
            parameters = new List<object> { linkName, kb };
        }

        using var cmd = new NpgsqlCommand(query, _client);
        for (int i = 0; i < parameters.Count; i++)
        {
            cmd.Parameters.AddWithValue(parameters[i]);
        }

        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object>>();

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

    /// <summary>
    /// Find all rows where mount_path = mountPath,
    /// optionally filtered by knowledge_base.
    /// </summary>
    /// <param name="mountPath">The mount path to search for</param>
    /// <param name="kb">Optional knowledge base filter</param>
    /// <returns>List of matching records</returns>
    public async Task<List<Dictionary<string, object>>> FindRecordsByMountPathAsync(
        string mountPath,
        string? kb = null)
    {
        if (string.IsNullOrEmpty(mountPath))
            throw new ArgumentException("Mount path cannot be null or empty", nameof(mountPath));

        string query;
        List<object> parameters;

        if (kb == null)
        {
            query = $"SELECT * FROM {_baseTable} WHERE mount_path = $1";
            parameters = new List<object> { mountPath };
        }
        else
        {
            query = $@"
                SELECT *
                FROM {_baseTable}
                WHERE mount_path = $1 AND knowledge_base = $2
            ";
            parameters = new List<object> { mountPath, kb };
        }

        using var cmd = new NpgsqlCommand(query, _client);
        for (int i = 0; i < parameters.Count; i++)
        {
            cmd.Parameters.AddWithValue(parameters[i]);
        }

        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object>>();

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

    /// <summary>
    /// Return all distinct link_name values, sorted ascending.
    /// </summary>
    /// <returns>List of distinct link names</returns>
    public async Task<List<string>> FindAllLinkNamesAsync()
    {
        var query = $@"
            SELECT DISTINCT link_name
            FROM {_baseTable}
            ORDER BY link_name
        ";

        using var cmd = new NpgsqlCommand(query, _client);
        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<string>();

        while (await reader.ReadAsync())
        {
            var linkName = reader["link_name"]?.ToString();
            if (!string.IsNullOrEmpty(linkName))
            {
                results.Add(linkName);
            }
        }

        return results;
    }

    /// <summary>
    /// Return all distinct mount_path values, sorted ascending.
    /// </summary>
    /// <returns>List of distinct mount paths</returns>
    public async Task<List<string>> FindAllMountPathsAsync()
    {
        var query = $@"
            SELECT DISTINCT mount_path
            FROM {_baseTable}
            ORDER BY mount_path
        ";

        using var cmd = new NpgsqlCommand(query, _client);
        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<string>();

        while (await reader.ReadAsync())
        {
            var mountPath = reader["mount_path"]?.ToString();
            if (!string.IsNullOrEmpty(mountPath))
            {
                results.Add(mountPath);
            }
        }

        return results;
    }
}

