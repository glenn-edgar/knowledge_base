using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;

public class KBLinkTable
{
    private readonly NpgsqlConnection _client;
    private readonly string _baseTable;

    /// <summary>
    /// Creates a new instance of KBLinkTable
    /// </summary>
    /// <param name="client">An instance of NpgsqlConnection (already connected)</param>
    /// <param name="baseTableName">The base name of your table (without "_link" suffix)</param>
    public KBLinkTable(NpgsqlConnection client, string baseTableName)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _baseTable = $"{baseTableName}_link";
    }

    /// <summary>
    /// Find all rows where link_name = linkName,
    /// optionally filtered by parent_node_kb.
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
                WHERE link_name = $1
                  AND parent_node_kb = $2
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
    /// Find all rows where parent_path = nodePath,
    /// optionally filtered by parent_node_kb.
    /// </summary>
    /// <param name="nodePath">The node path to search for</param>
    /// <param name="kb">Optional knowledge base filter</param>
    /// <returns>List of matching records</returns>
    public async Task<List<Dictionary<string, object>>> FindRecordsByNodePathAsync(
        string nodePath,
        string? kb = null)
    {
        if (string.IsNullOrEmpty(nodePath))
            throw new ArgumentException("Node path cannot be null or empty", nameof(nodePath));

        string query;
        List<object> parameters;

        if (kb == null)
        {
            query = $"SELECT * FROM {_baseTable} WHERE parent_path = $1";
            parameters = new List<object> { nodePath };
        }
        else
        {
            query = $@"
                SELECT *
                FROM {_baseTable}
                WHERE parent_path = $1
                  AND parent_node_kb = $2
            ";
            parameters = new List<object> { nodePath, kb };
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
    /// Return all distinct parent_path values, sorted ascending.
    /// </summary>
    /// <returns>List of distinct node names (parent paths)</returns>
    public async Task<List<string>> FindAllNodeNamesAsync()
    {
        var query = $@"
            SELECT DISTINCT parent_path
            FROM {_baseTable}
            ORDER BY parent_path
        ";

        using var cmd = new NpgsqlCommand(query, _client);
        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<string>();

        while (await reader.ReadAsync())
        {
            var parentPath = reader["parent_path"]?.ToString();
            if (!string.IsNullOrEmpty(parentPath))
            {
                results.Add(parentPath);
            }
        }

        return results;
    }
}

