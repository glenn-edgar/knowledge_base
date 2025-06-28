import { KBSearch } from './KBSearch';
import { Client } from 'pg';

export class KBStatusData {
  private kbSearch: KBSearch;
  private client: Client;
  private baseTable: string;

  constructor(kbSearch: KBSearch, database: string) {
    this.kbSearch = kbSearch;
    this.client = kbSearch.getConn();
    this.baseTable = `${database}_status`;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  public async findNodeId(
    kb: string,
    nodeName: string,
    properties?: Record<string, any>,
    nodePath?: string
  ): Promise<any> {
    const results = await this.findNodeIds(kb, nodeName, properties, nodePath);
    if (results.length === 0) {
      throw new Error(
        `No node found matching parameters: kb=${kb}, name=${nodeName}, properties=${JSON.stringify(properties)}, path=${nodePath}`
      );
    }
    if (results.length > 1) {
      throw new Error(
        `Multiple nodes (${results.length}) found matching parameters: kb=${kb}, name=${nodeName}, properties=${JSON.stringify(properties)}, path=${nodePath}`
      );
    }
    return results[0];
  }

  public async findNodeIds(
    kb?: string,
    nodeName?: string,
    properties?: Record<string, any>,
    nodePath?: string
  ): Promise<any[]> {
    try {
      this.kbSearch.clearFilters();
      this.kbSearch.searchLabel('KB_STATUS_FIELD');
      if (kb)         this.kbSearch.searchKb(kb);
      if (nodeName)   this.kbSearch.searchName(nodeName);
      if (properties) {
        for (const [k, v] of Object.entries(properties)) {
          this.kbSearch.searchPropertyValue(k, v);
        }
      }
      if (nodePath)   this.kbSearch.searchPath(nodePath);

      const nodeIds = await this.kbSearch.executeQuery();
      if (!nodeIds || nodeIds.length === 0) {
        throw new Error(
          `No nodes found matching parameters: kb=${kb}, name=${nodeName}, properties=${JSON.stringify(properties)}, path=${nodePath}`
        );
      }
      return nodeIds;
    } catch (e: any) {
      if (e.message.startsWith('No nodes found')) throw e;
      throw new Error(`Error finding node IDs: ${e.message}`);
    }
  }

  public async getStatusData(
    path: string
  ): Promise<[any, string]> {
    if (!path) throw new Error('Path cannot be empty or None');
    const query = `
      SELECT data, path
      FROM ${this.baseTable}
      WHERE path = $1
      LIMIT 1
    `;
    const res = await this.client.query(query, [path]);
    if (res.rows.length === 0) {
      throw new Error(`No data found for path: ${path}`);
    }
    let { data, path: pathValue } = res.rows[0];
    if (typeof data === 'string') {
      try {
        data = JSON.parse(data);
      } catch (err: any) {
        throw new Error(`Failed to decode JSON data for path '${path}': ${err.message}`);
      }
    }
    return [data, pathValue];
  }

  public async getMultipleStatusData(
    paths: string | string[]
  ): Promise<Record<string, any>> {
    let list = Array.isArray(paths) ? paths : [paths];
    if (list.length === 0) return {};
    const placeholders = list.map((_, i) => `$${i + 1}`).join(', ');
    const query = `
      SELECT data, path
      FROM ${this.baseTable}
      WHERE path IN (${placeholders})
    `;
    const res = await this.client.query(query, list);
    const output: Record<string, any> = {};
    for (const row of res.rows) {
      let { data, path: pathValue } = row;
      if (typeof data === 'string') {
        try { data = JSON.parse(data); } catch {
          console.warn(`Warning: Failed to parse JSON for path '${pathValue}'`);
        }
      }
      output[pathValue] = data;
    }
    return output;
  }

  public async setStatusData(
    path: string,
    data: Record<string, any>,
    retryCount = 3,
    retryDelay = 1000
  ): Promise<[boolean, string]> {
    if (!path) throw new Error('Path cannot be empty or None');
    if (typeof data !== 'object') throw new Error('Data must be a dictionary');
    if (retryCount < 0) throw new Error('Retry count must be non-negative');
    if (retryDelay < 0) throw new Error('Retry delay must be non-negative');

    const jsonData = JSON.stringify(data);
    const upsertQuery = `
      INSERT INTO ${this.baseTable} (path, data)
      VALUES ($1, $2)
      ON CONFLICT (path)
      DO UPDATE SET data = EXCLUDED.data
      RETURNING path, (xmax = 0) AS was_inserted
    `;

    let lastError: any;
    for (let attempt = 0; attempt <= retryCount; attempt++) {
      try {
        const res = await this.client.query(upsertQuery, [path, jsonData]);
        const row = res.rows[0];
        const wasInserted = row.was_inserted;
        const operation = wasInserted ? 'inserted' : 'updated';
        return [true, `Successfully ${operation} data for path: ${row.path}`];
      } catch (e: any) {
        lastError = e;
        if (attempt < retryCount) {
          await this.sleep(retryDelay);
          continue;
        }
      }
    }
    throw new Error(
      `Failed to set status data for path '${path}' after ${retryCount + 1} attempts: ${lastError.message}`
    );
  }

  public async setMultipleStatusData(
    pathDataPairs: Record<string, any> | Array<[string, any]>,
    retryCount = 3,
    retryDelay = 1000
  ): Promise<[boolean, string, Record<string, string>]> {
    const pairs: Array<[string, any]> =
      Array.isArray(pathDataPairs)
        ? pathDataPairs
        : Object.entries(pathDataPairs);
    if (pairs.length === 0) throw new Error('path_data_pairs cannot be empty');
    if (retryCount < 0) throw new Error('Retry count must be non-negative');
    if (retryDelay < 0) throw new Error('Retry delay must be non-negative');

    const jsonPairs = pairs.map(([p, d]) => [p, JSON.stringify(d)]) as [string, string][];
    const upsertQuery = `
      INSERT INTO ${this.baseTable} (path, data)
      VALUES ($1, $2)
      ON CONFLICT (path)
      DO UPDATE SET data = EXCLUDED.data
      RETURNING path, (xmax = 0) AS was_inserted
    `;

    let lastError: any;
    for (let attempt = 0; attempt <= retryCount; attempt++) {
      try {
        await this.client.query('BEGIN');
        const results: Record<string, string> = {};
        for (const [p, jd] of jsonPairs) {
          const res = await this.client.query(upsertQuery, [p, jd]);
          const row = res.rows[0];
          const op = row.was_inserted ? 'inserted' : 'updated';
          results[row.path] = op;
        }
        await this.client.query('COMMIT');
        return [true, `Successfully processed ${jsonPairs.length} records`, results];
      } catch (e: any) {
        lastError = e;
        await this.client.query('ROLLBACK');
        if (attempt < retryCount) {
          await this.sleep(retryDelay);
          continue;
        }
      }
    }
    throw new Error(
      `Failed to set multiple status data after ${retryCount + 1} attempts: ${lastError.message}`
    );
  }
}
