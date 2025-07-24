import { KBSearch } from './KBSearch';
import { Client } from 'pg';
import { v4 as uuidv4 } from 'uuid';

export class KBRpcClient {
  private kbSearch: KBSearch;
  private client: Client;
  private baseTable: string;

  constructor(kbSearch: KBSearch, database: string) {
    this.kbSearch = kbSearch;
    this.client = kbSearch.getConn();
    this.baseTable = `${database}_rpc_client`;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  public async findRpcClientId(
    kb?: string,
    nodeName?: string,
    properties?: Record<string, any>,
    nodePath?: string
  ): Promise<any> {
    const ids = await this.findRpcClientIds(kb, nodeName, properties, nodePath);
    if (ids.length === 0) throw new Error(
      `No node found matching parameters: ${nodeName}, ${JSON.stringify(properties)}, ${nodePath}`
    );
    if (ids.length > 1) throw new Error(
      `Multiple nodes found matching parameters: ${nodeName}, ${JSON.stringify(properties)}, ${nodePath}`
    );
    return ids[0];
  }

  public async findRpcClientIds(
    kb?: string,
    nodeName?: string,
    properties?: Record<string, any>,
    nodePath?: string
  ): Promise<any[]> {
    this.kbSearch.clearFilters();
    this.kbSearch.searchLabel('KB_RPC_CLIENT_FIELD');
    if (kb) this.kbSearch.searchKb(kb);
    if (nodeName) this.kbSearch.searchName(nodeName);
    if (properties) {
      for (const key of Object.keys(properties)) {
        this.kbSearch.searchPropertyValue(key, properties[key]);
      }
    }
    if (nodePath) this.kbSearch.searchPath(nodePath);

    const results = await this.kbSearch.executeQuery();
    if (!results || results.length === 0) throw new Error(
      `No node found matching parameters: ${nodeName}, ${JSON.stringify(properties)}, ${nodePath}`
    );
    return results;
  }

  public findRpcClientKeys(keyData: any[]): string[] {
    return keyData.map(r => r.path);
  }

  public async findFreeSlots(clientPath: string): Promise<number> {
    const query = `
      SELECT COUNT(*) AS total_records,
             COUNT(*) FILTER (WHERE is_new_result = FALSE) AS free_slots
      FROM ${this.baseTable}
      WHERE client_path = $1
    `;
    try {
      const res = await this.client.query(query, [clientPath]);
      const total = parseInt(res.rows[0].total_records, 10);
      const free = parseInt(res.rows[0].free_slots, 10);
      if (total === 0) throw new Error(`No records found for client_path: ${clientPath}`);
      return free;
    } catch (err: any) {
      throw new Error(`Database error when finding free slots: ${err.message}`);
    }
  }

  public async findQueuedSlots(clientPath: string): Promise<number> {
    const query = `
      SELECT COUNT(*) AS total_records,
             COUNT(*) FILTER (WHERE is_new_result = TRUE) AS queued_slots
      FROM ${this.baseTable}
      WHERE client_path = $1
    `;
    try {
      const res = await this.client.query(query, [clientPath]);
      const total = parseInt(res.rows[0].total_records, 10);
      const queued = parseInt(res.rows[0].queued_slots, 10);
      if (total === 0) throw new Error(`No records found for client_path: ${clientPath}`);
      return queued;
    } catch (err: any) {
      throw new Error(`Database error when finding queued slots: ${err.message}`);
    }
  }

  public async peakAndClaimReplyData(
    clientPath: string,
    maxRetries = 3,
    retryDelay = 1000
  ): Promise<Record<string, any>> {
    let attempt = 0;
    const table = this.baseTable;
    while (attempt < maxRetries) {
      try {
        await this.client.query('BEGIN');
        const updateQ = `
          UPDATE ${table}
          SET is_new_result = FALSE
          WHERE id = (
            SELECT id FROM ${table}
            WHERE client_path = $1 AND is_new_result = TRUE
            ORDER BY response_timestamp ASC
            FOR UPDATE SKIP LOCKED LIMIT 1
          )
          RETURNING *
        `;
        const res = await this.client.query(updateQ, [clientPath]);
        if (res.rows.length) {
          await this.client.query('COMMIT');
          return res.rows[0];
        }
        // check existence
        const existQ = `SELECT EXISTS(
          SELECT 1 FROM ${table}
          WHERE client_path = $1 AND is_new_result = TRUE
        ) AS exists`;
        const ex = await this.client.query(existQ, [clientPath]);
        if (!ex.rows[0].exists) {
          await this.client.query('ROLLBACK');
          return null as any;
        }
        await this.client.query('ROLLBACK');
        attempt++;
        await this.sleep(retryDelay);
      } catch (err: any) {
        await this.client.query('ROLLBACK');
        attempt++;
        await this.sleep(retryDelay);
      }
    }
    throw new Error(
      `Could not lock a new-reply row after ${maxRetries} attempts`
    );
  }

  public async clearReplyQueue(
    clientPath: string,
    maxRetries = 3,
    retryDelay = 1000
  ): Promise<number> {
    let attempt = 0;
    const table = this.baseTable;
    while (attempt < maxRetries) {
      try {
        await this.client.query('BEGIN');
        const selectQ = `
          SELECT id FROM ${table}
          WHERE client_path = $1 FOR UPDATE NOWAIT
        `;
        const rows = await this.client.query(selectQ, [clientPath]);
        if (rows.rows.length === 0) {
          await this.client.query('COMMIT');
          return 0;
        }
        let updated = 0;
        for (const r of rows.rows) {
          const upd = `
            UPDATE ${table}
            SET request_id = $1,
                server_path = $2,
                response_payload = $3,
                response_timestamp = NOW(),
                is_new_result = FALSE
            WHERE id = $4
          `;
          const uuid = uuidv4();
          await this.client.query(upd, [uuid, clientPath, JSON.stringify({}), r.id]);
          updated++;
        }
        await this.client.query('COMMIT');
        return updated;
      } catch (err: any) {
        await this.client.query('ROLLBACK');
        attempt++;
        await this.sleep(retryDelay);
      }
    }
    throw new Error(`Could not acquire lock after ${maxRetries} retries`);
  }

  public async pushAndClaimReplyData(
    clientPath: string,
    requestUuid: string,
    serverPath: string,
    rpcAction: string,
    transactionTag: string,
    replyData: any,
    maxRetries = 3,
    retryDelay = 1000
  ): Promise<void> {
    let attempt = 0;
    const table = this.baseTable;
    while (attempt <= maxRetries) {
      try {
        await this.client.query('BEGIN');
        const upsert = `
          WITH candidate AS (
            SELECT id FROM ${table}
            WHERE client_path = $1 AND is_new_result = FALSE
            ORDER BY response_timestamp ASC
            FOR UPDATE SKIP LOCKED LIMIT 1
          )
          UPDATE ${table}
          SET request_id = $2, server_path = $3, rpc_action = $4,
              transaction_tag = $5, response_payload = $6,
              is_new_result = TRUE, response_timestamp = CURRENT_TIMESTAMP
          FROM candidate
          WHERE ${table}.id = candidate.id
          RETURNING ${table}.id;
        `;
        const res = await this.client.query(upsert, [
          clientPath,
          requestUuid,
          serverPath,
          rpcAction,
          transactionTag,
          JSON.stringify(replyData)
        ]);
        if (!res.rows.length) {
          await this.client.query('ROLLBACK');
          throw new Error('No available record found');
        }
        await this.client.query('COMMIT');
        return;
      } catch (err: any) {
        await this.client.query('ROLLBACK');
        attempt++;
        if (attempt > maxRetries) throw err;
        await this.sleep(retryDelay);
      }
    }
  }

  public async listWaitingJobs(clientPath?: string): Promise<any[]> {
    const table = this.baseTable;
    let query = `
      SELECT id, request_id, client_path, server_path,
             response_payload, response_timestamp, is_new_result
      FROM ${table}
      WHERE is_new_result = TRUE
    `;
    const params: any[] = [];
    if (clientPath) {
      query += ` AND client_path = $1`;
      params.push(clientPath);
    }
    query += ` ORDER BY response_timestamp ASC`;

    try {
      const res = await this.client.query(query, params);
      return res.rows.map(row => {
        if (row.request_id) row.request_id = String(row.request_id);
        if (row.response_timestamp instanceof Date)
          row.response_timestamp = row.response_timestamp.toISOString();
        return row;
      });
    } catch (err: any) {
      throw new Error(`Database error when listing waiting jobs: ${err.message}`);
    }
  }
}
