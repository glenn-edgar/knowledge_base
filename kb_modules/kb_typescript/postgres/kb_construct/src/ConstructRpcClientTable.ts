// ConstructRPCClientTable.ts
import { Client, QueryResult } from 'pg';
import { ConstructKB } from './ConstructKB';
import { v4 as uuidv4 } from 'uuid';

export class ConstructRpcClientTable {
  private client: Client;
  private constructKb: ConstructKB;
  private database: string;
  private tableName: string;

  constructor(client: Client, constructKb: ConstructKB, database: string) {
    this.client = client;
    this.constructKb = constructKb;
    this.database = database;
    this.tableName = `${database}_rpc_client`;
  }

  /** Safely quote SQL identifiers */
  private ident(id: string): string {
    return `"${id.replace(/"/g, '""')}"`;
  }

  /** Drop & create RPC client table schema */
  async setupSchema(): Promise<void> {
    const t = this.ident(this.tableName);

    await this.client.query(`DROP TABLE IF EXISTS ${t} CASCADE;`);
    await this.client.query(`CREATE EXTENSION IF NOT EXISTS ltree;`);
    await this.client.query(
      `CREATE TABLE ${t} (
         id SERIAL PRIMARY KEY,
         request_id UUID NOT NULL,
         client_path LTREE NOT NULL,
         server_path LTREE NOT NULL,
         transaction_tag TEXT NOT NULL DEFAULT 'none',
         rpc_action TEXT NOT NULL DEFAULT 'none',
         response_payload JSONB NOT NULL,
         response_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         is_new_result BOOLEAN NOT NULL DEFAULT FALSE
       );`
    );
    console.log('RPC client table created.');
  }

  /** Add a new RPC client field via ConstructKB */
  async addRpcClientField(
    rpcClientKey: string,
    queueDepth: number,
    description: string,
  ): Promise<{ rpc_client: string; message: string; properties: { queue_depth: number }; data: string }> {
    if (typeof rpcClientKey !== 'string') {
      throw new TypeError('rpcClientKey must be a string');
    }
    if (!Number.isInteger(queueDepth)) {
      throw new TypeError('queueDepth must be an integer');
    }
    if (typeof description !== 'string') {
      throw new TypeError('description must be a string');
    }

    const properties = { queue_depth: queueDepth };
    await this.constructKb.addInfoNode(
      'KB_RPC_CLIENT_FIELD',
      rpcClientKey,
      properties,
      {},
      description,
    );
    console.log(`Added RPC client field '${rpcClientKey}' with properties: ${JSON.stringify(properties)}`);

    return {
      rpc_client: 'success',
      message: `RPC client field '${rpcClientKey}' added successfully`,
      properties,
      data: description,
    };
  }

  /** Remove entries not in specifiedClientPaths using a temp table */
  async removeUnspecifiedEntries(specifiedClientPaths: string[]): Promise<number> {
    if (specifiedClientPaths.length === 0) {
      console.warn('No client paths specified. No entries removed.');
      return 0;
    }
    const t = this.ident(this.tableName);
    const temp = 'valid_client_paths';

    await this.client.query(`CREATE TEMP TABLE IF NOT EXISTS ${temp}(path text);`);
    await this.client.query(`TRUNCATE ${temp};`);

    for (const p of specifiedClientPaths.filter(p => p != null)) {
      await this.client.query(`INSERT INTO ${temp}(path) VALUES($1);`, [String(p)]);
    }

    const delRes = await this.client.query(
      `DELETE FROM ${t} WHERE client_path::text NOT IN (SELECT path FROM ${temp});`
    );
    const deletedCount = delRes.rowCount ?? 0;
    await this.client.query(`DROP TABLE IF EXISTS ${temp};`);

    console.log(`Removed ${deletedCount} unspecified entries from ${t}`);
    return deletedCount;
  }

  /** Adjust queue lengths per client_path */
  async adjustQueueLength(
    specifiedClientPaths: string[],
    specifiedQueueLengths: number[],
  ): Promise<Record<string, any>> {
    if (specifiedClientPaths.length !== specifiedQueueLengths.length) {
      throw new Error('Paths and queue lengths arrays must match');
    }
    const t = this.ident(this.tableName);
    const results: Record<string, any> = {};

    for (let i = 0; i < specifiedClientPaths.length; i++) {
      const path = specifiedClientPaths[i];
      const target = specifiedQueueLengths[i];
      if (target < 0) {
        results[path] = { error: 'Invalid queue length (negative)' };
        continue;
      }
      // Count current records
      const cntRes: QueryResult = await this.client.query(
        `SELECT COUNT(*)::int AS cnt FROM ${t} WHERE client_path::text = $1;`,
        [path],
      );
      const current = cntRes.rows[0].cnt;
      const pathResult = { added: 0, removed: 0 };

      if (current > target) {
        const toRemove = current - target;
        const delRes = await this.client.query(
          `DELETE FROM ${t}
           WHERE id IN (
             SELECT id FROM ${t}
             WHERE client_path::text = $1
             ORDER BY response_timestamp ASC
             LIMIT $2
           );`,
          [path, toRemove],
        );
        pathResult.removed = delRes.rowCount ?? toRemove;
      } else if (current < target) {
        const toAdd = target - current;
        const insSql = `INSERT INTO ${t}
           (request_id, client_path, server_path, transaction_tag, rpc_action, response_payload, response_timestamp, is_new_result)
           VALUES ($1, $2::ltree, $3::ltree, $4, $5, $6::jsonb, NOW(), FALSE);`;
        for (let j = 0; j < toAdd; j++) {
          await this.client.query(insSql, [
            uuidv4(),
            path,
            path,
            'none',
            'none',
            JSON.stringify({}),
          ]);
          pathResult.added++;
        }
      }
      results[path] = pathResult;
    }
    return results;
  }

  /** Restore default values for all records */
  async restoreDefaultValues(): Promise<number> {
    const t = this.ident(this.tableName);
    const res = await this.client.query(
      `UPDATE ${t}
       SET
         request_id = gen_random_uuid(),
         server_path = client_path,
         transaction_tag = 'none',
         rpc_action = 'none',
         response_payload = '{}'::jsonb,
         response_timestamp = NOW(),
         is_new_result = FALSE
       RETURNING id;`
    );
    const count = res.rowCount ?? 0;
    console.log(`Restored default values for ${count} records`);
    return count;
  }

  /** Full installation: sync entries and reset defaults */
  async checkInstallation(): Promise<void> {
    const kbT = this.ident(this.database);
    const specRes: QueryResult = await this.client.query(
      `SELECT path, properties FROM ${kbT} WHERE label = 'KB_RPC_CLIENT_FIELD';`
    );
    const paths = specRes.rows.map(r => r.path as string);
    const lengths = specRes.rows.map(r => (r.properties.queue_depth as number));

    await this.removeUnspecifiedEntries(paths);
    await this.adjustQueueLength(paths, lengths);
    await this.restoreDefaultValues();
  }
}
