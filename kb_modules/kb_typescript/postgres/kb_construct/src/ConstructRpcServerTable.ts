// ConstructRPCServerTable.ts
import { Client, QueryResult } from 'pg';
import { ConstructKB } from './ConstructKB';
import { v4 as uuidv4 } from 'uuid';

export class ConstructRpcServerTable {
  private client: Client;
  private constructKb: ConstructKB;
  private database: string;
  private tableName: string;

  constructor(client: Client, constructKb: ConstructKB, database: string) {
    this.client = client;
    this.constructKb = constructKb;
    this.database = database;
    this.tableName = `${database}_rpc_server`;
  }

  /** Quote SQL identifiers safely */
  private ident(id: string): string {
    return `"${id.replace(/"/g, '""')}"`;
  }

  /** Drop & create RPC server table schema */
  async setupSchema(): Promise<void> {
    const t = this.ident(this.tableName);

    // Drop existing
    await this.client.query(`DROP TABLE IF EXISTS ${t} CASCADE;`);
    
    // Create table
    await this.client.query(
      `CREATE TABLE ${t} (
         id SERIAL PRIMARY KEY,
         server_path LTREE NOT NULL,
         request_id UUID NOT NULL DEFAULT gen_random_uuid(),
         rpc_action TEXT NOT NULL DEFAULT 'none',
         request_payload JSONB NOT NULL,
         request_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         transaction_tag TEXT NOT NULL,
         state TEXT NOT NULL DEFAULT 'empty' CHECK (state IN ('empty','new_job','processing')),
         priority INTEGER NOT NULL DEFAULT 0,
         processing_timestamp TIMESTAMPTZ DEFAULT NULL,
         completed_timestamp TIMESTAMPTZ DEFAULT NULL,
         rpc_client_queue LTREE
       );`
    );

    await this.client.query('COMMIT;');
    console.log('RPC server table created.');
  }

  /** Add a new RPC server field via ConstructKB */
  async addRpcServerField(
    rpcServerKey: string,
    queueDepth: number,
    description: string,
  ): Promise<{ status: string; message: string; properties: { queue_depth: number }; data: string }> {
    if (typeof rpcServerKey !== 'string') {
      throw new TypeError('rpcServerKey must be a string');
    }
    if (!Number.isInteger(queueDepth)) {
      throw new TypeError('queueDepth must be an integer');
    }
    if (typeof description !== 'string') {
      throw new TypeError('description must be a string');
    }

    const properties = { queue_depth: queueDepth };
    const data = {};

    await this.constructKb.addInfoNode(
      'KB_RPC_SERVER_FIELD',
      rpcServerKey,
      properties,
      data,
      description,
    );

    console.log(
      `Added RPC server field '${rpcServerKey}' with properties: ${JSON.stringify(properties)}`
    );

    return {
      status: 'success',
      message: `RPC server field '${rpcServerKey}' added successfully`,
      properties,
      data: description,
    };
  }

  /** Remove entries not in specifiedServerPaths using a temp table */
  async removeUnspecifiedEntries(specifiedServerPaths: string[]): Promise<number> {
    if (!specifiedServerPaths.length) {
      console.warn('No server paths specified. No entries removed.');
      return 0;
    }

    const t = this.ident(this.tableName);
    const temp = 'valid_server_paths';

    // create temp table
    await this.client.query(`CREATE TEMP TABLE IF NOT EXISTS ${temp}(path text);`);
    await this.client.query(`TRUNCATE ${temp};`);

    // insert valid paths
    for (const path of specifiedServerPaths.filter(p => p != null)) {
      await this.client.query(
        `INSERT INTO ${temp}(path) VALUES($1);`,
        [path],
      );
    }

    // set state empty for those in temp
    await this.client.query(
      `UPDATE ${t}
       SET state = 'empty'
       WHERE server_path::text IN (SELECT path FROM ${temp});`
    );

    // delete those not in temp
    const delRes = await this.client.query(
      `DELETE FROM ${t}
       WHERE server_path::text NOT IN (SELECT path FROM ${temp});`
    );
    const deletedCount = delRes.rowCount ?? 0;

    // drop temp
    await this.client.query(`DROP TABLE IF EXISTS ${temp};`);

    console.log(`Removed ${deletedCount} unspecified entries from ${t}`);
    return deletedCount;
  }

  /** Adjust queue lengths per path */
  async adjustQueueLength(
    specifiedServerPaths: string[],
    specifiedQueueLengths: number[],
  ): Promise<Record<string, any>> {
    if (specifiedServerPaths.length !== specifiedQueueLengths.length) {
      throw new Error('Mismatch between paths and lengths');
    }

    const t = this.ident(this.tableName);
    const results: Record<string, any> = {};

    for (let i = 0; i < specifiedServerPaths.length; i++) {
      const path = specifiedServerPaths[i];
      const target = specifiedQueueLengths[i];
      try {
        // count
        const cntRes: QueryResult = await this.client.query(
          `SELECT COUNT(*)::int AS cnt FROM ${t} WHERE server_path::text = $1;`,
          [path],
        );
        const current = cntRes.rows[0].cnt;

        // reset state
        await this.client.query(
          `UPDATE ${t} SET state = 'empty' WHERE server_path::text = $1;`,
          [path],
        );

        if (current > target) {
          // delete oldest
          const delCount = current - target;
          await this.client.query(
            `DELETE FROM ${t}
             WHERE id IN (
               SELECT id FROM ${t}
               WHERE server_path::text = $1
               ORDER BY request_timestamp ASC
               LIMIT $2
             );`,
            [path, delCount],
          );
          results[path] = { action: 'removed', count: delCount, new_total: target };
        } else if (current < target) {
          const addCount = target - current;
          const insSql = `INSERT INTO ${t}
             (server_path, request_payload, transaction_tag, state)
             VALUES ($1, $2, $3, 'empty');`;
          for (let j = 0; j < addCount; j++) {
            await this.client.query(insSql, [
              path,
              {},
              `placeholder_${uuidv4()}`,
            ]);
          }
          results[path] = { action: 'added', count: addCount, new_total: target };
        } else {
          results[path] = { action: 'unchanged', count: 0, new_total: current };
        }
      } catch (err: unknown) {
        console.error(`Error adjusting queue for ${path}:`, err);
        const message = err instanceof Error ? err.message : String(err);
        results[path] = { error: message };
      }
    }

    await this.client.query('COMMIT;');
    return results;
  }

  /** Restore default values for all records */
  async restoreDefaultValues(): Promise<number> {
    const t = this.ident(this.tableName);
    const res = await this.client.query(
      `UPDATE ${t}
       SET
         request_id = gen_random_uuid(),
         rpc_action = 'none',
         request_payload = '{}'::jsonb,
         request_timestamp = NOW(),
         transaction_tag = CONCAT('reset_', gen_random_uuid()::text),
         state = 'empty',
         priority = 0,
         processing_timestamp = NULL,
         completed_timestamp = NULL,
         rpc_client_queue = NULL
       RETURNING id;`
    );
    const count = res.rowCount ?? 0;
    console.log(`Restored default values for ${count} records`);
    return count;
  }

  /** Full installation: sync entries, adjust queue, reset defaults */
  async checkInstallation(): Promise<void> {
    // fetch KB-defined RPC fields
    const kbTable = this.ident(this.database);
    const specRes: QueryResult = await this.client.query(
      `SELECT path, properties FROM ${kbTable}
       WHERE label = 'KB_RPC_SERVER_FIELD';`
    );
    const paths = specRes.rows.map(r => r.path as string);
    const lengths = specRes.rows.map(r => (r.properties.queue_depth as number));

    await this.removeUnspecifiedEntries(paths);
    await this.adjustQueueLength(paths, lengths);
    await this.restoreDefaultValues();
  }
}
