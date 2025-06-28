// ConstructStreamTable.ts
import { Client, QueryResult } from 'pg';
import { ConstructKB } from './ConstructKB';

export class ConstructStreamTable {
  private client: Client;
  private constructKb: ConstructKB;
  private database: string;
  private tableName: string;

  constructor(client: Client, constructKb: ConstructKB, database: string) {
    this.client = client;
    this.constructKb = constructKb;
    this.database = database;
    this.tableName = `${database}_stream`;
  }

  /** Helper to quote identifiers safely */
  private ident(id: string): string {
    return `"${id.replace(/"/g, '""')}"`;
  }

  /** Initializes schema: drops and recreates stream table and indexes */
  async setupSchema(): Promise<void> {
    const t = this.ident(this.tableName);
    // Drop table if exists
    await this.client.query(`DROP TABLE IF EXISTS ${t} CASCADE;`);
    // Enable ltree extension
    await this.client.query(`CREATE EXTENSION IF NOT EXISTS ltree;`);
    // Drop again (mirrors Python logic)
    await this.client.query(`DROP TABLE IF EXISTS ${t} CASCADE;`);

    // Create table
    await this.client.query(
      `CREATE TABLE ${t}(
         id SERIAL PRIMARY KEY,
         path LTREE,
         recorded_at TIMESTAMPTZ DEFAULT NOW(),
         valid BOOLEAN DEFAULT FALSE,
         data JSONB
       );`
    );

    // Create indexes
    const indexes = [
      [`idx_${this.tableName}_path_gist`, `USING GIST (path)`],
      [`idx_${this.tableName}_path_btree`, `(path)`],
      [`idx_${this.tableName}_recorded_at`, `(recorded_at)`],
      [`idx_${this.tableName}_recorded_at_desc`, `(recorded_at DESC)`],
      [`idx_${this.tableName}_path_recorded_at`, `(path, recorded_at)`],
    ];

    for (const [idxName, clause] of indexes) {
      const idx = this.ident(idxName);
      await this.client.query(
        `CREATE INDEX IF NOT EXISTS ${idx} ON ${t} ${clause};`
      );
    }
  }

  /**
   * Adds a stream field via the ConstructKB instance and returns a summary.
   */
  async addStreamField(
    streamKey: string,
    streamLength: number,
    description: string,
  ): Promise<{
    stream: string;
    message: string;
    properties: { stream_length: number };
    data: string;
  }> {
    if (typeof streamKey !== 'string') {
      throw new TypeError('streamKey must be a string');
    }
    if (!Number.isInteger(streamLength)) {
      throw new TypeError('streamLength must be an integer');
    }

    const properties = { stream_length: streamLength };
    // Delegate to ConstructKB
    await this.constructKb.addInfoNode(
      'KB_STREAM_FIELD',
      streamKey,
      properties,
      {},
      description,
    );

    return {
      stream: 'success',
      message: `stream field '${streamKey}' added successfully`,
      properties,
      data: description,
    };
  }

  /**
   * Removes entries whose paths are in invalidStreamPaths, in chunks.
   */
  private async removeInvalidStreamFields(
    invalidStreamPaths: string[],
    chunkSize = 500,
  ): Promise<void> {
    if (invalidStreamPaths.length === 0) return;

    const t = this.ident(this.tableName);
    for (let i = 0; i < invalidStreamPaths.length; i += chunkSize) {
      const chunk = invalidStreamPaths.slice(i, i + chunkSize);
      const placeholders = chunk.map((_, idx) => `$${idx + 1}`).join(', ');
      const sql = `DELETE FROM ${t} WHERE path IN (${placeholders});`;
      await this.client.query(sql, chunk);
    }
  }

  /**
   * Ensures each path has exactly the target number of rows: deletes oldest or inserts new.
   */
  private async manageStreamTable(
    paths: string[],
    lengths: number[],
  ): Promise<void> {
    const t = this.ident(this.tableName);

    for (let i = 0; i < paths.length; i++) {
      const path = paths[i];
      const target = lengths[i];

      // count current rows
      const countRes: QueryResult = await this.client.query(
        `SELECT COUNT(*) AS cnt FROM ${t} WHERE path = $1;`,
        [path],
      );
      const current = parseInt((countRes.rows[0].cnt as unknown as string), 10);
      const diff = target - current;

      if (diff < 0) {
        // delete oldest
        const delSql = `
          DELETE FROM ${t}
          WHERE path = $1
            AND recorded_at IN (
              SELECT recorded_at FROM ${t}
              WHERE path = $1
              ORDER BY recorded_at ASC
              LIMIT $2
            );
        `;
        await this.client.query(delSql, [path, Math.abs(diff)]);
      } else if (diff > 0) {
        // insert placeholders
        const insSql = `INSERT INTO ${t} (path, recorded_at, data, valid)
                        VALUES ($1, NOW(), $2, FALSE);`;
        for (let j = 0; j < diff; j++) {
          await this.client.query(insSql, [path, {}]);
        }
      }
    }
  }

  /**
   * Synchronize stream table with KB entries: remove invalid, add missing.
   */
  async checkInstallation(): Promise<void> {
    const t = this.ident(this.tableName);

    // fetch existing stream paths
    const spRes = await this.client.query(
      `SELECT DISTINCT path::text AS path FROM ${t};`
    );
    const existingPaths = spRes.rows.map(r => r.path as string);

    // fetch KB-defined stream fields
    const kbTable = this.ident(this.database);
    const kbRes = await this.client.query(
      `SELECT path, properties FROM ${kbTable}
       WHERE label = 'KB_STREAM_FIELD';`
    );
    const definedPaths = kbRes.rows.map(r => r.path as string);
    const definedLengths = kbRes.rows.map(
      r => (r.properties.stream_length as number)
    );

    // compute diffs
    const invalid = existingPaths.filter(p => !definedPaths.includes(p));
    const missing = definedPaths.filter(p => !existingPaths.includes(p));

    await this.removeInvalidStreamFields(invalid);
    await this.manageStreamTable(definedPaths, definedLengths);
  }
}
