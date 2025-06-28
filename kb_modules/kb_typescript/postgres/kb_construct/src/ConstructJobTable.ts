// ConstructJobTable.ts
import { Client, QueryResult } from 'pg';
import { ConstructKB } from './ConstructKB';

export class ConstructJobTable {
  private client: Client;
  private constructKb: ConstructKB;
  private database: string;
  private tableName: string;

  constructor(client: Client, constructKb: ConstructKB, database: string) {
    this.client = client;
    this.constructKb = constructKb;
    this.database = database;
    this.tableName = `${database}_job`;
  }

  /** Safely quote SQL identifiers */
  private ident(id: string): string {
    return `"${id.replace(/"/g, '""')}"`;
  }

  /** Drop & create the job table and indexes */
  async setupSchema(): Promise<void> {
    const t = this.ident(this.tableName);

    // Enable ltree extension and drop existing table
    await this.client.query(`CREATE EXTENSION IF NOT EXISTS ltree;`);
    await this.client.query(`DROP TABLE IF EXISTS ${t} CASCADE;`);

    // Create the job table
    await this.client.query(`
      CREATE TABLE ${t}(
        id SERIAL PRIMARY KEY,
        path LTREE,
        schedule_at TIMESTAMPTZ DEFAULT NOW(),
        started_at TIMESTAMPTZ DEFAULT NOW(),
        completed_at TIMESTAMPTZ DEFAULT NOW(),
        is_active BOOLEAN DEFAULT FALSE,
        valid BOOLEAN DEFAULT FALSE,
        data JSONB
      );
    `);

    // Define and create indexes
    const indexes: [string, string][] = [
      [`idx_${this.tableName}_path_gist`, `USING GIST (path)`],
      [`idx_${this.tableName}_path_btree`, `(path)`],
      [`idx_${this.tableName}_schedule_at`, `(schedule_at)`],
      [`idx_${this.tableName}_is_active`, `(is_active)`],
      [`idx_${this.tableName}_valid`, `(valid)`],
      [`idx_${this.tableName}_active_schedule`, `(is_active, schedule_at)`],
      [`idx_${this.tableName}_started_at`, `(started_at)`],
      [`idx_${this.tableName}_completed_at`, `(completed_at)`],
    ];

    for (const [name, clause] of indexes) {
      const idx = this.ident(name);
      await this.client.query(`CREATE INDEX IF NOT EXISTS ${idx} ON ${t} ${clause};`);
    }

    console.log(`Job table '${this.tableName}' created with optimized indexes.`);
  }

  /** Add a new job field to the knowledge base */
  async addJobField(
    jobKey: string,
    jobLength: number,
    description: string
  ): Promise<{ job: string; message: string; properties: { job_length: number }; data: string }> {
    if (typeof jobKey !== 'string') throw new TypeError('jobKey must be a string');
    if (!Number.isInteger(jobLength)) throw new TypeError('jobLength must be an integer');
    if (typeof description !== 'string') throw new TypeError('description must be a string');

    const properties = { job_length: jobLength };
    await this.constructKb.addInfoNode(
      'KB_JOB_QUEUE',
      jobKey,
      properties,
      {},
      description
    );

    console.log(`Added job field '${jobKey}' with properties: ${JSON.stringify(properties)}`);
    return {
      job: 'success',
      message: `job field '${jobKey}' added successfully`,
      properties,
      data: description,
    };
  }

  /** Remove entries not in specifiedJobPaths in chunks */
  private async removeInvalidJobFields(
    invalidJobPaths: string[],
    chunkSize = 500
  ): Promise<void> {
    if (invalidJobPaths.length === 0) return;
    const t = this.ident(this.tableName);

    for (let i = 0; i < invalidJobPaths.length; i += chunkSize) {
      const chunk = invalidJobPaths.slice(i, i + chunkSize);
      const placeholders = chunk.map((_, j) => `$${j + 1}`).join(', ');
      const sql = `DELETE FROM ${t} WHERE path IN (${placeholders});`;
      await this.client.query(sql, chunk);
    }
  }

  /** Ensure each path has exactly the target number of records */
  private async manageJobTable(
    specifiedJobPaths: string[],
    specifiedJobLengths: number[]
  ): Promise<void> {
    const t = this.ident(this.tableName);

    for (let i = 0; i < specifiedJobPaths.length; i++) {
      const path = specifiedJobPaths[i];
      const target = specifiedJobLengths[i];

      // Count current records
      const cntRes: QueryResult = await this.client.query(
        `SELECT COUNT(*)::int AS cnt FROM ${t} WHERE path = $1;`,
        [path]
      );
      const current = cntRes.rows[0].cnt;
      const diff = target - current;

      if (diff < 0) {
        await this.client.query(
          `DELETE FROM ${t}
           WHERE path = $1 AND completed_at IN (
             SELECT completed_at FROM ${t}
             WHERE path = $1
             ORDER BY completed_at ASC
             LIMIT $2
           );`,
          [path, Math.abs(diff)]
        );
      } else if (diff > 0) {
        for (let j = 0; j < diff; j++) {
          await this.client.query(
            `INSERT INTO ${t}(path, data) VALUES($1, $2);`,
            [path, null]
          );
        }
      }
    }
  }

  /** Synchronize the job table with knowledge base definitions */
  async checkInstallation(): Promise<void> {
    const t = this.ident(this.tableName);
    // Fetch existing paths
    const pathsRes: QueryResult = await this.client.query(
      `SELECT DISTINCT path::text AS path FROM ${t};`
    );
    const uniquePaths = pathsRes.rows.map(r => r.path as string);

    // Fetch KB‐defined job queues
    const kbT = this.ident(this.database);
    const specRes: QueryResult = await this.client.query(
      `SELECT path, properties FROM ${kbT} WHERE label = 'KB_JOB_QUEUE';`
    );
    const specifiedPaths = specRes.rows.map(r => r.path as string);
    const specifiedLengths = specRes.rows.map(r => (r.properties.job_length as number));

    // Remove invalid and adjust counts
    const invalid = uniquePaths.filter(p => !specifiedPaths.includes(p));
    await this.removeInvalidJobFields(invalid);
    await this.manageJobTable(specifiedPaths, specifiedLengths);

    console.log('Job table management completed.');
  }
}

