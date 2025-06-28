// ConstructStatusTable.ts
import { Client, QueryResult } from 'pg';
import { ConstructKB } from './ConstructKB';

export class ConstructStatusTable {
  private client: Client;
  private constructKb: ConstructKB;
  private database: string;
  private tableName: string;

  constructor(client: Client, constructKb: ConstructKB, database: string) {
    this.client = client;
    this.constructKb = constructKb;
    this.database = database;
    this.tableName = `${database}_status`;
  }

  /**
   * Safely quote SQL identifiers
   */
  private ident(id: string): string {
    return `"${id.replace(/"/g, '""')}"`;
  }

  /**
   * Initialize schema: create ltree extension, drop and recreate status table and indexes
   */
  async setupSchema(): Promise<void> {
    const t = this.ident(this.tableName);

    // Enable ltree extension
    await this.client.query(`CREATE EXTENSION IF NOT EXISTS ltree;`);

    // Drop existing table
    await this.client.query(`DROP TABLE IF EXISTS ${t} CASCADE;`);

    // Create status table
    await this.client.query(
      `CREATE TABLE ${t} (
         id SERIAL PRIMARY KEY,
         data JSONB,
         path LTREE UNIQUE
       );`
    );

    // Create GIST index on path
    await this.client.query(
      `CREATE INDEX IF NOT EXISTS ${this.ident(
        `idx_${this.tableName}_path_gist`
      )} ON ${t} USING GIST (path);`
    );

    // Create B-tree index on path
    await this.client.query(
      `CREATE INDEX IF NOT EXISTS ${this.ident(
        `idx_${this.tableName}_path_btree`
      )} ON ${t} (path);`
    );
  }

  /**
   * Adds a status field via the ConstructKB instance
   */
  async addStatusField(
    statusKey: string,
    properties: Record<string, any> | null,
    description: string,
    initialData: Record<string, any>,
  ): Promise<{
    status: string;
    message: string;
    properties: Record<string, any>;
    data: Record<string, any>;
  }> {
    if (typeof statusKey !== 'string') {
      throw new TypeError('statusKey must be a string');
    }
    if (typeof description !== 'string') {
      throw new TypeError('description must be a string');
    }
    if (typeof initialData !== 'object' || Array.isArray(initialData)) {
      throw new TypeError('initialData must be a dictionary');
    }

    const props = properties ?? {};
    if (typeof props !== 'object' || Array.isArray(props)) {
      throw new TypeError('properties must be a dictionary');
    }

    // Delegate to ConstructKB
    await this.constructKb.addInfoNode(
      'KB_STATUS_FIELD',
      statusKey,
      props,
      initialData,
      description,
    );

    return {
      status: 'success',
      message: `Status field '${statusKey}' added successfully`,
      properties: props,
      data: initialData,
    };
  }

  /**
   * Synchronize the status table with KB entries
   */
  async checkInstallation(): Promise<{
    missingPathsAdded: number;
    notSpecifiedPathsRemoved: number;
  }> {
    const t = this.ident(this.tableName);
    const kbT = this.ident(this.database);

    // Fetch all paths from status table
    const allRes: QueryResult = await this.client.query(
      `SELECT path FROM ${t};`
    );
    const allPaths = allRes.rows.map(r => r.path as string);

    // Fetch specified paths from KB table
    const specRes: QueryResult = await this.client.query(
      `SELECT path FROM ${kbT} WHERE label = 'KB_STATUS_FIELD';`
    );
    const specifiedPaths = specRes.rows.map(r => r.path as string);

    // Determine differences
    const missing = specifiedPaths.filter(p => !allPaths.includes(p));
    const notSpecified = allPaths.filter(p => !specifiedPaths.includes(p));

    // Remove not specified paths
    for (const path of notSpecified) {
      await this.client.query(
        `DELETE FROM ${t} WHERE path = $1;`,
        [path],
      );
    }

    // Add missing paths
    for (const path of missing) {
      await this.client.query(
        `INSERT INTO ${t}(data, path) VALUES($1, $2);`,
        [{}, path],
      );
    }

    return {
      missingPathsAdded: missing.length,
      notSpecifiedPathsRemoved: notSpecified.length,
    };
  }
}
