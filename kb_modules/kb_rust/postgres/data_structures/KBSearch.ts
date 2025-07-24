import { Client } from 'pg';

interface Filter {
  condition: string;   // uses a single ‘#’ placeholder for each param
  params: any[];
}

export class KBSearch {
  public path: string[] = [];
  public host: string;
  public port: number;
  public dbName: string;
  public user: string;
  public password: string;
  public baseTable: string;
  public linkTable: string;
  public linkMountTable: string;
  public filters: Filter[] = [];
  public results: any[] | null = null;
  public pathValues: Record<string, any> = {};
  public client: Client | null = null;

  constructor(
    host: string,
    port: number,
    dbName: string,
    user: string,
    password: string,
    baseTable: string
  ) {
    this.host = host;
    this.port = port;
    this.dbName = dbName;
    this.user = user;
    this.password = password;
    this.baseTable = baseTable;
    this.linkTable = `${baseTable}_link`;
    this.linkMountTable = `${baseTable}_link_mount`;
    // fire-and-forget connect; errors will still bubble
    this._connect().catch(err => {
      console.error('Error connecting to database:', err);
      throw err;
    });
  }

  private async _connect(): Promise<void> {
    this.client = new Client({
      host: this.host,
      port: this.port,
      database: this.dbName,
      user: this.user,
      password: this.password,
    });
    await this.client.connect();
  }

  public async disconnect(): Promise<void> {
    if (this.client) {
      await this.client.end();
      this.client = null;
    }
  }

  /**
   * Throws if not connected.
   */
  public getConn(): Client {
    if (!this.client) {
      throw new Error('Not connected to database. Call _connect() first.');
    }
    return this.client;
  }

  public clearFilters(): void {
    this.filters = [];
    this.results = null;
  }

  public searchKb(kb: string): void {
    this.filters.push({ condition: 'knowledge_base = #', params: [kb] });
  }

  public searchLabel(label: string): void {
    this.filters.push({ condition: 'label = #', params: [label] });
  }

  public searchName(name: string): void {
    this.filters.push({ condition: 'name = #', params: [name] });
  }

  public searchPropertyKey(key: string): void {
    // JSONB ? operator
    this.filters.push({ condition: 'properties::jsonb ? #', params: [key] });
  }

  public searchPropertyValue(key: string, value: any): void {
    const jsonObject = { [key]: value };
    this.filters.push({
      condition: 'properties::jsonb @> #::jsonb',
      params: [JSON.stringify(jsonObject)],
    });
  }

  public searchStartingPath(startingPath: string): void {
    this.filters.push({ condition: 'path <@ #', params: [startingPath] });
  }

  public searchPath(pathExpr: string): void {
    this.filters.push({ condition: 'path ~ #', params: [pathExpr] });
  }

  public searchHasLink(): void {
    this.filters.push({ condition: 'has_link = TRUE', params: [] });
  }

  public searchHasLinkMount(): void {
    this.filters.push({ condition: 'has_link_mount = TRUE', params: [] });
  }

  /**
   * Builds and runs a WITH‐CTE chain of filters, returning all columns.
   */
  public async executeQuery(): Promise<any[]> {
    const client = this.getConn();
    const columnStr = '*';

    // no filters → simple SELECT
    if (this.filters.length === 0) {
      const res = await client.query(
        `SELECT ${columnStr} FROM ${this.baseTable}`
      );
      this.results = res.rows;
      return this.results;
    }

    // build CTEs
    const cteParts: string[] = [
      `base_data AS (SELECT ${columnStr} FROM ${this.baseTable})`,
    ];
    const combinedParams: any[] = [];
    let paramCount = 0;

    this.filters.forEach((filt, i) => {
      let cond = filt.condition;
      // for each param, replace one ‘#’ with $<n>
      filt.params.forEach(p => {
        paramCount++;
        const placeholder = `$${paramCount}`;
        cond = cond.replace('#', placeholder);
        combinedParams.push(p);
      });
      const cteName = `filter_${i}`;
      const prev = i === 0 ? 'base_data' : `filter_${i - 1}`;
      cteParts.push(
        `${cteName} AS (SELECT ${columnStr} FROM ${prev} WHERE ${cond})`
      );
    });

    const finalQuery = `WITH ${cteParts.join(
      ',\n'
    )}\nSELECT ${columnStr} FROM filter_${this.filters.length - 1}`;

    try {
      const res = await client.query(finalQuery, combinedParams);
      this.results = res.rows;
      return this.results;
    } catch (err) {
      console.error('Error executing query:', err);
      console.error('Query:', finalQuery);
      console.error('Parameters:', combinedParams);
      throw err;
    }
  }

  public findPathValues(keyData: any[] | any): string[] {
    if (!keyData) return [];
    const rows = Array.isArray(keyData) ? keyData : [keyData];
    return rows.map(r => r.path);
  }

  public getResults(): any[] {
    return this.results || [];
  }

  public findDescription(keyData: any[] | any): { [path: string]: string }[] {
    const rows = Array.isArray(keyData) ? keyData : [keyData];
    return rows.map(r => {
      const props = r.properties || {};
      return { [r.path]: props.description || '' };
    });
  }

  /**
   * Fetches `data` for one or many paths in a single query.
   */
  public async findDescriptionPaths(
    pathArray: string | string[]
  ): Promise<Record<string, any>> {
    const client = this.getConn();
    const paths = Array.isArray(pathArray) ? pathArray : [pathArray];
    if (paths.length === 0) return {};

    let query: string;
    let params: any[];
    if (paths.length === 1) {
      query = `SELECT path, data FROM ${this.baseTable} WHERE path = $1`;
      params = [paths[0]];
    } else {
      const placeholders = paths.map((_, i) => `$${i + 1}`).join(', ');
      query = `SELECT path, data FROM ${this.baseTable} WHERE path IN (${placeholders})`;
      params = paths;
    }

    try {
      const res = await client.query(query, params);
      const output: Record<string, any> = {};
      res.rows.forEach(r => {
        output[r.path] = r.data;
      });
      // fill missing
      paths.forEach(p => {
        if (!(p in output)) output[p] = null;
      });
      return output;
    } catch (err) {
      throw new Error(`Error retrieving data for paths: ${err}`);
    }
  }

  /**
   * Splits a link‐encoded LTREE into [kbName, [[link, name], …]].
   */
  public decodeLinkNodes(path: string): [string, [string, string][]] {
    if (!path) throw new Error('Path must be a non-empty string');
    const parts = path.split('.');
    if (parts.length < 3) {
      throw new Error(
        `Path must have at least 3 elements (kb.link.name), got ${parts.length}`
      );
    }
    const rem = parts.length - 1;
    if (rem % 2 !== 0) {
      throw new Error(
        `After kb identifier, must have even number of elements (link/name pairs), got ${rem}`
      );
    }
    const kb = parts[0];
    const pairs: [string, string][] = [];
    for (let i = 1; i < parts.length; i += 2) {
      pairs.push([parts[i], parts[i + 1]]);
    }
    return [kb, pairs];
  }
}
