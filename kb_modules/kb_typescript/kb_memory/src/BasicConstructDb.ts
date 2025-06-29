import { Pool, PoolClient } from 'pg';

/**
 * TypeScript translation of Python BasicConstructDB class
 */

export interface TreeNode {
  path: string;
  data: any;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface QueryResult {
  path: string;
  data: any;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface TreeStats {
  total_nodes: number;
  max_depth: number;
  avg_depth: number;
  root_nodes: number;
  leaf_nodes: number;
}

export interface SyncStats {
  imported: number;
  exported: number;
}

export interface ConnectionParams {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export class BasicConstructDB {
  public data: Map<string, TreeNode> = new Map();
  public kbDict: Map<string, { description: string }> = new Map();
  public pool: Pool;
  public tableName: string;
  public connectionParams: ConnectionParams;

  constructor(
    host: string,
    port: number,
    dbname: string,
    user: string,
    password: string,
    tableName: string
  ) {
    this.tableName = tableName;
    this.connectionParams = {
      host,
      port,
      database: dbname,
      user,
      password
    };
    this.pool = new Pool(this.connectionParams);
  }

  public addKb(kbName: string, description: string = ""): void {
    if (this.kbDict.has(kbName)) {
      throw new Error(`Knowledge base ${kbName} already exists`);
    }
    this.kbDict.set(kbName, { description });
  }

  private validatePath(path: string): boolean {
    if (!path) return false;
    const pattern = /^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/;
    if (!pattern.test(path)) return false;
    return path.split('.').every(label => label.length >= 1 && label.length <= 256);
  }

  private pathDepth(path: string): number {
    return path.split('.').length;
  }

  private pathLabels(path: string): string[] {
    return path.split('.');
  }

  private subpath(path: string, start: number, length?: number): string {
    const labels = this.pathLabels(path);
    if (start < 0) start = labels.length + start;
    return length === undefined
      ? labels.slice(start).join('.')
      : labels.slice(start, start + length).join('.');
  }

  private escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&');
  }

  private convertLtreeQueryToRegex(query: string): string {
    if (query.includes('@') && !query.startsWith('@') && !query.endsWith('@')) {
      return this.convertSimplePattern(query.replace(/@/g, '.'));
    }
    return this.convertLqueryPattern(query);
  }

  private convertLqueryPattern(pattern: string): string {
    let result = this.escapeRegex(pattern);
    result = result.replace(/\\\*\\\{(\d+),(\d+)\\\}/g, (_, n, m) =>
      `([^.]+\\.){${n},${m}}`
    );
    result = result.replace(/\\\*\\\{(\d+),\\\}/g, (_, n) =>
      `([^.]+\\.){${n},}`
    );
    result = result.replace(/\\\*\\\{,(\d+)\\\}/g, (_, m) =>
      `([^.]+\\.){0,${m}}`
    );
    result = result.replace(/\\\*\\\{(\d+)\\\}/g, (_, n) =>
      `([^.]+\\.){${n}}`
    );
    result = result.replace(/\\\*\\\*/g, '.*');
    result = result.replace(/\\\*/g, '[^.]+');
    result = result.replace(/\\\{([^}]+)\\\}/g, (_, grp: string) =>
      `(${grp.split(',').join('|')})`
    );
    result = result.replace(/\\\.\)\{([^}]+)\}/g, '){$1}[^.]*');
    return `^${result}$`;
  }

  private convertSimplePattern(pattern: string): string {
    const parts = pattern.split('.*');
    const escapedParts = parts.map(part => this.escapeRegex(part));
    let result = escapedParts.join('.*');
    result = result.replace(/\\\*\\\*/g, '.*');
    result = result.replace(/\\\*/g, '[^.]+');
    result = result.replace(/\\\{([^}]+)\\\}/g, (_, grp: string) =>
      `(${grp.split(',').join('|')})`
    );
    return `^${result}$`;
  }

  public ltreeMatch(path: string, query: string): boolean {
    try {
      const regexPattern = this.convertLtreeQueryToRegex(query);
      return new RegExp(regexPattern).test(path);
    } catch {
      return false;
    }
  }

  public ltxtqueryMatch(path: string, ltxtquery: string): boolean {
    const pathWords = new Set(path.split('.'));
    let query = ltxtquery.trim();
    if (!query.includes('&') && !query.includes('|') && !query.includes('!')) {
      return pathWords.has(query);
    }
    query = query.replace(/&/g, ' && ')
                 .replace(/\|/g, ' || ')
                 .replace(/!/g, ' !');
    const wordRegex = /\b\w+\b/g;
    const words = ltxtquery.match(wordRegex) || [];
    for (const word of words) {
      if (!['and','or','not'].includes(word.toLowerCase())) {
        const regex = new RegExp(`\\b${this.escapeRegex(word)}\\b`, 'g');
        query = query.replace(regex, pathWords.has(word) ? 'true' : 'false');
      }
    }
    try {
      return new Function(`return ${query}`)();
    } catch {
      return false;
    }
  }

  public ltreeAncestor(ancestor: string, descendant: string): boolean {
    if (ancestor === descendant) return false;
    return descendant.startsWith(ancestor + '.');
  }

  public ltreeDescendant(descendant: string, ancestor: string): boolean {
    return this.ltreeAncestor(ancestor, descendant);
  }

  public ltreeAncestorOrEqual(ancestor: string, descendant: string): boolean {
    return ancestor === descendant || this.ltreeAncestor(ancestor, descendant);
  }

  public ltreeDescendantOrEqual(descendant: string, ancestor: string): boolean {
    return descendant === ancestor || this.ltreeDescendant(descendant, ancestor);
  }

  public ltreeConcatenate(path1: string, path2: string): string {
    if (!path1) return path2;
    if (!path2) return path1;
    return `${path1}.${path2}`;
  }

  public nlevel(path: string): number {
    return path.split('.').length;
  }

  public subltree(path: string, start: number, end: number): string {
    const labels = path.split('.');
    return labels.slice(start, end).join('.');
  }

  public subpathFunc(path: string, offset: number, length?: number): string {
    return this.subpath(path, offset, length);
  }

  public indexFunc(path: string, subpath: string, offset: number = 0): number {
    const labels = path.split('.');
    const subLabels = subpath.split('.');
    for (let i = offset; i <= labels.length - subLabels.length; i++) {
      if (labels.slice(i, i + subLabels.length).join('.') === subpath) {
        return i;
      }
    }
    return -1;
  }

  public text2ltree(text: string): string {
    if (this.validatePath(text)) return text;
    throw new Error(`Cannot convert '${text}' to valid ltree format`);
  }

  public ltree2text(ltreePath: string): string {
    return ltreePath;
  }

  public lca(...paths: string[]): string | null {
    if (!paths.length) return null;
    if (paths.length === 1) return paths[0];
    const allLabels = paths.map(p => p.split('.'));
    const minLength = Math.min(...allLabels.map(l => l.length));
    const common: string[] = [];
    for (let i = 0; i < minLength; i++) {
      const label = allLabels[0][i];
      if (allLabels.every(l => l[i] === label)) common.push(label);
      else break;
    }
    return common.length ? common.join('.') : null;
  }

  public store(
    path: string,
    data: any,
    createdAt?: string | null,
    updatedAt?: string | null
  ): boolean {
    if (!this.validatePath(path)) {
      throw new Error(`Invalid ltree path: ${path}`);
    }
    this.data.set(path, {
      path,
      data: this.deepClone(data),
      createdAt: createdAt || null,
      updatedAt: updatedAt || null
    });
    return true;
  }

  public get(path: string): any | null {
    if (!this.validatePath(path)) {
      throw new Error(`Invalid ltree path: ${path}`);
    }
    const node = this.data.get(path);
    return node ? this.deepClone(node.data) : null;
  }

  public getNode(path: string): TreeNode | null {
    if (!this.validatePath(path)) {
      throw new Error(`Invalid ltree path: ${path}`);
    }
    const node = this.data.get(path);
    return node ? this.deepClone(node) : null;
  }

  public query(pattern: string): QueryResult[] {
    const res: QueryResult[] = [];
    this.data.forEach((node, p) => {
      if (this.ltreeMatch(p, pattern)) {
        res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
      }
    });
    return res.sort((a, b) => a.path.localeCompare(b.path));
  }

  public queryLtxtquery(ltxtquery: string): QueryResult[] {
    const res: QueryResult[] = [];
    this.data.forEach((node, p) => {
      if (this.ltxtqueryMatch(p, ltxtquery)) {
        res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
      }
    });
    return res.sort((a, b) => a.path.localeCompare(b.path));
  }

  public queryByOperator(
    operator: string,
    path1: string,
    path2?: string
  ): QueryResult[] {
    const res: QueryResult[] = [];
    if (operator === '@>') {
      this.data.forEach((node, p) => {
        if (this.ltreeAncestor(p, path1)) res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
      });
    } else if (operator === '<@') {
      this.data.forEach((node, p) => {
        if (this.ltreeDescendant(p, path1)) res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
      });
    } else if (operator === '~') {
      return this.query(path1);
    } else if (operator === '@@') {
      return this.queryLtxtquery(path1);
    }
    return res.sort((a, b) => a.path.localeCompare(b.path));
  }

  public queryAncestors(path: string): QueryResult[] {
    if (!this.validatePath(path)) throw new Error(`Invalid ltree path: ${path}`);
    const res: QueryResult[] = [];
    this.data.forEach((node, p) => {
      if (this.ltreeAncestor(p, path)) res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
    });
    return res.sort((a, b) => this.nlevel(a.path) - this.nlevel(b.path));
  }

  public queryDescendants(path: string): QueryResult[] {
    if (!this.validatePath(path)) throw new Error(`Invalid ltree path: ${path}`);
    const res: QueryResult[] = [];
    this.data.forEach((node, p) => {
      if (this.ltreeDescendant(p, path)) res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
    });
    return res.sort((a, b) => a.path.localeCompare(b.path));
  }

  public querySubtree(path: string): QueryResult[] {
    const res: QueryResult[] = [];
    if (this.exists(path)) {
      const node = this.data.get(path)!;
      res.push({ path, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
    }
    return [...res, ...this.queryDescendants(path)].sort((a, b) => a.path.localeCompare(b.path));
  }

  public exists(path: string): boolean {
    return this.data.has(path) && this.validatePath(path);
  }

  public delete(path: string): boolean {
    return this.data.delete(path);
  }

  public addSubtree(path: string, subtree: QueryResult[]): boolean {
    if (!this.validatePath(path)) throw new Error(`Invalid ltree path: ${path}`);
    if (!this.exists(path)) throw new Error(`Path ${path} does not exist`);
    for (const node of subtree) {
      this.store(`${path}.${node.path}`, node.data);
    }
    return true;
  }

  public deleteSubtree(path: string): number {
    const toDel: string[] = [];
    this.data.forEach((_, p) => {
      if (p === path || this.ltreeDescendant(p, path)) toDel.push(p);
    });
    toDel.forEach(p => this.data.delete(p));
    return toDel.length;
  }

  public async importFromPostgres(
    tableName: string = 'tree_data',
    pathColumn: string = 'path',
    dataColumn: string = 'data',
    createdAtColumn: string = 'created_at',
    updatedAtColumn: string = 'updated_at'
  ): Promise<number> {
    const client = await this.pool.connect();
    try {
      const tableCheck = await client.query<{ exists: boolean }>(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables
          WHERE table_name = $1
        );
      `, [tableName]);
      if (!tableCheck.rows[0].exists) {
        throw new Error(`Table '${tableName}' does not exist`);
      }
      const result = await client.query(`
        SELECT
          ${pathColumn}::text as path,
          ${dataColumn} as data,
          ${createdAtColumn}::text as created_at,
          ${updatedAtColumn}::text as updated_at
        FROM ${tableName}
        ORDER BY ${pathColumn};
      `);
      let count = 0;
      for (const row of result.rows) {
        let data = row.data;
        if (typeof data === 'string') {
          try { data = JSON.parse(data); } catch {};
        }
        this.store(row.path, data, row.created_at || null, row.updated_at || null);
        count++;
      }
      return count;
    } finally {
      client.release();
    }
  }

  public async exportToPostgres(
    tableName: string = 'tree_data',
    createTable: boolean = true,
    clearExisting: boolean = false
  ): Promise<number> {
    const client = await this.pool.connect();
    try {
      await client.query('CREATE EXTENSION IF NOT EXISTS ltree;');
      if (createTable) {
        await client.query(`
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id SERIAL PRIMARY KEY,
            path LTREE UNIQUE NOT NULL,
            data JSONB,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
          );
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS ${tableName}_path_idx ON ${tableName} USING GIST (path);`);
        await client.query(`CREATE INDEX IF NOT EXISTS ${tableName}_data_idx ON ${tableName} USING GIN (data);`);
      }
      if (clearExisting) {
        await client.query(`TRUNCATE TABLE ${tableName}`);
      }
      let count = 0;
      for (const [path, node] of this.data) {
        try {
          await client.query(`
            INSERT INTO ${tableName} (path, data, created_at, updated_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (path)
            DO UPDATE SET
              data = EXCLUDED.data,
              updated_at = EXCLUDED.updated_at;
          `, [path, JSON.stringify(node.data), node.createdAt, node.updatedAt]);
          count++;
        } catch (e) {
          console.error(`Error exporting path ${path}:`, e);
        }
      }
      return count;
    } finally {
      client.release();
    }
  }

  public async syncWithPostgres(
    direction: 'import' | 'export' | 'both' = 'both'
  ): Promise<SyncStats> {
    const stats: SyncStats = { imported: 0, exported: 0 };
    if (direction === 'import' || direction === 'both') {
      try { stats.imported = await this.importFromPostgres(this.tableName); } catch (e) { console.error('Import failed:', e); }
    }
    if (direction === 'export' || direction === 'both') {
      try { stats.exported = await this.exportToPostgres(this.tableName); } catch (e) { console.error('Export failed:', e); }
    }
    return stats;
  }

  public getStats(): TreeStats {
    if (this.data.size === 0) {
      return { total_nodes: 0, max_depth: 0, avg_depth: 0, root_nodes: 0, leaf_nodes: 0 };
    }
    const paths = Array.from(this.data.keys());
    const depths = paths.map(p => this.nlevel(p));
    const rootCount = paths.filter(p => this.nlevel(p) === 1).length;
    let leafCount = 0;
    for (const p of paths) {
      if (!paths.some(o => this.ltreeAncestor(p, o))) leafCount++;
    }
    return {
      total_nodes: this.data.size,
      max_depth: Math.max(...depths),
      avg_depth: depths.reduce((s, d) => s + d, 0) / depths.length,
      root_nodes: rootCount,
      leaf_nodes: leafCount
    };
  }

  public clear(): void {
    this.data.clear();
  }

  public size(): number {
    return this.data.size;
  }

  public getAllPaths(): string[] {
    return Array.from(this.data.keys()).sort();
  }

  private deepClone<T>(obj: T): T {
    if (obj === null || typeof obj !== 'object') return obj as T;
    if (obj instanceof Date)    return new Date(obj.getTime()) as any;
    if (Array.isArray(obj))      return obj.map(i => this.deepClone(i)) as any;
  
    if (obj instanceof Map) {
      const cloned = new Map<any, any>();
      for (const [k, v] of obj as Map<any, any>) {
        cloned.set(k, this.deepClone(v));
      }
      return cloned as any;
    }
  
    if (obj instanceof Set) {
      const cloned = new Set<any>();
      for (const v of obj as Set<any>) {
        cloned.add(this.deepClone(v));
      }
      return cloned as any;
    }
  
    const clonedObj: any = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        clonedObj[key] = this.deepClone((obj as any)[key]);
      }
    }
    return clonedObj;
  }

  public async close(): Promise<void> {
  
  
    await this.pool.end();
    
    
  }
}
