import { BasicConstructDB, TreeNode } from './BasicConstructDb';

/**
 * TypeScript translation of Python SearchMemDB
 */
export class SearchMemDB extends BasicConstructDB {
  public kbs: Record<string, string[]> = {};
  public labels: Record<string, string[]> = {};
  public names: Record<string, string[]> = {};
  public decodedKeys: Record<string, string[]> = {};
  public filterResults: Map<string, TreeNode> = new Map();

  constructor(
    host: string,
    port: number,
    dbname: string,
    user: string,
    password: string,
    tableName: string
  ) {
    super(host, port, dbname, user, password, tableName);
    // Import existing data and initialize filters
    this.importFromPostgres(this.tableName)
      .then(() => {
        this.filterResults = new Map(this.data);
        this.generateDecodedKeys();
      })
      .catch(err => {
        throw new Error(`Failed to load data: ${err}`);
      });
  }

  private generateDecodedKeys(): void {
    this.kbs = {};
    this.labels = {};
    this.names = {};
    this.decodedKeys = {};

    for (const key of this.data.keys()) {
      const parts = key.split('.');
      this.decodedKeys[key] = parts;
      const kb = parts[0];
      const label = parts[parts.length - 2];
      const name = parts[parts.length - 1];

      if (!this.kbs[kb]) this.kbs[kb] = [];
      this.kbs[kb].push(key);

      if (!this.labels[label]) this.labels[label] = [];
      this.labels[label].push(key);

      if (!this.names[name]) this.names[name] = [];
      this.names[name].push(key);
    }
  }

  /** Clear all filters and reset to full data */
  public clearFilters(): void {
    this.filterResults = new Map(this.data);
  }

  /** Filter by knowledge base */
  public searchKb(kbName: string): Map<string, TreeNode> {
    const result = new Map<string, TreeNode>();
    const keys = this.kbs[kbName] || [];
    for (const key of keys) {
      if (this.filterResults.has(key)) {
        result.set(key, this.filterResults.get(key)!);
      }
    }
    this.filterResults = result;
    return this.filterResults;
  }

  /** Filter by label */
  public searchLabel(label: string): Map<string, TreeNode> {
    const result = new Map<string, TreeNode>();
    const keys = this.labels[label] || [];
    for (const key of keys) {
      if (this.filterResults.has(key)) {
        result.set(key, this.filterResults.get(key)!);
      }
    }
    this.filterResults = result;
    return this.filterResults;
  }

  /** Filter by name */
  public searchName(name: string): Map<string, TreeNode> {
    const result = new Map<string, TreeNode>();
    const keys = this.names[name] || [];
    for (const key of keys) {
      if (this.filterResults.has(key)) {
        result.set(key, this.filterResults.get(key)!);
      }
    }
    this.filterResults = result;
    return this.filterResults;
  }

  /** Filter by presence of property key */
  public searchPropertyKey(dataKey: string): Map<string, TreeNode> {
    const result = new Map<string, TreeNode>();
    for (const [key, node] of this.filterResults) {
      if (node.data && dataKey in node.data) {
        result.set(key, node);
      }
    }
    this.filterResults = result;
    return this.filterResults;
  }

  /** Filter by property key/value match */
  public searchPropertyValue(dataKey: string, dataValue: any): Map<string, TreeNode> {
    const result = new Map<string, TreeNode>();
    for (const [key, node] of this.filterResults) {
      if (node.data && node.data[dataKey] === dataValue) {
        result.set(key, node);
      }
    }
    this.filterResults = result;
    return this.filterResults;
  }

  /** Filter by starting path and its descendants */
  public searchStartingPath(startingPath: string): Map<string, TreeNode> {
    if (typeof startingPath !== 'string') {
      throw new Error('startingPath must be a string');
    }

    const result = new Map<string, TreeNode>();
    // exact match
    if (this.filterResults.has(startingPath)) {
      result.set(startingPath, this.filterResults.get(startingPath)!);
    }
    // descendants
    const descendants = this.queryDescendants(startingPath);
    for (const row of descendants) {
      if (this.filterResults.has(row.path)) {
        result.set(row.path, this.filterResults.get(row.path)!);
      }
    }

    this.filterResults = result;
    return this.filterResults;
  }

  /** Filter by LTREE operator */
  public searchPath(operator: string, pathExpr: string): Map<string, TreeNode> {
    const searchResults = this.queryByOperator(operator, pathExpr);
    const result = new Map<string, TreeNode>();
    for (const item of searchResults) {
      if (this.filterResults.has(item.path)) {
        result.set(item.path, this.filterResults.get(item.path)!);
      }
    }
    this.filterResults = result;
    return this.filterResults;
  }

  /** Extract description for each node */
  public findDescriptions(): Record<string, string> {
    const descs: Record<string, string> = {};
    for (const [key, node] of this.data) {
      descs[key] = node.data.description || '';
    }
    return descs;
  }
}
