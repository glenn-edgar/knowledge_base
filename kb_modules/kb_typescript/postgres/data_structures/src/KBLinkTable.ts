import { Client } from 'pg';

export class KBLinkTable {
  private client: Client;
  private baseTable: string;

  /**
   * @param client  An instance of pg.Client (already connected)
   * @param base    The base name of your table (without “_link” suffix)
   */
  constructor(client: Client, base: string) {
    this.client = client;
    this.baseTable = `${base}_link`;
  }

  /**
   * Find all rows where link_name = linkName,
   * optionally filtered by parent_node_kb.
   */
  public async findRecordsByLinkName(
    linkName: string,
    kb?: string
  ): Promise<any[]> {
    let query: string;
    let params: any[];

    if (kb == null) {
      query = `SELECT * FROM ${this.baseTable} WHERE link_name = $1`;
      params = [linkName];
    } else {
      query = `
        SELECT *
          FROM ${this.baseTable}
         WHERE link_name = $1
           AND parent_node_kb = $2
      `;
      params = [linkName, kb];
    }

    const res = await this.client.query(query, params);
    return res.rows;
  }

  /**
   * Find all rows where parent_path = nodePath,
   * optionally filtered by parent_node_kb.
   */
  public async findRecordsByNodePath(
    nodePath: string,
    kb?: string
  ): Promise<any[]> {
    let query: string;
    let params: any[];

    if (kb == null) {
      query = `SELECT * FROM ${this.baseTable} WHERE parent_path = $1`;
      params = [nodePath];
    } else {
      query = `
        SELECT *
          FROM ${this.baseTable}
         WHERE parent_path = $1
           AND parent_node_kb = $2
      `;
      params = [nodePath, kb];
    }

    const res = await this.client.query(query, params);
    return res.rows;
  }

  /**
   * Return all distinct link_name values, sorted ascending.
   */
  public async findAllLinkNames(): Promise<string[]> {
    const query = `
      SELECT DISTINCT link_name
        FROM ${this.baseTable}
      ORDER BY link_name
    `;
    const res = await this.client.query(query);
    return res.rows.map(row => row.link_name);
  }

  /**
   * Return all distinct parent_path values, sorted ascending.
   */
  public async findAllNodeNames(): Promise<string[]> {
    const query = `
      SELECT DISTINCT parent_path
        FROM ${this.baseTable}
      ORDER BY parent_path
    `;
    const res = await this.client.query(query);
    return res.rows.map(row => row.parent_path);
  }
}
