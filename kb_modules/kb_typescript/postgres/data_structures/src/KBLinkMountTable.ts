import { Client } from 'pg';

export class KBLinkMountTable {
  private client: Client;
  private baseTable: string;

  /**
   * @param client  An instance of pg.Client (already connected)
   * @param base    The base name of your table (without "_link_mount" suffix)
   */
  constructor(client: Client, base: string) {
    this.client = client;
    this.baseTable = `${base}_link_mount`;
  }

  /**
   * Find all rows where link_name = linkName,
   * optionally filtered by knowledge_base.
   */
  public async findRecordsByLinkName(
    linkName: string,
    kb?: string
  ): Promise<Record<string, any>[]> {
    let query: string;
    let params: any[];

    if (kb == null) {
      query = `SELECT * FROM ${this.baseTable} WHERE link_name = $1`;
      params = [linkName];
    } else {
      query = `
        SELECT *
        FROM ${this.baseTable}
        WHERE link_name = $1 AND knowledge_base = $2
      `;
      params = [linkName, kb];
    }

    const res = await this.client.query(query, params);
    return res.rows;
  }

  /**
   * Find all rows where mount_path = mountPath,
   * optionally filtered by knowledge_base.
   */
  public async findRecordsByMountPath(
    mountPath: string,
    kb?: string
  ): Promise<Record<string, any>[]> {
    let query: string;
    let params: any[];

    if (kb == null) {
      query = `SELECT * FROM ${this.baseTable} WHERE mount_path = $1`;
      params = [mountPath];
    } else {
      query = `
        SELECT *
        FROM ${this.baseTable}
        WHERE mount_path = $1 AND knowledge_base = $2
      `;
      params = [mountPath, kb];
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
    return res.rows.map(r => r.link_name);
  }

  /**
   * Return all distinct mount_path values, sorted ascending.
   */
  public async findAllMountPaths(): Promise<string[]> {
    const query = `
      SELECT DISTINCT mount_path
      FROM ${this.baseTable}
      ORDER BY mount_path
    `;
    const res = await this.client.query(query);
    return res.rows.map(r => r.mount_path);
  }
}
