import { Client, ClientConfig, QueryResult } from 'pg';

export interface ConnectionParams extends ClientConfig {
  host: string;
  database: string;
  user: string;
  password: string;
  port: number;
}

export class KnowledgeBaseManager {
  protected client: Client;
  protected tableName: string;

  constructor(tableName: string, connectionParams: ConnectionParams) {
    this.tableName = tableName;
    this.client = new Client(connectionParams);
  }

  private ident(id: string): string {
    // double‐quote and escape identifiers
    return `"${id.replace(/"/g, '""')}"`;
  }

  /** Connects and ensures ltree extension + tables exist */
  async init(): Promise<void> {
    try {
      await this.client.connect();
      await this.client.query('CREATE EXTENSION IF NOT EXISTS ltree;');
      await this._createTables();
    } catch (err) {
      console.error('Error initializing KnowledgeBaseManager:', err);
      throw err;
    }
  }

  /** Close connection */
  async disconnect(): Promise<void> {
    await this.client.end();
  }

  /** Drop a table if exists */
  private async _deleteTable(table: string, schema = 'public'): Promise<void> {
    const sql = `DROP TABLE IF EXISTS ${this.ident(schema)}.${this.ident(table)} CASCADE;`;
    await this.client.query(sql);
  }

  /** Build all four tables + indexes */
  private async _createTables(): Promise<void> {
    // drop any existing
    for (const t of [
      this.tableName,
      `${this.tableName}_info`,
      `${this.tableName}_link`,
      `${this.tableName}_link_mount`,
    ]) {
      await this._deleteTable(t);
    }

    // helper to format with identifiers
    const T = (t: string) => this.ident(t);

    // table DDLs
    const ddl = [
      // main KB table
      `
      CREATE TABLE ${T(this.tableName)} (
        id SERIAL PRIMARY KEY,
        knowledge_base VARCHAR NOT NULL,
        label VARCHAR NOT NULL,
        name VARCHAR NOT NULL,
        properties JSON,
        data JSON,
        has_link BOOLEAN DEFAULT FALSE,
        has_link_mount BOOLEAN DEFAULT FALSE,
        path LTREE UNIQUE
      );`,
      // info table
      `
      CREATE TABLE ${T(this.tableName + '_info')} (
        id SERIAL PRIMARY KEY,
        knowledge_base VARCHAR NOT NULL UNIQUE,
        description VARCHAR
      );`,
      // link table
      `
      CREATE TABLE ${T(this.tableName + '_link')} (
        id SERIAL PRIMARY KEY,
        link_name VARCHAR NOT NULL,
        parent_node_kb VARCHAR NOT NULL,
        parent_path LTREE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(link_name, parent_node_kb, parent_path)
      );`,
      // link‐mount table
      `
      CREATE TABLE ${T(this.tableName + '_link_mount')} (
        id SERIAL PRIMARY KEY,
        link_name VARCHAR NOT NULL UNIQUE,
        knowledge_base VARCHAR NOT NULL,
        mount_path LTREE NOT NULL,
        description VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(knowledge_base, mount_path)
      );`,
    ];

    // indexes
    const idx = [
      // main table
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_kb')} ON ${T(this.tableName)} (knowledge_base);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_path')} ON ${T(this.tableName)} USING GIST (path);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_label')} ON ${T(this.tableName)} (label);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_name')} ON ${T(this.tableName)} (name);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_has_link')} ON ${T(this.tableName)} (has_link);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_has_link_mount')} ON ${T(this.tableName)} (has_link_mount);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_kb_path')} ON ${T(this.tableName)} (knowledge_base, path);`,
      // info table
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_info_kb')} ON ${T(this.tableName + '_info')} (knowledge_base);`,
      // link table
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_link_name')} ON ${T(this.tableName + '_link')} (link_name);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_link_parent_kb')} ON ${T(this.tableName + '_link')} (parent_node_kb);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_link_parent_path')} ON ${T(this.tableName + '_link')} USING GIST (parent_path);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_link_created')} ON ${T(this.tableName + '_link')} (created_at);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_link_composite')} ON ${T(this.tableName + '_link')} (link_name, parent_node_kb);`,
      // mount table
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_mount_link_name')} ON ${T(this.tableName + '_link_mount')} (link_name);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_mount_kb')} ON ${T(this.tableName + '_link_mount')} (knowledge_base);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_mount_path')} ON ${T(this.tableName + '_link_mount')} USING GIST (mount_path);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_mount_created')} ON ${T(this.tableName + '_link_mount')} (created_at);`,
      `CREATE INDEX IF NOT EXISTS ${T('idx_' + this.tableName + '_mount_composite')} ON ${T(this.tableName + '_link_mount')} (knowledge_base, mount_path);`,
    ];

    try {
      for (const s of ddl) {
        await this.client.query(s);
      }
      for (const s of idx) {
        await this.client.query(s);
      }
    } catch (err) {
      console.error('Error creating tables/indexes:', err);
      throw err;
    }
  }

  /** Add a new KB entry */
  async addKb(kbName: string, description = ''): Promise<void> {
    if (typeof kbName !== 'string' || typeof description !== 'string') {
      throw new TypeError('kbName and description must be strings');
    }
    const infoT = this.ident(this.tableName + '_info');
    const sql = `
      INSERT INTO ${infoT}(knowledge_base, description)
      VALUES($1, $2)
      ON CONFLICT (knowledge_base) DO NOTHING;
    `;
    await this.client.query(sql, [kbName, description]);
  }

  /** Add a node */
  async addNode(
    kbName: string,
    label: string,
    name: string,
    properties: Record<string, any> = {},
    data: Record<string, any> = {},
    path = ''
  ): Promise<void> {
    // validations
    for (const [v, t] of [
      [kbName, 'string'],
      [label, 'string'],
      [name, 'string'],
      [path, 'string']
    ] as const) {
      if (typeof v !== t) throw new TypeError(`${v} must be a ${t}`);
    }
    if (typeof properties !== 'object' || Array.isArray(properties))
      throw new TypeError('properties must be an object');
    if (typeof data !== 'object' || Array.isArray(data))
      throw new TypeError('data must be an object');

    // ensure KB exists
    const infoT = this.ident(this.tableName + '_info');
    const { rowCount } = await this.client.query(
      `SELECT 1 FROM ${infoT} WHERE knowledge_base = $1;`,
      [kbName]
    );
    if (rowCount === 0) {
      throw new Error(`Knowledge base '${kbName}' not found`);
    }

    // insert
    const mainT = this.ident(this.tableName);
    const sql = `
      INSERT INTO ${mainT}
        (knowledge_base,label,name,properties,data,has_link,path)
      VALUES($1,$2,$3,$4,$5,false,$6);
    `;
    await this.client.query(sql, [
      kbName,
      label,
      name,
      Object.keys(properties).length ? JSON.stringify(properties) : null,
      Object.keys(data).length ? JSON.stringify(data) : null,
      path,
    ]);
  }

  async addLink(parentKb: string, parentPath: string, linkName: string): Promise<void> {
    // 1) validate inputs
    for (const arg of [parentKb, parentPath, linkName]) {
      if (typeof arg !== 'string') throw new TypeError('All args must be strings');
    }
  
    const infoT  = this.ident(this.tableName + '_info');
    const mainT  = this.ident(this.tableName);
    const linkT  = this.ident(this.tableName + '_link');
    const mountT = this.ident(this.tableName + '_link_mount');
  
    // 2) ensure parent KB exists
    let res = await this.client.query(
      `SELECT 1 FROM ${infoT} WHERE knowledge_base = $1;`,
      [parentKb]
    );
    if (res.rowCount === 0) {
      throw new Error(`Parent knowledge base '${parentKb}' not found`);
    }
  
    // 3) ensure parent node exists
    res = await this.client.query(
      `SELECT 1 FROM ${mainT} WHERE path = $1;`,
      [parentPath]
    );
    if (res.rowCount === 0) {
      throw new Error(`Parent node '${parentPath}' not found`);
    }
  
    // 4) ensure link‐mount exists
    res = await this.client.query(
      `SELECT 1 FROM ${mountT} WHERE link_name = $1;`,
      [linkName]
    );
    if (res.rowCount === 0) {
      throw new Error(`Link mount '${linkName}' does not exist`);
    }
  
    // 5) insert into link table
    await this.client.query(
      `INSERT INTO ${linkT}(link_name, parent_node_kb, parent_path) VALUES($1,$2,$3);`,
      [linkName, parentKb, parentPath]
    );
  
    // 6) update the has_link flag on the main table
    await this.client.query(
      `UPDATE ${mainT} SET has_link = TRUE WHERE path = $1;`,
      [parentPath]
    );
  }

  /** Add a link‐mount */
  async addLinkMount(
    kb: string,
    path: string,
    linkMountName: string,
    description = ''
  ): Promise<{ knowledge_base: string; mount_path: string }> {
    for (const arg of [kb, path, linkMountName, description]) {
      if (typeof arg !== 'string') throw new TypeError('All args must be strings');
    }

    const infoT = this.ident(this.tableName + '_info');
    const mainT = this.ident(this.tableName);
    const mountT = this.ident(this.tableName + '_link_mount');

    // 1) KB exists?
    let res = await this.client.query(
      `SELECT 1 FROM ${infoT} WHERE knowledge_base=$1;`,
      [kb]
    );
    if (res.rowCount === 0) throw new Error(`KB '${kb}' not found`);

    // 2) path exists
    res = await this.client.query(
      `SELECT id FROM ${mainT} WHERE knowledge_base=$1 AND path=$2;`,
      [kb, path]
    );
    if (res.rowCount === 0) throw new Error(`Path '${path}' not found in KB '${kb}'`);

    // 3) mount name unique
    res = await this.client.query(
      `SELECT 1 FROM ${mountT} WHERE link_name=$1;`,
      [linkMountName]
    );
    if ((res.rowCount ?? 0) > 0) {
      throw new Error(`Link name '${linkMountName}' already exists`);
    }

    // 4) insert mount
    await this.client.query(
      `INSERT INTO ${mountT}(link_name,knowledge_base,mount_path,description)
       VALUES($1,$2,$3,$4);`,
      [linkMountName, kb, path, description]
    );

    // 5) flag main table
    await this.client.query(
      `UPDATE ${mainT} SET has_link_mount = true
       WHERE knowledge_base=$1 AND path=$2;`,
      [kb, path]
    );

    return { knowledge_base: kb, mount_path: path };
  }
}
