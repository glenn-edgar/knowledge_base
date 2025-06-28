// ConstructDataTables.ts
import promptSync from 'prompt-sync';
import { Client } from 'pg';
import { ConstructKB } from './ConstructKB';
import { ConstructStatusTable } from './ConstructStatusTable';
import { ConstructJobTable } from './ConstructJobTable';
import { ConstructStreamTable } from './ConstructStreamTable';
import { ConstructRpcClientTable } from './ConstructRpcClientTable';
import { ConstructRpcServerTable } from './ConstructRpcServerTable';

export class ConstructDataTables {
  public kb: ConstructKB;
  public statusTable!: ConstructStatusTable;
  public jobTable!: ConstructJobTable;
  public streamTable!: ConstructStreamTable;
  public rpcClientTable!: ConstructRpcClientTable;
  public rpcServerTable!: ConstructRpcServerTable;

  public path!: Record<string, string[]>;

  // Delegated methods
  public addKb!: (kbName: string, description?: string) => Promise<void>;
  public selectKb!: (kbName: string) => void;
  public addLinkNode!: (linkName: string) => Promise<void>;
  public addLinkMount!: (
    linkMountName: string,
    description?: string
  ) => Promise<{ knowledge_base: string; mount_path: string }>;
  public addHeaderNode!: (
    link: string,
    nodeName: string,
    nodeProperties: Record<string, any>,
    nodeData: Record<string, any>,
    description?: string
  ) => Promise<void>;
  public addInfoNode!: (
    link: string,
    nodeName: string,
    nodeProperties: Record<string, any>,
    nodeData: Record<string, any>,
    description?: string
  ) => Promise<void>;
  public leaveHeaderNode!: (label: string, name: string) => void;
  public disconnect!: () => Promise<void>;
  public addStreamField!: (
    streamKey: string,
    streamLength: number,
    description: string
  ) => Promise<any>;
  public addRpcClientField!: (
    rpcClientKey: string,
    queueDepth: number,
    description: string
  ) => Promise<any>;
  public addRpcServerField!: (
    rpcServerKey: string,
    queueDepth: number,
    description: string
  ) => Promise<any>;
  public addStatusField!: (
    statusKey: string,
    properties: Record<string, any>,
    description: string,
    initialData: Record<string, any>
  ) => Promise<any>;
  public addJobField!: (
    jobKey: string,
    jobLength: number,
    description: string
  ) => Promise<any>;

  constructor(
    private host: string,
    private port: number,
    private dbname: string,
    private user: string,
    private password: string,
    private database: string,
  ) {
    this.kb = new ConstructKB(
      this.host,
      this.port,
      this.dbname,
      this.user,
      this.password,
      this.database,
    );
  }

  /** Initializes all components: connects, builds schemas, and delegates methods */
  async init(): Promise<void> {
    // Init KB (connect + tables)
    await this.kb.init();
    const client: Client = this.kb.getDbObjects();

    // Instantiate table constructors and setup their schemas
    this.statusTable = new ConstructStatusTable(
      client,
      this.kb,
      this.database,
    );
    await this.statusTable.setupSchema();

    this.jobTable = new ConstructJobTable(
      client,
      this.kb,
      this.database,
    );
    await this.jobTable.setupSchema();

    this.streamTable = new ConstructStreamTable(
      client,
      this.kb,
      this.database,
    );
    await this.streamTable.setupSchema();

    this.rpcClientTable = new ConstructRpcClientTable(
      client,
      this.kb,
      this.database,
    );
    await this.rpcClientTable.setupSchema();

    this.rpcServerTable = new ConstructRpcServerTable(
      client,
      this.kb,
      this.database,
    );
    await this.rpcServerTable.setupSchema();

    // Delegate properties and methods from KB and tables
    this.path = this.kb.path;
    this.addKb = this.kb.addKb.bind(this.kb);
    this.selectKb = this.kb.selectKb.bind(this.kb);
    this.addLinkNode = this.kb.addLinkNode.bind(this.kb);
    this.addLinkMount = this.kb.addLinkMount.bind(this.kb);
    this.addHeaderNode = this.kb.addHeaderNode.bind(this.kb);
    this.addInfoNode = this.kb.addInfoNode.bind(this.kb);
    this.leaveHeaderNode = this.kb.leaveHeaderNode.bind(this.kb);
    this.disconnect = this.kb.disconnect.bind(this.kb);

    this.addStreamField = this.streamTable.addStreamField.bind(
      this.streamTable,
    );
    this.addRpcClientField = this.rpcClientTable.addRpcClientField.bind(
      this.rpcClientTable,
    );
    this.addRpcServerField = this.rpcServerTable.addRpcServerField.bind(
      this.rpcServerTable,
    );
    this.addStatusField = this.statusTable.addStatusField.bind(
      this.statusTable,
    );
    this.addJobField = this.jobTable.addJobField.bind(this.jobTable);
  }

  /** Checks installation across all tables */
  async checkInstallation(): Promise<void> {
    await this.kb.checkInstallation();
    await this.statusTable.checkInstallation();
    await this.jobTable.checkInstallation();
    await this.streamTable.checkInstallation();
    await this.rpcClientTable.checkInstallation();
    await this.rpcServerTable.checkInstallation();
  }
}

