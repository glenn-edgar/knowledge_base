// ConstructKB.ts
// ConstructKB.ts
import type { Client } from 'pg';
import { KnowledgeBaseManager, ConnectionParams } from './KnowledgeBaseManager';

export class ConstructKB extends KnowledgeBaseManager {
  public path: Record<string, string[]> = {};
  public pathValues: Record<string, Record<string, boolean>> = {};
  public workingKb?: string;

  constructor(
    host: string,
    port: number,
    dbname: string,
    user: string,
    password: string,
    tableName = 'knowledge_base',
  ) {
    const connParams: ConnectionParams = { host, port, database: dbname, user, password };
    super(tableName, connParams);
  }

  /**
   * Connects and creates tables (must be called before use)
   */
  async init(): Promise<void> {
    await super.init();
  }

  /**
   * Expose the raw pg Client
   */
  getDbObjects(): Client {
    return this.client;
  }

  /**
   * Add a new knowledge base and initialize its path stack
   */
  async addKb(kbName: string, description = ''): Promise<void> {
    if (this.path[kbName]) {
      throw new Error(`Knowledge base ${kbName} already exists`);
    }
    this.path[kbName] = [kbName];
    this.pathValues[kbName] = {};
    await super.addKb(kbName, description);
  }

  /**
   * Select which knowledge base to work on
   */
  selectKb(kbName: string): void {
    if (!this.path[kbName]) {
      throw new Error(`Knowledge base ${kbName} does not exist`);
    }
    this.workingKb = kbName;
  }

  /**
   * Add a header node under the current KB, pushing [link, nodeName] on the stack
   */
  async addHeaderNode(
    link: string,
    nodeName: string,
    nodeProperties: Record<string, any>,
    nodeData: Record<string, any>,
    description = '',
  ): Promise<void> {
    if (typeof description !== 'string') {
      throw new TypeError('description must be a string');
    }
    if (typeof nodeProperties !== 'object' || Array.isArray(nodeProperties)) {
      throw new TypeError('nodeProperties must be a dictionary');
    }
    if (!this.workingKb) {
      throw new Error('No knowledge base selected');
    }

    if (description) {
      nodeProperties.description = description;
    }

    this.path[this.workingKb].push(link, nodeName);
    const nodePath = this.path[this.workingKb].join('.');

    if (this.pathValues[this.workingKb][nodePath]) {
      throw new Error(`Path ${nodePath} already exists in knowledge base`);
    }
    this.pathValues[this.workingKb][nodePath] = true;

    await super.addNode(
      this.workingKb,
      link,
      nodeName,
      nodeProperties,
      nodeData,
      nodePath,
    );
  }

  /**
   * Add an info node and immediately pop back up one level
   */
  async addInfoNode(
    link: string,
    nodeName: string,
    nodeProperties: Record<string, any>,
    nodeData: Record<string, any>,
    description = '',
  ): Promise<void> {
    await this.addHeaderNode(link, nodeName, nodeProperties, nodeData, description);
    if (this.workingKb) {
      this.path[this.workingKb].pop(); // nodeName
      this.path[this.workingKb].pop(); // link
    }
  }

  /**
   * Leave the current header node, verifying the popped values match
   */
  leaveHeaderNode(label: string, name: string): void {
    if (!this.workingKb) {
      throw new Error('No knowledge base selected');
    }
    const stack = this.path[this.workingKb];
    if (stack.length < 3) {
      throw new Error('Cannot leave a header node: path is empty or too short');
    }

    const refName  = stack.pop()!;
    const refLabel = stack.pop()!;
    const errors: string[] = [];

    if (refName !== name)  errors.push(`Expected name '${name}', got '${refName}'`);
    if (refLabel !== label) errors.push(`Expected label '${label}', got '${refLabel}'`);
    if (errors.length) {
      // restore state
      stack.push(refLabel, refName);
      throw new Error(errors.join('; '));
    }
  }

  /**
   * Add a link node under the current path
   */
  async addLinkNode(linkName: string): Promise<void> {
    if (!this.workingKb) {
      throw new Error('No knowledge base selected');
    }
    const currentPath = this.path[this.workingKb].join('.');
    await super.addLink(this.workingKb, currentPath, linkName);
  }

  /**
   * Add a link mount under the current path
   */
  async addLinkMount(
    linkMountName: string,
    description = '',
  ): Promise<{ knowledge_base: string; mount_path: string }> {
    if (!this.workingKb) {
      throw new Error('No knowledge base selected');
    }
    const currentPath = this.path[this.workingKb].join('.');
    return await super.addLinkMount(
      this.workingKb,
      currentPath,
      linkMountName,
      description,
    );
  }

  /**
   * Verify that every KB path stack is back to its root, then disconnect.
   */
  async checkInstallation(): Promise<void> {
    for (const kbName in this.path) {
      const stack = this.path[kbName];
      if (stack.length !== 1 || stack[0] !== kbName) {
        throw new Error(
          `Installation check failed for '${kbName}'. Path: [${stack.join(', ')}]`
        );
      }
    }
    
  }
}
