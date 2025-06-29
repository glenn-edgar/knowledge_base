import { BasicConstructDB } from './BasicConstructDb';

export class ConstructMemDB extends BasicConstructDB {
  private workingKb: string | null = null;
  private compositePath: Map<string, string[]> = new Map();
  private compositePathValues: Map<string, Map<string, boolean>> = new Map();

  constructor(
    host: string,
    port: number,
    dbname: string,
    user: string,
    password: string,
    tableName: string
  ) {
    super(host, port, dbname, user, password, tableName);
  }

  /**
   * Create a new knowledge base.
   */
  public addKb(kbName: string, description: string = ''): void {
    if (this.compositePath.has(kbName)) {
      throw new Error(`Knowledge base "${kbName}" already exists.`);
    }
    this.compositePath.set(kbName, [kbName]);
    this.compositePathValues.set(kbName, new Map());
    super.addKb(kbName, description);
  }

  /**
   * Select which knowledge base to operate on.
   */
  public selectKb(kbName: string): void {
    if (!this.compositePath.has(kbName)) {
      throw new Error(`Knowledge base "${kbName}" does not exist.`);
    }
    this.workingKb = kbName;
  }

  /**
   * Pushes a header node onto the current path and stores it.
   */
  public addHeaderNode(
    link: string,
    nodeName: string,
    nodeData: Record<string, any>,
    description: string = ''
  ): void {
    if (!this.workingKb) {
      throw new Error('No knowledge base selected.');
    }
    if (typeof description !== 'string') {
      throw new TypeError('description must be a string');
    }
    if (typeof nodeData !== 'object' || nodeData === null || Array.isArray(nodeData)) {
      throw new TypeError('nodeData must be a non-array object');
    }

    // Annotate nodeData
    if (description) {
      nodeData.description = description;
    }

    const pathStack = this.compositePath.get(this.workingKb)!;
    pathStack.push(link, nodeName);
    const fullPath = pathStack.join('.');

    const values = this.compositePathValues.get(this.workingKb)!;
    if (values.has(fullPath)) {
      throw new Error(`Path "${fullPath}" already exists in knowledge base.`);
    }
    values.set(fullPath, true);

    super.store(fullPath, nodeData);
  }

  /**
   * Temporarily pushes a header and immediately pops it back off.
   */
  public addInfoNode(
    link: string,
    nodeName: string,
    nodeData: Record<string, any>,
    description: string = ''
  ): void {
    this.addHeaderNode(link, nodeName, nodeData, description);

    // roll back the path so only the header remains
    const stack = this.compositePath.get(this.workingKb!)!;
    stack.pop(); // nodeName
    stack.pop(); // link
  }

  /**
   * Pop a header node, validating that it matches the expected label/name.
   */
  public leaveHeaderNode(label: string, name: string): void {
    if (!this.workingKb) {
      throw new Error('No knowledge base selected.');
    }
    const stack = this.compositePath.get(this.workingKb)!;
    if (stack.length < 3) {
      throw new Error('Cannot leave header node: insufficient path elements.');
    }

    const poppedName = stack.pop()!;
    const poppedLabel = stack.pop()!;

    const errors: string[] = [];
    if (poppedLabel !== label) {
      errors.push(`Expected label "${label}", but got "${poppedLabel}"`);
    }
    if (poppedName !== name) {
      errors.push(`Expected name "${name}", but got "${poppedName}"`);
    }
    if (errors.length) {
      // push them back so state is unchanged
      stack.push(poppedLabel, poppedName);
      throw new Error(errors.join('; '));
    }
  }

  /**
   * Ensure every KB’s path is back to its root. Throws if anything
   * was left open.
   */
  public checkInstallation(): boolean {
    for (const [kb, path] of this.compositePath.entries()) {
      if (path.length !== 1 || path[0] !== kb) {
        throw new Error(
          `Installation check failed for "${kb}". Current path: ${path.join('.')}`
        );
      }
    }
    return true;
  }
}
