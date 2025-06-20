import { Client, QueryResult } from 'pg';

/**
 * This class is designed to construct a knowledge base structure with header
 * and info nodes, using a stack-based approach to manage the path. It also
 * manages a connection to a PostgreSQL database and sets up the schema.
 */
class Construct_KB {
  private path: string[] = []; // Stack to keep track of the path (levels/nodes)
  private host: string;
  private port: string | number;
  private dbname: string;
  private user: string;
  private password: string;
  private conn: Client | null = null; // Connection object

  /**
   * Initializes the Construct_KB object and connects to the PostgreSQL database.
   * Also sets up the database schema.
   *
   * @param host - The database host.
   * @param port - The database port.
   * @param dbname - The name of the database.
   * @param user - The database user.
   * @param password - The password for the database user.
   * @param database - (Redundant with dbname, but kept for compatibility)
   */
  constructor(
    host: string,
    port: string | number,
    dbname: string,
    user: string,
    password: string,
    database: string
  ) {
    this.host = host;
    this.port = port;
    this.dbname = dbname;
    this.user = user;
    this.password = password;
    this._connect(); // Establish the database connection and schema during initialization
  }

  /**
   * Establishes a connection to the PostgreSQL database and sets up the schema.
   * This is a helper method called by the constructor.
   */
  private async _connect(): Promise<void> {
    try {
      this.conn = new Client({
        host: this.host,
        port: typeof this.port === 'string' ? parseInt(this.port, 10) : this.port,
        database: this.dbname,
        user: this.user,
        password: this.password
      });

      await this.conn.connect();
      console.log(`Connected to PostgreSQL database ${this.dbname} on ${this.host}:${this.port}`);

      // Execute the SQL script to set up the schema
      await this._setupSchema();
    } catch (e) {
      console.error(`Error connecting to PostgreSQL or setting up schema: ${e}`);
      if (this.conn) {
        // No explicit rollback in node-postgres client - connections with errors are automatically closed
      }
      throw e; // Re-raise the exception
    }
  }

  /**
   * Sets up the database schema (tables, functions, etc.).
   */
  private async _setupSchema(): Promise<void> {
    try {
      // Create extensions
      await this.conn?.query(`
        CREATE EXTENSION IF NOT EXISTS hstore;
        CREATE EXTENSION IF NOT EXISTS ltree;
      `);

      // Drop the table if it exists
      await this.conn?.query("DROP TABLE IF EXISTS knowledge_base;");

      // Create the knowledge_base table
      await this.conn?.query(`
        CREATE TABLE knowledge_base (
          id SERIAL PRIMARY KEY,
          label VARCHAR NOT NULL,
          name VARCHAR NOT NULL,
          properties JSONB,
          data JSONB,
          path LTREE
        );
      `);

      console.log("Knowledge base table created.");
    } catch (e) {
      console.error(`Error setting up database schema: ${e}`);
      throw e; // Re-raise the exception
    }
  }

  /**
   * Closes the connection to the PostgreSQL database. This is a helper
   * method called by check_installation.
   */
  private async _disconnect(): Promise<void> {
    if (this.conn) {
      await this.conn.end();
      console.log(`Disconnected from PostgreSQL database ${this.dbname} on ${this.host}:${this.port}`);
    }
    this.conn = null;
  }

  /**
   * Adds a header node to the knowledge base.
   *
   * @param link - The link associated with the header node.
   * @param nodeName - The name of the header node.
   * @param nodeProperties - Properties associated with the header node.
   * @param nodeData - Data associated with the header node.
   */
  public async addHeaderNode(
    link: string,
    nodeName: string,
    nodeProperties: Record<string, any> | null,
    nodeData: Record<string, any> | null
  ): Promise<void> {
    this.path.push(link);
    this.path.push(nodeName);
    const nodePath = this.path.join(".");
    console.log(link, nodeName, nodeProperties, nodeData, nodePath);

    // Insert into database
    if (this.conn) {
      try {
        // Convert TypeScript objects to JSON strings
        const jsonProperties = nodeProperties ? JSON.stringify(nodeProperties) : null;
        const jsonData = nodeData ? JSON.stringify(nodeData) : null;

        await this.conn.query(
          `INSERT INTO knowledge_base (label, name, properties, data, path)
           VALUES ($1, $2, $3, $4, $5);`,
          [link, nodeName, jsonProperties, jsonData, nodePath]
        );
      } catch (e) {
        console.error(`Error inserting header node: ${e}`);
        throw e;
      }
    }
  }

  /**
   * Adds an info node to the knowledge base. This function adds a node
   * and then immediately removes its link and name from the path.
   * It now checks that the path has a length greater than 1 before adding.
   *
   * @param link - The link associated with the info node.
   * @param nodeName - The name of the info node.
   * @param nodeProperties - Properties associated with the info node.
   * @param nodeData - Data associated with the header node.
   */
  public async addInfoNode(
    link: string,
    nodeName: string,
    nodeProperties: Record<string, any> | null,
    nodeData: Record<string, any> | null
  ): Promise<void> {
    if (this.path.length <= 1) {
      throw new Error("Path length must be greater than 1 when adding an info node.");
    }

    this.path.push(link);
    this.path.push(nodeName);
    const nodePath = this.path.join(".");

    // Insert into database
    if (this.conn) {
      try {
        // Convert TypeScript objects to JSON strings
        const jsonProperties = nodeProperties ? JSON.stringify(nodeProperties) : null;
        const jsonData = nodeData ? JSON.stringify(nodeData) : null;

        await this.conn.query(
          `INSERT INTO knowledge_base (label, name, properties, data, path)
           VALUES ($1, $2, $3, $4, $5);`,
          [link, nodeName, jsonProperties, jsonData, nodePath]
        );
      } catch (e) {
        console.error(`Error inserting info node: ${e}`);
        throw e;
      }
    }

    this.path.pop(); // Remove nodeName
    this.path.pop(); // Remove link
  }

  /**
   * Leaves a header node, verifying the label and name.
   * If an error occurs, the knowledge_base table is deleted if it exists
   * and the PostgreSQL connection is terminated.
   *
   * @param label - The expected link of the header node.
   * @param name - The expected name of the header node.
   */
  public async leaveHeaderNode(label: string, name: string): Promise<void> {
    try {
      // Try to pop the expected values
      if (!this.path.length) {
        throw new Error("Cannot leave a header node: path is empty");
      }

      const refName = this.path.pop();
      if (!this.path.length) {
        // Put the name back and raise an error
        this.path.push(refName!);
        throw new Error("Cannot leave a header node: not enough elements in path");
      }

      const refLabel = this.path.pop();

      // Verify the popped values
      if (refName !== name || refLabel !== label) {
        // Put the values back in case of mismatch
        this.path.push(refLabel!);
        this.path.push(refName!);

        // Create a descriptive error message
        const errorMsg: string[] = [];
        if (refName !== name) {
          errorMsg.push(`Expected name '${name}', but got '${refName}'`);
        }
        if (refLabel !== label) {
          errorMsg.push(`Expected label '${label}', but got '${refLabel}'`);
        }

        throw new Error(errorMsg.join(", "));
      }
    } catch (e) {
      // An error occurred, clean up the database
      console.error(`Error in leaveHeaderNode: ${e}`);

      // Drop the knowledge_base table if connection exists
      if (this.conn) {
        try {
          await this.conn.query("DROP TABLE IF EXISTS knowledge_base;");
          console.log("knowledge_base table has been dropped due to error.");
        } catch (dropError) {
          console.error(`Error dropping knowledge_base table: ${dropError}`);
        }

        // Terminate the PostgreSQL connection
        try {
          await this._disconnect();
          console.log("PostgreSQL connection terminated due to error.");
        } catch (connError) {
          console.error(`Error terminating PostgreSQL connection: ${connError}`);
        }
      }

      // Re-raise the exception
      throw e;
    }
  }

  /**
   * Checks if the installation is correct by verifying that the path is empty.
   * If the path is not empty, the knowledge_base table is deleted if present,
   * the database connection is closed, and an exception is raised.
   * If the path is empty, the database connection is closed normally.
   *
   * @returns True if installation check passed
   * @throws Error if the path is not empty
   */
  public async checkInstallation(): Promise<boolean> {
    try {
      if (this.path.length !== 0) {
        // Path is not empty, which is an error condition
        console.log(`Installation check failed: Path is not empty. Path: ${this.path}`);

        // Drop the knowledge_base table if it exists
        if (this.conn) {
          try {
            await this.conn.query("DROP TABLE IF EXISTS knowledge_base;");
            console.log("knowledge_base table has been dropped due to error.");
          } catch (e) {
            console.error(`Error dropping knowledge_base table: ${e}`);
          }
        }

        // Close the database connection
        await this._disconnect();

        // Raise exception
        throw new Error(`Installation check failed: Path is not empty. Path: ${this.path}`);
      }

      // If we reach here, the path is empty, so disconnect normally
      await this._disconnect();
      console.log("Installation check passed: Path is empty and database disconnected.");
      return true;
    } catch (e) {
      // For any other exception, make sure the table is dropped and connection closed
      if (this.conn) {
        try {
          await this.conn.query("DROP TABLE IF EXISTS knowledge_base;");
        } catch (dropError) {
          console.error(`Error during cleanup: ${dropError}`);
        } finally {
          await this._disconnect();
        }
      }

      // Re-raise the original exception
      throw e;
    }
  }
}

export default Construct_KB;
