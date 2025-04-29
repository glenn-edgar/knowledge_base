import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt, AsIs


class Construct_KB:
    """
    This class is designed to construct a knowledge base structure with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a PostgreSQL database and sets up the schema.
    """

    def __init__(self, host, port, dbname, user, password, database):
        """
        Initializes the Construct_KB object and connects to the PostgreSQL database.
        Also sets up the database schema.

        Args:
            host (str): The database host.
            port (str): The database port.
            dbname (str): The name of the database.
            user (str): The database user.
            password (str): The password for the database user.
            database (str): (Redundant with dbname, but kept for compatibility)
        """
        self.path = []  # Stack to keep track of the path (levels/nodes)
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None  # Connection obje
        self.cursor = None  # Cursor object
        self._connect()  # Establish the database connection and schema during initialization
        
        
    def get_db_objects(self):
        """
        Returns both the database connection and cursor objects.

        Returns:
            tuple: A tuple containing (connection, cursor)
                - connection (psycopg2.extensions.connection): The PostgreSQL database connection
                - cursor (psycopg2.extensions.cursor): The PostgreSQL database cursor
        """
        return self.conn, self.cursor


    def _connect(self):
        """
        Establishes a connection to the PostgreSQL database and sets up the schema.
        This is a helper method called by __init__.
        """
        self.path_values = {}
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self.cursor = self.conn.cursor()
        print(f"Connected to PostgreSQL database {self.dbname} on {self.host}:{self.port}")

        # Execute the SQL script to set up the schema
        self._setup_schema()

    def _setup_schema(self):
        """
        Sets up the database schema (tables, functions, etc.).
        """
        # Use psycopg2.sql module to construct SQL queries safely. This prevents SQL injection.
        # ltree extension needs to be created.
        create_extensions_script = sql.SQL("""
  
            CREATE EXTENSION IF NOT EXISTS ltree;
        """)
        self.cursor.execute(create_extensions_script)

        # Drop the table if it exists
        drop_table_script = sql.SQL("DROP TABLE IF EXISTS knowledge_base.knowledge_base;")
        self.cursor.execute(drop_table_script)
        # Drop the schema if it exists
        drop_table_script = sql.SQL("DROP SCHEMA IF EXISTS knowledge_base;")
        self.cursor.execute(drop_table_script)
        # Create the knowledge_base table
        create_table_script = sql.SQL("""
            CREATE SCHEMA IF NOT EXISTS knowledge_base;
            CREATE TABLE knowledge_base.knowledge_base (
                id SERIAL PRIMARY KEY,
                label VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                properties JSON,
                data JSON,
                path LTREE UNIQUE
            );
        """)
        self.cursor.execute(create_table_script)
        self.conn.commit()  # Commit the changes
        print("Knowledge base table created.")

    def _disconnect(self):
        """
        Closes the connection to the PostgreSQL database. This is a helper
        method called by check_installation.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print(f"Disconnected from PostgreSQL database {self.dbname} on {self.host}:{self.port}")
        self.cursor = None
        self.conn = None
        
    def add_header_node(self, link, node_name, node_properties, node_data):
        """
        Adds a header node to the knowledge base.

        Args:
            link: The link associated with the header node.
            node_name: The name of the header node.
            node_properties: Properties associated with the header node.
            node_data: Data associated with the header node.
        """
        if not self.conn or not self.cursor:
            raise ValueError("Database connection not established")

        self.path.append(link)
        self.path.append(node_name)
        node_path = ".".join(self.path)

        if node_path in self.path_values:
            raise ValueError(f"Path {node_path} already exists in knowledge base")
        
        self.path_values[node_path] = True
        
        insert_query = sql.SQL("""
            INSERT INTO knowledge_base.knowledge_base (label, name, properties, data, path)
            VALUES (%s, %s, %s, %s, %s);
        """)
        
        # Convert Python dictionaries to JSON strings
        json_properties = json.dumps(node_properties) if node_properties else None
        json_data = json.dumps(node_data) if node_data else None
        
        self.cursor.execute(insert_query, (link, node_name, json_properties, json_data, node_path))
        self.conn.commit()

    def add_info_node(self, link, node_name, node_properties, node_data):
        self.add_header_node(link, node_name, node_properties, node_data)
     
        self.path.pop()  # Remove node_name
        self.path.pop()  # Remove link
        
        
    def leave_header_node(self, label, name):
        """
        Leaves a header node, verifying the label and name.
        If an error occurs, the knowledge_base table is deleted if it exists
        and the PostgreSQL connection is terminated.

        Args:
            label: The expected link of the header node.
            name: The expected name of the header node.
        """
        # Try to pop the expected values
        if not self.path:
            raise ValueError("Cannot leave a header node: path is empty")
        
        ref_name = self.path.pop()
        if not self.path:
            # Put the name back and raise an error
            self.path.append(ref_name)
            raise ValueError("Cannot leave a header node: not enough elements in path")
            
        ref_label = self.path.pop()
        
        # Verify the popped values
        if ref_name != name or ref_label != label:
            # Create a descriptive error message
            error_msg = []
            if ref_name != name:
                error_msg.append(f"Expected name '{name}', but got '{ref_name}'")
            if ref_label != label:
                error_msg.append(f"Expected label '{label}', but got '{ref_label}'")
                
            raise AssertionError(", ".join(error_msg))
        

    def __del__(self):
        """
        Destructor to ensure the database connection is closed when the object
        is garbage collected. This is a safety measure. It's best to
        explicitly call check_installation(), which now calls _disconnect(),
        but this destructor provides a backup.
        """
        self._disconnect()
        
    def check_installation(self):
        """
        Checks if the installation is correct by verifying that the path is empty.
        If the path is not empty, the knowledge_base table is deleted if present,
        the database connection is closed, and an exception is raised.
        If the path is empty, the database connection is closed normally.

        Returns:
            bool: True if installation check passed

        Raises:
            RuntimeError: If the path is not empty
        """
        if len(self.path) != 0:
            # Path is not empty, which is an error condition
            print(f"Installation check failed: Path is not empty. Path: {self.path}")

            # Drop the knowledge_base table if it exists
            if self.conn and self.cursor:
                self.cursor.execute("DROP TABLE IF EXISTS knowledge_base;")
                self.conn.commit()
                print("knowledge_base table has been dropped due to error.")
            
            # Close the database connection
            self._disconnect()
            
            # Raise exception
            raise RuntimeError(f"Installation check failed: Path is not empty. Path: {self.path}")
        
      
        print("Installation check passed: Path is empty and database disconnected.")
        return True

if __name__ == '__main__':
    # Example Usage
    # Replace with your actual database credentials
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "knowledge_base"
    DB_USER = "gedgar"
    DB_PASSWORD = "ready2go"
    DATABASE = "knowledge_base"

    kb = Construct_KB(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATABASE)

    print("Initial state:")
    print(f"Path: {kb.path}")

    kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, "header1_data")
    print("\nAfter add_header_node:")
    print(f"Path: {kb.path}")

    kb.add_info_node("info1_link", "info1_name", {"prop2": "val2"}, "info1_data")
    print("\nAfter add_info_node:")
    print(f"Path: {kb.path}")

    kb.leave_header_node("header1_link", "header1_name")
    print("\nAfter leave_header_node:")
    print(f"Path: {kb.path}")

    kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, "header2_data")
    kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, "info2_data")
    kb.leave_header_node("header2_link", "header2_name")
    print("\nAfter adding and leaving another header node:")
    print(f"Path: {kb.path}")

    # Example of check_installation
    try:
        kb.check_installation()
        kb._disconnect()
    except RuntimeError as e:
        print(f"Error during installation check: {e}")

