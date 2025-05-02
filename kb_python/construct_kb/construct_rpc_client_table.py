import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt

class Construct_RPC_Client_Table:
    """
    This class is designed to construct a rpc_client table with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a PostgreSQL database and sets up the schema.
    """
    def __init__(self, conn, cursor,construct_kb):
        self.conn = conn
        self.cursor = cursor
        self.construct_kb = construct_kb
 # Execute the SQL script to set up the schema
        self._setup_schema()

    def _setup_schema(self):
        """
        Sets up the database schema (tables, functions, etc.).
   
        # Use psycopg2.sql module to construct SQL queries safely. This prevents SQL injection.
        # ltree extension needs to be created.
        """
        create_extensions_script = sql.SQL("""
            CREATE EXTENSION IF NOT EXISTS ltree;
        """)
        
        # Create the knowledge_base table
        create_table_script = sql.SQL("""
            CREATE SCHEMA IF NOT EXISTS rpc_client_table;
            CREATE TABLE IF NOT EXISTS rpc_client_table.rpc_client_table(
                id SERIAL PRIMARY KEY,
                waiting_for_response BOOLEAN DEFAULT FALSE,
                queue_at TIMESTAMP DEFAULT NOW(),
                processed_at TIMESTAMP DEFAULT NOW(),
                data JSON,
                path LTREE UNIQUE
            );
        """)
        self.cursor.execute(create_table_script)
        self.conn.commit()  # Commit the changes
        print("rpc_client table created.")

    def add_rpc_client_field(self, rpc_client_key, description):
        """
        Add a new rpc_client field to the knowledge base
        
        Args:
            rpc_client_key (str): The key/name of the rpc_client field
            description (str): The description of the rpc_client field
            
        Raises:
            TypeError: If rpc_client_key is not a string or initial_properties is not a dictionary
        """
        if not isinstance(rpc_client_key, str):
            raise TypeError("rpc_client_key must be a string")
        if not isinstance(description, str):
            raise TypeError("description must be a string")
       
        properties_json = json.dumps("")
        # Convert dictionaries to JSON strings
        data_json = json.dumps(description)
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_RPC_CLIENT_FIELD", rpc_client_key, properties_json, data_json)
        
        print(f"Added rpc_client field '{rpc_client_key}' with properties: {properties_json} and data: {data_json}")
        
        return {
            "rpc_client": "success",
            "message": f"rpc_client field '{rpc_client_key}' added successfully",
            "properties": "",
            "data": description
        }
    
    def check_installation(self):     
        """
        Synchronize the knowledge_base and rpc_client_table based on paths.
        - Remove entries from rpc_client_table that don't exist in knowledge_base with label "KB_RPC_CLIENT_FIELD"
        - Add entries to rpc_client_table for paths in knowledge_base that don't exist in rpc_client_table
        """
        # Get all paths from rpc_client_table
        self.cursor.execute("""
            SELECT path FROM rpc_client_table.rpc_client_table;
        """)
        all_paths = [row[0] for row in self.cursor.fetchall()]
            
        # Get specified paths (paths with label "KB_RPC_CLIENT_FIELD") from knowledge_table
        self.cursor.execute("""
            SELECT path, label, name FROM knowledge_base.knowledge_base 
            WHERE label = 'KB_RPC_CLIENT_FIELD';
        """)
        specified_paths_data = self.cursor.fetchall()
        specified_paths = [row[0] for row in specified_paths_data]
        
        # Find missing_paths: paths in specified_paths that are not in all_paths
        missing_paths = [path for path in specified_paths if path not in all_paths]
        
        # Find not_specified_paths: paths in all_paths that are not in specified_paths
        not_specified_paths = [path for path in all_paths if path not in specified_paths]
        
        # Process not_specified_paths: remove entries from rpc_client_table
        for path in not_specified_paths:
            self.cursor.execute("""
                DELETE FROM rpc_client_table.rpc_client_table
                WHERE path = %s;
            """, (path,))
        
        # Process missing_paths: add entries to rpc_client_table
        for path in missing_paths:
            # Find the corresponding data in specified_paths_data
            for sp_path, label, name in specified_paths_data:
                if sp_path == path:
                    # Insert with empty JSON objects for properties and data
                    self.cursor.execute("""
                        INSERT INTO rpc_client_table.rpc_client_table 
                        (label, name, properties, data, path)
                        VALUES (%s, %s, %s, %s, %s);
                    """, (label, name, '{}', '{}', path))
                    break
        
        # Commit the changes
        self.conn.commit()
        
        return {
            "missing_paths_added": len(missing_paths),
            "not_specified_paths_removed": len(not_specified_paths)
        }
        
   
    
    