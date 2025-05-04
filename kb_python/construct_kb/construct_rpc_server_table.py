import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt

class Construct_RPC_Server_Table:
    """
    This class is designed to construct a status table with header
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
            CREATE SCHEMA IF NOT EXISTS rpc_server_table;
            CREATE TABLE IF NOT EXISTS rpc_server_table.rpc_server_table(
                path LTREE,
                posted_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP DEFAULT NOW(),
                data JSON,
                valid BOOLEAN DEFAULT FALSE
            );
        """)
        self.cursor.execute(create_table_script)
        self.conn.commit()  # Commit the changes
        print("rpc_server table created.")

    def add_rpc_server_field(self, rpc_server_key, rpc_server_length,info_data  ):
        """
        Add a new status field to the knowledge base
        
        Args:
            rpc_server_key (str): The key/name of the status field
            rpc_server_length (int): The length of the rpc_server
          
            
        Raises:
            TypeError: If status_key is not a string or properties is not a dictionary
        """
        if not isinstance(rpc_server_key, str):
            raise TypeError("rpc_server_key must be a string")
        
        if not isinstance(rpc_server_length, int):
            raise TypeError("rpc_server_length must be an integer")
        properties = {'rpc_server_length': rpc_server_length}
        properties_json = json.dumps(properties)
       
        data_json = json.dumps(info_data)
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_RPC_SERVER_FIELD", rpc_server_key, properties_json, data_json)
        
        print(f"Added rpc_server field '{rpc_server_key}' with properties: {properties_json} and data: {data_json}")
        
        return {
            "status": "success",
            "message": f"RPC server field '{rpc_server_key}' added successfully",
            "properties": properties,
            "data": info_data
        }
        
        
    def _remove_invalid_rpc_server_fields(self, invalid_rpc_server_paths, chunk_size=500):
        """
        Removes all database entries with paths that match any in the invalid_rpc_server_paths array.
        Processes the deletion in chunks to avoid SQL statement limitations.
        
        Args:
            invalid_rpc_server_paths (list): Array of LTREE paths that should be removed from the database
            chunk_size (int): Maximum number of paths to process in a single query
        """
        if not invalid_rpc_server_paths:
            return  # Nothing to do if array is empty
        
        # Process in chunks to avoid SQL limitations
        for i in range(0, len(invalid_rpc_server_paths), chunk_size):
            # Get current chunk
            chunk = invalid_rpc_server_paths[i:i + chunk_size]
            
            # Construct placeholders for SQL IN clause
            placeholders = ','.join(['%s'] * len(chunk))
            
            # Delete entries with paths in current chunk
            self.cursor.execute(f"""
                DELETE FROM rpc_server_table.rpc_server_table
                WHERE path IN ({placeholders});
            """, chunk)
        
        # Commit after all chunks are processed
        self.conn.commit()
        
    def _manage_rpc_server_table(self, specified_rpc_server_paths, specified_rpc_server_length):
        """
        Manages the number of records in server_table.job_table to match specified server lengths for each path.
        Removes older records first if necessary and adds new ones with None for JSON data.
        
        Args:
            specified_server_paths (list): Array of valid LTREE paths
            specified_server_length (list): Array of corresponding lengths for each path
        """
        # Iterate through the arrays of paths and lengths
        for i in range(len(specified_rpc_server_paths)):
            path = specified_rpc_server_paths[i]
            target_length = specified_rpc_server_length[i]
            
            # Get current count for this path
            self.cursor.execute("SELECT COUNT(*) FROM rpc_server_table.rpc_server_table WHERE path = %s;", (path,))
            current_count = self.cursor.fetchone()[0]
            
            # Calculate the difference
            diff = target_length - current_count
            
            if diff < 0:
                # Need to remove records (oldest first) for this path
                self.cursor.execute("""
                    DELETE FROM rpc_server_table.rpc_server_table
                    WHERE path = %s AND posted_at IN (
                        SELECT posted_at 
                        FROM rpc_server_table.rpc_server_table 
                        WHERE path = %s
                        ORDER BY posted_at ASC 
                        LIMIT %s
                    );
                """, (path, path, abs(diff)))
                
            elif diff > 0:
                # Need to add records for this path
                for _ in range(diff):
                    self.cursor.execute("""
                        INSERT INTO rpc_server_table.rpc_server_table (path, posted_at, started_at, completed_at, data, valid)
                        VALUES (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, FALSE);
                    """, (path,))
        
        # Commit all changes at once
        self.conn.commit()
        
        
    def check_installation(self):     
        """
        Synchronize the knowledge_base and rpc_server_table based on paths.
        - Remove entries from status_table that don't exist in knowledge_base with label "KB_STATUS_FIELD"
        - Add entries to status_table for paths in knowledge_base that don't exist in status_table
        """
        
        # Get all paths from status_table
        self.cursor.execute("""
            SELECT DISTINCT path::text FROM rpc_server_table.rpc_server_table;
        """)
        unique_rpc_server_paths = [row[0] for row in self.cursor.fetchall()]
        
        # Get specified paths (paths with label "KB_RPC_SERVER_FIELD") from knowledge_table
        self.cursor.execute("""
            SELECT path, label, name,properties FROM knowledge_base.knowledge_base 
            WHERE label = 'KB_RPC_SERVER_FIELD';
        """)
        specified_rpc_server_data = self.cursor.fetchall()
        specified_rpc_server_paths = [row[0] for row in specified_rpc_server_data]
       
   
     # Extract rpc_server_length values from specified data
        specified_rpc_server_length = []
        for row in specified_rpc_server_data:
            properties_json = row[3]
            properties = json.loads(properties_json)
            length = properties['rpc_server_length']
            specified_rpc_server_length.append(length)

        invalid_rpc_server_paths = [path for path in unique_rpc_server_paths if path not in specified_rpc_server_paths]
        missing_rpc_server_paths = [path for path in specified_rpc_server_paths if path not in unique_rpc_server_paths]
        self._remove_invalid_rpc_server_fields(invalid_rpc_server_paths)
        self._manage_rpc_server_table(specified_rpc_server_paths,specified_rpc_server_length)
