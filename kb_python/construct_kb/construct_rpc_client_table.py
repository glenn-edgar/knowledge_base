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
             CREATE TABLE  IF NOT EXISTS  rpc_client_table.rpc_client_table (
                id SERIAL PRIMARY KEY,
                
                -- Reference to the request
                request_id UUID NOT NULL,
                
                -- Path to identify the RPC client queue for routing responses
                client_path ltree NOT NULL,
                server_path ltree NOT NULL,
                
                -- Response information
                response_payload JSONB NOT NULL,
                response_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- UTC timestamp
                
                -- Boolean to identify new/unprocessed results
                is_new_result BOOLEAN NOT NULL DEFAULT TRUE,
                
                -- Additional useful fields
                error_message TEXT
            );
        """)
        self.cursor.execute(create_table_script)
        self.conn.commit()  # Commit the changes
        print("rpc_client table created.")

    def add_rpc_client_field(self, rpc_client_key,queue_depth, description):
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
        if not isinstance(queue_depth, int):
            raise TypeError("queue_depth must be an integer")
        
        properties = {"queue_depth": queue_depth}
        
        
        # Convert dictionaries to JSON strings
        data_json = json.dumps({})
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_RPC_CLIENT_FIELD", rpc_client_key, properties, data_json,description)
        
        print(f"Added rpc_client field '{rpc_client_key}' with properties: {properties} and data: {data_json}")
        
        return {
            "rpc_client": "success",
            "message": f"rpc_client field '{rpc_client_key}' added successfully",
            "properties": properties,
            "data": description
        }
    
    def remove_unspecified_entries(self, specified_client_paths):
        """
        Remove entries from rpc_client_table where the client_path is not in the specified list,
        handling large lists of paths efficiently.
        
        Args:
            specified_client_paths (list): List of valid client_paths to keep
            
        Returns:
            int: Number of deleted records
        """
        try:
            # Create a temporary table to store valid paths
            self.cursor.execute("""
                CREATE TEMP TABLE valid_paths (path text) ON COMMIT DROP
            """)
            
            # Insert paths in batches to avoid parameter limits
            batch_size = 1000
            for i in range(0, len(specified_client_paths), batch_size):
                batch = specified_client_paths[i:i+batch_size]
                args = [(path,) for path in batch]
                self.cursor.executemany("""
                    INSERT INTO valid_paths VALUES (%s)
                """, args)
            
            # Delete records where path is not in our temp table
            self.cursor.execute("""
                DELETE FROM rpc_client_table.rpc_client_table
                WHERE client_path::text NOT IN (
                    SELECT path FROM valid_paths
                )
                RETURNING id
            """)
            
            # Get count of deleted records
            deleted_count = len(self.cursor.fetchall())
            
            # Commit the transaction (also drops the temp table due to ON COMMIT DROP)
            self.conn.commit()
            
            return deleted_count
        
        except Exception as e:
            # Roll back in case of error
            self.conn.rollback()
            raise Exception(f"Error removing unspecified entries: {str(e)}")
        
    def adjust_queue_length(self, specified_client_paths, specified_queue_lengths):
        """
        Adjust the number of records for multiple client paths to match their specified queue lengths.
        
        Args:
            specified_client_paths (list): List of client paths to adjust
            specified_queue_lengths (list): List of desired queue lengths corresponding to each client path
            
        Returns:
            dict: A dictionary with client paths as keys and operation results as values
        """
        if len(specified_client_paths) != len(specified_queue_lengths):
            raise ValueError("The specified_client_paths and specified_queue_lengths lists must be of equal length")
        
        results = {}
        
        try:
            # Start transaction
            self.conn.autocommit = False
            
            # Process each client path with its corresponding queue length
            for i, client_path in enumerate(specified_client_paths):
                queue_length = specified_queue_lengths[i]
                
                # Skip invalid queue lengths
                if queue_length < 0:
                    results[client_path] = {"error": "Invalid queue length (negative)"}
                    continue
                    
                # Get current count of records for this client path
                self.cursor.execute("""
                    SELECT COUNT(*) 
                    FROM rpc_client_table.rpc_client_table 
                    WHERE client_path = %s::ltree
                """, (client_path,))
                
                current_count = self.cursor.fetchone()[0]
                
                path_result = {'added': 0, 'removed': 0}
                
                # If we need to remove records (current count exceeds specified length)
                if current_count > queue_length:
                    records_to_remove = current_count - queue_length
                    
                    # Remove oldest records (based on response_timestamp)
                    self.cursor.execute("""
                        DELETE FROM rpc_client_table.rpc_client_table
                        WHERE id IN (
                            SELECT id
                            FROM rpc_client_table.rpc_client_table
                            WHERE client_path = %s::ltree
                            ORDER BY response_timestamp ASC
                            LIMIT %s
                        )
                        RETURNING id
                    """, (client_path, records_to_remove))
                    
                    path_result['removed'] = len(self.cursor.fetchall())
                
                # If we need to add records (current count is less than specified length)
                elif current_count < queue_length:
                    records_to_add = queue_length - current_count
                    
                    # Generate a UUID for the request_id
                    self.cursor.execute("SELECT gen_random_uuid()")
                    new_request_id = self.cursor.fetchone()[0]
                    
                    # Prepare batch insert for better performance
                    insert_values = []
                    for _ in range(records_to_add):
                        insert_values.append((
                            new_request_id,
                            client_path,
                            client_path,  # Using client path as server path by default
                            '{}',  # Empty JSON object as default payload
                        ))
                    
                    # Use executemany for batch insert
                    self.cursor.executemany("""
                        INSERT INTO rpc_client_table.rpc_client_table
                        (request_id, client_path, server_path, response_payload, is_new_result)
                        VALUES
                        (%s, %s::ltree, %s::ltree, %s::jsonb, FALSE)
                    """, insert_values)
                    
                    path_result['added'] = records_to_add
                
                results[client_path] = path_result
            
            # Commit the transaction
            self.conn.commit()
            self.conn.autocommit = True
            
            return results
            
        except Exception as e:
            # Roll back in case of error
            self.conn.rollback()
            self.conn.autocommit = True
            raise Exception(f"Error adjusting queue lengths: {str(e)}")      
        
    def restore_default_values(self):
        """
        Restore default values for all fields in rpc_client_table except for client_path.
        
        This method will:
        1. Generate a unique UUID for request_id for each record
        2. Set server_path to match client_path
        3. Set response_payload to an empty JSON object
        4. Set response_timestamp to current time
        5. Set is_new_result to TRUE
        6. Clear error_message (set to NULL)
        
        Returns:
            int: Number of records updated
        """
        try:
            # Start transaction
            self.conn.autocommit = False
            
            # Update all records with default values while preserving client_path
            # Each record gets a unique UUID through the subquery
            self.cursor.execute("""
                UPDATE rpc_client_table.rpc_client_table
                SET 
                    request_id = (SELECT gen_random_uuid()),  -- Unique UUID per record
                    server_path = client_path,  -- Set server_path to match client_path
                    response_payload = '{}'::jsonb,
                    response_timestamp = NOW(),
                    is_new_result = FALSE,
                    error_message = NULL
                RETURNING id
            """)
            
            # Get count of updated records
            updated_count = len(self.cursor.fetchall())
            
            # Commit the transaction
            self.conn.commit()
            self.conn.autocommit = True
            
            return updated_count
            
        except Exception as e:
            # Roll back in case of error
            self.conn.rollback()
            self.conn.autocommit = True
            raise Exception(f"Error restoring default values: {str(e)}")     
            
    def check_installation(self):     

        try:
            self.cursor.execute("""
            SELECT * FROM knowledge_base.knowledge_base 
            WHERE label = 'KB_RPC_CLIENT_FIELD';
             """)
            
            specified_paths_data = self.cursor.fetchall()
            
            paths = []
            lengths = []
            for row in specified_paths_data:
                paths.append(row[5])
                properties = row[3]
                lengths.append(properties['queue_depth'])
            # Create a dictionary with path as key and other fields as a nested dictionary
          
        except Exception as e:
            raise Exception(f"Error retrieving knowledge base fields: {str(e)}")
        self.remove_unspecified_entries(paths)
        self.adjust_queue_length(paths,lengths)
        self.restore_default_values()
    
    