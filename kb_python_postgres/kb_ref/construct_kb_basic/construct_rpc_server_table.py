import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt
import uuid

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

        -- Create table if it doesn't exist
        CREATE TABLE IF NOT EXISTS rpc_server_table.rpc_server_table (
            id SERIAL PRIMARY KEY,
            server_path LTREE NOT NULL,
            
            -- Request information
            request_id UUID NOT NULL DEFAULT gen_random_uuid(),
            rpc_action TEXT NOT NULL DEFAULT 'none',
            request_payload JSONB NOT NULL,
            request_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            
            -- Tag to prevent duplicate transactions
            transaction_tag TEXT NOT NULL,
            
            -- Status tracking
            state TEXT NOT NULL DEFAULT 'empty'
                CHECK (state IN ('empty', 'new_job', 'processing')),
            
            -- Additional useful fields
            priority INTEGER NOT NULL DEFAULT 0,
            
            -- New fields as requested
            processing_timestamp TIMESTAMPTZ DEFAULT NULL,
            completed_timestamp TIMESTAMPTZ DEFAULT NULL,
            rpc_client_queue LTREE
        );
                """)


        self.cursor.execute(create_table_script)
        self.conn.commit()  # Commit the changes
        print("rpc_server table created.")

    def add_rpc_server_field(self, rpc_server_key,queue_depth, description):
        """
        Add a new status field to the knowledge base
        
        Args:
            rpc_server_key (str): The key/name of the status field
            queue_depth (int): The length of the rpc_server
            description (str): The description of the rpc_server
            
        Raises:
            TypeError: If status_key is not a string or properties is not a dictionary
        """
        if not isinstance(rpc_server_key, str):
            raise TypeError("rpc_server_key must be a string")
        
        if not isinstance(queue_depth, int):
            raise TypeError("queue_depth must be an integer")
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        properties = {'queue_depth': queue_depth}
 
       
        data_json = json.dumps({})
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_RPC_SERVER_FIELD", rpc_server_key, properties, data_json,description)

        print(f"Added rpc_server field '{rpc_server_key}' with properties: {properties} and data: {data_json}")
        
        return {
            "status": "success",
            "message": f"RPC server field '{rpc_server_key}' added successfully",
            "properties": properties,
            "data": description
        }
        
            
    def remove_unspecified_entries(self, specified_server_paths):
        """
        Remove entries from rpc_server_table where the server_path is not in the specified list,
        handling large lists of paths efficiently.
        
        Args:
            specified_server_paths (list): List of valid server_paths to keep
            
        Returns:
            int: Number of deleted records
        """
        try:
            if not specified_server_paths:
                print("Warning: No server_paths specified. No entries will be removed.")
                return 0
            
            # Convert paths list to a string format suitable for PostgreSQL ANY operation
            paths_str = ", ".join([f"'{path}'" for path in specified_server_paths])
            
            # Set state to empty for remaining records before deleting unspecified ones
            update_query = f"""
                UPDATE rpc_server_table.rpc_server_table
                SET state = 'empty'
                WHERE server_path ::text IN ({paths_str})
            """
            self.cursor.execute(update_query)
            
            # Use parameterized query for better security and handling of large lists
            delete_query = f"""
                DELETE FROM rpc_server_table.rpc_server_table
                WHERE server_path::text NOT IN ({paths_str})
            """
            
            self.cursor.execute(delete_query)
            deleted_count = self.cursor.rowcount
            
            print(f"Removed {deleted_count} unspecified entries from rpc_server_table")
            return deleted_count
            
        except Exception as e:
            print(f"Error in remove_unspecified_entries: {e}")
            # Consider transaction rollback if necessary
            if hasattr(self, 'conn') and self.conn:
                self.conn.rollback()
            raise Exception(f"Error in remove_unspecified_entries: {e}")

    def adjust_queue_length(self, specified_server_paths, specified_queue_lengths):
        """
        Adjust the number of records for multiple client paths to match their specified queue lengths.
        
        Args:
            specified_server_paths (list): List of client paths to adjust
            specified_queue_lengths (list): List of desired queue lengths corresponding to each client path
            
        Returns:
            dict: A dictionary with client paths as keys and operation results as values
        """
        results = {}
        
        try:
            if len(specified_server_paths) != len(specified_queue_lengths):
                raise ValueError("Mismatch between paths and lengths lists")
                
            for i, server_path in enumerate(specified_server_paths):
                try:
                    target_length = int(specified_queue_lengths[i])
                    
                    # Get current count
                    self.cursor.execute("""
                        SELECT COUNT(*) FROM rpc_server_table.rpc_server_table 
                        WHERE server_path::text = %s
                    """, (server_path,))
                    
                    current_count = self.cursor.fetchone()[0]
                    
                    # Set state to empty for all records with this server_path
                    self.cursor.execute("""
                        UPDATE rpc_server_table.rpc_server_table
                        SET state = 'empty'
                        WHERE server_path::text = %s
                    """, (server_path,))
                    
                    if current_count > target_length:
                        # Need to remove excess records - remove oldest first
                        self.cursor.execute("""
                            DELETE FROM rpc_server_table.rpc_server_table
                            WHERE id IN (
                                SELECT id FROM rpc_server_table.rpc_server_table
                                WHERE server_path::text = %s
                                ORDER BY request_timestamp ASC
                                LIMIT %s
                            )
                        """, (server_path, current_count - target_length))
                        
                        results[server_path] = {
                            'action': 'removed',
                            'count': current_count - target_length,
                            'new_total': target_length
                        }
                        
                    elif current_count < target_length:
                        # Need to add placeholder records
                        records_to_add = target_length - current_count
                        
                        for _ in range(records_to_add):
                            self.cursor.execute("""
                                INSERT INTO rpc_server_table.rpc_server_table (
                                    server_path, request_payload, transaction_tag, state
                                ) VALUES (
                                     %s, %s, %s, 'empty'
                                )
                            """, (
                                 
                                server_path,  # Use server_path as server_path for placeholders
                                '{}',         # Empty JSON object
                                f"placeholder_{uuid.uuid4()}"  # Unique transaction tag
                            ))
                        
                        results[server_path] = {
                            'action': 'added',
                            'count': records_to_add,
                            'new_total': target_length
                        }
                        
                    else:
                        results[server_path] = {
                            'action': 'unchanged',
                            'count': 0,
                            'new_total': current_count
                        }
                        
                except Exception as path_error:
                    print(f"Error adjusting queue for path {server_path}: {path_error}")
                    results[server_path] = {'error': str(path_error)}
                    
            return results
            
        except Exception as e:
            print(f"Error in adjust_queue_length: {e}")
            if hasattr(self, 'conn') and self.conn:
                self.conn.rollback()
            raise Exception(f"Error in adjust_queue_length: {e}")

    def restore_default_values(self):
        """
        Restore default values for all fields in rpc_server_table except for server_path.
        
        This method will:
        1. Generate a unique UUID for request_id for each record
        2. Set server_path to match server_path
        3. Set request_payload to an empty JSON object
        4. Set request_timestamp to current time
        5. Set state to empty
        6. Clear completed_timestamp (set to NULL)
        7. Generate a new transaction_tag
        
        Returns:
            int: Number of records updated
        """
        try:
            # Updated to match the new table structure
            update_query = """
                UPDATE rpc_server_table.rpc_server_table
                SET 
                    request_id = gen_random_uuid(),
                    request_payload = '{}',
                    request_timestamp = NOW(),
                    state = 'empty',
                    completed_timestamp = NULL,
                    transaction_tag = CONCAT('reset_', gen_random_uuid()::text)
            """
            
            self.cursor.execute(update_query)
            updated_count = self.cursor.rowcount
            
            print(f"Restored default values for {updated_count} records")
            return updated_count
            
        except Exception as e:
            print(f"Error in restore_default_values: {e}")
            if hasattr(self, 'conn') and self.conn:
                self.conn.rollback()
            raise Exception(f"Error in restore_default_values: {e}")
        
        
    
    def check_installation(self):     
 
    
        
        # Get specified paths (paths with label "KB_RPC_SERVER_FIELD") from knowledge_table
        self.cursor.execute("""
            SELECT * FROM knowledge_base.knowledge_base 
            WHERE label = 'KB_RPC_SERVER_FIELD';
        """)
        
        specified_paths_data = self.cursor.fetchall()
        
        paths = []
        lengths = []
        for row in specified_paths_data:
            paths.append(row[5])
            properties = row[3]
            lengths.append(properties['queue_depth'])
        print(f"paths: {paths}",f"lengths: {lengths}")
  
        self.remove_unspecified_entries(paths)
        self.adjust_queue_length(paths,lengths)
        self.restore_default_values()
    
    