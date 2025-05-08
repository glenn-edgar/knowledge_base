import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt

class Construct_Stream_Table:
    """
    This class is designed to construct a stream table with header
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
            CREATE SCHEMA IF NOT EXISTS stream_table;
            CREATE TABLE IF NOT EXISTS stream_table.stream_table(
                id SERIAL PRIMARY KEY,
                path LTREE,
                recorded_at TIMESTAMP DEFAULT NOW(),
                data JSON
              
            );
        """)
        self.cursor.execute(create_table_script)
        self.conn.commit()  # Commit the changes
        print("stream table created.")

    def add_stream_field(self, stream_key, stream_length, description):
        """
        Add a new stream field to the knowledge base
        
        Args:
            stream_key (str): The key/name of the stream field
            stream_length (int): The length of the stream
          
            
        Raises:
            TypeError: If stream_key is not a string or properties is not a dictionary
        """
        if not isinstance(stream_key, str):
            raise TypeError("stream_key must be a string")
        
        if not isinstance(stream_length, int):
            raise TypeError("stream_length must be an integer")
        properties = {"stream_length": stream_length}
       
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_STREAM_FIELD", stream_key, properties, description)
        
        print(f"Added stream field '{stream_key}' with properties: {properties} ")
        
        return {
            "stream": "success",
            "message": "stream field '{stream_key}' added successfully",
            "properties": properties,
            "data": description
        }
        
        
    def _remove_invalid_stream_fields(self, invalid_stream_paths, chunk_size=500):
        """
        Removes all database entries with paths that match any in the invalid_stream_paths array.
        Processes the deletion in chunks to avoid SQL statement limitations.
        
        Args:
            invalid_stream_paths (list): Array of LTREE paths that should be removed from the database
            chunk_size (int): Maximum number of paths to process in a single query
        """
        if not invalid_stream_paths:
            return  # Nothing to do if array is empty
        
        # Process in chunks to avoid SQL limitations
        for i in range(0, len(invalid_stream_paths), chunk_size):
            # Get current chunk
            chunk = invalid_stream_paths[i:i + chunk_size]
            
            # Construct placeholders for SQL IN clause
            placeholders = ','.join(['%s'] * len(chunk))
            
            # Delete entries with paths in current chunk
            self.cursor.execute("""
                DELETE FROM stream_table.stream_table
                WHERE path IN ({placeholders});
            """, chunk)
        
        # Commit after all chunks are processed
        self.conn.commit()
        
    def _manage_stream_table(self, specified_stream_paths, specified_stream_length):
        """
        Manages the number of records in stream_table.job_table to match specified stream lengths for each path.
        Removes older records first if necessary and adds new ones with None for JSON data.
        
        Args:
            specified_stream_paths (list): Array of valid LTREE paths
            specified_stream_length (list): Array of corresponding lengths for each path
        """
        # Iterate through the arrays of paths and lengths
        for i in range(len(specified_stream_paths)):
            path = specified_stream_paths[i]
            target_length = specified_stream_length[i]
            
            # Get current count for this path
            self.cursor.execute("SELECT COUNT(*) FROM stream_table.stream_table WHERE path = %s;", (path,))
            current_count = self.cursor.fetchone()[0]
            
            # Calculate the difference
            diff = target_length - current_count
            
            if diff < 0:
                # Need to remove records (oldest first) for this path
                self.cursor.execute("""
                    DELETE FROM stream_table.stream_table
                    WHERE path = %s AND recorded_at IN (
                        SELECT recorded_at 
                        FROM stream_table.stream_table 
                        WHERE path = %s
                        ORDER BY recorded_at ASC 
                        LIMIT %s
                    );
                """, (path, path, abs(diff)))
                
            elif diff > 0:
                # Need to add records for this path
                for _ in range(diff):
                    self.cursor.execute("""
                        INSERT INTO stream_table.stream_table (path, recorded_at, data)
                        VALUES (%s, CURRENT_TIMESTAMP, '{}');
                    """, (path,))
        
        # Commit all changes at once
        self.conn.commit()
        
        
    def check_installation(self):     
        """
        Synchronize the knowledge_base and stream_table based on paths.
        - Remove entries from stream_table that don't exist in knowledge_base with label "KB_stream_FIELD"
        - Add entries to stream_table for paths in knowledge_base that don't exist in stream_table
        """
        
        # Get all paths from stream_table
        self.cursor.execute("""
            SELECT DISTINCT path::text FROM stream_table.stream_table; 
        """)
        unique_stream_paths = [row[0] for row in self.cursor.fetchall()]
        print(f"unique_stream_paths: {unique_stream_paths}")
        # Get specified paths (paths with label "KB_stream_FIELD") from knowledge_table
        self.cursor.execute("""
            SELECT path, label, name,properties FROM knowledge_base.knowledge_base 
            WHERE label = 'KB_STREAM_FIELD';
        """ )
        specified_stream_data = self.cursor.fetchall()
        print(f"specified_stream_data: {specified_stream_data}")
        specified_stream_paths = [row[0] for row in specified_stream_data]
        specified_stream_length = [row[3]['stream_length'] for row in specified_stream_data]
        print(f"specified_stream_paths: {specified_stream_paths}")
        print(f"specified_stream_length: {specified_stream_length}")
        invalid_stream_paths = [path for path in unique_stream_paths if path not in specified_stream_paths]
        missing_stream_paths = [path for path in specified_stream_paths if path not in unique_stream_paths]
 
        self._remove_invalid_stream_fields(invalid_stream_paths)
        self._manage_stream_table(specified_stream_paths,specified_stream_length)
    
    