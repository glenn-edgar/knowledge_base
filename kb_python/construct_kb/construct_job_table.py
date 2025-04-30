import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt  


class Construct_job_Table:
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
            CREATE SCHEMA IF NOT EXISTS job_table;
            CREATE TABLE job_table.job_table(
                id SERIAL PRIMARY KEY,
                path LTREE 
                schedule_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP DEFAULT NOW(),
                is_active BOOLEAN DEFAULT FALSE,
                valid BOOLEAN DEFAULT FALSE,
                data JSON,
                
            );
        """)
        self.cursor.execute(create_table_script)
        self.conn.commit()  # Commit the changes
        print("Status table created.")

    def add_job_field(self, job_key, job_length):
        """
        Add a new status field to the knowledge base
        
        Args:
            job_key (str): The key/name of the status field
            job_length (int): The length of the job
          
            
        Raises:
            TypeError: If status_key is not a string or properties is not a dictionary
        """
        if not isinstance(job_key, str):
            raise TypeError("job_key must be a string")
        
        if not isinstance(job_length, int):
            raise TypeError("job_length must be an integer")
        properties = {'job_length': job_length}
        properties_json = json.dumps(properties)
       
        data_json = ""
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_JOB_QUEUE", job_key, properties_json, data_json)
        
        print(f"Added job field '{job_key}' with properties: {properties_json} and data: {data_json}")
        
        return {
            "status": "success",
            "message": f"Status field '{job_key}' added successfully",
            "properties": properties,
            "data":data_json
        }
        
    def _manage_job_table(self, specified_job_paths, specified_job_length):
        """
        Manages the number of records in job_table.job_table to match specified job lengths for each path.
        Removes older records first if necessary and adds new ones with None for JSON data.
        
        Args:
            specified_job_paths (list): Array of valid LTREE paths
            specified_job_length (list): Array of corresponding lengths for each path
        """
        # Iterate through the arrays of paths and lengths
        for i in range(len(specified_job_paths)):
            path = specified_job_paths[i]
            target_length = specified_job_length[i]
            
            # Get current count for this path
            self.cursor.execute("SELECT COUNT(*) FROM job_table.job_table WHERE path = %s;", (path,))
            current_count = self.cursor.fetchone()[0]
            
            # Calculate the difference
            diff = target_length - current_count
            
            if diff < 0:
                # Need to remove records (oldest first) for this path
                self.cursor.execute("""
                    DELETE FROM job_table.job_table
                    WHERE path = %s AND completed_at IN (
                        SELECT recorded_at 
                        FROM job_table.job_table 
                        WHERE path = %s
                        ORDER BY recorded_at ASC 
                        LIMIT %s
                    );
                """, (path, path, abs(diff)))
                
            elif diff > 0:
                # Need to add records for this path
                for _ in range(diff):
                    self.cursor.execute("""
                        INSERT INTO job_table.job_table (path, schedule_at,started_at,completed_at ,  is_active,vald data)
                        VALUES (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,FALSE,NULL);
                    """, (path,))
        
        # Commit all changes at once
        self.conn.commit()
       
    def _remove_invalid_job_fields(self, invalid_job_paths, chunk_size=500):
        """
        Removes all database entries with paths that match any in the invalid_job_paths array.
        Processes the deletion in chunks to avoid SQL statement limitations.
        
        Args:
            invalid_job_paths (list): Array of LTREE paths that should be removed from the database
            chunk_size (int): Maximum number of paths to process in a single query
        """
        if not invalid_job_paths:
            return  # Nothing to do if array is empty
        
        # Process in chunks to avoid SQL limitations
        for i in range(0, len(invalid_job_paths), chunk_size):
            # Get current chunk
            chunk = invalid_job_paths[i:i + chunk_size]
            
            # Construct placeholders for SQL IN clause
            placeholders = ','.join(['%s'] * len(chunk))
            
            # Delete entries with paths in current chunk
            self.cursor.execute(f"""
                DELETE FROM job_table.job_table
                WHERE path IN ({placeholders});
            """, chunk)
        
        # Commit after all chunks are processed
        self.conn.commit()
    def check_installation(self):     
        """
        Synchronize the knowledge_base and job_table based on paths.
        - Remove entries from job_table that don't exist in knowledge_base with label "KB_JOB_QUEUE"
        - Add entries to status_table for paths in knowledge_base that don't exist in status_table
        """
        
        # Get all paths from status_table
        self.cursor.execute("""
            SELECT DISTINCT path::text FROM job_table.job_table;
        unique_job_paths = [row[0] for row in self.cursor.fetchall()]
        
        # Get specified paths (paths with label "KB_JOB_QUEUE") from knowledge_table
        self.cursor.execute("""
            SELECT path, label, name,properties FROM knowledge_base.knowledge_base 
            WHERE label = 'KB_JOB_QUEUE';
        """)
        specified_job_data = self.cursor.fetchall()
        specified_job_paths = [row[0] for row in specified_job_data]
        specified_job_length = [row[3]['job_length'] for row in specified_job_data]
        invalid_job_paths = [path for path in unique_job_paths if path not in specified_job_paths]
        missing_job_paths = [path for path in specified_job_paths if path not in unique_job_paths]
 
        self._remove_invalid_job_fields(invalid_job_paths)
        self._manage_job_table(self, specified_job_paths,specified_job_length)
        
        