import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt

class Construct_Status_Table:
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
            CREATE SCHEMA IF NOT EXISTS status_table;
            CREATE TABLE IF NOT EXISTS status_table.status_table(
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
        print("Status table created.")

    def add_status_field(self, status_key,  description,initial_data):
        """
        Add a new status field to the knowledge base
        
        Args:
            status_key (str): The key/name of the status field
            initial_properties (dict): Initial properties for the status field
            description (str): The description of the status field
            initial_data (dict): Initial data for the status field
            
        Raises:
            TypeError: If status_key is not a string or initial_properties is not a dictionary
        """
        if not isinstance(status_key, str):
            raise TypeError("status_key must be a string")
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        if not isinstance(initial_data, dict):
            raise TypeError("initial_data must be a dictionary")
            
        # Convert dictionaries to JSON strings
        properties_json = json.dumps(initial_data)
        data_json = json.dumps(description)
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_STATUS_FIELD", status_key, properties_json, data_json)
        
        print(f"Added status field '{status_key}' with properties: {properties_json} and data: {data_json}")
        
        return {
            "status": "success",
            "message": f"Status field '{status_key}' added successfully",
            "properties": initial_data,
            "data": description
        }
    
    def check_installation(self):     
        """
        Synchronize the knowledge_base and status_table based on paths.
        - Remove entries from status_table that don't exist in knowledge_base with label "KB_STATUS_FIELD"
        - Add entries to status_table for paths in knowledge_base that don't exist in status_table
        """
        # Get all paths from status_table
        self.cursor.execute("""
            SELECT path FROM status_table.status_table;
        """)
        all_paths = [row[0] for row in self.cursor.fetchall()]
        
        # Get specified paths (paths with label "KB_STATUS_FIELD") from knowledge_table
        self.cursor.execute("""
            SELECT path, label, name FROM knowledge_base.knowledge_base 
            WHERE label = 'KB_STATUS_FIELD';
        """)
        specified_paths_data = self.cursor.fetchall()
        specified_paths = [row[0] for row in specified_paths_data]
        
        # Find missing_paths: paths in specified_paths that are not in all_paths
        missing_paths = [path for path in specified_paths if path not in all_paths]
        
        # Find not_specified_paths: paths in all_paths that are not in specified_paths
        not_specified_paths = [path for path in all_paths if path not in specified_paths]
        
        # Process not_specified_paths: remove entries from status_table
        for path in not_specified_paths:
            self.cursor.execute("""
                DELETE FROM status_table.status_table
                WHERE path = %s;
            """, (path,))
        
        # Process missing_paths: add entries to status_table
        for path in missing_paths:
            # Find the corresponding data in specified_paths_data
            for sp_path, label, name in specified_paths_data:
                if sp_path == path:
                    # Insert with empty JSON objects for properties and data
                    self.cursor.execute("""
                        INSERT INTO status_table.status_table 
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
        
   
    
    def diag_function(self):
        """
        Diagnostic function that displays label, name, and node ID for all values
        in both knowledge_base and status_table where label = 'KB_STATUS_FIELD'
        
        Returns:
            dict: Dictionary containing diagnostic information from both tables
        """
        results = {
            "knowledge_table": [],
            "status_table": []
        }
        
        # Query knowledge_base table
        self.cursor.execute("""
            SELECT id, label, name, path::text
            FROM knowledge_base.knowledge_base 
            WHERE label = 'KB_STATUS_FIELD'
            ORDER BY path;
        """)
        kb_rows = self.cursor.fetchall()
        
        # Query status_table
        self.cursor.execute("""
            SELECT id, label, name, path::text
            FROM status_table.status_table
            WHERE label = 'KB_STATUS_FIELD'
            ORDER BY path;
        """)
        st_rows = self.cursor.fetchall()
        
        # Process knowledge_base results
        print("=== KNOWLEDGE BASE TABLE ENTRIES ===")
        if kb_rows:
            # Print header
            print(f"{'ID':<6} {'LABEL':<20} {'NAME':<30} {'PATH'}")
            print("-" * 80)
            
            # Print data rows
            for row in kb_rows:
                node_id, label, name, path = row
                print(f"{node_id:<6} {label:<20} {name:<30} {path}")
                results["knowledge_table"].append({
                    "id": node_id,
                    "label": label,
                    "name": name,
                    "path": path
                })
        else:
            print("No entries with label 'KB_STATUS_FIELD' found")
        
        # Process status_table results
        print("\n=== STATUS TABLE ENTRIES ===")
        if st_rows:
            # Print header
            print(f"{'ID':<6} {'LABEL':<20} {'NAME':<30} {'PATH'}")
            print("-" * 80)
            
            # Print data rows
            for row in st_rows:
                node_id, label, name, path = row
                print(f"{node_id:<6} {label:<20} {name:<30} {path}")
                results["status_table"].append({
                    "id": node_id,
                    "label": label,
                    "name": name,
                    "path": path
                })
        else:
            print("No entries with label 'KB_STATUS_FIELD' found")
        
        # Print summary
        print("\n=== SUMMARY ===")
        print(f"Knowledge Base: {len(kb_rows)} entries with label 'KB_STATUS_FIELD'")
        print(f"Status Table: {len(st_rows)} entries with label 'KB_STATUS_FIELD'")
        
        # Return the raw data as well for programmatic use
        return results