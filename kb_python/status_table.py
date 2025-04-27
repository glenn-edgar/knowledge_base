

import psycopg2
import json
import kb_query_support

class Status_Table(KB_Search):
    def __init__(self, conn, cursor):
        """
        Initialize Status_Table with PostgreSQL connection and cursor
        
        Parameters:
        conn: PostgreSQL connection object
        cursor: PostgreSQL cursor object
        """
        self.conn = conn
        self.cursor = cursor
        
        # Call parent class method to set connection and cursor
        self.set_conn_and_cursor(conn, cursor)
        
        # Create status_table if it doesn't exist
        self._create_table()
    
    def _create_table(self):
        """Create the status_table if it doesn't exist"""
        create_table_query = """
        CREATE EXTENSION IF NOT EXISTS ltree;
        
        CREATE TABLE IF NOT EXISTS status_table (
            id SERIAL PRIMARY KEY,
            path ltree UNIQUE NOT NULL,
            properties jsonb NOT NULL,
            data jsonb NOT NULL
        );
        """
        self.cursor.execute(create_table_query)
        self.conn.commit()
        
    def find_path(self, name, path):
        """
        Find the requested path based on name and path criteria
        
        Parameters:
        name: Node name to search for (can be None)
        path: Path pattern to search for
        
        Returns:
        The path of the matching node
        """
        self.clear_filter()
        self.search_label("STATUS_NODE")
        if name != None:
            self.search_name(name)
        self.search_path(path)
        self.execute_query()
        temp = self.get_results()
        
        if len(temp) == 0:
            raise ValueError("Node not found")
        if len(temp) != 1:
            raise ValueError("Multiple nodes found")
        
        return temp[0]['path']  
    
    
    def setValue(self, path, key, value):
        """
        Update or set a value in the data field for a specific path and key
        
        Parameters:
        path: ltree path to find the record
        key: key to update in the data field
        value: value to set
        """
        # First, check if the path exists
        self.cursor.execute(
            "SELECT id, properties, data FROM status_table WHERE path = %s",
            (path,)
        )
        result = self.cursor.fetchone()
        
        if not result:
            raise ValueError(f"Path '{path}' not found in status_table")
        
        row_id, properties, data = result
        
        # Check if key exists in properties
        if key not in properties:
            raise ValueError(f"Key '{key}' not found in properties for path '{path}'")
        
        # Update the data field
        data[key] = value
        
        self.cursor.execute(
            "UPDATE status_table SET data = %s WHERE id = %s",
            (json.dumps(data), row_id)
        )
        self.conn.commit()
    
    def getValue(self, path, key):
        """
        Get a value from the data field for a specific path and key
        
        Parameters:
        path: ltree path to find the record
        key: key to retrieve from the data field
        
        Returns:
        The value associated with the key
        """
        # Find the record with the matching path
        self.cursor.execute(
            "SELECT properties, data FROM status_table WHERE path = %s",
            (path,)
        )
        result = self.cursor.fetchone()
        
        if not result:
            raise ValueError(f"Path '{path}' not found in status_table")
        
        properties, data = result
        
        # Validate key exists in properties
        if key not in properties:
            raise ValueError(f"Key '{key}' not found in properties for path '{path}'")
        
        # Return the value from data
        return data.get(key)
```

The code now has all exception handlers removed. Any errors that occur during database operations or validation will be raised directly for the user to handle.