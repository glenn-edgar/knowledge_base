import json

class KB_Status_Data:
    """
    A class to handle the status data for the knowledge base.
    """
    def __init__(self, kb_search):
        self.kb_search = kb_search
    
    def find_node_id(self, node_name, properties, node_path):
        """
        Find the node id for a given node name, properties, node path, and data.
        """
        print(node_name, properties, node_path)
        result = self.find_node_ids(node_name, properties, node_path)
        if len(result) == 0:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        if len(result) > 1:
            raise ValueError(f"Multiple nodes found matching path parameters: {node_name}, {properties}, {node_path}")
        return result
    
    def find_node_ids(self, node_name, properties, node_path):
        """
        Find the node id for a given node name, properties, node path :
        """
        print(node_name, properties, node_path)
        self.kb_search.clear_filters()
        self.kb_search.search_label("KB_STATUS_FIELD")
        if node_name is not None:
            self.kb_search.search_name(node_name)
        if properties is not None:
            for key in properties:
                self.kb_search.search_property_value(key, properties[key])
        if node_path is not None:
            self.kb_search.search_path(node_path)
        node_ids = self.kb_search.execute_query()
        
        if node_ids is None:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        if len(node_ids) == 0:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        return node_ids
    
    def find_status_table_keys(self, key_data):
       
            
        return_values = []
        for key in key_data:
            
            return_values.append(key[5])
        return return_values
    

    
    def get_status_data(self, path):
        """
        Retrieve status data for a given path.
        
        Args:
            path (str): The path to search for
            
        Returns:
            tuple: A tuple containing (data_dict, path) where data_dict is a Python dictionary
            
        Raises:
            Exception: If no data is found or a database error occurs
        """
        try:
            # Prepare the SQL query
            query = """
            SELECT data, path
            FROM status_table.status_table
            WHERE path = %s
            """
            
            self.kb_search.cursor.execute(query, (path,))
            result = self.kb_search.cursor.fetchone()
            
            if result is None:
                raise Exception(f"No data found for path: {path}")
            
            # Convert JSON data to Python dictionary
            # PostgreSQL's JSON type should already be converted by the psycopg2 driver
            # but we'll handle both possibilities
            data, path_value = result
            
            # If data is still a string (some older psycopg2 versions), parse it
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    raise Exception(f"Failed to decode JSON data for path: {path}")
                    
            return data, path_value
            
        except Exception as e:
            # If it's not our custom exception, wrap it with more context
            if not str(e).startswith(("No data found", "Failed to decode")):
                raise Exception(f"Error retrieving status data: {str(e)}")
            raise
        
    def set_status_data(self, path, data):
        """
        Update status data for a given path.
        
        Args:
            path (str): The path to update
            data (dict): The data to store
            
        Returns:
            tuple: (bool, str) indicating success/failure and a message
            
        Raises:
            ValueError: If data is not a dictionary
            Exception: If the update fails or a database error occurs
        """
        # Ensure data is a dictionary
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")
        
        try:
            json_data = json.dumps(data)  # Remove indent for storage efficiency
            
            # First check if the record exists
            check_query = """
            SELECT 1 FROM status_table.status_table
            WHERE path = %s
            """
            
            self.kb_search.cursor.execute(check_query, (path,))
            record_exists = self.kb_search.cursor.fetchone() is not None
            
            if record_exists:
                # Update existing record
                update_query = """
                UPDATE status_table.status_table
                SET data = %s
                WHERE path = %s
                RETURNING path
                """
                
                self.kb_search.cursor.execute(update_query, (json_data, path))
                operation = "updated"
            else:
                # Insert new record
                insert_query = """
                INSERT INTO status_table.status_table (path, data)
                VALUES (%s, %s)
                RETURNING path
                """
                
                self.kb_search.cursor.execute(insert_query, (path, json_data))
                operation = "inserted"
            
            # Fetch the result
            result = self.kb_search.cursor.fetchone()
            
            # Commit the transaction
            self.kb_search.conn.commit()
            
            if result is not None:
                return True, f"Successfully {operation} data for path: {result[0]}"
            else:
                # Rollback in case of logical errors
                self.kb_search.conn.rollback()
                raise Exception(f"Database operation completed but no path was returned")
                
        except Exception as e:
            # Rollback the transaction on error
            self.kb_search.conn.rollback()
            
            # Re-raise ValueError, but wrap other exceptions
            if isinstance(e, ValueError):
                raise
            else:
                raise Exception(f"Error setting status data: {str(e)}")