import json

class KB_Status_Data:
    """
    A class to handle the status data for the knowledge base.
    """
    def __init__(self, kb_search):
        self.kb_search = kb_search
    
    def find_node_id(self, node_name, properties, node_path, data):
        """
        Find the node id for a given node name, properties, node path, and data.
        """
        self.kb_search.clear_filters()
        self.kb_search.search_label("KB_STATUS_FIELD")
        self.kb_search.search_name(node_name)
        for key in properties:
            self.kb_search.search_property_value(key, properties[key])
        self.kb_search.search_path(node_path)
        self.kb_search.execute_query()
        node_id = self.kb_search.cursor.fetchone()[0]
        if node_id is None:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}, {data}")
        return node_id
    
    def get_status_data(self, path):
        # Prepare the SQL query
        query = """
        SELECT data
        FROM status_table.status_table
        WHERE path = %s;
        """
        
        # Execute the query
        self.kb_search.cursor.execute(query, (path,))
        
        # Fetch the result
        result = self.kb_search.cursor.fetchone()
        if result is None:
            raise ValueError(f"No data found for path: {path}")
        return json.loads(result[0])
    
    def set_status_data(self, path, data):
        # Ensure data is a dictionary
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")
        
        json_data = json.dumps(data, indent=4)
        
        # Prepare the SQL query to update the data column
        update_query = """
        UPDATE status_table.status_table
        SET data = %s
        WHERE path = %s
        RETURNING id;
        """
        
        # Execute the update query
        self.kb_search.cursor.execute(update_query, (json_data, path))
        
        # Fetch the result
        updated_id = self.kb_search.cursor.fetchone()
        
        self.kb_search.conn.commit()
        
        if updated_id:
            return True, f"Successfully updated data for record with ID: {updated_id[0]}"
        else:
            raise ValueError("Update operation failed for unknown reason")
