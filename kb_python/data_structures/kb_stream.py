import json

class KB_Stream:
    """
    A class to handle the status data for the knowledge base.
    """
    def __init__(self, kb_search):
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        self.cursor = self.kb_search.cursor 
    
    def find_stream_id(self, node_name, properties, node_path):
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
    
    def find_stream_ids(self, node_name, properties, node_path):
        """
        Find the node id for a given node name, properties, node path :
        """
        print(node_name, properties, node_path)
        self.kb_search.clear_filters()
        self.kb_search.search_label("KB_STREAM_FIELD")
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
    
    def find_stream_table_keys(self, key_data):
       
            
        return_values = []
        for key in key_data:
            
            return_values.append(key[5])
        return return_values
    

    
    def push_stream_data(self, path, data):
        """
        Find an available record (valid=False) for the given path with the earliest completed_at time,
        update it with new data, and prepare it for scheduling.
        
        Args:
            path (str): The path in LTREE format
            data (dict): The JSON data to insert
            
        Returns:
            int: The ID of the updated record
            
        Raises:
            Exception: If no available record is found
        """
        # Start a transaction
        try:
            # Find and lock a record with the given path where valid is false
            find_query = """
                SELECT id
                FROM stream_table.stream_table
                WHERE path = %s
                ORDER BY recorded_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """
            
            self.cursor.execute(find_query, (path,))
            result = self.cursor.fetchone()
            
            if result is None:
                self.conn.rollback()
                raise Exception(f"No available record found for path: {path}")
            
            job_id = result[0]
            
            # Update the found record with the new data
            update_query = """
                UPDATE stream_table.stream_table
                SET data = %s,
                    recorded_at = NOW()
                WHERE id = %s
                RETURNING id
            """
            
            self.cursor.execute(update_query, (json.dumps(data), job_id))
            update_result = self.cursor.fetchone()
            
            if update_result is None:
                self.conn.rollback()
                raise Exception(f"Failed to update record with id: {job_id}")
                
            self.conn.commit()
            return job_id
            
        except Exception as e:
            self.conn.rollback()
            raise e
    

        
    def list_stream_data(self, path, limit=None, offset=0, recorded_after=None, recorded_before=None):
        """
        List all jobs for a given path where valid is False and is_active is False,
        ordered by completed_at with earliest first.
        
        Args:
            path (str): The path to search for in LTREE format
            limit (int, optional): Maximum number of jobs to return
            offset (int, optional): Number of jobs to skip
            completed_after (datetime, optional): Only include jobs completed after this time
            completed_before (datetime, optional): Only include jobs completed before this time
        
        Returns:
            list: A list of dictionaries containing all job details
            
        Raises:
            Exception: If there's an error executing the query
        """
        try:
            # Build the base query
            query = """
                SELECT id, path, recorded_at, data
                FROM stream_table.stream_table
                WHERE path = %s
                
            """
            
            params = [path]
            
            # Add optional time-based filters
            if recorded_after is not None:
                query += " AND recorded_at >= %s"
                params.append(recorded_after)
                
            if recorded_before is not None:
                query += " AND recorded_at <= %s"
                params.append(recorded_before)
                
            # Add ordering
            query += " ORDER BY recorded_at ASC"
            
            # Add optional pagination
            if limit is not None:
                query += " LIMIT %s"
                params.append(limit)
                
            if offset > 0:
                query += " OFFSET %s"
                params.append(offset)
                
            self.cursor.execute(query, params)
            results = self.cursor.fetchall()
            
            # Get column names from cursor description
            column_names = [desc[0] for desc in self.cursor.description]
            
            # Convert results to a list of dictionaries using column names
            jobs = []
            for row in results:
                job = dict(zip(column_names, row))
                jobs.append(job)
            
            return jobs
        
        except Exception as e:
            # This is a read-only operation, so no need to rollback
            raise Exception(f"Error stream data: {str(e)}")
        