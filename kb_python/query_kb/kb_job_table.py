import json

class KB_Job_Queue:
    """
    A class to handle the status data for the knowledge base.
    """
    def __init__(self, kb_search):
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        self.cursor = self.kb_search.cursor 
    
    def find_job_id(self, node_name, properties, node_path):
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
    
    def find_job_ids(self, node_name, properties, node_path):
        """
        Find the node id for a given node name, properties, node path :
        """
        print(node_name, properties, node_path)
        self.kb_search.clear_filters()
        self.kb_search.search_label("KB_JOB_QUEUE")
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
    
    def find_job_table_keys(self, key_data):
       
            
        return_values = []
        for key in key_data:
            
            return_values.append(key[5])
        return return_values
    
    def get_queued_number(self, path):
        """
        Count the number of job entries where valid is true for a given path.
        
        Args:
            path (str): The path to search for in LTREE format
        
        Returns:
            int: Number of valid jobs for the given path
        """
        query = """
            SELECT COUNT(*)
            FROM job_table.job_table
            WHERE path = %s
            AND valid = TRUE
        """
        
        self.cursor.execute(query, (path,))
        count = self.cursor.fetchone()[0]
        return count
        
    def get_free_number(self, path):
        """
        Count the number of job entries where valid is false for a given path.
        
        Args:
            path (str): The path to search for in LTREE format
        
        Returns:
            int: Number of invalid jobs for the given path
        """
        query = """
            SELECT COUNT(*)
            FROM job_table.job_table
            WHERE path = %s
            AND valid = FALSE
        """
        
        self.cursor.execute(query, (path,))
        count = self.cursor.fetchone()[0]
        return count
    
    def peak_job_data(self, path):
        """
        Find the job with the earliest schedule_at time for a given path where 
        valid is true, update its started_at timestamp to current time,
        set is_active to false, and return the data and schedule_at.
        
        Args:
            path (str): The path to search for in LTREE format
        
        Returns:
            tuple/None: A tuple containing (id, data, schedule_at) from the job, or None if no jobs found
        """
        # Find the job with the earliest schedule_at time
        find_query = """
            SELECT id, data, schedule_at
            FROM job_table.job_table
            WHERE path = %s
            AND valid = TRUE
            ORDER BY schedule_at ASC
            LIMIT 1
        """
        
        self.cursor.execute(find_query, (path,))
        result = self.cursor.fetchone()
        
        if result:
            job_id, job_data, schedule_at = result
            
            # Update the started_at field to current time and set is_active to false
            update_query = """
                UPDATE job_table.job_table
                SET started_at = NOW(),
                    is_active = TRUE
                WHERE id = %s
            """
            
            self.cursor.execute(update_query, (job_id,))
            self.conn.commit()  # Commit the transaction
            
            return job_id, job_data, schedule_at  # Return both the data JSON field and schedule_at
        else:
            return None
    
    def delete_job_data(self, id):
        """
        Mark a record matching the given id as completed.
        
        Args:
            id (int): The ID of the job record
            
        Returns:
            bool: True if the operation was successful
            
        Raises:
            Exception: If no matching record is found
        """
        # Update the record directly and check if it was found
        update_query = """
            UPDATE job_table.job_table
            SET completed_at = NOW(),
                valid = FALSE,
                is_active = FALSE
            WHERE id = %s
            RETURNING id
        """
        
        self.cursor.execute(update_query, (id,))
        result = self.cursor.fetchone()
        self.conn.commit()
        
        if result is None:
            raise Exception(f"No record found for id: {id}")
        
        return True
    
    def push_job_data(self, path, data):
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
        # Find a record with the given path where valid is false
        find_query = """
            SELECT id
            FROM job_table.job_table
            WHERE path = %s
            AND valid = FALSE
            ORDER BY completed_at ASC
            LIMIT 1
        """
        
        self.cursor.execute(find_query, (path,))
        result = self.cursor.fetchone()
        
        if result is None:
            raise Exception(f"No available record found for path: {path}")
        
        job_id = result[0]
        
        # Update the found record with the new data
        update_query = """
            UPDATE job_table.job_table
            SET data = %s,
                valid = TRUE,
                is_active = FALSE,
                schedule_at = NOW()
            WHERE id = %s
            RETURNING id
        """
        
        self.cursor.execute(update_query, (json.dumps(data), job_id))
        update_result = self.cursor.fetchone()
        self.conn.commit()
        
        return job_id 
    
    def list_pending_jobs(self, path):
        """
        List all jobs for a given path where valid is True and is_active is False,
        ordered by schedule_at with earliest first.
        
        Args:
            path (str): The path to search for in LTREE format
        
        Returns:
            list: A list of dictionaries containing all job details
        """
        query = """
            SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
            FROM job_table.job_table
            WHERE path = %s
            AND valid = TRUE
            AND is_active = FALSE
            ORDER BY schedule_at ASC
        """
        
        self.cursor.execute(query, (path,))
        results = self.cursor.fetchall()
        
        # Convert results to a list of dictionaries
        jobs = []
        for row in results:
            job = {
                'id': row[0],
                'path': row[1],
                'schedule_at': row[2],
                'started_at': row[3],
                'completed_at': row[4],
                'is_active': row[5],
                'valid': row[6],
                'data': row[7]
            }
            jobs.append(job)
        
        return jobs
    
    
    def list_active_jobs(self, path):
        """
        List all jobs for a given path where valid is True and is_active is True,
        ordered by schedule_at with earliest first.
        
        Args:
            path (str): The path to search for in LTREE format
        
        Returns:
            list: A list of dictionaries containing all job details
        """
        query = """
            SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
            FROM job_table.job_table
            WHERE path = %s
            AND valid = TRUE
            AND is_active = TRUE
            ORDER BY started_at ASC
        """
        
        self.cursor.execute(query, (path,))
        results = self.cursor.fetchall()
        
        # Convert results to a list of dictionaries
        jobs = []
        for row in results:
            job = {
                'id': row[0],
                'path': row[1],
                'schedule_at': row[2],
                'started_at': row[3],
                'completed_at': row[4],
                'is_active': row[5],
                'valid': row[6],
                'data': row[7]
            }
            jobs.append(job)
        
        return jobs
    
    def list_completed_jobs(self, path):
        """
        List all jobs for a given path where valid is False and is_active is False,
        ordered by completed_at with earliest first.
        
        Args:
            path (str): The path to search for in LTREE format
        
        Returns:
            list: A list of dictionaries containing all job details
        """
        query = """
            SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
            FROM job_table.job_table
            WHERE path = %s
            AND valid = FALSE
            AND is_active = FALSE
            ORDER BY completed_at ASC
        """
        
        self.cursor.execute(query, (path,))
        results = self.cursor.fetchall()
        
        # Convert results to a list of dictionaries
        jobs = []
        for row in results:
            job = {
                'id': row[0],
                'path': row[1],
                'schedule_at': row[2],
                'started_at': row[3],
                'completed_at': row[4],
                'is_active': row[5],
                'valid': row[6],
                'data': row[7]
            }
            jobs.append(job)
        
        return jobs