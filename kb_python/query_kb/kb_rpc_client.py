import json
import datetime
import uuid
from datetime import datetime, timezone
class KB_RPC_Client:
    """
    A class to handle the RPC client for the knowledge base.
    """
    def __init__(self, kb_search):
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        self.cursor = self.kb_search.cursor 
    
    def find_rpc_client_id(self, node_name, properties, node_path):
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
    
    def find_rpc_client_ids(self, node_name, properties, node_path):
        """
        Find the node id for a given node name, properties, node path :
        """
        print(node_name, properties, node_path)
        self.kb_search.clear_filters()
        self.kb_search.search_label("KB_RPC_CLIENT_FIELD")
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
    
    def find_rpc_client_keys(self, key_data):
       
            
        return_values = []
        for key in key_data:
            
            return_values.append(key[5])
        return return_values    
    
    def find_free_slots(self, client_path):
        """
        Find the number of free slots (records with is_new_result=FALSE) for a given client_path.
        
        Args:
            client_path (str): LTree compatible path for client
            
        Returns:
            int: Number of free slots available for the client_path
            
        Raises:
            Exception: If no records exist for the specified client_path
        """
        try:
            # First, verify that there are records with the given client_path
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM rpc_client_table.rpc_client_table 
                WHERE client_path = %s
            """, (client_path,))
            
            total_records = self.cursor.fetchone()[0]
            
            if total_records == 0:
                raise Exception(f"No records found for client_path: {client_path}")
            
            # Now, count the number of free slots (where is_new_result is FALSE)
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM rpc_client_table.rpc_client_table 
                WHERE client_path = %s AND is_new_result = FALSE
            """, (client_path,))
            
            free_slots = self.cursor.fetchone()[0]
            
            return free_slots
            
        except psycopg2.Error as e:
            # Handle database errors
            raise Exception(f"Database error when finding free slots: {str(e)}")
        
    def find_queued_slots(self, client_path):
        """
        Find the number of queued slots (records with is_new_result=FALSE) for a given client_path.
        
        Args:
            client_path (str): LTree compatible path for client
            
        Returns:
            int: Number of queued slots available for the client_path
            
        Raises:
            Exception: If no records exist for the specified client_path
        """
        try:
            # First, verify that there are records with the given client_path
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM rpc_client_table.rpc_client_table 
                WHERE client_path = %s
            """, (client_path,))
            
            total_records = self.cursor.fetchone()[0]
            
            if total_records == 0:
                raise Exception(f"No records found for client_path: {client_path}")
            
            # Count the number of queued slots (where is_new_result is FALSE)
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM rpc_client_table.rpc_client_table 
                WHERE client_path = %s AND is_new_result = TRUE
            """, (client_path,))
            
            queued_slots = self.cursor.fetchone()[0]
            
            return queued_slots
            
        except psycopg2.Error as e:
            # Handle database errors
            raise Exception(f"Database error when finding queued slots: {str(e)}")
    
    def peak_reply_data(self, client_path):
        """
        For a given client_path, find the first record where is_new_result is TRUE
        with the earliest response_timestamp.
        
        Args:
            client_path (str): LTree compatible path for client
            
        Returns:
            tuple: (id, data_dict) where:
                - id is the record ID
                - data_dict is a dictionary containing all fields of the record
                
        Raises:
            Exception: If no records with is_new_result=TRUE are found for the client_path
        """
        try:
            query = """
                SELECT id, request_id, client_path, server_path, transaction_tag, rpc_action,
                       response_payload, response_timestamp, is_new_result, error_message
                FROM rpc_client_table.rpc_client_table
                WHERE client_path = %s AND is_new_result = TRUE
                ORDER BY response_timestamp ASC
                LIMIT 1
            """
            
            # Execute the query
            self.cursor.execute(query, (client_path,))
            
            # Fetch the result
            record = self.cursor.fetchone()
            
            # Check if a record was found
            if record is None:
                return None, None
            
            # Create a dictionary of field names and values
            column_names = [desc[0] for desc in self.cursor.description]
            data_dict = dict(zip(column_names, record))
            
            # Convert UUID to string
            if 'request_id' in data_dict and data_dict['request_id'] is not None:
                data_dict['request_id'] = str(data_dict['request_id'])
            
            # Convert datetime to ISO format string
            if 'response_timestamp' in data_dict and isinstance(data_dict['response_timestamp'], datetime):
                data_dict['response_timestamp'] = data_dict['response_timestamp'].isoformat()
            
            # Get the record ID
            record_id = data_dict.pop('id')
            
            return record_id, data_dict
            
        except psycopg2.Error as e:
            # Handle database errors
            raise Exception(f"Database error when peeking reply data: {str(e)}")


    
    def release_rpc_data(self, client_path, id, max_retries=3, retry_delay=1):
        """
        For a given ID, verify the record matches the client_path and is_new_result is TRUE,
        then set is_new_result to FALSE. Contains protection against parallel transactions.
        
        Args:
            client_path (str): LTree compatible path for client
            id (int): The ID of the record to release
            max_retries (int): Maximum number of retries in case of lock conflicts
            retry_delay (float): Delay in seconds between retries
            
        Returns:
            bool: True if a record was successfully released, False if no matching record was found
            
        Raises:
            Exception: If database error occurs or cannot obtain lock after retries
        """
        retries = 0
        
        while retries <= max_retries:
            try:
                # Create a new cursor for this attempt to ensure clean transaction state
                with self.conn.cursor() as transaction_cursor:
                    # Begin a transaction
                    self.conn.autocommit = False
                    
                    # First, verify the record exists with the given ID, client_path, and is_new_result=TRUE
                    # Use NOWAIT to immediately get an error if the row is locked
                    verify_query = """
                        SELECT id
                        FROM rpc_client_table.rpc_client_table
                        WHERE id = %s AND client_path = %s AND is_new_result = TRUE
                        FOR UPDATE NOWAIT
                    """
                    
                    transaction_cursor.execute(verify_query, (id, client_path))
                    
                    # Check if a record was found
                    if transaction_cursor.fetchone() is None:
                        # No matching record found
                        self.conn.rollback()
                        return False
                    
                    # Update the record
                    update_query = """
                        UPDATE rpc_client_table.rpc_client_table
                        SET is_new_result = FALSE
                        WHERE id = %s
                    """
                    
                    transaction_cursor.execute(update_query, (id,))
                    
                    # Commit the transaction
                    self.conn.commit()
                    
                    # Return success
                    return True
                    
            except psycopg2.errors.LockNotAvailable:
                # This specific exception means another transaction has locked the row
                self.conn.rollback()
                
                if retries < max_retries:
                    # Wait before retrying
                    time.sleep(retry_delay)
                    retries += 1
                else:
                    # Max retries exceeded
                    raise Exception(f"Could not obtain lock on record ID {id} after {max_retries} attempts")
                    
            except psycopg2.Error as e:
                # Handle other database errors
                self.conn.rollback()
                raise Exception(f"Database error when releasing RPC data: {str(e)}")
                
            finally:
                # Reset autocommit to its default state
                self.conn.autocommit = True
        
        # This should not be reached due to the exception in the retry loop,
        # but added as an extra safety measure
        raise Exception(f"Failed to release RPC data for ID {id} after {max_retries} attempts")    


    def clear_reply_queue(self):
        """
        Clear the reply queue by resetting all records in the table.
        For each record:
        - Sets a unique UUID for request_id
        - Sets server_path equal to client_path
        - Resets response_payload to empty JSON object
        - Updates response_timestamp to current UTC time
        - Sets is_new_result to FALSE
        - Clears error_message
        
        Returns:
            int: Number of records updated
        """
        try:
            # Create the update query
            query = """
                UPDATE rpc_client_table.rpc_client_table
                SET request_id = gen_random_uuid(),  -- PostgreSQL function to generate unique UUIDs
                    server_path = client_path,
                    response_payload = '{}',
                    response_timestamp = NOW(),  -- Current UTC timestamp
                    is_new_result = FALSE,
                    error_message = ''
            """
            
            # Execute the query
            self.cursor.execute(query)
            
            # Get the number of affected rows
            row_count = self.cursor.rowcount
            
            # Commit the transaction
            self.conn.commit()
            
            return row_count
            
        except psycopg2.Error as e:
            # Rollback in case of error
            self.conn.rollback()
            raise Exception(f"Failed to clear reply queue: {str(e)}") 
           
    def push_reply_data(self, client_path, request_uuid, server_path, rpc_action, transaction_tag, reply_data, error_text):
        """
        Update the earliest record with is_new_result=False that matches the client_path
        
        Args:
            client_path (str): LTree compatible path for client
            request_uuid (str): UUID of the request
            server_path (str): LTree compatible path for server
            rpc_action (str): Action to be performed
            transaction_tag (str): Transaction tag
            reply_data (dict): Dictionary containing reply data
            error_text (str): Error message if any
            
        Raises:
            Exception: If no available record with is_new_result=False is found
        """
        try:
            # Convert request_uuid to UUID object if it's a string
            if isinstance(request_uuid, str):
                request_uuid = uuid.UUID(request_uuid)
                
            # Start a transaction
            with self.conn.cursor() as cursor:
                # Select for update to lock the row
                cursor.execute("""
                    SELECT id 
                    FROM rpc_client_table.rpc_client_table
                    WHERE client_path = %s AND is_new_result = FALSE
                    ORDER BY response_timestamp ASC
                    LIMIT 1
                    FOR UPDATE NOWAIT
                """, (client_path,))
                
                result = cursor.fetchone()
                if not result:
                    raise Exception("Table_full_exception: No available record with is_new_result=False")
                
                record_id = result[0]
                
                # Update the record
                cursor.execute("""
                    UPDATE rpc_client_table.rpc_client_table
                    SET request_id = %s,
                        server_path = %s,
                        rpc_action = %s,
                        transaction_tag = %s,
                        response_payload = %s,
                        response_timestamp = %s,
                        is_new_result = TRUE,
                        error_message = %s
                    WHERE id = %s
                """, (
                    request_uuid,
                    server_path,
                    rpc_action,
                    transaction_tag,
                    json.dumps(reply_data),
                    datetime.now(timezone.utc),
                    error_text,
                    record_id
                ))
                
                # Commit the transaction
                self.conn.commit()
                
        except psycopg2.Error as e:
            self.conn.rollback()
            if "could not obtain lock" in str(e):
                raise Exception("Parallel operation detected, try again later")
            else:
                raise e
    
    
    
    def list_waiting_jobs(self, client_path=None):
        """
        List all rows in the table where is_new_result is TRUE and optionally 
        matching a specific client_path.
        
        Args:
            client_path (str, optional): If provided, filter results to this client_path
        
        Returns:
            list: A list of dictionaries, each containing the data for one waiting job
                  Each dictionary has keys corresponding to table column names
                  
        Raises:
            Exception: If a database error occurs
        """
        try:
            if client_path is None:
                # No client_path filter, return all waiting jobs
                query = """
                    SELECT id, request_id, client_path, server_path, 
                           response_payload, response_timestamp, is_new_result, error_message
                    FROM rpc_client_table.rpc_client_table
                    WHERE is_new_result = TRUE
                    ORDER BY response_timestamp ASC
                """
                params = ()
            else:
                # Filter by client_path
                query = """
                    SELECT id, request_id, client_path, server_path, 
                           response_payload, response_timestamp, is_new_result, error_message
                    FROM rpc_client_table.rpc_client_table
                    WHERE is_new_result = TRUE AND client_path = %s
                    ORDER BY response_timestamp ASC
                """
                params = (client_path,)
            
            # Execute the query
            self.cursor.execute(query, params)
            
            # Get column names from cursor description
            column_names = [desc[0] for desc in self.cursor.description]
            
            # Fetch all matching records
            records = self.cursor.fetchall()
            
            # Convert to list of dictionaries
            result = []
            for record in records:
                # Create a dictionary for this record
                record_dict = dict(zip(column_names, record))
                
                # Convert UUID to string
                if 'request_id' in record_dict and record_dict['request_id'] is not None:
                    record_dict['request_id'] = str(record_dict['request_id'])
                
                # Convert datetime to ISO format string
                if 'response_timestamp' in record_dict and isinstance(record_dict['response_timestamp'], datetime):
                    record_dict['response_timestamp'] = record_dict['response_timestamp'].isoformat()
                
                # Convert ltree to string
                if 'client_path' in record_dict and record_dict['client_path'] is not None:
                    record_dict['client_path'] = str(record_dict['client_path'])
                
                if 'server_path' in record_dict and record_dict['server_path'] is not None:
                    record_dict['server_path'] = str(record_dict['server_path'])
                
                # Add to result list
                result.append(record_dict)
            
            return result
            
        except psycopg2.Error as e:
            # Handle database errors
            raise Exception(f"Database error when listing waiting jobs: {str(e)}")