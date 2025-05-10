import uuid
from datetime import datetime, timezone
import json
import time
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter, AsIs

class KB_RPC_Server:
    """
    A class to handle the RPC server for the knowledge base.
    """
    def __init__(self, kb_search):
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        self.cursor = self.kb_search.cursor 
    
    def find_rpc_server_id(self, node_name, properties, node_path):
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
    
    def find_rpc_server_ids(self, node_name, properties, node_path):
        """
        Find the node id for a given node name, properties, node path :
        """
       
        self.kb_search.clear_filters()
        self.kb_search.search_label("KB_RPC_SERVER_FIELD")
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
    
    def find_rpc_table_keys(self, key_data):
       
            
        return_values = []
        for key in key_data:
            
            return_values.append(key[5])
        return return_values    
    
    def list_processing_jobs(self, server_path):
        """
        List records in the table where server_path matches and status is 'processing'.
        
        Args:
            server_path (str): The server path to match against.
            
        Returns:
            list: A list of records that match the criteria.
            
        Raises:
            Exception: Reraises any exception that occurs during database operations.
        """
        try:
            query = """
                SELECT *
                FROM rpc_server_table.rpc_server_table
                WHERE server_path = %s
                AND status = 'processing'
                ORDER BY priority DESC, request_timestamp ASC
            """
            
            # Convert server_path to ltree format if it's not already
            # This assumes server_path is provided as a string like 'path.to.server'
            formatted_server_path = server_path
            
            # Execute the query
            formatted_results = []
            with self.conn.cursor() as cursor:
                cursor.execute(query, (formatted_server_path,))
                results = cursor.fetchall()
                
                # Format results if needed
                column_names = [desc[0] for desc in cursor.description]
                for row in results:
                    row_dict = dict(zip(column_names, row))
                    formatted_results.append(row_dict)
                    
            return formatted_results
        
        except Exception as e:
            print(f"Exception in list_waiting_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            raise  # Reraise the exception after printing it
        
    def list_completed_jobs(self, server_path):
        """
        List records in the table where server_path matches and status is 'completed'.
        
        Args:
            server_path (str): The server path to match against.
            
        Returns:
            list: A list of records that match the criteria.
            
        Raises:
            Exception: Reraises any exception that occurs during database operations.
        """
        try:
            query = """
                SELECT *
                FROM rpc_server_table.rpc_server_table
                WHERE server_path = %s
                AND status = 'completed'
                ORDER BY priority DESC, request_timestamp ASC
            """
            
            # Convert server_path to ltree format if it's not already
            # This assumes server_path is provided as a string like 'path.to.server'
            formatted_server_path = server_path
            
            # Execute the query
            formatted_results = []
            with self.conn.cursor() as cursor:
                cursor.execute(query, (formatted_server_path,))
                results = cursor.fetchall()
                
                # Format results by creating dictionaries using column names
                column_names = [desc[0] for desc in cursor.description]
                for row in results:
                    row_dict = dict(zip(column_names, row))
                    formatted_results.append(row_dict)
                    
            return formatted_results
        
        except Exception as e:
            print(f"Exception in list_completed_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            raise  # Reraise the exception after printing it
        
    def list_failed_jobs(self, server_path=None):
        """
        List records in the table where status is 'failed' and optionally filtered by server_path.
        
        Args:
            server_path (str, optional): The server path to match against. If None, returns all failed jobs.
            
        Returns:
            list: A list of records that match the criteria.
            
        Raises:
            Exception: Reraises any exception that occurs during database operations.
        """
        try:
            if server_path is not None:
                query = """
                    SELECT *
                    FROM rpc_server_table.rpc_server_table
                    WHERE server_path = %s
                    AND status = 'failed'
                    ORDER BY priority DESC, request_timestamp ASC
                """
                params = (server_path,)
            else:
                query = """
                    SELECT *
                    FROM rpc_server_table.rpc_server_table
                    WHERE status = 'failed'
                    ORDER BY priority DESC, request_timestamp ASC
                """
                params = ()
            
            # Execute the query
            formatted_results = []
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                # Format results by creating dictionaries using column names
                column_names = [desc[0] for desc in cursor.description]
                for row in results:
                    row_dict = dict(zip(column_names, row))
                    formatted_results.append(row_dict)
                    
            return formatted_results
        
        except Exception as e:
            print(f"Exception in list_failed_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            raise  # Reraise the exception after printing it
        
        
    def list_pending_jobs(self, server_path):
        """
        List records in the table where server_path matches and status is 'pending'.
        
        Args:
            server_path (str): The server path to match against.
            
        Returns:
            list: A list of records that match the criteria.
            
        Raises:
            Exception: Reraises any exception that occurs during database operations.
        """
        try:
            query = """
                SELECT *
                FROM rpc_server_table.rpc_server_table
                WHERE server_path = %s
                AND status = 'pending'
                ORDER BY priority DESC, request_timestamp ASC
            """
            
            # Execute the query
            formatted_results = []
            with self.conn.cursor() as cursor:
                cursor.execute(query, (server_path,))
                results = cursor.fetchall()
                
                # Format results by creating dictionaries using column names
                column_names = [desc[0] for desc in cursor.description]
                for row in results:
                    row_dict = dict(zip(column_names, row))
                    formatted_results.append(row_dict)
                    
            return formatted_results
        
        except Exception as e:
            print(f"Exception in list_pending_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            raise  # Reraise the exception after printing it
        
    def count_pending_jobs(self, server_path=None):
        """
        Count the number of records with status 'pending', optionally filtered by server_path.
        
        Args:
            server_path (str, optional): The server path to match against. If None, counts all pending jobs.
            
        Returns:
            int: The number of pending jobs.
            
        Raises:
            Exception: Reraises any exception that occurs during database operations.
        """
        try:
            if server_path is not None:
                query = """
                    SELECT COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    WHERE server_path = %s
                    AND status = 'pending'
                """
                params = (server_path,)
            else:
                query = """
                    SELECT COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    WHERE status = 'pending'
                """
                params = ()
            
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                count = cursor.fetchone()[0]
                
            return count
        
        except Exception as e:
            print(f"Exception in count_pending_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def count_processing_jobs(self, server_path=None):
        """
        Count the number of records with status 'processing', optionally filtered by server_path.
        
        Args:
            server_path (str, optional): The server path to match against. If None, counts all processing jobs.
            
        Returns:
            int: The number of processing jobs.
            
        Raises:
            Exception: Reraises any exception that occurs during database operations.
        """
        try:
            if server_path is not None:
                query = """
                    SELECT COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    WHERE server_path = %s
                    AND status = 'processing'
                """
                params = (server_path,)
            else:
                query = """
                    SELECT COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    WHERE status = 'processing'
                """
                params = ()
            
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                count = cursor.fetchone()[0]
                
            return count
        
        except Exception as e:
            print(f"Exception in count_processing_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def count_completed_jobs(self, server_path=None):
        """
        Count the number of records with status 'completed', optionally filtered by server_path.
        
        Args:
            server_path (str, optional): The server path to match against. If None, counts all completed jobs.
            
        Returns:
            int: The number of completed jobs.
            
        Raises:
            Exception: Reraises any exception that occurs during database operations.
        """
        try:
            if server_path is not None:
                query = """
                    SELECT COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    WHERE server_path = %s
                    AND status = 'completed'
                """
                params = (server_path,)
            else:
                query = """
                    SELECT COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    WHERE status = 'completed'
                """
                params = ()
            
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                count = cursor.fetchone()[0]
                
            return count
        
        except Exception as e:
            print(f"Exception in count_completed_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def count_failed_jobs(self, server_path=None):
        """
        Count the number of records with status 'failed', optionally filtered by server_path.
        
        Args:
            server_path (str, optional): The server path to match against. If None, counts all failed jobs.
            
        Returns:
            int: The number of failed jobs.
            
        Raises:
            Exception: Reraises any exception that occurs during database operations.
        """
        try:
            if server_path is not None:
                query = """
                    SELECT COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    WHERE server_path = %s
                    AND status = 'failed'
                """
                params = (server_path,)
            else:
                query = """
                    SELECT COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    WHERE status = 'failed'
                """
                params = ()
            
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                count = cursor.fetchone()[0]
                
            return count
        
        except Exception as e:
            print(f"Exception in count_failed_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def get_job_counts(self, server_path=None):
        """
        Get counts of jobs in all statuses, optionally filtered by server_path.
        
        Args:
            server_path (str, optional): The server path to match against. If None, counts all jobs.
            
        Returns:
            dict: A dictionary with counts for each status ('pending', 'processing', 'completed', 'failed').
            
        Raises:
            Exception: Reraises any exception that occurs during database operations.
        """
        try:
            if server_path is not None:
                query = """
                    SELECT status, COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    WHERE server_path = %s
                    GROUP BY status
                """
                params = (server_path,)
            else:
                query = """
                    SELECT status, COUNT(*)
                    FROM rpc_server_table.rpc_server_table
                    GROUP BY status
                """
                params = ()
            
            # Initialize counts dictionary with zeros
            counts = {
                'pending': 0,
                'processing': 0,
                'completed': 0,
                'failed': 0
            }
            
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                # Update counts with actual values
                for status, count in results:
                    counts[status] = count
                
            return counts
        
        except Exception as e:
            print(f"Exception in get_job_counts: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        
        
    def push_rpc_queue(self, server_path, request_id, rpc_action, request_payload, transaction_tag, 
                      priority, rpc_client_queue, max_retries=5, wait_time=0.5):
        """
        Push a request to the RPC queue.
        
        Args:
            server_path (str): The server path in ltree format (e.g. 'root.node1.node2')
            request_id (str): UUID for the request
            request_payload (dict): JSON-serializable payload for the request
            transaction_tag (str): Tag to prevent duplicate transactions
            priority (int): Priority of the request (higher number = higher priority)
            rpc_client_queue (str): Client queue in ltree format (e.g. 'client.queue1')
            max_retries (int, optional): Maximum number of retries for transaction conflicts
            wait_time (float, optional): Initial wait time between retries in seconds
            
        Returns:
            dict: The updated record
            
        Raises:
            ValueError: If any parameters fail validation
            NoMatchingRecordError: If no matching record is found to update
            psycopg2.Error: For database errors
            RuntimeError: If max retries exceeded
        """
        # Validate server_path (ltree format)
        if not server_path or not isinstance(server_path, str) or not self._is_valid_ltree(server_path):
            raise ValueError("server_path must be a valid ltree format (e.g. 'root.node1.node2')")
        
        # Validate request_id (UUID)
        try:
            if not request_id:
                request_id = str(uuid.uuid4())
            else:
                request_id = str(uuid.UUID(request_id))
        except (ValueError, AttributeError, TypeError):
            
            raise ValueError("request_id must be a valid UUID string or None")
        if not rpc_action or not isinstance(rpc_action, str):
            raise ValueError("rpc_action must be a non-empty string")      
        # Validate request_payload (JSON-serializable)
        if request_payload is None:
            raise ValueError("request_payload cannot be None")
        try:
            # Test if serializable
            json.dumps(request_payload)
        except (TypeError, OverflowError):
            raise ValueError("request_payload must be JSON-serializable")
        
        # Validate transaction_tag
        if not transaction_tag or not isinstance(transaction_tag, str):
            raise ValueError("transaction_tag must be a non-empty string")
        
        # Validate rpc_client_queue (ltree format)
        if rpc_client_queue is not None and (not isinstance(rpc_client_queue, str) or 
                                           not self._is_valid_ltree(rpc_client_queue)):
            raise ValueError("rpc_client_queue must be None or a valid ltree format")
        
        # Validate priority
        if not isinstance(priority, int):
            raise ValueError("priority must be an integer")
        
        # Process with retry logic for transaction conflicts
        retries = 0
        current_wait = wait_time
        
        while retries < max_retries:
            conn = None
            try:
                conn = psycopg2.connect(**self.connection_params)
                conn.autocommit = False
                cursor = conn.cursor()
                
                # First get a lock to prevent concurrent operations
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('rpc_server_table'))")
                
                # Find the earliest completed/failed record with is_new_result=True
                cursor.execute("""
                    SELECT id FROM rpc_server_table.rpc_server_table
                    WHERE is_new_result = TRUE AND status IN ('completed', 'failed')
                    ORDER BY completed_timestamp ASC
                    LIMIT 1
                    FOR UPDATE
                """)
                
                record = cursor.fetchone()
                
                if not record:
                    conn.rollback()
                    raise NoMatchingRecordError("No matching record found with is_new_result=True and status in ('completed', 'failed')")
                
                record_id = record[0]
                
                # Update the record
                cursor.execute("""
                    UPDATE rpc_server_table.rpc_server_table
                    SET server_path = %s,
                        request_id = %s,
                        rpc_action = %s,
                        request_payload = %s,
                        transaction_tag = %s,
                        priority = %s,
                        rpc_client_queue = %s,
                        is_new_result = TRUE,
                        request_timestamp = NOW() AT TIME ZONE 'UTC',
                        status = 'pending',
                        completed_timestamp = NULL
                    WHERE id = %s
                    RETURNING *
                """, (server_path, request_id,rpc_action, json.dumps(request_payload), transaction_tag, 
                       priority, rpc_client_queue, record_id))
                
                result = cursor.fetchone()
                conn.commit()
                
                # Convert result to dictionary for easier handling
                columns = [desc[0] for desc in cursor.description]
                result_dict = dict(zip(columns, result))
                
                return result_dict
                
            except psycopg2.errors.SerializationFailure:
                if conn:
                    conn.rollback()
                retries += 1
                if retries >= max_retries:
                    raise RuntimeError(f"Failed to push to RPC queue after {max_retries} retries")
                
                # Exponential backoff
                time.sleep(current_wait)
                current_wait *= 2
                
            except NoMatchingRecordError:
                # Don't retry if no matching record is found - just propagate the exception
                raise
                
            except Exception as e:
                if conn:
                    conn.rollback()
                raise e
                
            finally:
                if conn:
                    conn.close()
        
        raise RuntimeError(f"Failed to push to RPC queue after {max_retries} retries")
    
    def _is_valid_ltree(self, path):
        """
        Validate if a string is a valid ltree path.
        
        Args:
            path (str): The path to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not path or not isinstance(path, str):
            return False
        
        # Basic ltree validation - each label must start with a letter or underscore
        # and contain only letters, numbers, and underscores
        parts = path.split('.')
        if not parts:
            return False
        
        for part in parts:
            if not part:
                return False
            if not (part[0].isalpha() or part[0] == '_'):
                return False
            if not all(c.isalnum() or c == '_' for c in part):
                return False
        
        return True
    
    def peak_server_queue(self, server_path, retries=5, wait_time=10):
        """
        Finds and processes one pending record from the server queue.
        
        Args:
            server_path: The server path to search for records
            retries: Number of retry attempts if transaction conflicts occur
            wait_time: Wait time in seconds between retries
            
        Returns:
            Dictionary containing record details (id, request_id, rpc_action, 
            request_payload, transaction_tag, priority, rpc_client_queue) or None if no record found
        """
        import time
        from psycopg2 import sql
        import psycopg2.errors
        
        attempt = 0
        while attempt < retries:
            try:
                # Set transaction isolation level
                self.cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                
                # Find one pending record matching the criteria, ordered by priority and timestamp
                query = sql.SQL("""
                    SELECT id, request_id, rpc_action, request_payload, transaction_tag, 
                        priority, rpc_client_queue
                    FROM rpc_server_table.rpc_server_table
                    WHERE server_path = %s
                    AND is_new_result = TRUE
                    AND status = 'pending'
                    ORDER BY priority DESC, request_timestamp ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """)
                
                self.cursor.execute(query, (server_path,))
                record = self.cursor.fetchone()
                
                if not record:
                    # No pending records found
                    self.conn.commit()
                    return None
                    
                # Update the record status
                update_query = sql.SQL("""
                    UPDATE rpc_server_table.rpc_server_table
                    SET is_new_result = FALSE,
                        status = 'processing',
                        completed_timestamp = NOW()
                    WHERE id = %s
                    RETURNING id
                """)
                
                self.cursor.execute(update_query, (record[0],))
                
                # Commit the transaction
                self.conn.commit()
                
                # Return record details
                return {
                    "id": record[0],
                    "request_id": record[1],
                    "rpc_action": record[2],
                    "request_payload": record[3],
                    "transaction_tag": record[4],
                    "priority": record[5],
                    "rpc_client_queue": record[6]
                }
                
            except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected) as e:
                # Handle transaction conflicts
                self.conn.rollback()
                attempt += 1
                if attempt < retries:
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to peak server queue after {retries} attempts: {str(e)}")
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error in peak_server_queue: {str(e)}")
        
        return None
    
    
    def mark_job_completion(self, id, server_path, retries=5, wait_time=10):
        """
        Marks a job as completed in the server queue.
        
        Args:
            id: The ID of the record to update
            server_path: The server path to verify
            retries: Number of retry attempts if transaction conflicts occur
            wait_time: Wait time in seconds between retries
            
        Returns:
            Boolean indicating success or failure
        """
        import time
        from psycopg2 import sql
        import psycopg2.errors
        
        attempt = 0
        while attempt < retries:
            try:
                # Set transaction isolation level
                self.cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                
                # Verify the record exists with the correct conditions
                verify_query = sql.SQL("""
                    SELECT id FROM rpc_server_table.rpc_server_table
                    WHERE id = %s
                    AND server_path = %s
                    AND is_new_result = FALSE
                    AND status = 'processing'
                    FOR UPDATE
                """)
                
                self.cursor.execute(verify_query, (id, server_path))
                record = self.cursor.fetchone()
                
                if not record:
                    self.conn.rollback()
                    return False  # Record does not exist or doesn't meet conditions
                    
                # Update the record status to completed
                update_query = sql.SQL("""
                    UPDATE rpc_server_table.rpc_server_table
                    SET status = 'completed',
                        completed_timestamp = NOW()
                    WHERE id = %s
                    RETURNING id
                """)
                
                self.cursor.execute(update_query, (id,))
                updated = self.cursor.fetchone()
                
                # Commit the transaction
                self.conn.commit()
                
                return True if updated else False
            
            except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected) as e:
                # Handle transaction conflicts
                self.conn.rollback()
                attempt += 1
                if attempt < retries:
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to mark job as completed after {retries} attempts: {str(e)}")
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error in mark_job_completion: {str(e)}")
    
        return False

    def mark_job_failure(self, id, server_path, retries=5, wait_time=10):
        """
        Marks a job as failed in the server queue.
        
        Args:
            id: The ID of the record to update
            server_path: The server path to verify
            retries: Number of retry attempts if transaction conflicts occur
            wait_time: Wait time in seconds between retries
            
        Returns:
            Boolean indicating success or failure
        """
        import time
        from psycopg2 import sql
        import psycopg2.errors
        
        attempt = 0
        while attempt < retries:
            try:
                # Set transaction isolation level
                self.cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                
                # Verify the record exists with the correct conditions
                verify_query = sql.SQL("""
                    SELECT id FROM rpc_server_table.rpc_server_table
                    WHERE id = %s
                    AND server_path = %s
                    AND is_new_result = FALSE
                    AND status = 'processing'
                    FOR UPDATE
                """)
                
                self.cursor.execute(verify_query, (id, server_path))
                record = self.cursor.fetchone()
                
                if not record:
                    self.conn.rollback()
                    return False  # Record does not exist or doesn't meet conditions
                    
                # Update the record status to failed
                update_query = sql.SQL("""
                    UPDATE rpc_server_table.rpc_server_table
                    SET status = 'failed',
                        completed_timestamp = NOW()
                    WHERE id = %s
                    RETURNING id
                """)
                
                self.cursor.execute(update_query, (id,))
                updated = self.cursor.fetchone()
                
                # Commit the transaction
                self.conn.commit()
                
                return True if updated else False
                
            except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected) as e:
                # Handle transaction conflicts
                self.conn.rollback()
                attempt += 1
                if attempt < retries:
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to mark job as failed after {retries} attempts: {str(e)}")
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error in mark_job_failure: {str(e)}")
        
        return False