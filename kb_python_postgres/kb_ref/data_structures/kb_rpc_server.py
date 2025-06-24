import uuid
from datetime import datetime, timezone
import json
import time
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter, AsIs

class NoMatchingRecordError(Exception):
    pass

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
    
        result = self.find_rpc_server_ids(node_name, properties, node_path)
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
    
    def find_rpc_server_table_keys(self, key_data):
       
            
        return_values = []
        for key in key_data:
            
            return_values.append(key[5])
        return return_values    
    
 

    def list_jobs_job_types(self, server_path,state):
        """
        List records in the table where server_path matches and status is 'processing'.

        Args:
            server_path (str): The server path to match against (ltree format, e.g., 'root.node1.node2').
            state (str): The state to match against (e.g., 'processing', 'completed_job').
        Returns:
            list: A list of dictionaries containing records that match the criteria.

        Raises:
            ValueError: If server_path is invalid or not in ltree format.
            psycopg2.Error: For database errors.
        """
        # Validate server_path
        if not server_path or not isinstance(server_path, str) or not self._is_valid_ltree(server_path):
            raise ValueError("server_path must be a valid ltree format (e.g., 'root.node1.node2')")
        if state not in {'empty', 'new_job', 'processing'}:
            raise ValueError("state must be one of: 'empty', 'new_job', 'processing'")
        try:
            # Ensure connection is valid
            if self.conn.closed:
                raise psycopg2.Error("Database connection is closed")

            # Begin transaction
            self.cursor.execute("BEGIN")

            # Construct safe query
            query = sql.SQL("""
                SELECT *
                FROM rpc_server_table.rpc_server_table
                WHERE server_path = %s
                AND state = %s
                ORDER BY priority DESC, request_timestamp ASC
            """)

            # Execute query
            formatted_results = []
            self.cursor.execute(query, (server_path,state))
            results = self.cursor.fetchall()

            # Format results as dictionaries
            column_names = [desc[0] for desc in self.cursor.description]
            for row in results:
                row_dict = dict(zip(column_names, row))
                formatted_results.append(row_dict)

            # Commit transaction
            self.conn.commit()
            return formatted_results

        except psycopg2.Error as e:
            self.conn.rollback()
            raise psycopg2.Error(f"Database error in list_processing_jobs: {str(e)}")
        finally:
            # Ensure transaction is closed
            self.cursor.execute("COMMIT")
            
    def count_all_jobs(self, server_path):
        """
        Count all records in the table where server_path matches.
        """
        print("empty jobs", self.count_empty_jobs(server_path))
        print("new jobs", self.count_new_jobs(server_path))
        print("processing jobs", self.count_processing_jobs(server_path))
      
    
 
    def count_processing_jobs(self, server_path):
        """
        Count records in the table where server_path matches and state is 'processing'.
        """
        return self.count_jobs_job_types(server_path, 'processing')

    def count_new_jobs(self, server_path):
        """
        Count records in the table where server_path matches and state is 'new_job'.
        """
        return self.count_jobs_job_types(server_path, 'new_job')
    
    
    def count_empty_jobs(self, server_path):
        """
        Count records in the table where server_path matches and state is 'empty'.
        """
        return self.count_jobs_job_types(server_path, 'empty')

    def count_jobs_job_types(self, server_path,state):
        """
        Count records in the table where server_path matches and state is 'processing'.

        Args:
            server_path (str): The server path to match against (ltree format, e.g., 'root.node1.node2').
            state (str): The state to match against (e.g., 'empty', 'new_job', 'processing', 'completed_job').
        Returns:
            int: The number of records that match the criteria.

        Raises:
            ValueError: If server_path is invalid or not in ltree format.
            psycopg2.Error: For database errors.
        """
        # Validate server_path
        if not server_path or not isinstance(server_path, str) or not self._is_valid_ltree(server_path):
            raise ValueError("server_path must be a valid ltree format (e.g., 'root.node1.node2')")
        if state not in {'empty', 'new_job', 'processing', 'completed_job'}:
            raise ValueError("state must be one of: 'empty', 'new_job', 'processing', 'completed_job'")
        try:
            # Ensure connection is valid
            if self.conn.closed:
                raise psycopg2.Error("Database connection is closed")

            # Begin transaction
            self.cursor.execute("BEGIN")

            # Construct safe query
            query = sql.SQL("""
                SELECT count(*)
                FROM rpc_server_table.rpc_server_table
                WHERE server_path = %s
                AND state = %s
                
            """)

            # Execute query
            formatted_results = []
            self.cursor.execute(query, (server_path,state))
            results = self.cursor.fetchall()

            # Format results as dictionaries
            column_names = [desc[0] for desc in self.cursor.description]
            for row in results:
                row_dict = dict(zip(column_names, row))
                formatted_results.append(row_dict)

            # Commit transaction
            self.conn.commit()
            return formatted_results

        except psycopg2.Error as e:
            self.conn.rollback()
            raise psycopg2.Error(f"Database error in list_processing_jobs: {str(e)}")
        finally:
            # Ensure transaction is closed
            self.cursor.execute("COMMIT")



    def push_rpc_queue(self, server_path, request_id, rpc_action, request_payload, transaction_tag,
                       priority=0, rpc_client_queue=None, max_retries=5, wait_time=0.5):
        """
        Push a request to the RPC queue.

        Args:
            server_path (str): The server path in ltree format (e.g. 'root.node1.node2')
            request_id (str): UUID for the request
            rpc_action (str): RPC action name
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

        # Validate rpc_action
        if not rpc_action or not isinstance(rpc_action, str):
            raise ValueError("rpc_action must be a non-empty string")

        # Validate request_payload (JSON-serializable)
        if request_payload is None:
            raise ValueError("request_payload cannot be None")
        try:
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
        attempt = 0
        current_wait = wait_time
        max_wait = 8  # Cap maximum wait time at 8 seconds

        while attempt < max_retries:
            try:
                # Ensure connection is valid
                if self.conn.closed:
                    raise Exception("Database connection is closed")

                # Explicitly begin transaction
                self.cursor.execute("BEGIN")
                self.cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                # Acquire advisory lock (schema-specific)
                lock_key = hash(f"rpc_server_table.rpc_server_table:{server_path}")
                self.cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))

                # Find the earliest completed/failed record with is_new_result=True
                query = sql.SQL("""
                    SELECT id FROM rpc_server_table.rpc_server_table
                    WHERE state = 'empty'
                    ORDER BY priority DESC, request_timestamp ASC
                    LIMIT 1
                    FOR UPDATE
                """)
                self.cursor.execute(query)
                record = self.cursor.fetchone()

                if not record:
                    self.conn.rollback()
                    raise NoMatchingRecordError("No matching record found with state = 'pending'")

                record_id = record[0]

                # Update the record
                update_query = sql.SQL("""
                    UPDATE rpc_server_table.rpc_server_table
                    SET server_path = %s,
                        request_id = %s,
                        rpc_action = %s,
                        request_payload = %s,
                        transaction_tag = %s,
                        priority = %s,
                        rpc_client_queue = %s,
                        state = 'new_job',
                        request_timestamp = NOW() AT TIME ZONE 'UTC',
                       
                        completed_timestamp = NULL
                    WHERE id = %s
                    RETURNING *
                """)
                self.cursor.execute(update_query, (server_path, request_id, rpc_action, json.dumps(request_payload),
                                                transaction_tag, priority, rpc_client_queue, record_id))
                result = self.cursor.fetchone()

                if not result:
                    self.conn.rollback()
                    raise Exception("Failed to update record in RPC queue")

                # Commit the transaction
                self.conn.commit()

                # Convert result to dictionary
                columns = [desc[0] for desc in self.cursor.description]
                result_dict = dict(zip(columns, result))
                return result_dict

            except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected) as e:
                self.conn.rollback()
                attempt += 1
                if attempt < max_retries:
                    sleep_time = min(current_wait * (2 ** attempt), max_wait)  # Exponential backoff with cap
                    time.sleep(sleep_time)
                else:
                    raise RuntimeError(f"Failed to push to RPC queue after {max_retries} retries: {str(e)}")
            except psycopg2.Error as e:
                self.conn.rollback()
                raise psycopg2.Error(f"Database error in push_rpc_queue: {str(e)}")
            except NoMatchingRecordError:
                self.conn.rollback()
                raise
            finally:
                # No need to reset transaction isolation level (it resets automatically after COMMIT/ROLLBACK)
                pass

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
    def peak_server_queue(self, server_path, retries=5, wait_time=1):
        """
        Finds and processes one pending record from the server queue.

        Args:
            server_path: The server path to search for records
            retries: Number of retry attempts if transaction conflicts occur
            wait_time: Initial wait time in seconds between retries (uses exponential backoff)

        Returns:
            Dictionary containing record details (id, request_id, rpc_action,
            request_payload, transaction_tag, priority, rpc_client_queue) or None if no record found

        Raises:
            Exception: If the database operation fails after retries or due to other errors
        """
        attempt = 0
        while attempt < retries:
            try:
                # Ensure connection is valid
                if self.conn.closed:
                    raise Exception("Database connection is closed")

                # Explicitly begin transaction
                self.cursor.execute("BEGIN")
                self.cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                # Find one new job record matching the criteria, ordered by priority and timestamp
                query = sql.SQL("""
                    SELECT id, request_id, rpc_action, request_payload, transaction_tag,
                        priority, rpc_client_queue
                    FROM rpc_server_table.rpc_server_table
                    WHERE server_path = %s
                    AND state = 'new_job'
                    ORDER BY priority DESC, request_timestamp ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED;
                """)
                self.cursor.execute(query, (server_path,))
                record = self.cursor.fetchone()

                if not record:
                    # No pending records found
                    self.conn.rollback()
                    return None

                # Update the record status
                update_query = sql.SQL("""
                    UPDATE rpc_server_table.rpc_server_table
                    SET state = 'processing',
                        processing_timestamp = NOW() AT TIME ZONE 'UTC'
                    WHERE id = %s
                    RETURNING id
                """)
                self.cursor.execute(update_query, (record[0],))
                updated = self.cursor.fetchone()

                if not updated:
                    self.conn.rollback()
                    raise Exception(f"Failed to update record status to processing for id: {record[0]}")

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
                self.conn.rollback()
                attempt += 1
                if attempt < retries:
                    sleep_time = wait_time * (2 ** attempt)  # Exponential backoff
                    time.sleep(sleep_time)
                else:
                    raise Exception(f"Failed to peak server queue after {retries} attempts: {str(e)}")
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error in peak_server_queue: {str(e)}")
            finally:
                # No need to reset isolation level; transaction is already committed or rolled back
                pass

        return None
    
    def mark_job_completion(self, server_path, id, retries=5, wait_time=1):
        """
        Marks a job as completed in the server queue.

        Args:
            id: The ID of the record to update
            server_path: The server path to verify
            retries: Number of retry attempts if transaction conflicts occur
            wait_time: Initial wait time in seconds between retries (uses exponential backoff)

        Returns:
            Boolean indicating success or failure
        """
        attempt = 0
        
        while attempt < retries:
            try:
                # Ensure connection is valid
                if self.conn.closed:
                    raise Exception("Database connection is closed")

                # Explicitly begin transaction
                self.cursor.execute("BEGIN")
                self.cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                # Verify the record exists with the correct conditions
                verify_query = sql.SQL("""
                    SELECT id FROM rpc_server_table.rpc_server_table
                    WHERE id = %s
                    AND server_path = %s
                    AND state = 'processing'
                    FOR UPDATE
                """)

                self.cursor.execute(verify_query, (id, server_path))
                record = self.cursor.fetchone()

                if not record:
                    self.conn.rollback()
                    return False

                # Update the record status to completed
                update_query = sql.SQL("""
                    UPDATE rpc_server_table.rpc_server_table
                    SET state = 'empty',
                        completed_timestamp = NOW() AT TIME ZONE 'UTC'
                    WHERE id = %s
                    RETURNING id
                """)

                self.cursor.execute(update_query, (id,))
                updated = self.cursor.fetchone()

                # Commit the transaction
                self.conn.commit()
                return True if updated else False

            except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected) as e:
                self.conn.rollback()
                attempt += 1
                if attempt < retries:
                    sleep_time = wait_time * (2 ** attempt)  # Exponential backoff
                    time.sleep(sleep_time)
                else:
                    raise Exception(f"Failed to mark job as completed after {retries} attempts: {str(e)}")
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error in mark_job_completion: {str(e)}")
            finally:
                # No need to reset isolation level explicitly, as it resets after commit/rollback
                pass

        return False
    
    def clear_server_queue(self, server_path, max_retries=3, retry_delay=1):
        """
        Clear the reply queue by resetting records matching the specified server_path.
        
        For each matching record:
        - Sets a unique UUID for request_id
        - Resets request_payload to empty JSON object
        - Updates completed_timestamp to current UTC time
        - Sets state to 'empty'
        - Clears rpc_client_queue to NULL
        
        Includes record locking with retries to handle concurrent access.
        
        Args:
            server_path (str): The server path to match for clearing records
            max_retries (int): Maximum number of retries for acquiring the lock
            retry_delay (float): Delay in seconds between retry attempts
        
        Returns:
            int: Number of records updated
        """
        retry_count = 0
        row_count = 0
        original_autocommit = self.conn.autocommit
        
        while retry_count < max_retries:
            try:
                # Ensure no open transaction
                if self.conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                    self.conn.rollback()
                self.conn.autocommit = False
                
                # Use context manager for connection
                with self.conn:
                    # Lock relevant records
                    lock_query = """
                        SELECT 1 FROM rpc_server_table.rpc_server_table
                        WHERE server_path = %s::ltree
                        FOR UPDATE NOWAIT
                    """
                    self.cursor.execute(lock_query, (server_path,))
                    
                    # Update records with all specified changes
                    update_query = """
                        UPDATE rpc_server_table.rpc_server_table
                        SET request_id = gen_random_uuid(),
                            request_payload = '{}',
                            completed_timestamp = CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
                            state = 'empty',
                            rpc_client_queue = NULL
                        WHERE server_path = %s::ltree
                    """
                    self.cursor.execute(update_query, (server_path,))
                    
                    row_count = self.cursor.rowcount
                    self.conn.commit()
                    
                    return row_count
                
            except psycopg2.errors.LockNotAvailable:
                self.conn.rollback()
                retry_count += 1
                
                if retry_count < max_retries:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to acquire lock after {max_retries} attempts for server path: {server_path}")
            
            except psycopg2.Error as e:
                self.conn.rollback()
                raise Exception(f"Failed to clear reply queue for {server_path}: {str(e)}")
            
            finally:
                self.conn.autocommit = original_autocommit
        
        return row_count