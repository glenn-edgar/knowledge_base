import time
import uuid
import json
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import OperationalError
from psycopg2 import errors
from psycopg2 import sql
import psycopg2.extras
psycopg2.extras.register_uuid()
from psycopg2 import errors
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
      
        result = self.find_node_ids(node_name, properties, node_path)
        if len(result) == 0:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        if len(result) > 1:
            raise ValueError(f"Multiple nodes found matching path parameters: {node_name}, {properties}, {node_path}")
        return result
    
    def find_rpc_client_ids(self, node_name, properties, node_path):
        """
        Find the node id for a given node name, properties, node path.
        """
       
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
        """
        Extract key values from key_data.
        """
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
            with self.conn.cursor() as cursor:
                # First, verify that there are records with the given client_path
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM rpc_client_table.rpc_client_table 
                    WHERE client_path = %s
                """, (client_path,))
                
                total_records = cursor.fetchone()[0]
                
                if total_records == 0:
                    raise Exception(f"No records found for client_path: {client_path}")
                
                # Now, count the number of free slots (where is_new_result is FALSE)
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM rpc_client_table.rpc_client_table 
                    WHERE client_path = %s AND is_new_result = FALSE
                """, (client_path,))
                
                free_slots = cursor.fetchone()[0]
                
                return free_slots
                
        except psycopg2.Error as e:
            raise Exception(f"Database error when finding free slots: {str(e)}")
        
    def find_queued_slots(self, client_path):
        """
        Find the number of queued slots (records with is_new_result=TRUE) for a given client_path.
        
        Args:
            client_path (str): LTree compatible path for client
            
        Returns:
            int: Number of queued slots available for the client_path
            
        Raises:
            Exception: If no records exist for the specified client_path
        """
        try:
            with self.conn.cursor() as cursor:
                # First, verify that there are records with the given client_path
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM rpc_client_table.rpc_client_table 
                    WHERE client_path = %s
                """, (client_path,))
                
                total_records = cursor.fetchone()[0]
                
                if total_records == 0:
                    raise Exception(f"No records found for client_path: {client_path}")
                
                # Count the number of queued slots (where is_new_result is TRUE)
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM rpc_client_table.rpc_client_table 
                    WHERE client_path = %s AND is_new_result = TRUE
                """, (client_path,))
                
                queued_slots = cursor.fetchone()[0]
                
                return queued_slots
                
        except psycopg2.Error as e:
            raise Exception(f"Database error when finding queued slots: {str(e)}")
    
    def peak_reply_data(self,
                        client_path: str,
                        max_retries: int = 3,
                        retry_delay: float = 1.0
                        ) -> tuple[int, dict]:
        """
        For a given client_path, find the first record where is_new_result is TRUE
        with the earliest response_timestamp.

        Uses row‐level locking with retries to avoid blocking on concurrent consumers.

        Args:
            client_path (str): LTree compatible path for client
            max_retries (int): Maximum number of retries in case of lock conflicts
            retry_delay (float): Delay in seconds between retries

        Returns:
            tuple: (id, data_dict) where:
                - id is the record ID
                - data_dict is a dictionary containing all fields of the record

        Raises:
            Exception: If no records with is_new_result=TRUE are ever found
            RuntimeError: If all matching rows are locked after max_retries
        """
        attempt = 0
        while attempt < max_retries:
            try:
                with self.conn.cursor() as cur:
                    # 1) make sure there's something to peek
                    cur.execute("""
                        SELECT COUNT(*) 
                          FROM rpc_client_table.rpc_client_table
                         WHERE client_path = %s
                           AND is_new_result = TRUE
                    """, (client_path,))
                    total = cur.fetchone()[0]
                    if total == 0:
                        raise Exception(f"No new replies for client_path={client_path!r}")

                    # 2) grab the earliest by timestamp, skipping any locked by others
                    cur.execute("""
                        SELECT *
                          FROM rpc_client_table.rpc_client_table
                         WHERE client_path = %s
                           AND is_new_result = TRUE
                         ORDER BY response_timestamp ASC
                         FOR UPDATE SKIP LOCKED
                         LIMIT 1
                    """, (client_path,))

                    row = cur.fetchone()
                    if not row:
                        # rows exist but all are locked → retry
                        self.conn.rollback()
                        attempt += 1
                        time.sleep(retry_delay)
                        continue

                    # 3) map columns to values
                    cols = [desc.name for desc in cur.description]
                    data = dict(zip(cols, row))
                    record_id = data.pop('id')

                    # 4) rollback (we only wanted to peek, not modify)
                    self.conn.rollback()
                    return record_id, data

            except errors.LockNotAvailable:
                # lock conflict: rollback and retry
                self.conn.rollback()
                attempt += 1
                time.sleep(retry_delay)

        # after all retries we still couldn't lock anything
        raise RuntimeError(f"Could not lock a new-reply row after {max_retries} attempts")
            

    def release_rpc_data(self,
                         client_path: str,
                         record_id: int,
                         max_retries: int = 3,
                         retry_delay: float = 1.0
                         ) -> bool:
        """
        For a given ID, verify the record matches the client_path and is_new_result is TRUE,
        then set is_new_result to FALSE. Contains protection against parallel transactions.

        Args:
            client_path (str): LTree compatible path for client
            record_id (int): The ID of the record to release
            max_retries (int): Maximum number of retries in case of lock conflicts
            retry_delay (float): Delay in seconds between retries

        Returns:
            bool: True if a record was successfully released, False if no matching record was found

        Raises:
            RuntimeError: If cannot obtain lock after retries
            Exception: On any other database error
        """
        attempt = 0
        while attempt < max_retries:
            try:
                with self.conn.cursor() as cur:
                    # start a transaction
                    cur.execute("BEGIN;")
                    
                    # try to lock the specific row
                    cur.execute("""
                        SELECT id
                          FROM rpc_client_table.rpc_client_table
                         WHERE id = %s
                           AND client_path = %s
                           AND is_new_result = TRUE
                         FOR UPDATE NOWAIT
                    """, (record_id, client_path))
                    row = cur.fetchone()

                    # if no match (wrong id, path, or already released), bail out
                    if not row:
                        cur.execute("COMMIT;")
                        return False

                    # perform the release
                    cur.execute("""
                        UPDATE rpc_client_table.rpc_client_table
                           SET is_new_result = FALSE
                         WHERE id = %s
                    """, (record_id,))

                    cur.execute("COMMIT;")
                    return True

            except errors.LockNotAvailable:
                # someone else holds the lock—rollback, wait, and retry
                self.conn.rollback()
                attempt += 1
                time.sleep(retry_delay)

            except Exception:
                # any other error: rollback and re‐raise
                self.conn.rollback()
                raise

        # if we exhaust retries without locking, signal failure
        raise RuntimeError(f"Could not lock record id={record_id} after {max_retries} attempts")
    def clear_reply_queue(self,
                          client_path: str,
                          max_retries: int = 3,
                          retry_delay: float = 1.0
                          ) -> int:
        """
        Clear the reply queue by resetting records matching the specified client_path.

        For each matching record:
        - Sets a unique UUID for request_id
        - Sets server_path equal to client_path
        - Resets response_payload to empty JSON object
        - Updates response_timestamp to current UTC time
        - Sets is_new_result to FALSE

        Includes record locking with retries to handle concurrent access.

        Args:
            client_path (ltree value): The client path to match for clearing records
            max_retries (int): Maximum number of retries for acquiring the lock
            retry_delay (float): Delay in seconds between retry attempts

        Returns:
            int: Number of records updated
        """
        attempt = 0
        while attempt < max_retries:
            try:
                with self.conn.cursor() as cur:
                    # BEGIN a transaction explicitly
                    cur.execute("BEGIN;")

                    # Lock all matching rows without waiting
                    cur.execute("""
                        SELECT id
                        FROM rpc_client_table.rpc_client_table
                        WHERE client_path = %s
                        FOR UPDATE NOWAIT
                    """, (client_path,))
                    rows = cur.fetchall()

                    # If no rows found, nothing to do
                    if not rows:
                        cur.execute("COMMIT;")
                        return 0

                    updated = 0
                    # Update each locked row individually, assigning a fresh UUID per row
                    for (row_id,) in rows:
                        new_uuid = str(uuid.uuid4())
                        cur.execute("""
                            UPDATE rpc_client_table.rpc_client_table
                            SET
                                request_id        = %s,
                                server_path       = %s,
                                response_payload  = %s,
                                response_timestamp= NOW(),
                                is_new_result     = FALSE
                            WHERE id = %s
                        """, (new_uuid, client_path, json.dumps({}), row_id))
                        updated += cur.rowcount

                    # Commit all updates
                    cur.execute("COMMIT;")
                    return updated

            except errors.LockNotAvailable:
                # Roll back and retry after a short pause
                self.conn.rollback()
                attempt += 1
                time.sleep(retry_delay)

        # If we exit the loop, we never acquired the lock
        raise RuntimeError(f"Could not acquire lock after {max_retries} retries")
    
    def push_reply_data(self, client_path, request_uuid, server_path, rpc_action, transaction_tag, reply_data, max_retries=3, retry_delay=1):
        """
        Update the earliest record with is_new_result=False that matches the client_path in rpc_client_table.rpc_client_table.
        
        Args:
            self.conn and self.cursor are postgres connectors
            client_path (str): LTree compatible path for client
            request_uuid (str): UUID of the request
            server_path (str): LTree compatible path for server
            rpc_action (str): Action to be performed
            transaction_tag (str): Transaction tag
            reply_data (dict): Dictionary containing reply data
            max_retries (int, optional): Maximum number of retry attempts for transient errors. Defaults to 3.
            retry_delay (float, optional): Delay in seconds between retry attempts. Defaults to 1.
            
        Raises:
            Exception: If no available record with is_new_result=False is found or if all retries fail
        """
        attempt = 0
        last_error = None
        
        while attempt <= max_retries:
            try:
                # Query to find the earliest record with is_new_result=False for the client_path
                select_query = """
                    SELECT id 
                    FROM rpc_client_table.rpc_client_table 
                    WHERE client_path = %s 
                    AND is_new_result = FALSE 
                    ORDER BY response_timestamp ASC 
                    LIMIT 1
                """
                self.cursor.execute(select_query, (client_path,))
                record = self.cursor.fetchone()
                
                if not record:
                    raise Exception("No available record with is_new_result=False found for the given client_path")
                
                record_id = record[0]
                
                # Update the record with provided data
                update_query = """
                    UPDATE rpc_client_table.rpc_client_table 
                    SET request_id = %s,
                        server_path = %s,
                        rpc_action = %s,
                        transaction_tag = %s,
                        response_payload = %s,
                        is_new_result = TRUE,
                        response_timestamp = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                self.cursor.execute(update_query, (
                    request_uuid,
                    server_path,
                    rpc_action,
                    transaction_tag,
                    json.dumps(reply_data),  # Convert dict to JSONB for PostgreSQL
                    record_id
                ))
                
                # Commit the transaction
                self.conn.commit()
                return  # Success, exit the function
                
            except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
                # Rollback in case of error
                self.conn.rollback()
                last_error = e
                attempt += 1
                
                if attempt > max_retries:
                    raise Exception(f"Failed after {max_retries} retries: {str(last_error)}")
                
                # Wait before retrying
                time.sleep(retry_delay)
            except Exception as e:
                # Rollback for non-transient errors and re-raise immediately
                self.conn.rollback()
                raise e
        
        # This line should never be reached due to the raise in the retry loop
        raise Exception(f"Failed after {max_retries} retries: {str(last_error)}")
    
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
            with self.conn.cursor() as cursor:
                if client_path is None:
                    query = """
                        SELECT id, request_id, client_path, server_path, 
                               response_payload, response_timestamp, is_new_result
                        FROM rpc_client_table.rpc_client_table
                        WHERE is_new_result = TRUE
                        ORDER BY response_timestamp ASC
                    """
                    params = ()
                else:
                    query = """
                        SELECT id, request_id, client_path, server_path, 
                               response_payload, response_timestamp, is_new_result
                        FROM rpc_client_table.rpc_client_table
                        WHERE is_new_result = TRUE AND client_path = %s
                        ORDER BY response_timestamp ASC
                    """
                    params = (client_path,)
                
                cursor.execute(query, params)
                
                column_names = [desc[0] for desc in cursor.description]
                
                records = cursor.fetchall()
                
                result = []
                for record in records:
                    record_dict = dict(zip(column_names, record))
                    
                    if 'request_id' in record_dict and record_dict['request_id'] is not None:
                        record_dict['request_id'] = str(record_dict['request_id'])
                    
                    if 'response_timestamp' in record_dict and isinstance(record_dict['response_timestamp'], datetime):
                        record_dict['response_timestamp'] = record_dict['response_timestamp'].isoformat()
                    
                    if 'client_path' in record_dict and record_dict['client_path'] is not None:
                        record_dict['client_path'] = str(record_dict['client_path'])
                    
                    if 'server_path' in record_dict and record_dict['server_path'] is not None:
                        record_dict['server_path'] = str(record_dict['server_path'])
                    
                    result.append(record_dict)
                
                return result
                
        except psycopg2.Error as e:
            raise Exception(f"Database error when listing waiting jobs: {str(e)}")