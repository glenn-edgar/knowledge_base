import time
import json
import psycopg2
from psycopg2 import errors, sql


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
            
        Raises:
            Exception: If there's an error executing the query
        """
        try:
            query = """
                SELECT COUNT(*)
                FROM job_table.job_table
                WHERE path = %s
                AND valid = TRUE
            """
            
            self.cursor.execute(query, (path,))
            result = self.cursor.fetchone()
            
            # Handle potential None result
            if result is None:
                return 0
                
            return result[0]
            
        except Exception as e:
            # Since this is a read-only operation, no need to rollback
            # But we should propagate the error
            raise Exception(f"Error counting queued jobs: {str(e)}")
        
    def get_free_number(self, path):
        """
        Count the number of job entries where valid is false for a given path.
        
        Args:
            path (str): The path to search for in LTREE format
            
        Returns:
            int: Number of invalid jobs for the given path
            
        Raises:
            Exception: If there's an error executing the query
        """
        try:
            query = """
                SELECT COUNT(*)
                FROM job_table.job_table
                WHERE path = %s
                AND valid = FALSE
            """
            
            self.cursor.execute(query, (path,))
            result = self.cursor.fetchone()
            
            # Handle potential None result
            if result is None:
                return 0
                
            return result[0]
        
        except Exception as e:
            # Since this is a read-only operation, no need to rollback
            # But we should propagate the error
            raise Exception(f"Error counting free jobs: {str(e)}")
        
    def peak_job_data(self, path, max_retries=3, retry_delay=1):
        """
        Find the job with the earliest schedule_at time for a given path where 
        valid is true and is_active is false, update its started_at timestamp to current time,
        set is_active to true, and return the job information.

        Args:
            path (str): The path to search for in LTREE format
            max_retries (int): Maximum number of retries in case of lock conflicts
            retry_delay (float): Delay in seconds between retries

        Returns:
            tuple/None: A tuple containing (id, data, schedule_at) from the job, or None if no jobs found
        """
        attempt = 0
        while attempt < max_retries:
            try:
                # 1) try to select & lock the next available job
                find_query = """
                    SELECT id, data, schedule_at
                      FROM job_table.job_table
                     WHERE path = %s
                       AND valid = TRUE
                       AND is_active = FALSE
                     ORDER BY schedule_at ASC
                     FOR UPDATE SKIP LOCKED
                     LIMIT 1
                """
                self.cursor.execute(find_query, (path,))
                result = self.cursor.fetchone()

                # 2) no jobs available
                if result is None:
                    self.conn.rollback()
                    return None

                job_id, job_data, schedule_at = result

                # 3) mark it as active
                update_query = """
                    UPDATE job_table.job_table
                       SET started_at = NOW(),
                           is_active  = TRUE
                     WHERE id = %s
                     RETURNING id
                """
                self.cursor.execute(update_query, (job_id,))
                update_result = self.cursor.fetchone()

                if update_result is None:
                    # shouldn't happen, but safe guard
                    self.conn.rollback()
                    return None

                self.conn.commit()
                return job_id, job_data, schedule_at

            except errors.LockNotAvailable:
                # someone else holds the lock: rollback & retry
                self.conn.rollback()
                attempt += 1
                time.sleep(retry_delay)

            except Exception:
                # any other error: rollback & propagate
                self.conn.rollback()
                raise

        # exhausted retries without getting a lock
        raise RuntimeError(
            f"Could not lock and claim a job for path={path!r} after {max_retries} retries"
        )
    
    def mark_job_completed(self,
                           id: int,
                           max_retries: int = 3,
                           retry_delay: float = 1.0
                           ) -> bool:
        """
        For a record matching the given id, set completed_at to current time,
        set valid to FALSE, and set is_active to FALSE. Protects against
        parallel transactions with retries.

        Args:
            id (int): The ID of the job record
            max_retries (int): Maximum number of retries in case of lock conflicts
            retry_delay (float): Delay in seconds between retries

        Returns:
            bool: True if the operation was successful

        Raises:
            Exception: If no matching record is found
            RuntimeError: If unable to obtain lock after max_retries
        """
        attempt = 0
        while attempt < max_retries:
            try:
                with self.conn.cursor() as cur:
                    # begin transaction
                    cur.execute("BEGIN;")

                    # try to lock the specific row
                    cur.execute("""
                        SELECT id
                          FROM job_table.job_table
                         WHERE id = %s
                         FOR UPDATE NOWAIT
                    """, (id,))
                    row = cur.fetchone()

                    # if no row, nothing to complete
                    if not row:
                        cur.execute("COMMIT;")
                        raise Exception(f"No job found with id={id}")

                    # perform the update to mark completion
                    cur.execute("""
                        UPDATE job_table.job_table
                           SET completed_at = NOW(),
                               valid        = FALSE,
                               is_active    = FALSE
                         WHERE id = %s
                    """, (id,))

                    # commit and return success
                    cur.execute("COMMIT;")
                    return True

            except errors.LockNotAvailable:
                # another transaction holds the lock: rollback and retry
                self.conn.rollback()
                attempt += 1
                time.sleep(retry_delay)

            except Exception:
                # rollback and propagate any other error
                self.conn.rollback()
                raise

        # if we exhaust retries without locking, raise an error
        raise RuntimeError(f"Could not lock job id={id} after {max_retries} attempts")
    
    def push_job_data(self, path, data, max_retries=3, retry_delay=1):
        """
        Find an available record (valid=False) for the given path with the earliest completed_at time,
        update it with new data, and prepare it for scheduling.

        Args:
            path (str): The path in LTREE format
            data (dict): The JSON data to insert
            max_retries (int): Maximum number of retries in case of lock conflicts
            retry_delay (float): Delay in seconds between retries

        Returns:
            int: The ID of the updated record

        Raises:
            Exception: If no available record is found or if locks aren’t obtained after retries
        """
        select_sql = sql.SQL("""
            SELECT id
            FROM job_table.job_table
            WHERE path = %s
            AND valid = FALSE
            ORDER BY completed_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """)
        update_sql = sql.SQL("""
            UPDATE job_table.job_table
            SET data = %s,
                schedule_at = timezone('UTC', now()),
                started_at  = timezone('UTC', now()),
                completed_at= timezone('UTC', now()),
                valid      = TRUE,
                is_active  = FALSE
            WHERE id = %s
            RETURNING id
        """)

        for attempt in range(1, max_retries + 1):
            try:
                # start a new transaction block
                with self.conn:
                    with self.conn.cursor() as cur:
                        # try to grab a row
                        cur.execute(select_sql, (path,))
                        row = cur.fetchone()
                        if not row:
                            raise Exception(f"No available job slot for path '{path}'")

                        job_id = row[0]
                        # update it
                        cur.execute(update_sql, (json.dumps(data), job_id))
                        updated = cur.fetchone()
                        return updated[0]

            except (psycopg2.OperationalError, psycopg2.errors.LockNotAvailable) as e:
                # Lock not available → retry
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise Exception(f"Could not acquire lock after {max_retries} attempts") from e
        
    def list_pending_jobs(self, path, limit=None, offset=0):
        """
        List all jobs for a given path where valid is True and is_active is False,
        ordered by schedule_at with earliest first.
        
        Args:
            path (str): The path to search for in LTREE format
            limit (int, optional): Maximum number of jobs to return
            offset (int, optional): Number of jobs to skip
        
        Returns:
            list: A list of dictionaries containing all job details
            
        Raises:
            Exception: If there's an error executing the query
        """
        try:
            # Build the query with optional LIMIT and OFFSET
            query = """
                SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
                FROM job_table.job_table
                WHERE path = %s
                AND valid = TRUE
                AND is_active = FALSE
                ORDER BY schedule_at ASC
            """
            
            params = [path]
            
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
            raise Exception(f"Error listing pending jobs: {str(e)}")
        
        
    def list_active_jobs(self, path, limit=None, offset=0):
        """
        List all jobs for a given path where valid is True and is_active is True,
        ordered by started_at with earliest first.
        
        Args:
            path (str): The path to search for in LTREE format
            limit (int, optional): Maximum number of jobs to return
            offset (int, optional): Number of jobs to skip
        
        Returns:
            list: A list of dictionaries containing all job details
            
        Raises:
            Exception: If there's an error executing the query
        """
        try:
            # Build the query with optional LIMIT and OFFSET
            query = """
                SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
                FROM job_table.job_table
                WHERE path = %s
                AND valid = TRUE
                AND is_active = TRUE
                ORDER BY started_at ASC
            """
            
            params = [path]
            
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
            raise Exception(f"Error listing active jobs: {str(e)}")
        
    def list_completed_jobs(self, path, limit=None, offset=0, completed_after=None, completed_before=None):
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
                SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
                FROM job_table.job_table
                WHERE path = %s
                AND valid = FALSE
                AND is_active = FALSE
            """
            
            params = [path]
            
            # Add optional time-based filters
            if completed_after is not None:
                query += " AND completed_at >= %s"
                params.append(completed_after)
                
            if completed_before is not None:
                query += " AND completed_at <= %s"
                params.append(completed_before)
                
            # Add ordering
            query += " ORDER BY completed_at ASC"
            
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
            raise Exception(f"Error listing completed jobs: {str(e)}")
        
        
    def clear_job_queue(self, path):
        try:
            # First acquire a lock on the table to prevent concurrent access
            self.cursor.execute("LOCK TABLE job_table.job_table IN EXCLUSIVE MODE;")
            
            # Prepare the query to update only rows with matching path
            # Using SQL's NOW() function for timestamps
            update_query = """
                UPDATE job_table.job_table
                SET schedule_at = NOW(),
                    started_at = NOW(),
                    completed_at = NOW(),
                    is_active = %s,
                    valid = %s,
                    data = %s
                WHERE path = %s;
            """
            
            # Execute the update with our parameters including the path
            # NOW() is handled by SQL so it's not in the parameter tuple
            self.cursor.execute(update_query, (False, False, '{}', path))
            
            # Get number of rows affected
            row_count = self.cursor.rowcount
            
            # Commit the transaction which will automatically release all locks
            self.conn.commit()
            
            return row_count
            
        except Exception as e:
            # Rollback in case of error, which will also release the lock
            self.conn.rollback()
            print(f"Error in clear_job_queue: {str(e)}")
            return -1