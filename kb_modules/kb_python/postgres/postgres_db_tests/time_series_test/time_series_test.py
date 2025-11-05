import os
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import pandas as pd  # For demonstration of data handling; optional but useful

class TimeseriesManager:
    """
    A general-purpose class for managing timeseries data in a PostgreSQL database
    with TimescaleDB extension. Assumes TimescaleDB is installed and enabled.
    
    Note: Several TimescaleDB operations (create_hypertable, continuous aggregate
    operations) must run outside transaction blocks. These methods temporarily
    enable autocommit mode to handle this requirement.
    
    Args:
        conn (psycopg2.connection): An active PostgreSQL connection object.
    """
    
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()
    
    def is_hypertable(self, table_name):
        """
        Check if a table is already a hypertable.
        
        Args:
            table_name (str): Name of the table to check.
            
        Returns:
            bool: True if table is a hypertable, False otherwise.
        """
        query = sql.SQL("""
            SELECT EXISTS (
                SELECT 1 FROM timescaledb_information.hypertables 
                WHERE hypertable_name = {}
            );
        """).format(sql.Literal(table_name))
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def table_exists(self, table_name, schema='public'):
        """
        Check if a table exists in the database.
        
        Args:
            table_name (str): Name of the table.
            schema (str): Schema name, defaults to 'public'.
            
        Returns:
            bool: True if table exists, False otherwise.
        """
        query = sql.SQL("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = {} AND table_name = {}
            );
        """).format(sql.Literal(schema), sql.Literal(table_name))
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def drop_table(self, table_name, cascade=False):
        """
        Drop a table (works for both regular tables and hypertables).
        
        Args:
            table_name (str): Name of the table to drop.
            cascade (bool): If True, automatically drop objects that depend on the table.
        """
        cascade_sql = sql.SQL("CASCADE") if cascade else sql.SQL("")
        drop_query = sql.SQL("""
            DROP TABLE IF EXISTS {} {}
        """).format(
            sql.Identifier(table_name),
            cascade_sql
        )
        
        # Save current autocommit state
        old_autocommit = self.conn.autocommit
        
        try:
            # Some DROP operations may need to run outside transaction blocks
            self.conn.commit()  # Commit any pending transaction
            self.conn.autocommit = True
            
            self.cursor.execute(drop_query)
            
        finally:
            # Restore original autocommit state
            self.conn.autocommit = old_autocommit
    
    def create_table(self, table_name, columns_sql):
        """
        Create a regular table.
        
        Args:
            table_name (str): Name of the table.
            columns_sql (str): SQL fragment defining columns, e.g., "(time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION)".
        """
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} {}
        """).format(
            sql.Identifier(table_name),
            sql.SQL(columns_sql)
        )
        self.cursor.execute(create_table_query)
        self.conn.commit()
    
    def create_hypertable(self, table_name, time_column_name, chunk_time_interval=None, if_not_exists=True):
        """
        Convert a table to a hypertable for timeseries optimization.
        
        Args:
            table_name (str): Name of the table.
            time_column_name (str): Name of the timestamp column.
            chunk_time_interval (str, optional): Chunk interval, e.g., '1 day'. Defaults to '1 day' if None.
            if_not_exists (bool): If True, skip if already a hypertable. Defaults to True.
        """
        # Check if already a hypertable
        if if_not_exists and self.is_hypertable(table_name):
            print(f"Table '{table_name}' is already a hypertable, skipping creation.")
            return
        
        if chunk_time_interval is None:
            chunk_time_interval = '1 day'
        
        # Fixed: Properly compose the interval - sql.Literal already handles quoting
        create_hypertable_query = sql.SQL("""
            SELECT create_hypertable({}, {}, chunk_time_interval => INTERVAL {});
        """).format(
            sql.Literal(table_name),
            sql.Literal(time_column_name),
            sql.Literal(chunk_time_interval)
        )
        self.cursor.execute(create_hypertable_query)
        self.conn.commit()
    
    def insert_data(self, table_name, data, columns=None):
        """
        Insert timeseries data into the table. Data can be a list of tuples or a DataFrame.
        
        Args:
            table_name (str): Name of the table.
            data: List of tuples (e.g., [(timestamp, value), ...]) or pandas DataFrame.
            columns (list, optional): List of column names. If None, assumes ['time', 'value'].
        """
        if columns is None:
            columns = ['time', 'value']
        
        if isinstance(data, pd.DataFrame):
            # Convert DataFrame to list of tuples for insertion
            data = [tuple(row) for row in data.itertuples(index=False)]
        
        # Fixed: Use proper identifier composition for columns
        columns_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
        insert_query = sql.SQL("""
            INSERT INTO {} ({}) VALUES %s
        """).format(
            sql.Identifier(table_name),
            columns_sql
        )
        
        # Use psycopg2.extras.execute_values for batch insert
        from psycopg2.extras import execute_values
        execute_values(self.cursor, insert_query, data)
        self.conn.commit()
    
    def query_time_bucket(self, table_name, bucket_interval, start_time, end_time, agg_func='avg'):
        """
        Query aggregated data using time_bucket for bucketing.
        
        Args:
            table_name (str): Name of the table.
            bucket_interval (str): Bucket size, e.g., '1 hour'.
            start_time (str or datetime): Start timestamp.
            end_time (str or datetime): End timestamp.
            agg_func (str): Aggregation function, e.g., 'avg', 'sum', 'count'.
        
        Returns:
            list: List of tuples with (bucket_time, aggregated_value).
        """
        # Validate aggregation function to prevent SQL injection
        valid_agg_funcs = {'avg', 'sum', 'count', 'min', 'max', 'stddev'}
        if agg_func.lower() not in valid_agg_funcs:
            raise ValueError(f"Invalid aggregation function. Must be one of {valid_agg_funcs}")
        
        if isinstance(start_time, datetime):
            start_time = start_time.isoformat()
        if isinstance(end_time, datetime):
            end_time = end_time.isoformat()
        
        # Fixed: Removed incorrect quote wrapping - sql.Literal handles it
        query = sql.SQL("""
            SELECT time_bucket(INTERVAL {}, time) AS bucket,
                   {} (value) AS {}
            FROM {}
            WHERE time >= {} AND time <= {}
            GROUP BY bucket
            ORDER BY bucket;
        """).format(
            sql.Literal(bucket_interval),
            sql.SQL(agg_func.upper()),
            sql.Identifier(agg_func),
            sql.Identifier(table_name),
            sql.Literal(start_time),
            sql.Literal(end_time)
        )
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def add_continuous_aggregate(self, table_name, agg_name, bucket_interval, refresh_interval=None):
        """
        Create a continuous aggregate view for real-time aggregation.
        
        Args:
            table_name (str): Name of the hypertable.
            agg_name (str): Name of the continuous aggregate view.
            bucket_interval (str): Bucket interval for aggregation.
            refresh_interval (str, optional): Materialized refresh interval, e.g., '1 hour'.
        """
        if refresh_interval is None:
            refresh_interval = bucket_interval
        
        # Save current autocommit state
        old_autocommit = self.conn.autocommit
        
        try:
            # Continuous aggregates must be created outside transaction blocks
            self.conn.commit()  # Commit any pending transaction
            self.conn.autocommit = True
            
            # Fixed: Removed incorrect quote wrapping
            create_cagg_query = sql.SQL("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS {} 
                WITH (timescaledb.continuous) AS
                SELECT time_bucket(INTERVAL {}, time) AS bucket,
                       AVG(value) AS avg_value,
                       COUNT(value) AS count_value
                FROM {}
                GROUP BY bucket;
            """).format(
                sql.Identifier(agg_name),
                sql.Literal(bucket_interval),
                sql.Identifier(table_name)
            )
            self.cursor.execute(create_cagg_query)
            
            # Fixed: Proper interval composition and policy parameters
            # start_offset should be NULL or INTERVAL '0' for real-time aggregates
            add_policy_query = sql.SQL("""
                SELECT add_continuous_aggregate_policy({},
                    start_offset => NULL,
                    end_offset => INTERVAL {},
                    schedule_interval => INTERVAL {});
            """).format(
                sql.Literal(agg_name),
                sql.Literal('1 hour'),  # End offset - how far from now to aggregate
                sql.Literal(refresh_interval)
            )
            self.cursor.execute(add_policy_query)
            
        finally:
            # Restore original autocommit state
            self.conn.autocommit = old_autocommit
    
    def query_continuous_aggregate(self, agg_name, start_time, end_time):
        """
        Query the continuous aggregate view.
        
        Args:
            agg_name (str): Name of the continuous aggregate view.
            start_time (str or datetime): Start timestamp.
            end_time (str or datetime): End timestamp.
        
        Returns:
            list: List of tuples with (bucket, avg_value, count_value).
        """
        if isinstance(start_time, datetime):
            start_time = start_time.isoformat()
        if isinstance(end_time, datetime):
            end_time = end_time.isoformat()
        
        # Fixed: Removed incorrect quote wrapping
        query = sql.SQL("""
            SELECT * FROM {}
            WHERE bucket >= {} AND bucket <= {}
            ORDER BY bucket;
        """).format(
            sql.Identifier(agg_name),
            sql.Literal(start_time),
            sql.Literal(end_time)
        )
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def refresh_continuous_aggregate(self, agg_name, start_time=None, end_time=None):
        """
        Manually refresh a continuous aggregate view.
        
        Args:
            agg_name (str): Name of the continuous aggregate view.
            start_time (str or datetime, optional): Start of refresh window. NULL = beginning.
            end_time (str or datetime, optional): End of refresh window. NULL = now.
        """
        if start_time is not None and isinstance(start_time, datetime):
            start_time = start_time.isoformat()
        if end_time is not None and isinstance(end_time, datetime):
            end_time = end_time.isoformat()
        
        # Build the refresh call with proper NULL handling
        if start_time is None:
            start_param = sql.SQL("NULL")
        else:
            start_param = sql.Literal(start_time)
        
        if end_time is None:
            end_param = sql.SQL("NULL")
        else:
            end_param = sql.Literal(end_time)
        
        refresh_query = sql.SQL("""
            CALL refresh_continuous_aggregate({}, {}, {});
        """).format(
            sql.Literal(agg_name),
            start_param,
            end_param
        )
        
        # Save current autocommit state
        old_autocommit = self.conn.autocommit
        
        try:
            # Refresh must be called outside transaction blocks
            self.conn.commit()  # Commit any pending transaction
            self.conn.autocommit = True
            
            self.cursor.execute(refresh_query)
            
        finally:
            # Restore original autocommit state
            self.conn.autocommit = old_autocommit
    
    def list_hypertables(self):
        """
        List all hypertables in the database.
        
        Returns:
            list: List of tuples with (schema_name, table_name).
        """
        query = sql.SQL("""
            SELECT hypertable_schema, hypertable_name 
            FROM timescaledb_information.hypertables
            ORDER BY hypertable_schema, hypertable_name;
        """)
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def list_continuous_aggregates(self):
        """
        List all continuous aggregates in the database.
        
        Returns:
            list: List of tuples with (view_schema, view_name, hypertable_name).
        """
        query = sql.SQL("""
            SELECT view_schema, view_name, hypertable_name
            FROM timescaledb_information.continuous_aggregates
            ORDER BY view_schema, view_name;
        """)
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def drop_continuous_aggregate(self, agg_name, cascade=False):
        """
        Drop a continuous aggregate view.
        
        Args:
            agg_name (str): Name of the continuous aggregate view to drop.
            cascade (bool): If True, automatically drop dependent objects.
        """
        cascade_sql = sql.SQL("CASCADE") if cascade else sql.SQL("")
        drop_query = sql.SQL("""
            DROP MATERIALIZED VIEW IF EXISTS {} {}
        """).format(
            sql.Identifier(agg_name),
            cascade_sql
        )
        
        # Save current autocommit state
        old_autocommit = self.conn.autocommit
        
        try:
            # Some DROP operations may need to run outside transaction blocks
            self.conn.commit()  # Commit any pending transaction
            self.conn.autocommit = True
            
            self.cursor.execute(drop_query)
            
        finally:
            # Restore original autocommit state
            self.conn.autocommit = old_autocommit
    
    def close(self):
        """Close the cursor and connection."""
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    # Database connection parameters
    HOST = 'localhost'  # Assuming local host; adjust if needed
    PORT = 5434
    USERNAME = 'gedgar'
    PASSWORD = os.environ.get('POSTGRES_PASSWORD')
    DBNAME = 'postgres'  # Assuming default database; adjust if needed
    
    if not PASSWORD:
        raise ValueError("Environment variable POSTGRES_PASSWORD must be set.")
    
    # Establish connection
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USERNAME,
        password=PASSWORD,
        dbname=DBNAME
    )
    
    try:
        # Instantiate the manager
        ts_manager = TimeseriesManager(conn)
        
        # Show existing hypertables and continuous aggregates
        print("=== Existing Hypertables ===")
        hypertables = ts_manager.list_hypertables()
        if hypertables:
            for schema, name in hypertables:
                print(f"  {schema}.{name}")
        else:
            print("  (none)")
        
        print("\n=== Existing Continuous Aggregates ===")
        caggs = ts_manager.list_continuous_aggregates()
        if caggs:
            for schema, name, hypertable in caggs:
                print(f"  {schema}.{name} (from {hypertable})")
        else:
            print("  (none)")
        
        # Cleanup from previous runs
        table_name = 'test_timeseries'
        agg_name = 'test_cagg'
        print(f"\n=== Cleaning up previous test runs ===")
        ts_manager.drop_continuous_aggregate(agg_name, cascade=True)
        ts_manager.drop_table(table_name, cascade=True)
        print("Cleanup complete.\n")
        
        # Test 1: Create table and hypertable
        print("=== Test 1: Create Table and Hypertable ===")
        ts_manager.create_table(table_name, '(time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION)')
        ts_manager.create_hypertable(table_name, 'time', '1 hour')
        print(f"Created hypertable: {table_name}")
        
        # Test 2: Generate and insert sample data (last 24 hours, hourly values)
        print("\n=== Test 2: Insert Sample Data ===")
        now = datetime.now()
        sample_data = []
        for i in range(24):
            timestamp = now - timedelta(hours=i)
            value = 20 + 5 * (i % 5) + (i * 0.1)  # Simulated trending data
            sample_data.append((timestamp, value))
        sample_df = pd.DataFrame(sample_data, columns=['time', 'value'])
        ts_manager.insert_data(table_name, sample_df)
        print(f"Inserted {len(sample_data)} rows of sample data.")
        
        # Test 3: Query with time bucketing (hourly average)
        print("\n=== Test 3: Time-Bucketed Query ===")
        start_time = now - timedelta(days=1)
        end_time = now
        bucketted_data = ts_manager.query_time_bucket(table_name, '1 hour', start_time, end_time, 'avg')
        print(f"Time-bucketed query results ({len(bucketted_data)} buckets):")
        for bucket, avg_val in bucketted_data[:5]:  # Show first 5
            print(f"  {bucket}: {avg_val:.2f}")
        if len(bucketted_data) > 5:
            print(f"  ... ({len(bucketted_data) - 5} more buckets)")
        
        # Test 4: Create continuous aggregate and query it
        print("\n=== Test 4: Continuous Aggregate ===")
        ts_manager.add_continuous_aggregate(table_name, agg_name, '6 hours')
        print(f"Created continuous aggregate: {agg_name}")
        
        # Manually refresh the continuous aggregate to populate it with data
        ts_manager.refresh_continuous_aggregate(agg_name, start_time, end_time)
        print("Refreshed continuous aggregate")
        
        cagg_data = ts_manager.query_continuous_aggregate(agg_name, start_time, end_time)
        print(f"Continuous aggregate query results ({len(cagg_data)} buckets):")
        for bucket, avg_val, count in cagg_data:
            print(f"  {bucket}: avg={avg_val:.2f}, count={count}")
        
        # Show final state
        print("\n=== Final State ===")
        print("Hypertables:")
        for schema, name in ts_manager.list_hypertables():
            print(f"  {schema}.{name}")
        print("Continuous Aggregates:")
        for schema, name, hypertable in ts_manager.list_continuous_aggregates():
            print(f"  {schema}.{name} (from {hypertable})")
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during tests: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        ts_manager.close()