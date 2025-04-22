import psycopg2
from psycopg2 import sql
import logging

class PostgresConnector:
    """
    A class to handle PostgreSQL database operations including
    connection, disconnection, and SQL script execution.
    """
    
    def __init__(self, host='localhost', port=5432, dbname=None, 
                 user=None, password=None):
        """
        Initialize the PostgreSQL connector with connection parameters.
        
        Args:
            host (str): Database host address
            port (int): Port number
            dbname (str): Database name
            user (str): Username
            password (str): Password
        """
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """
        Establish a connection to the PostgreSQL database.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        try:
            # Create connection
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            
            # Create cursor
            self.cursor = self.conn.cursor()
            
            self.logger.info(f"Successfully connected to database: {self.dbname}")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection error: {str(e)}")
            return False
    
    def disconnect(self):
        """
        Close the database connection and cursor.
        
        Returns:
            bool: True if disconnection was successful, False otherwise
        """
        try:
            if self.cursor:
                self.cursor.close()
                self.logger.info("Database cursor closed")
            
            if self.conn:
                self.conn.close()
                self.logger.info("Database connection closed")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Disconnection error: {str(e)}")
            return False
    
    def execute_script(self, script_path):
        """
        Execute SQL statements from a script file.
        
        Args:
            script_path (str): Path to the SQL script file
            
        Returns:
            bool: True if execution was successful, False otherwise
        """
        if not self.conn or not self.cursor:
            self.logger.error("Not connected to database")
            return False
            
        try:
            # Read SQL script
            with open(script_path, 'r') as file:
                sql_script = file.read()
                
            # Execute script
            self.cursor.execute(sql_script)
            
            # Commit changes
            self.conn.commit()
            
            self.logger.info(f"Successfully executed script: {script_path}")
            return True
            
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Script execution error: {str(e)}")
            return False
    
    def execute_query(self, query, params=None):
        """
        Execute a single SQL query with optional parameters.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            
        Returns:
            list: Query results if successful, None otherwise
        """
        if not self.conn or not self.cursor:
            self.logger.error("Not connected to database")
            return None
            
        try:
            # Execute query
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
                
            # Fetch results if any
            if self.cursor.description:
                results = self.cursor.fetchall()
                self.logger.info("Query executed successfully")
                return results
            else:
                # For non-SELECT queries
                self.conn.commit()
                self.logger.info("Query executed successfully")
                return True
                
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Query execution error: {str(e)}")
            return None
        
if __name__ == "__main__":
    """
    Test driver for PostgresConnector class.
    This will execute when the script is run directly.
    """
    import sys
    import os
    
    # Set up test parameters (can be overridden with command line arguments)
    host = 'localhost'
    port = 5432
    dbname = 'knowledge_base'
    user = 'gedgar'
    password = 'ready2go'
    
    test_script = '../sql/create_knowledge_table.sql'
    
    # Parse command line arguments if provided
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    if len(sys.argv) > 3:
        dbname = sys.argv[3]
    if len(sys.argv) > 4:
        user = sys.argv[4]
    if len(sys.argv) > 5:
        password = sys.argv[5]
    
    # Create connector instance
    connector = PostgresConnector(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    
    print(f"Testing PostgresConnector with database: {dbname} on {host}:{port}")
    
    # Test connection
    print("\nTesting connection...")
    if connector.connect():
        print("✅ Connection successful")
        
        # Test query execution
        print("\nTesting query execution...")
        results = connector.execute_query("SELECT version();")
        if results:
            print(f"✅ Query executed successfully. PostgreSQL version: {results[0][0]}")
        else:
            print("❌ Query execution failed")
        
        # Test script execution if a test script is available
        
        if os.path.exists(test_script):
            print(f"\nTesting script execution with {test_script}...")
            if connector.execute_script(test_script):
                print("✅ Script executed successfully")
            else:
                print("❌ Script execution failed")
        else:
            print(f"\nSkipping script execution test ('{test_script}' not found)")
        
        # Test disconnection
        print("\nTesting disconnection...")
        if connector.disconnect():
            print("✅ Disconnection successful")
        else:
            print("❌ Disconnection failed")
    else:
        print("❌ Connection failed")
    
    print("\nTest completed.")