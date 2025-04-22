import psycopg2
import json
from psycopg2.extras import RealDictCursor

def select_and_print_table(host, port, dbname, user, password, table_name):
    """
    Executes a SELECT * query on the specified table and prints all rows.
    
    Args:
        host (str): The database host
        port (str): The database port
        dbname (str): The name of the database
        user (str): The database user
        password (str): The password for the database user
        table_name (str): The name of the table to query
    """
    try:
        # Connect to the database with RealDictCursor to get column names
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Execute the query
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Check if any rows were returned
        if not rows:
            print(f"No data found in table '{table_name}'")
            return
        
        # Print the number of rows
        print(f"Found {len(rows)} rows in table '{table_name}':")
        print("-" * 80)
        
        # Print each row
        for i, row in enumerate(rows, 1):
            print(f"Row {i}:")
            for column, value in row.items():
                # Format JSON data for better readability
                if isinstance(value, dict) or (isinstance(value, str) and (value.startswith('{') or value.startswith('['))):
                    try:
                        # Try to parse as JSON if it's a string that looks like JSON
                        if isinstance(value, str):
                            parsed_value = json.loads(value)
                            formatted_value = json.dumps(parsed_value, indent=4)
                        else:
                            formatted_value = json.dumps(value, indent=4)
                        print(f"  {column}: {formatted_value}")
                    except (json.JSONDecodeError, TypeError):
                        print(f"  {column}: {value}")
                else:
                    print(f"  {column}: {value}")
            print("-" * 80)
        
    except psycopg2.Error as e:
        print(f"Error executing query: {e}")
    
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Example usage:
if __name__ == "__main__":
    # Replace with your actual database credentials
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "knowledge_base"
    DB_USER = "gedgar"
    DB_PASSWORD = "ready2go"
    TABLE_NAME = "knowledge_base"
    
    select_and_print_table(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, TABLE_NAME)