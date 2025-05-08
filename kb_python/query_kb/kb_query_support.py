import psycopg2
from psycopg2.extras import RealDictCursor

class KB_Search:
    """
    A class to handle SQL filtering for the knowledge_base table.
    Uses Common Table Expressions (CTEs) for reentrant queries.
    Always selects all columns from the knowledge_base table.
    """
    

        
    def __init__(self, host, port, dbname, user, password):
        """
        Initializes the Construct_KB object and connects to the PostgreSQL database.
        Also sets up the database schema.

        Args:
            host (str): The database host.
            port (str): The database port.
            dbname (str): The name of the database.
            user (str): The database user.
            password (str): The password for the database user.
           
        """
        self.path = []  # Stack to keep track of the path (levels/nodes)
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.base_table = "knowledge_base.knowledge_base"
        self.filters = []

        self.results = None

        self._connect()  # Establish the database connection and schema during initialization
        
 
    def _connect(self):
        """
        Establishes a connection to the PostgreSQL database and sets up the schema.
        This is a helper method called by __init__.
        """
        self.path_values = {}
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
    
        self.cursor = self.conn.cursor()
        print(f"Connected to PostgreSQL database {self.dbname} on {self.host}:{self.port}")
        
    def disconnect(self):
        """
        Closes the connection to the PostgreSQL database. This is a helper
        method called by check_installation.
        """
       
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print(f"Disconnected from PostgreSQL database {self.dbname} on {self.host}:{self.port}")
        self.cursor = None
        self.conn = None    
        
    def get_conn_and_cursor(self):
        """
        Get the database connection and cursor.
        
        Returns:
            tuple: (connection, cursor)
        """
        if not self.conn or not self.cursor:
            raise ValueError("Not connected to database. Call connect() first.")
        return True, self.conn, self.cursor
        

    
   
    
    def clear_filters(self):
        """
        Clear all filters and reset the query state.
        """
        self.filters = []
        self.results = None
    
    def search_label(self, label):
        """
        Add a filter to search for rows matching the specified label.
        
        Args:
            label: The label value to match
        """
        self.filters.append({
            "condition": "label = %(label)s",
            "params": {"label": label}
        })
    
    def search_name(self, name):
        """
        Add a filter to search for rows matching the specified name.
        
        Args:
            name: The name value to match
        """
        self.filters.append({
            "condition": "name = %(name)s",
            "params": {"name": name}
        })
    
    def search_property_key(self, key):
        """
        Add a filter to search for rows where the properties JSON field contains the specified key.
        
        Args:
            key: The JSON key to check for existence
        """
        # Use the ? operator to check if the key exists in the properties JSON
        self.filters.append({
            "condition": "properties::jsonb ? %(property_key)s",
            "params": {"property_key": key}
        })
    
    def search_property_value(self, key, value):
        """
        Add a filter to search for rows where the properties JSON field contains 
        the specified key with the specified value.
        
        Args:
            key: The JSON key to search
            value: The value to match for the key
        """
        # Convert the key and value to a JSON object string
        json_object = {key: value}
        
        # Use the @> containment operator to check if properties contains this key-value pair
        self.filters.append({
            "condition": "properties::jsonb @> %(json_object)s::jsonb",
            "params": {"json_object": psycopg2.extras.Json(json_object)}
        })
    
    def search_path(self, path_expression):
        """
        Add a filter to search for rows matching the specified LTREE path expression.
        
        Args:
            path_expression: The LTREE path expression to match
                Examples:
                - 'docs.technical' for exact match
                - 'docs.*' for all immediate children of docs
                - 'docs.*{1,3}' for children up to 3 levels deep
                - 'docs.technical.*' for all children of docs.technical
                - '*.technical.*' for any path containing 'technical'
        """
        self.filters.append({
            "condition": "path ~ %(path_expr)s",
            "params": {"path_expr": path_expression}
        })
    
    def execute_query(self):
        """
        Execute the progressive query with all added filters using CTEs.
        """
        
        if not self.conn or not self.cursor:
            raise ValueError("Not connected to database. Call connect() first.")
            
        # Always select all columns
        column_str = "*"
        
        # If no filters, execute a simple query
        if not self.filters:
            print("No filters, executing simple query",self.base_table,column_str)
            query = f"SELECT {column_str} FROM {self.base_table}"
            self.cursor.execute(query)
            self.results = self.cursor.fetchall()
    
            return self.results
        
        # Start building the CTE query
        cte_parts = []
        combined_params = {}
        
        # Initial CTE starts with the base table
        cte_parts.append(f"base_data AS (SELECT {column_str} FROM {self.base_table})")
        
        # Process each filter in sequence, building a chain of CTEs
        for i, filter_info in enumerate(self.filters):
            condition = filter_info.get('condition', '')
            params = filter_info.get('params', {})
            
            # Update the combined parameters dictionary with prefixed parameter names
            prefixed_params = {}
            prefixed_condition = condition
            
            for param_name, param_value in params.items():
                prefixed_name = f"p{i}_{param_name}"
                prefixed_params[prefixed_name] = param_value
                # Replace parameter placeholder in the condition
                prefixed_condition = prefixed_condition.replace(
                    f"%({param_name})s", 
                    f"%({prefixed_name})s"
                )
            
            combined_params.update(prefixed_params)
            
            # Define the CTE name for this step
            cte_name = f"filter_{i}"
            prev_cte = f"base_data" if i == 0 else f"filter_{i-1}"
            
            # Build this CTE part
            if condition:
                cte_query = f"{cte_name} AS (SELECT {column_str} FROM {prev_cte} WHERE {prefixed_condition})"
            else:
                cte_query = f"{cte_name} AS (SELECT {column_str} FROM {prev_cte})"
            
            cte_parts.append(cte_query)
        
        # Build the final query with all CTEs
        with_clause = "WITH " + ",\n".join(cte_parts)
        final_select = f"SELECT {column_str} FROM filter_{len(self.filters)-1}"
        
        # Combine into the complete query
        final_query = f"{with_clause}\n{final_select}"
        
        # Execute the query with the combined parameters
        self.cursor.execute(final_query, combined_params)
        self.results = self.cursor.fetchall()
        
        return self.results
    
    def get_results(self):
        """
        Get the results of the last executed query.
        
        Returns:
            List of dictionaries representing the query results,
            or empty list if no results or query hasn't been executed
        """
        return self.results if self.results else []
    
    def find_description(self, key_data):
        if isinstance(key_data, tuple):
            key_data = [key_data]
        return_values = []
       
        for key in key_data:
    
            properties = key[3]
            if "description" in properties:
               description = properties["description"]
            else:
               description = ""
            return_values.append({key[5]:description})
        return return_values
   
    def find_description_paths(self, path_array):
        """
        Find data for specified paths in the knowledge base.
        
        Args:
            path_array (str or list): A single path or list of paths to search for
            
        Returns:
            dict: A dictionary mapping paths to their corresponding data values
            
        Raises:
            Exception: If a database error occurs
        """
        # Normalize input to always be a list
        if not isinstance(path_array, list):
            path_array = [path_array]
        
        # Handle empty input case
        if not path_array:
            return {}
        
        return_values = {}
        
        try:
            for path in path_array:
                # Query specifically selects the data column
                query_string = '''
                SELECT data
                FROM knowledge_base.knowledge_base
                WHERE path = %s;
                '''
                
                self.cursor.execute(query_string, (path,))
                result = self.cursor.fetchone()
                
                if result:
                    # Extract the data value
                    return_values[path] = result[0]
                else:
                    # Handle case where path doesn't exist
                    return_values[path] = None
                    
            return return_values
            
        except Exception as e:
            # Handle errors gracefully
            raise Exception(f"Error retrieving data for paths: {str(e)}")
# Example usage:
if __name__ == "__main__":
    # Create a new KB_Search instance
    kb_search = KB_Search(
        dbname="knowledge_base",
        user="gedgar",
        password="ready2go",
        host="localhost",
        port="5432"
    )
    
    
   
    kb_search.clear_filters()
    kb_search.search_property_key('prop3')
    #kb_search.search_property_value('prop3', 'val3')
    #kb_search.search_name("info1_status")  
    #kb_search.search_label("KB_STATUS_FIELD")

    #kb_search.search_path( "header1_link.header1_name.KB_STATUS_FIELD.info1_status")  
    
    
    # Execute the query with all filters
    if kb_search.execute_query():
        # Get and process results
        results = kb_search.get_results()
   
        print(f"Found {len(results)} matching rows:")
       
            
        for row in results:
                print(f"ID: {row[0]}, Label: {row[1]}, Name: {row[2]}")
                print(f"Properties: {row[3]}")
                print(f"Data: {row[4]}")
                print(f"Path: {row[5]}")
                print("---")
       
    
    # Clean up
    kb_search.disconnect()