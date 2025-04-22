import psycopg2
from psycopg2.extras import RealDictCursor

class KB_Search:
    """
    A class to handle SQL filtering for the knowledge_base table.
    Uses Common Table Expressions (CTEs) for reentrant queries.
    Always selects all columns from the knowledge_base table.
    """
    
    def __init__(self):
        """
        Initialize the KB_Search object.
        """
        self.base_table = "knowledge_base"
        self.filters = []
        self.conn = None
        self.cursor = None
        self.results = None
        
    def connect(self, dbname, user, password, host="localhost", port="5432"):
        """
        Connect to PostgreSQL database with explicit parameters.
        
        Args:
            dbname: Database name
            user: Username
            password: Password
            host: Database host (default: localhost)
            port: Database port (default: 5432)
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def disconnect(self):
        """
        Disconnect from PostgreSQL database and clean up resources.
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
                
            self.cursor = None
            self.conn = None
            return True
        except Exception as e:
            print(f"Disconnection error: {e}")
            return False
    
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
        
        Returns:
            bool: True if query executed successfully, False otherwise
        """
        if not self.conn or not self.cursor:
            print("Not connected to database. Call connect() first.")
            return False
            
        try:
            # Always select all columns
            column_str = "*"
            
            # If no filters, execute a simple query
            if not self.filters:
                query = f"SELECT {column_str} FROM {self.base_table}"
                self.cursor.execute(query)
                self.results = self.cursor.fetchall()
                return True
            
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
            
            return True
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            print(f"Query execution error: {e}")
            return False
    
    def get_results(self):
        """
        Get the results of the last executed query.
        
        Returns:
            List of dictionaries representing the query results,
            or empty list if no results or query hasn't been executed
        """
        return self.results if self.results else []


# Example usage:
if __name__ == "__main__":
    # Create a new KB_Search instance
    kb_search = KB_Search()
    
    # Connect to the database
    kb_search.connect(
        dbname="knowledge_base",
        user="gedgar",
        password="ready2go",
        host="localhost",
        port="5432"
    )
    '''  
    kb_search.clear_filters()
    kb_search.search_property_key('prop1')
    kb_search.search_property_value('prop1', 'val1')
    kb_search.search_name("header1_name")  
    kb_search.search_label("header1_link")
    '''
    kb_search.search_path( "header1_link.*")  
    
    
    # Execute the query with all filters
    if kb_search.execute_query():
        # Get and process results
        results = kb_search.get_results()
        print(f"Found {len(results)} matching rows:")
        for row in results:
            print(f"ID: {row['id']}, Label: {row['label']}, Name: {row['name']}")
            print(f"Path: {row['path']}")
            print(f"Properties: {row['properties']}")
            print("---")
    else:
        print("Query execution failed")
    
    # Clean up
    kb_search.disconnect()