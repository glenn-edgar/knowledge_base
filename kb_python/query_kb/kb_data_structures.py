import psycopg2
from psycopg2.extras import RealDictCursor
from kb_query_support import KB_Search
from kb_status_data import KB_Status_Data

class KB_Data_Structures:
    """
    A class to handle the data structures for the knowledge base.
    """
    def __init__(self, host, port, dbname, user, password):
        self.query_support = KB_Search(host, port, dbname, user, password)
        self.status_data = KB_Status_Data(self.query_support)
 
 # Example usage:
if __name__ == "__main__":
    # Create a new KB_Search instance
    kb_data_structures = KB_Data_Structures(
        dbname="knowledge_base",
        user="gedgar",
        password="ready2go",
        host="localhost",
        port="5432"
    )   
    print(kb_data_structures.query_support.disconnect)
    kb_data_structures.query_support.disconnect()
    # Connect to the database
