import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt, AsIs
from construct_kb import Construct_KB
from construct_status_table import Construct_Status_Table
from construct_job_table import Construct_Job_Table
from construct_stream_table import Construct_Stream_Table
from construct_rpc_client_table import Construct_RPC_Client_Table
from construct_rpc_server_table import Construct_RPC_Server_Table

class Construct_Data_Tables(Construct_KB,Construct_Status_Table,Construct_Job_Table,Construct_Stream_Table):
    """
    This class is designed to construct data tables with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a PostgreSQL database and sets up the schema.
    """
    def __init__(self, host, port, dbname, user, password, database):
        """
        Initializes the Construct_KB object and connects to the PostgreSQL database.
        Also sets up the database schema.

        Args:
            host (str): The database host.
            port (str): The database port.
            dbname (str): The name of the database.
            user (str): The database user.
            password (str): The password for the database user.
            database (str): (Redundant with dbname, but kept for compatibility)
        """
        
  
        Construct_KB.__init__(self, host, port, dbname, user, password, database)
        conn, cursor = Construct_KB.get_db_objects(self)
        Construct_Status_Table.__init__(self, conn, cursor,Construct_KB)
        Construct_Job_Table.__init__(self, conn, cursor,Construct_KB)
        Construct_Stream_Table.__init__(self, conn, cursor,Construct_KB)
        Construct_RPC_Client_Table.__init__(self, conn, cursor,Construct_KB)    
        Construct_RPC_Server_Table.__init__(self, conn, cursor,Construct_KB)
        
    def check_installation(self):
        Construct_KB.check_installation(self)
        Construct_Status_Table.check_installation(self)
        Construct_Job_Table.check_installation(self)
        Construct_Stream_Table.check_installation(self)
        Construct_RPC_Client_Table.check_installation(self)
        Construct_RPC_Server_Table.check_installation(self)
        
if __name__ == '__main__':
    # Example Usage
    # Replace with your actual database credentials
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "knowledge_base"
    DB_USER = "gedgar"
    DB_PASSWORD = "ready2go"
    DATABASE = "knowledge_base"

    kb = Construct_Data_Tables(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATABASE)

    print("Initial state:")
    print(f"Path: {kb.path}")

    kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, "header1_data")
    print("\nAfter add_header_node:")
    print(f"Path: {kb.path}")

    kb.add_info_node("info1_link", "info1_name", {"prop2": "val2"}, "info1_data")
    print("\nAfter add_info_node:")
    print(f"Path: {kb.path}")

    kb.leave_header_node("header1_link", "header1_name")
    print("\nAfter leave_header_node:")
    print(f"Path: {kb.path}")

    kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, "header2_data")
    kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, "info2_data")
    kb.leave_header_node("header2_link", "header2_name")
    print("\nAfter adding and leaving another header node:")
    print(f"Path: {kb.path}")

    # Example of check_installation
    try:
        kb.check_installation()
        kb._disconnect()
    except RuntimeError as e:
        print(f"Error during installation check: {e}")

