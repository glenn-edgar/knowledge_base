import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt, AsIs
from construct_kb import Construct_KB
from construct_status_table import Construct_Status_Table

class Construct_Data_Tables(Construct_KB,Construct_Status_Table):
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
        
    def check_installation(self):
        Construct_KB.check_installation(self)
        Construct_Status_Table.check_installation(self)
        
   