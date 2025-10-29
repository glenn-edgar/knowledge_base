import sqlite3
import json

from .construct_kb import Construct_KB
from .construct_status import Construct_Status_Table
from .construct_job import Construct_Job_Table
from .construct_stream import Construct_Stream_Table
from .construct_rpc_client import Construct_RPC_Client_Table
from .construct_rpc_server import Construct_RPC_Server_Table


class Construct_Data_Tables:
    """
    This class is designed to construct data tables with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a SQLite database and sets up the schema.
    """
    def __init__(self, db_path, database, ltree_extension_path=None):
        """
        Initializes the Construct_Data_Tables object by creating instances of all required
        table constructor classes and connecting to the SQLite database.
        
        Args:
            db_path (str): Path to the SQLite database file
            database (str): Base knowledge base table name
            ltree_extension_path (str): Path to ltree extension (without .so/.dylib)
                                       If None, will auto-detect from common locations
        """
        # Create KB as an attribute instead of inheriting from it
        self.kb = Construct_KB(db_path, database, ltree_extension_path)
        
        # Create instances of all table constructors as attributes
        self.status_table = Construct_Status_Table(self.kb.conn, self.kb.cursor, construct_kb=self.kb, database=database)
        
        self.job_table = Construct_Job_Table(self.kb.conn, self.kb.cursor, self.kb, database=database)
        self.stream_table = Construct_Stream_Table(self.kb.conn, self.kb.cursor, self.kb, database=database)
        self.rpc_client_table = Construct_RPC_Client_Table(self.kb.conn, self.kb.cursor, self.kb, database=database)
        self.rpc_server_table = Construct_RPC_Server_Table(self.kb.conn, self.kb.cursor, self.kb, database=database)
        
        # Expose KB methods and attributes
        self.path = self.kb.path
        self.add_kb = self.kb.add_kb
        self.select_kb = self.kb.select_kb
        self.add_link_node = self.kb.add_link_node
        self.add_link_mount = self.kb.add_link_mount
        self.add_header_node = self.kb.add_header_node
        self.add_info_node = self.kb.add_info_node
        self.leave_header_node = self.kb.leave_header_node
        self.disconnect = self.kb.disconnect
        
        # Expose table-specific methods
        self.add_stream_field = self.stream_table.add_stream_field
        self.add_rpc_client_field = self.rpc_client_table.add_rpc_client_field
        self.add_rpc_server_field = self.rpc_server_table.add_rpc_server_field
        self.add_status_field = self.status_table.add_status_field
        self.add_job_field = self.job_table.add_job_field
        
    def check_installation(self):
        """
        Checks the installation status of all table components
        """
        # Call check_installation on each component instance
        self.kb.check_installation()
        self.status_table.check_installation()
        self.job_table.check_installation()
        self.stream_table.check_installation()
        self.rpc_client_table.check_installation()
        self.rpc_server_table.check_installation()


if __name__ == '__main__':
    # Example Usage with SQLite
    DB_PATH = "knowledge_base.db"
    DATABASE = "knowledge_base"
    
    # Optional: specify ltree extension path
    # LTREE_EXT = "/usr/local/lib/ltree"  # Will auto-detect if None

    print("="*70)
    print("Test 1: Complete functionality test")
    print("="*70)
    
    kb = Construct_Data_Tables(DB_PATH, DATABASE)

    print("\nInitial state:")
    print(f"Path: {kb.path}")
    
    kb.add_kb("kb1", "First knowledge base")
    kb.select_kb("kb1")
    
    kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, {"data": "header1_data"})
    print("\nAfter add_header_node:")
    print(f"Path: {kb.path}")

    kb.add_info_node("info1_link", "info1_name", {"prop2": "val2"}, {"data": "info1_data"})
    print("\nAfter add_info_node:")
    print(f"Path: {kb.path}")

    kb.add_rpc_server_field("info1_server", 25, "info1_server_data")
    kb.add_status_field("info1_status", {"prop3": "val3"}, "info1_status_description", {"prop3": "val3"})
    kb.add_status_field("info2_status", {"prop3": "val3"}, "info2_status_description", {"prop3": "val3"})
    kb.add_status_field("info3_status", {"prop3": "val3"}, "info3_status_description", {"prop3": "val3"})
    kb.add_job_field("info1_job", 100, "info1_job_description")
    kb.add_stream_field("info1_stream", 95, "info1_stream")
    kb.add_rpc_client_field("info1_client", 10, "info1_client_description")
    kb.add_link_mount("info1_link_mount", "info1_link_mount_description")
    
    kb.leave_header_node("header1_link", "header1_name")
    print("\nAfter leave_header_node:")
    print(f"Path: {kb.path}")

    kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, {"data": "header2_data"})
    kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, {"data": "info2_data"})
    kb.add_link_node("info1_link_mount")
    kb.leave_header_node("header2_link", "header2_name")
    print("\nAfter adding and leaving another header node:")
    print(f"Path: {kb.path}")

    # Check installation
    try:
        kb.check_installation()
        print("\n✓ Test 1 check_installation passed")
        kb.disconnect()
        print("✓ Test 1 completed successfully")
    except RuntimeError as e:
        print(f"✗ Error during installation check: {e}")

    print("\n" + "="*70)
    print("Test 2: Modified fields test")
    print("="*70)
    
    kb = Construct_Data_Tables(DB_PATH, DATABASE)

    print("\nInitial state:")
    print(f"Path: {kb.path}")
    
    kb.add_kb("kb1", "First knowledge base")
    kb.select_kb("kb1")
    
    kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, {"data": "header1_data"})
    print("\nAfter add_header_node:")
    print(f"Path: {kb.path}")

    kb.add_info_node("info1_link", "info1_name", {"prop2": "val2"}, {"data": "info1_data"})
    print("\nAfter add_info_node:")
    print(f"Path: {kb.path}")

    kb.add_rpc_server_field("info1_server", 25, "info1_server_data")
    kb.add_status_field("info1_status", {"prop3": "val3"}, "info1_status_description", {"prop3": "val3"})
    kb.add_status_field("info2_status", {"prop3": "val3"}, "info2_status_description", {"prop3": "val3"})
    kb.add_status_field("info3_status", {"prop3": "val3"}, "info3_status_description", {"prop3": "val3"})
    
    kb.add_job_field("info2_job", 100, "info1_job_description")
    kb.add_stream_field("info2_status", 100, "info1_stream")
    kb.add_rpc_client_field("info2_client", 10, "info1_client_description")
    
    kb.leave_header_node("header1_link", "header1_name")
    print("\nAfter leave_header_node:")
    print(f"Path: {kb.path}")

    kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, {"data": "header2_data"})
    kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, {"data": "info2_data"})
    kb.leave_header_node("header2_link", "header2_name")
    print("\nAfter adding and leaving another header node:")
    print(f"Path: {kb.path}")

    # Check installation
    try:
        kb.check_installation()
        print("\n✓ Test 2 check_installation passed")
        kb.disconnect()
        print("✓ Test 2 completed successfully")
    except RuntimeError as e:
        print(f"✗ Error during installation check: {e}")

    print("\n" + "="*70)
    print("Test 3: Reduced queue sizes test")
    print("="*70)
    
    kb = Construct_Data_Tables(DB_PATH, DATABASE)

    print("\nInitial state:")
    print(f"Path: {kb.path}")
    
    kb.add_kb("kb1", "First knowledge base")
    kb.select_kb("kb1")
    
    kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, {"data": "header1_data"})
    print("\nAfter add_header_node:")
    print(f"Path: {kb.path}")

    kb.add_info_node("info1_link", "info1_name", {"prop2": "val2"}, {"data": "info1_data"})
    print("\nAfter add_info_node:")
    print(f"Path: {kb.path}")

    kb.add_rpc_server_field("info1_server", 25, "info1_server_data")
    
    kb.add_job_field("info1_job", 50, "info1_job_description")
    kb.add_stream_field("info1_status", 50, "info1_stream")
    kb.add_rpc_client_field("info1_client", 5, "info1_client_description")
    
    kb.leave_header_node("header1_link", "header1_name")
    print("\nAfter leave_header_node:")
    print(f"Path: {kb.path}")

    kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, {"data": "header2_data"})
    kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, {"data": "info2_data"})
    kb.leave_header_node("header2_link", "header2_name")
    print("\nAfter adding and leaving another header node:")
    print(f"Path: {kb.path}")

    # Check installation
    try:
        kb.check_installation()
        print("\n✓ Test 3 check_installation passed")
        kb.disconnect()
        print("✓ Test 3 completed successfully")
    except RuntimeError as e:
        print(f"✗ Error during installation check: {e}")

    print("\n" + "="*70)
    print("All tests completed!")
    print("="*70)
    print("\nDatabase file: knowledge_base.db")
    print("You can inspect it with: sqlite3 knowledge_base.db")

