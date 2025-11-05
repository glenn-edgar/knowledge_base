#!/usr/bin/env python3
"""
LTree Document Database
A PostgreSQL table manager using ltree for hierarchical paths and jsonb for document storage.
"""

import os
import json
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from typing import Optional, List, Dict, Any


class LTreeDocumentDB_builder:
    """
    Manages a PostgreSQL table with ltree hierarchical paths and jsonb document storage.
    
    Table schema:
        id: SERIAL PRIMARY KEY
        ltree: LTREE (hierarchical path, acts as document key)
        type: TEXT (document type/category)
        data: JSONB (document data)
        locked_by: TEXT (identifier of lock holder)
        locked_at: TIMESTAMP (when lock was acquired)
        lock_expires: TIMESTAMP (when lock expires)
        created_at: TIMESTAMP
        updated_at: TIMESTAMP
    """
    
    def __init__(self, conn, table_name: str = "knowledge_base_documents", upload_flag: bool = False):
        """
        Initialize with an existing PostgreSQL connection.
        
        Args:
            conn: psycopg2 connection object
            table_name: Name of the table to manage
            upload_flag: If True, skip table creation
        """
        self.conn = conn
        self.table_name = table_name
        
        # Ensure ltree extension is enabled
        self._enable_ltree_extension()
        if upload_flag == False:
            self._create_table()
    
    def _enable_ltree_extension(self):
        """Enable the ltree extension if not already enabled."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS ltree;")
                self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to enable ltree extension: {e}")
    
    def _create_table(self):
        """Create the ltree documents table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            ltree LTREE NOT NULL UNIQUE,
            type TEXT,
            data JSONB DEFAULT '{{}}'::jsonb,
            locked_by TEXT,
            locked_at TIMESTAMP,
            lock_expires TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS {self.table_name}_ltree_idx 
            ON {self.table_name} USING GIST (ltree);
        
        CREATE INDEX IF NOT EXISTS {self.table_name}_data_idx 
            ON {self.table_name} USING GIN (data);
            
        CREATE INDEX IF NOT EXISTS {self.table_name}_type_idx
            ON {self.table_name} (type);
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_table_sql)
                self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to create table: {e}")
    
    def add_record(self, 
                   ltree_path: str, 
                   doc_type: Optional[str] = None,
                   data: Optional[Dict[str, Any]] = None) -> int:
        """
        Add a new record to the table.
        
        Args:
            ltree_path: The ltree path (e.g., 'root.child.grandchild')
            doc_type: Optional type/category for the document
            data: Optional jsonb data dictionary
        
        Returns:
            The id of the newly created record
        """
        insert_sql = f"""
        INSERT INTO {self.table_name} (ltree, type, data)
        VALUES (%s::ltree, %s, %s)
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(insert_sql, (ltree_path, doc_type, Json(data or {})))
                record_id = cur.fetchone()[0]
                self.conn.commit()
                return record_id
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to add record: {e}")
    
    def delete_record(self, record_id: int) -> bool:
        """
        Delete a record by its id.
        
        Args:
            record_id: The id of the record to delete
        
        Returns:
            True if a record was deleted, False if no record found
        """
        delete_sql = f"DELETE FROM {self.table_name} WHERE id = %s;"
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(delete_sql, (record_id,))
                deleted = cur.rowcount > 0
                self.conn.commit()
                return deleted
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to delete record: {e}")
    
    def list_ltree_ids(self) -> List[Dict[str, Any]]:
        """
        List all ltree paths and their corresponding ids.
        
        Returns:
            List of dictionaries with 'id' and 'ltree' keys
        """
        select_sql = f"""
        SELECT id, ltree::text as ltree, type 
        FROM {self.table_name} 
        ORDER BY ltree;
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(select_sql)
                results = cur.fetchall()
                return [dict(row) for row in results]
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to list ltree ids: {e}")
    
    def get_record(self, record_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a record by its id.
        
        Args:
            record_id: The id of the record to retrieve
        
        Returns:
            Dictionary with record data or None if not found
        """
        select_sql = f"""
        SELECT id, ltree::text as ltree, type, data, 
               locked_by, locked_at, lock_expires,
               created_at, updated_at
        FROM {self.table_name}
        WHERE id = %s;
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(select_sql, (record_id,))
                result = cur.fetchone()
                return dict(result) if result else None
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to get record: {e}")
    
    def query_by_ltree(self, ltree_pattern: str) -> List[Dict[str, Any]]:
        """
        Query records using ltree pattern matching.
        
        Args:
            ltree_pattern: Pattern to match (e.g., 'root.*' for all descendants of root)
        
        Returns:
            List of matching records
        """
        select_sql = f"""
        SELECT id, ltree::text as ltree, type, data
        FROM {self.table_name}
        WHERE ltree ~ %s::lquery
        ORDER BY ltree;
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(select_sql, (ltree_pattern,))
                results = cur.fetchall()
                return [dict(row) for row in results]
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to query by ltree: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - does not close connection as it's externally managed."""
        pass
    
    def sync_ltree_paths(self, 
                     target_paths: List[str],
                     default_type: Optional[str] = None,
                     default_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Synchronize table records with a target list of ltree paths.
        Adds missing paths and deletes paths not in the target list.
        
        Args:
            target_paths: List of ltree paths that should exist in the table
            default_type: Optional default type for newly added records
            default_data: Optional default data for newly added records
        
        Returns:
            Dictionary with 'added' and 'deleted' lists containing affected record info
        """
        # Get current ltree paths from the table
        current_records = self.list_ltree_ids()
        current_paths = {record['ltree'] for record in current_records}
        target_paths_set = set(target_paths)
        
        # Determine what needs to be added and deleted
        paths_to_add = target_paths_set - current_paths
        paths_to_delete = current_paths - target_paths_set
        
        added_records = []
        deleted_records = []
        
        try:
            # Add missing paths
            for path in sorted(paths_to_add):
                record_id = self.add_record(
                    ltree_path=path,
                    doc_type=default_type,
                    data=default_data
                )
                added_records.append({'id': record_id, 'ltree': path})
            
            # Delete extra paths
            # Create a map of ltree path to id for efficient lookup
            path_to_id = {record['ltree']: record['id'] for record in current_records}
            
            for path in sorted(paths_to_delete):
                record_id = path_to_id[path]
                if self.delete_record(record_id):
                    deleted_records.append({'id': record_id, 'ltree': path})
            
            return {
                'added': added_records,
                'deleted': deleted_records,
                'summary': {
                    'added_count': len(added_records),
                    'deleted_count': len(deleted_records),
                    'total_records': len(current_records) - len(deleted_records) + len(added_records)
                }
            }
            
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to sync ltree paths: {e}")


if __name__ == "__main__":
    """Test the LTreeDocumentDB class."""
    
    print("=" * 60)
    print("Testing LTreeDocumentDB_builder")
    print("=" * 60)
    
    # Setup PostgreSQL connection
    conn = None
    try:
        # Get password from environment
        password = os.environ.get('POSTGRES_PASSWORD', '')
        
        # Create connection
        conn = psycopg2.connect(
            dbname="knowledge_base",
            user="gedgar",
            password=password,
            host="localhost",
            port=5432
        )
        conn.autocommit = False
        print("✓ Connected to PostgreSQL")
        
        # Create database instance with the connection
        db = LTreeDocumentDB_builder(conn=conn)
        print(f"✓ Table 'knowledge_base_documents' ready")
        print()
        
        # Test 1: Add records
        print("Test 1: Adding records")
        print("-" * 60)
        
        id1 = db.add_record(
            ltree_path="root.system.config",
            doc_type="configuration",
            data={
                "name": "System Configuration",
                "version": "1.0",
                "settings": {"timeout": 30, "retries": 3}
            }
        )
        print(f"✓ Added record with id={id1}, path='root.system.config'")
        
        id2 = db.add_record(
            ltree_path="root.system.logs",
            doc_type="log_config",
            data={
                "name": "Log Settings",
                "level": "INFO",
                "destinations": ["file", "console"]
            }
        )
        print(f"✓ Added record with id={id2}, path='root.system.logs'")
        
        id3 = db.add_record(
            ltree_path="root.sensors.temperature",
            doc_type="sensor_data",
            data={
                "sensor_id": "temp001",
                "location": "room_a",
                "value": 22.5,
                "unit": "celsius"
            }
        )
        print(f"✓ Added record with id={id3}, path='root.sensors.temperature'")
        
        id4 = db.add_record(
            ltree_path="root.sensors.humidity",
            doc_type="sensor_data",
            data={
                "sensor_id": "hum001",
                "location": "room_a",
                "value": 45.2,
                "unit": "percent"
            }
        )
        print(f"✓ Added record with id={id4}, path='root.sensors.humidity'")
        print()
        
        # Test 2: List ltree ids
        print("Test 2: Listing all ltree paths and ids")
        print("-" * 60)
        records = db.list_ltree_ids()
        for record in records:
            print(f"  id={record['id']:2d}  ltree='{record['ltree']:30s}'  type='{record['type']}'")
        print()
        
        # Test 3: Get specific record
        print("Test 3: Retrieving specific record")
        print("-" * 60)
        record = db.get_record(id1)
        if record:
            print(f"Record id={record['id']}:")
            print(f"  Path: {record['ltree']}")
            print(f"  Type: {record['type']}")
            print(f"  Data: {json.dumps(record['data'], indent=2)}")
            print(f"  Locked by: {record['locked_by']}")
        print()
        
        # Test 4: Query by ltree pattern
        print("Test 4: Query by ltree pattern (root.sensors.*)")
        print("-" * 60)
        sensor_records = db.query_by_ltree("root.sensors.*")
        print(f"Found {len(sensor_records)} sensor records:")
        for record in sensor_records:
            print(f"  id={record['id']}  {record['ltree']}  -> {record['data'].get('sensor_id')}")
        print()
        
        # Test 5: Delete a record
        print("Test 5: Deleting record")
        print("-" * 60)
        deleted = db.delete_record(id2)
        if deleted:
            print(f"✓ Deleted record with id={id2}")
        else:
            print(f"✗ No record found with id={id2}")
        print()
        
        # Test 6: List after deletion
        print("Test 6: Listing after deletion")
        print("-" * 60)
        records = db.list_ltree_ids()
        print(f"Total records: {len(records)}")
        for record in records:
            print(f"  id={record['id']:2d}  ltree='{record['ltree']:30s}'  type='{record['type']}'")
        print()
        
        print("Cleanup: Deleting remaining test records...")
        for record in records:
            db.delete_record(record['id'])
        print("✓ Cleanup complete")
        print()
        
        print("=" * 60)
        print("All tests completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("✓ Database connection closed")