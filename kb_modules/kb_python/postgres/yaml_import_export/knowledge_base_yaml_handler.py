import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, Json
import json
import yaml
from typing import Optional, Dict, Any, List
from datetime import datetime, date
from decimal import Decimal


class KnowledgeBaseYAMLHandler:
    """
    Standalone class for exporting and importing knowledge base data to/from YAML.
    Does not create or modify table structures.
    
    Import Modes:
    -------------
    1. Skip conflicts (default): import_from_yaml(file)
       - Inserts new records only
       - Skips records that would conflict with existing data
       
    2. Update existing: import_from_yaml(file, update_existing=True)
       - Inserts new records
       - Updates existing records when conflicts occur (upsert)
       - Preserves id and created_at fields
       
    3. Clear and import: import_from_yaml(file, clear_existing=True)
       - Deletes all existing data first (or filtered by kb_name)
       - Then inserts all records from file
       - Use with caution!
       
    Note: clear_existing=True takes precedence over update_existing
    """
    
    def __init__(self, table_name: str, connection_params: Dict[str, Any]):
        """
        Initialize the YAML handler with database connection parameters.
        
        Args:
            table_name: Base name of the knowledge base tables
            connection_params: Dictionary containing database connection parameters
                             (host, database, user, password, port)
        """
        self.table_name = table_name
        self.connection_params = connection_params
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            raise
            
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        
    # ========== EXPORT METHODS ==========
    
    def export_table_data(self, table_name: str, 
                         where_clause: Optional[str] = None,
                         order_by: Optional[str] = None,
                         exclude_columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Export records from a table.
        
        Args:
            table_name: Name of the table to export
            where_clause: Optional WHERE clause (without WHERE keyword)
            order_by: Optional ORDER BY clause (without ORDER BY keyword)
            exclude_columns: List of column names to exclude from export
            
        Returns:
            List of dictionaries, one per row
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as dict_cursor:
            query = f"SELECT * FROM {table_name}"
            
            if where_clause:
                query += f" WHERE {where_clause}"
            
            if order_by:
                query += f" ORDER BY {order_by}"
            
            dict_cursor.execute(query)
            records = dict_cursor.fetchall()
            
            # Convert to regular dicts and serialize special types
            serialized_records = [self._serialize_record(dict(record)) for record in records]
            
            # Exclude specified columns
            if exclude_columns:
                serialized_records = [
                    {k: v for k, v in record.items() if k not in exclude_columns}
                    for record in serialized_records
                ]
            
            return serialized_records

    def export_all_kb_data(self, order_by_path: bool = True) -> Dict[str, List[Dict[str, Any]]]:
        """
        Export all knowledge base related tables.
        Excludes has_link and has_link_mount from main table export.
        
        Args:
            order_by_path: If True, order main table and links by path/created_at
            
        Returns:
            Dictionary mapping table names to lists of records
        """
        tables = {
            self.table_name: self.export_table_data(
                self.table_name, 
                order_by='path' if order_by_path else None,
                exclude_columns=['has_link', 'has_link_mount']
            ),
            f"{self.table_name}_info": self.export_table_data(
                f"{self.table_name}_info"
            ),
            f"{self.table_name}_link": self.export_table_data(
                f"{self.table_name}_link",
                order_by='created_at' if order_by_path else None
            ),
            f"{self.table_name}_link_mount": self.export_table_data(
                f"{self.table_name}_link_mount",
                order_by='created_at' if order_by_path else None
            )
        }
        
        return tables

    def export_kb_by_name(self, kb_name: str) -> Dict[str, Any]:
        """
        Export all data for a specific knowledge base.
        Excludes has_link and has_link_mount from node export.
        
        Args:
            kb_name: Name of the knowledge base to export
            
        Returns:
            Dictionary with all related records for this KB
        """
        return {
            'knowledge_base': kb_name,
            'nodes': self.export_table_data(
                self.table_name,
                where_clause=f"knowledge_base = '{kb_name}'",
                order_by='path',
                exclude_columns=['has_link', 'has_link_mount']
            ),
            'info': self.export_table_data(
                f"{self.table_name}_info",
                where_clause=f"knowledge_base = '{kb_name}'"
            ),
            'links': self.export_table_data(
                f"{self.table_name}_link",
                where_clause=f"parent_node_kb = '{kb_name}'",
                order_by='created_at'
            ),
            'link_mounts': self.export_table_data(
                f"{self.table_name}_link_mount",
                where_clause=f"knowledge_base = '{kb_name}'",
                order_by='created_at'
            )
        }

    def export_to_yaml(self, filename: str, 
                      kb_name: Optional[str] = None,
                      include_metadata: bool = True) -> None:
        """
        Export knowledge base data to YAML file.
        
        Args:
            filename: Output YAML filename
            kb_name: If provided, export only this KB; otherwise export all
            include_metadata: If True, include record counts
        """
        if kb_name:
            data = self.export_kb_by_name(kb_name)
        else:
            data = self.export_all_kb_data()
        
        self.save_to_yaml(data, filename, include_metadata)
        print(f"Exported data to {filename}")

    def save_to_yaml(self, data: Any, filename: str, 
                     include_metadata: bool = True) -> None:
        """
        Save data to YAML file.
        
        Args:
            data: Data to save (list or dict)
            filename: Output YAML filename
            include_metadata: If True, include record counts and export timestamp
        """
        output = {}
        
        if include_metadata:
            output['metadata'] = {
                'exported_at': datetime.now().isoformat(),
                'table_name': self.table_name
            }
        
        if isinstance(data, dict):
            # Add record counts for multi-table exports
            if include_metadata and any(isinstance(v, list) for v in data.values()):
                output['record_counts'] = {
                    k: len(v) if isinstance(v, list) else 1 
                    for k, v in data.items()
                }
            output['data'] = data
        else:
            output['data'] = data
        
        with open(filename, 'w') as f:
            yaml.dump(output, f, default_flow_style=False, 
                     sort_keys=False, allow_unicode=True)

    def _serialize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert PostgreSQL types to YAML-serializable types.
        
        Args:
            record: Dictionary containing a database row
            
        Returns:
            Dictionary with serialized values
        """
        serialized = {}
        for key, value in record.items():
            if value is None:
                serialized[key] = None
            elif isinstance(value, (datetime, date)):
                serialized[key] = value.isoformat()
            elif isinstance(value, Decimal):
                serialized[key] = float(value)
            elif isinstance(value, bytes):
                serialized[key] = value.hex()
            else:
                # Handles str, int, float, bool, dict, list
                serialized[key] = value
        
        return serialized

    # ========== IMPORT METHODS ==========
    
    def load_from_yaml(self, filename: str) -> Dict[str, Any]:
        """
        Load data from YAML file.
        
        Args:
            filename: Input YAML filename
            
        Returns:
            Dictionary containing the loaded data
        """
        with open(filename, 'r') as f:
            content = yaml.safe_load(f)
        
        # Handle both wrapped (with metadata) and unwrapped formats
        if isinstance(content, dict) and 'data' in content:
            return content['data']
        return content

    def import_from_yaml(self, filename: str, 
                        clear_existing: bool = False,
                        update_existing: bool = False,
                        kb_name_filter: Optional[str] = None) -> Dict[str, int]:
        """
        Import knowledge base data from YAML file.
        Recalculates has_link and has_link_mount flags after import.
        
        Args:
            filename: Input YAML filename
            clear_existing: If True, delete existing data before import
            update_existing: If True, update existing records on conflict (upsert)
                           If False, skip records that conflict with existing data
            kb_name_filter: If provided, only import data for this KB
            
        Returns:
            Dictionary with counts of imported records per table
            
        Note: clear_existing takes precedence over update_existing
        """
        data = self.load_from_yaml(filename)
        counts = {}
        
        try:
            # Handle single KB export format
            if 'knowledge_base' in data:
                kb_filter = data['knowledge_base']
                if kb_name_filter and kb_filter != kb_name_filter:
                    print(f"Skipping KB {kb_filter} (filter: {kb_name_filter})")
                    return counts
                    
                tables_data = {
                    f"{self.table_name}_info": data.get('info', []),
                    self.table_name: data.get('nodes', []),
                    f"{self.table_name}_link": data.get('links', []),
                    f"{self.table_name}_link_mount": data.get('link_mounts', [])
                }
            else:
                # Handle full export format
                tables_data = data
            
            # Clear existing data if requested
            if clear_existing:
                self._clear_tables(kb_name_filter)
            
            # Import in proper order (info first, then nodes, then links)
            import_order = [
                f"{self.table_name}_info",
                self.table_name,
                f"{self.table_name}_link_mount",
                f"{self.table_name}_link"
            ]
            
            for table in import_order:
                if table in tables_data:
                    records = tables_data[table]
                    if not isinstance(records, list):
                        records = [records]
                    
                    count = self._import_table_data(table, records, kb_name_filter, update_existing)
                    counts[table] = count
            
            # Recalculate has_link and has_link_mount flags
            self._recalculate_link_flags()
            
            self.conn.commit()
            print(f"Successfully imported data from {filename}")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error importing data: {e}")
            raise
        
        return counts

    def clear_tables(self, kb_name_filter: Optional[str] = None) -> Dict[str, int]:
        """
        Clear existing data from tables.
        Deletes in proper order to maintain referential integrity.
        
        Args:
            kb_name_filter: If provided, only clear data for this KB
            
        Returns:
            Dictionary with counts of deleted records per table
            
        Example:
            # Clear all data
            counts = handler.clear_tables()
            
            # Clear only specific KB
            counts = handler.clear_tables(kb_name_filter='kb1')
        """
        try:
            counts = {}
            
            if kb_name_filter:
                where = f"WHERE knowledge_base = '{kb_name_filter}'"
                where_parent = f"WHERE parent_node_kb = '{kb_name_filter}'"
            else:
                where = ""
                where_parent = ""
            
            # Delete in reverse order (links first, then nodes, then info)
            self.cursor.execute(f"DELETE FROM {self.table_name}_link {where_parent}")
            counts[f"{self.table_name}_link"] = self.cursor.rowcount
            
            self.cursor.execute(f"DELETE FROM {self.table_name}_link_mount {where}")
            counts[f"{self.table_name}_link_mount"] = self.cursor.rowcount
            
            self.cursor.execute(f"DELETE FROM {self.table_name} {where}")
            counts[self.table_name] = self.cursor.rowcount
            
            self.cursor.execute(f"DELETE FROM {self.table_name}_info {where}")
            counts[f"{self.table_name}_info"] = self.cursor.rowcount
            
            self.conn.commit()
            
            return counts
            
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error clearing tables: {e}")
            raise

    def _clear_tables(self, kb_name_filter: Optional[str] = None):
        """Internal method to clear tables (called by import_from_yaml)."""
        self.clear_tables(kb_name_filter)

    def _import_table_data(self, table_name: str, records: List[Dict[str, Any]], 
                          kb_name_filter: Optional[str] = None,
                          update_existing: bool = False) -> int:
        """
        Import records into a table.
        
        Args:
            table_name: Name of the table
            records: List of record dictionaries
            kb_name_filter: If provided, only import records for this KB
            update_existing: If True, update existing records on conflict
            
        Returns:
            Number of records imported/updated
        """
        if not records:
            return 0
        
        # Define conflict columns for each table
        conflict_targets = {
            self.table_name: '(path)',
            f"{self.table_name}_info": '(knowledge_base)',
            f"{self.table_name}_link": '(link_name, parent_node_kb, parent_path)',
            f"{self.table_name}_link_mount": '(link_name)'
        }
        
        count = 0
        for record in records:
            # Filter by KB name if specified
            if kb_name_filter:
                kb_field = None
                if 'knowledge_base' in record:
                    kb_field = 'knowledge_base'
                elif 'parent_node_kb' in record:
                    kb_field = 'parent_node_kb'
                
                if kb_field and record[kb_field] != kb_name_filter:
                    continue
            
            # Remove fields that should be auto-generated
            record = {k: v for k, v in record.items() 
                     if k not in ['id', 'has_link', 'has_link_mount']}
            
            # Handle JSON fields - wrap dicts with Json() adapter
            if table_name == self.table_name:
                for json_field in ['properties', 'data']:
                    if json_field in record:
                        # If it's a dict or list, wrap it with Json()
                        if isinstance(record[json_field], (dict, list)):
                            record[json_field] = Json(record[json_field])
                        # If it's a string, parse it first then wrap
                        elif isinstance(record[json_field], str):
                            try:
                                parsed = json.loads(record[json_field])
                                record[json_field] = Json(parsed)
                            except json.JSONDecodeError:
                                # If it fails to parse, leave it as is
                                pass
            
            # Build INSERT query
            columns = list(record.keys())
            values = [record[col] for col in columns]
            
            placeholders = ', '.join(['%s'] * len(columns))
            columns_sql = ', '.join(columns)
            
            # Get the conflict target for this table
            conflict_target = conflict_targets.get(table_name, 'id')
            
            if update_existing:
                # Build UPDATE clause for all columns except the conflict target
                update_cols = [col for col in columns if col not in ['id', 'created_at']]
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                
                query = f"""
                    INSERT INTO {table_name} ({columns_sql})
                    VALUES ({placeholders})
                    ON CONFLICT {conflict_target} DO UPDATE SET
                    {update_set}
                """
            else:
                query = f"""
                    INSERT INTO {table_name} ({columns_sql})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING
                """
            
            self.cursor.execute(query, values)
            count += self.cursor.rowcount
        
        return count

    def _recalculate_link_flags(self):
        """
        Recalculate has_link and has_link_mount flags based on link tables.
        """
        # Reset all flags to False
        self.cursor.execute(f"""
            UPDATE {self.table_name} 
            SET has_link = FALSE, has_link_mount = FALSE
        """)
        
        # Set has_link = TRUE for nodes that have entries in link table
        self.cursor.execute(f"""
            UPDATE {self.table_name} AS kb
            SET has_link = TRUE
            FROM {self.table_name}_link AS link
            WHERE kb.path = link.parent_path
            AND kb.knowledge_base = link.parent_node_kb
        """)
        
        # Set has_link_mount = TRUE for nodes that have entries in link_mount table
        self.cursor.execute(f"""
            UPDATE {self.table_name} AS kb
            SET has_link_mount = TRUE
            FROM {self.table_name}_link_mount AS mount
            WHERE kb.path = mount.mount_path
            AND kb.knowledge_base = mount.knowledge_base
        """)


# Example usage
if __name__ == "__main__":
    import os
    password = os.getenv("POSTGRES_PASSWORD")
    if password is None:
        raise ValueError("POSTGRES_PASSWORD environment variable is not set")
    
    conn_params = {
        'host': 'localhost',
        'database': 'knowledge_base',
        'user': 'gedgar',
        'password': password,
        'port': 5432
    }
    
    # Using context manager for automatic connection handling
    print("\n=== EXPORT Example ===")
    with KnowledgeBaseYAMLHandler('knowledge_base', conn_params) as handler:
        # Export all data
        handler.export_to_yaml('kb_export_all.yaml')
        
        # Export specific KB
        handler.export_to_yaml('kb_export_kb1.yaml', kb_name='kb1')
        
        # Export without metadata
        handler.export_to_yaml('kb_export_simple.yaml', include_metadata=False)
    
    
    print("\n=== IMPORT Example ===")
    with KnowledgeBaseYAMLHandler('knowledge_base', conn_params) as handler:
        # Import from file (skip conflicts - default behavior)
        counts = handler.import_from_yaml('kb_export_kb1.yaml')
        print(f"Import counts (skip conflicts): {counts}")
        
        # Import and update existing records (upsert)
        counts = handler.import_from_yaml('kb_export_kb1.yaml', 
                                          clear_existing=False,
                                          update_existing=True)
        print(f"Import counts (upsert): {counts}")
        
        # Import and replace all existing data for specific KB
        # counts = handler.import_from_yaml('kb_export_kb1.yaml', 
        #                                   clear_existing=True,
        #                                   kb_name_filter='kb1')
        # print(f"Import counts (clear and import): {counts}")