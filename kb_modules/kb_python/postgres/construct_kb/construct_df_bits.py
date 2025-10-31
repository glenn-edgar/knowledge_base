#!/usr/bin/env python3
"""
ChainTree Bitfield Management with Table Tracking
Each table gets its own field_name and dedicated SQL functions
Includes registry tracking for all bitfield-enabled tables
"""

import psycopg2
from psycopg2 import sql
from typing import Dict, List, Tuple, Any, Optional
import re
from datetime import datetime


class BitfieldTableTracker:
    """
    Tracks all tables that use bitfield functionality.
    Provides queries and management for the bitfield registry.
    Safe to use on clean databases - creates necessary tables automatically.
    """
    
    def __init__(self, conn):
        """
        Initialize the bitfield table tracker.
        
        Args:
            conn: psycopg2 connection
        """
        self.conn = conn
        self._ensure_registry_schema()
        self._ensure_definitions_schema()
    
    def _ensure_registry_schema(self):
        """Create bitfield registry table if it doesn't exist"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bitfield_registry (
                table_name TEXT PRIMARY KEY,
                field_name TEXT NOT NULL UNIQUE,
                flags_column TEXT NOT NULL DEFAULT 'status_flags',
                description TEXT,
                flag_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_bitfield_registry_field 
            ON bitfield_registry(field_name)
        """)
        
        self.conn.commit()
        cursor.close()
    
    def _ensure_definitions_schema(self):
        """Create bitfield definitions table if it doesn't exist"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bitfield_definitions (
                field_name TEXT NOT NULL,
                flag_name TEXT NOT NULL,
                bit_position INTEGER NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (field_name, flag_name),
                CHECK (bit_position >= 0 AND bit_position < 64)
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_bitfield_definitions_field 
            ON bitfield_definitions(field_name)
        """)
        
        self.conn.commit()
        cursor.close()
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    
    def list_all_tables(self) -> List[Tuple[str, str, str, int, datetime]]:
        """
        List all tables registered for bitfield operations.
        
        Returns:
            List of tuples: (table_name, field_name, flags_column, flag_count, updated_at)
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT table_name, field_name, flags_column, flag_count, updated_at
            FROM bitfield_registry
            ORDER BY table_name
        """)
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific tracked table.
        
        Args:
            table_name: Name of the table to query
            
        Returns:
            Dictionary with table info or None if not found
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT table_name, field_name, flags_column, description, 
                   flag_count, created_at, updated_at
            FROM bitfield_registry
            WHERE table_name = %s
        """, (table_name,))
        
        row = cursor.fetchone()
        cursor.close()
        
        if row:
            return {
                'table_name': row[0],
                'field_name': row[1],
                'flags_column': row[2],
                'description': row[3],
                'flag_count': row[4],
                'created_at': row[5],
                'updated_at': row[6]
            }
        return None
    
    def get_table_flags(self, table_name: str) -> List[Tuple[str, int, str]]:
        """
        Get all flag definitions for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of tuples: (flag_name, bit_position, description)
        """
        if not self._table_exists('bitfield_definitions'):
            return []
        
        cursor = self.conn.cursor()
        
        # First get the field_name for this table
        cursor.execute("""
            SELECT field_name FROM bitfield_registry WHERE table_name = %s
        """, (table_name,))
        
        result = cursor.fetchone()
        if not result:
            cursor.close()
            return []
        
        field_name = result[0]
        
        # Now get the flags
        cursor.execute("""
            SELECT flag_name, bit_position, description
            FROM bitfield_definitions
            WHERE field_name = %s
            ORDER BY bit_position
        """, (field_name,))
        
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def unregister_table(self, table_name: str, 
                        drop_table: bool = False,
                        drop_definitions: bool = False,
                        drop_functions: bool = False,
                        confirm: bool = False) -> Dict[str, Any]:
        """
        Remove a table from the bitfield registry with optional cleanup.
        
        Args:
            table_name: Name of the table to unregister
            drop_table: If True, drop the actual database table
            drop_definitions: If True, delete flag definitions from bitfield_definitions
            drop_functions: If True, drop the SQL helper functions
            confirm: Must be True if any drop operation is requested (safety check)
            
        Returns:
            Dictionary with results of the operation
        """
        result = {
            'unregistered': False,
            'table_dropped': False,
            'definitions_deleted': False,
            'functions_dropped': [],
            'errors': []
        }
        
        # Safety check
        if (drop_table or drop_definitions or drop_functions) and not confirm:
            raise ValueError(
                "Must set confirm=True to perform destructive operations. "
                "This prevents accidental data loss."
            )
        
        cursor = self.conn.cursor()
        
        # Get table info before unregistering
        cursor.execute("""
            SELECT field_name, flags_column FROM bitfield_registry 
            WHERE table_name = %s
        """, (table_name,))
        
        row = cursor.fetchone()
        if not row:
            result['errors'].append(f"Table '{table_name}' not found in registry")
            cursor.close()
            return result
        
        field_name, flags_column = row
        
        # 1. Drop SQL functions if requested
        if drop_functions:
            functions_to_drop = [
                f'has_{field_name}',
                f'has_any_{field_name}',
                f'set_{field_name}',
                f'clear_{field_name}',
                f'toggle_{field_name}',
            ]
            
            for func_name in functions_to_drop:
                try:
                    cursor.execute(sql.SQL("""
                        DROP FUNCTION IF EXISTS {func}(BIGINT, BIGINT) CASCADE
                    """).format(func=sql.Identifier(func_name)))
                    result['functions_dropped'].append(func_name)
                except Exception as e:
                    result['errors'].append(f"Error dropping function {func_name}: {e}")
            
            # format_ function has different signature
            try:
                cursor.execute(sql.SQL("""
                    DROP FUNCTION IF EXISTS {func}(BIGINT) CASCADE
                """).format(func=sql.Identifier(f'format_{field_name}')))
                result['functions_dropped'].append(f'format_{field_name}')
            except Exception as e:
                result['errors'].append(f"Error dropping format function: {e}")
        
        # 2. Delete flag definitions if requested
        if drop_definitions and self._table_exists('bitfield_definitions'):
            try:
                cursor.execute("""
                    DELETE FROM bitfield_definitions
                    WHERE field_name = %s
                """, (field_name,))
                deleted_count = cursor.rowcount
                result['definitions_deleted'] = deleted_count
            except Exception as e:
                result['errors'].append(f"Error deleting definitions: {e}")
        
        # 3. Drop the actual table if requested
        if drop_table:
            try:
                cursor.execute(sql.SQL("""
                    DROP TABLE IF EXISTS {table} CASCADE
                """).format(table=sql.Identifier(table_name)))
                result['table_dropped'] = True
            except Exception as e:
                result['errors'].append(f"Error dropping table: {e}")
        
        # 4. Unregister from tracking
        try:
            cursor.execute("""
                DELETE FROM bitfield_registry WHERE table_name = %s
            """, (table_name,))
            result['unregistered'] = cursor.rowcount > 0
        except Exception as e:
            result['errors'].append(f"Error unregistering: {e}")
        
        self.conn.commit()
        cursor.close()
        
        return result
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get overall statistics about bitfield usage.
        
        Returns:
            Dictionary with statistics
        """
        cursor = self.conn.cursor()
        
        # Total tables
        cursor.execute("SELECT COUNT(*) FROM bitfield_registry")
        total_tables = cursor.fetchone()[0]
        
        # Total flags across all tables
        cursor.execute("SELECT SUM(flag_count) FROM bitfield_registry")
        total_flags = cursor.fetchone()[0] or 0
        
        # Average flags per table
        avg_flags = total_flags / total_tables if total_tables > 0 else 0
        
        # Most recent update
        cursor.execute("SELECT MAX(updated_at) FROM bitfield_registry")
        last_update = cursor.fetchone()[0]
        
        cursor.close()
        
        return {
            'total_tables': total_tables,
            'total_flags': total_flags,
            'average_flags_per_table': round(avg_flags, 2),
            'last_update': last_update
        }
    
    def find_tables_with_flag(self, flag_name: str) -> List[str]:
        """
        Find all tables that have a specific flag name defined.
        
        Args:
            flag_name: Name of the flag to search for
            
        Returns:
            List of table names that have this flag
        """
        if not self._table_exists('bitfield_definitions'):
            return []
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT r.table_name
            FROM bitfield_registry r
            JOIN bitfield_definitions d ON r.field_name = d.field_name
            WHERE d.flag_name = %s
            ORDER BY r.table_name
        """, (flag_name.upper(),))
        
        results = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return results
    
    def validate_consistency(self) -> List[Dict[str, Any]]:
        """
        Check for inconsistencies between registry and definitions.
        
        Returns:
            List of issues found (empty if all consistent)
        """
        if not self._table_exists('bitfield_definitions'):
            return []
        
        issues = []
        cursor = self.conn.cursor()
        
        # Check for registry entries without definitions
        cursor.execute("""
            SELECT r.table_name, r.field_name
            FROM bitfield_registry r
            LEFT JOIN bitfield_definitions d ON r.field_name = d.field_name
            WHERE d.field_name IS NULL
        """)
        
        for table_name, field_name in cursor.fetchall():
            issues.append({
                'type': 'missing_definitions',
                'table_name': table_name,
                'field_name': field_name,
                'message': f"Table '{table_name}' registered but has no flag definitions"
            })
        
        # Check for incorrect flag counts
        cursor.execute("""
            SELECT r.table_name, r.field_name, r.flag_count, COUNT(d.flag_name) as actual_count
            FROM bitfield_registry r
            LEFT JOIN bitfield_definitions d ON r.field_name = d.field_name
            GROUP BY r.table_name, r.field_name, r.flag_count
            HAVING r.flag_count != COUNT(d.flag_name)
        """)
        
        for table_name, field_name, recorded, actual in cursor.fetchall():
            issues.append({
                'type': 'flag_count_mismatch',
                'table_name': table_name,
                'field_name': field_name,
                'recorded_count': recorded,
                'actual_count': actual,
                'message': f"Table '{table_name}' reports {recorded} flags but has {actual}"
            })
        
        cursor.close()
        return issues
    
    def repair_flag_counts(self) -> int:
        """
        Synchronize flag_count in registry with actual definition counts.
        
        Returns:
            Number of tables updated
        """
        if not self._table_exists('bitfield_definitions'):
            return 0
        
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE bitfield_registry r
            SET flag_count = subq.actual_count,
                updated_at = CURRENT_TIMESTAMP
            FROM (
                SELECT field_name, COUNT(*) as actual_count
                FROM bitfield_definitions
                GROUP BY field_name
            ) subq
            WHERE r.field_name = subq.field_name
              AND r.flag_count != subq.actual_count
        """)
        
        updated = cursor.rowcount
        self.conn.commit()
        cursor.close()
        
        return updated
    
    def cleanup_orphaned_definitions(self) -> int:
        """
        Remove flag definitions that have no corresponding registry entry.
        
        Returns:
            Number of orphaned definitions deleted
        """
        if not self._table_exists('bitfield_definitions'):
            return 0
        
        cursor = self.conn.cursor()
        cursor.execute("""
            DELETE FROM bitfield_definitions
            WHERE field_name NOT IN (
                SELECT field_name FROM bitfield_registry
            )
        """)
        
        deleted = cursor.rowcount
        self.conn.commit()
        cursor.close()
        
        return deleted
    
    def list_orphaned_functions(self) -> List[str]:
        """
        Find SQL functions that may be orphaned (no registry entry).
        
        Returns:
            List of potentially orphaned function names
        """
        cursor = self.conn.cursor()
        
        # Get all field names from registry
        cursor.execute("SELECT field_name FROM bitfield_registry")
        valid_fields = {row[0] for row in cursor.fetchall()}
        
        # Get all functions matching our naming pattern
        cursor.execute("""
            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_schema = 'public'
              AND routine_type = 'FUNCTION'
              AND (routine_name LIKE 'has_%' 
                OR routine_name LIKE 'has_any_%'
                OR routine_name LIKE 'set_%'
                OR routine_name LIKE 'clear_%'
                OR routine_name LIKE 'toggle_%'
                OR routine_name LIKE 'format_%')
        """)
        
        all_functions = {row[0] for row in cursor.fetchall()}
        cursor.close()
        
        # Check which functions don't match valid fields
        orphaned = []
        for func in all_functions:
            is_valid = False
            for field in valid_fields:
                if (func == f'has_{field}' or
                    func == f'has_any_{field}' or
                    func == f'set_{field}' or
                    func == f'clear_{field}' or
                    func == f'toggle_{field}' or
                    func == f'format_{field}'):
                    is_valid = True
                    break
            
            if not is_valid:
                orphaned.append(func)
        
        return sorted(orphaned)
    
    def drop_orphaned_functions(self, confirm: bool = False) -> List[str]:
        """
        Drop SQL functions that have no corresponding registry entry.
        
        Args:
            confirm: Must be True to actually drop functions (safety check)
            
        Returns:
            List of dropped function names
        """
        if not confirm:
            raise ValueError(
                "Must set confirm=True to drop functions. "
                "Use list_orphaned_functions() to preview first."
            )
        
        orphaned = self.list_orphaned_functions()
        dropped = []
        
        cursor = self.conn.cursor()
        for func_name in orphaned:
            try:
                # Try both signatures (BIGINT, BIGINT) and (BIGINT)
                cursor.execute(sql.SQL("""
                    DROP FUNCTION IF EXISTS {func}(BIGINT, BIGINT) CASCADE
                """).format(func=sql.Identifier(func_name)))
                
                cursor.execute(sql.SQL("""
                    DROP FUNCTION IF EXISTS {func}(BIGINT) CASCADE
                """).format(func=sql.Identifier(func_name)))
                
                dropped.append(func_name)
            except Exception as e:
                print(f"Warning: Could not drop {func_name}: {e}")
        
        self.conn.commit()
        cursor.close()
        
        return dropped
    
    def full_cleanup(self, table_name: str, confirm: bool = False) -> Dict[str, Any]:
        """
        Convenience method: completely remove a table and all associated metadata.
        Equivalent to unregister_table with all drop flags set to True.
        
        Args:
            table_name: Name of the table to completely remove
            confirm: Must be True (safety check)
            
        Returns:
            Dictionary with results of all operations
        """
        return self.unregister_table(
            table_name=table_name,
            drop_table=True,
            drop_definitions=True,
            drop_functions=True,
            confirm=confirm
        )
    
    def get_cleanup_preview(self) -> Dict[str, Any]:
        """
        Preview what would be cleaned up without actually doing it.
        Safe to call - performs no destructive operations.
        
        Returns:
            Dictionary showing what would be affected
        """
        tracked = self.list_all_tables()
        
        preview = {
            'tracked_tables': [],
            'total_tables': len(tracked),
            'total_flags': 0,
            'orphaned_definitions': 0,
            'orphaned_functions': []
        }
        
        for table_name, field_name, flags_col, flag_count, updated in tracked:
            preview['tracked_tables'].append({
                'table_name': table_name,
                'field_name': field_name,
                'flags_column': flags_col,
                'flag_count': flag_count
            })
            preview['total_flags'] += flag_count
        
        # Count orphaned items (only if table exists)
        if self._table_exists('bitfield_definitions'):
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT COUNT(*)
                FROM bitfield_definitions
                WHERE field_name NOT IN (
                    SELECT field_name FROM bitfield_registry
                )
            """)
            preview['orphaned_definitions'] = cursor.fetchone()[0]
            cursor.close()
        
        preview['orphaned_functions'] = self.list_orphaned_functions()
        
        return preview
    
    def cleanup_all_tables(self, drop_tables: bool = False, confirm: bool = False) -> Dict[str, Any]:
        """
        Cleanup ALL tracked bitfield tables at once.
        Perfect for resetting when creating a new knowledge base.
        
        Args:
            drop_tables: If True, drop the actual database tables
            confirm: Must be True for destructive operations
            
        Returns:
            Dictionary with summary of all cleanup operations
        """
        if not confirm:
            raise ValueError(
                "Must set confirm=True to perform cleanup operations. "
                "This is a destructive operation that affects ALL tracked tables."
            )
        
        # Get all tracked tables
        tracked = self.list_all_tables()
        
        summary = {
            'total_tables': len(tracked),
            'tables_cleaned': [],
            'tables_failed': [],
            'total_definitions_deleted': 0,
            'total_functions_dropped': 0,
            'errors': []
        }
        
        if len(tracked) == 0:
            print("No tracked tables found. Already clean!")
            return summary
        
        print(f"\nCleaning up {len(tracked)} tracked tables...")
        
        for table_name, field_name, flags_col, flag_count, updated in tracked:
            print(f"  Cleaning {table_name}...", end=' ')
            
            try:
                result = self.unregister_table(
                    table_name=table_name,
                    drop_table=drop_tables,
                    drop_definitions=True,
                    drop_functions=True,
                    confirm=True
                )
                
                if result['unregistered']:
                    summary['tables_cleaned'].append(table_name)
                    summary['total_definitions_deleted'] += result.get('definitions_deleted', 0)
                    summary['total_functions_dropped'] += len(result.get('functions_dropped', []))
                    print("✓")
                else:
                    summary['tables_failed'].append(table_name)
                    print("✗")
                
                if result['errors']:
                    summary['errors'].extend(result['errors'])
                    
            except Exception as e:
                summary['tables_failed'].append(table_name)
                summary['errors'].append(f"Error cleaning {table_name}: {e}")
                print(f"✗ ({e})")
        
        # Cleanup any orphaned items
        print("\n  Cleaning orphaned definitions...", end=' ')
        orphaned_defs = self.cleanup_orphaned_definitions()
        summary['orphaned_definitions_cleaned'] = orphaned_defs
        print(f"✓ ({orphaned_defs})")
        
        print("  Cleaning orphaned functions...", end=' ')
        try:
            orphaned_funcs = self.drop_orphaned_functions(confirm=True)
            summary['orphaned_functions_cleaned'] = len(orphaned_funcs)
            print(f"✓ ({len(orphaned_funcs)})")
        except Exception as e:
            summary['orphaned_functions_cleaned'] = 0
            print(f"✗ ({e})")
        
        return summary
    
    def reset_all_bitfield_metadata(self, confirm: bool = False) -> Dict[str, Any]:
        """
        Complete reset: Remove ALL bitfield metadata but keep the actual tables.
        This unregisters all tables, deletes all definitions, and drops all functions,
        but leaves the actual database tables intact.
        
        Perfect for when you want to redefine bitfields from scratch while keeping data.
        
        Args:
            confirm: Must be True (safety check)
            
        Returns:
            Summary dictionary
        """
        return self.cleanup_all_tables(drop_tables=False, confirm=confirm)
    
    def drop_all_bitfield_tables(self, confirm: bool = False) -> Dict[str, Any]:
        """
        Nuclear option: Drop ALL tracked tables and their bitfield metadata.
        
        Args:
            confirm: Must be True (safety check)
            
        Returns:
            Summary dictionary
        """
        return self.cleanup_all_tables(drop_tables=True, confirm=confirm)



class BitfieldDefinitionManager:
    """
    Setup class for defining bitfield names per table.
    Each table gets its own field_name and dedicated SQL functions.
    Automatically registers tables in the tracking system.
    """
    
    def __init__(self, conn, table_name: str, description: Optional[str] = None):
        """
        Initialize bitfield definition manager for a specific table.
        
        Args:
            conn: psycopg2 connection
            table_name: ltree table name (e.g., 'motor_nodes', 'sensor_nodes')
            description: Optional description of this table's purpose
        """
        self.conn = conn
        self.table_name = table_name
        self.field_name = self._sanitize_name(table_name)
        self.description = description
        
        self._ensure_schema()
        self._register_in_tracker()
        
        print(f"Created BitfieldDefinitionManager for table '{table_name}'")
        print(f"  Field name: {self.field_name}")
        print(f"  Will create functions: has_{self.field_name}(), set_{self.field_name}(), etc.")
    
    def _sanitize_name(self, name: str) -> str:
        """Convert table name to valid SQL identifier for field_name"""
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if sanitized and sanitized[0].isdigit():
            sanitized = '_' + sanitized
        return sanitized.lower()
    
    def _ensure_schema(self):
        """Create bitfield_definitions table if it doesn't exist"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bitfield_definitions (
                field_name TEXT NOT NULL,
                flag_name TEXT NOT NULL,
                bit_position INTEGER NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (field_name, flag_name),
                CHECK (bit_position >= 0 AND bit_position < 64)
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_bitfield_definitions_field 
            ON bitfield_definitions(field_name)
        """)
        
        self.conn.commit()
        cursor.close()
    
    def _register_in_tracker(self, flags_column: str = 'status_flags'):
        """Register this table in the bitfield tracker"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT INTO bitfield_registry (table_name, field_name, flags_column, description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_name) 
            DO UPDATE SET 
                field_name = EXCLUDED.field_name,
                flags_column = EXCLUDED.flags_column,
                description = EXCLUDED.description,
                updated_at = CURRENT_TIMESTAMP
        """, (self.table_name, self.field_name, flags_column, self.description))
        
        self.conn.commit()
        cursor.close()
    
    def _update_flag_count(self):
        """Update the flag count in the registry"""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE bitfield_registry
            SET flag_count = (
                SELECT COUNT(*) 
                FROM bitfield_definitions 
                WHERE field_name = %s
            ),
            updated_at = CURRENT_TIMESTAMP
            WHERE table_name = %s
        """, (self.field_name, self.table_name))
        
        self.conn.commit()
        cursor.close()
    
    
    def create_table(self, additional_columns: Optional[Dict[str, str]] = None,
                    primary_key: str = 'path LTREE',
                    flags_column: str = 'status_flags') -> bool:
        """
        Create the actual database table with bitfield column.
        
        Args:
            additional_columns: Dictionary of {column_name: column_definition}
                               e.g., {'name': 'TEXT', 'value': 'INTEGER'}
            primary_key: Primary key definition (default: 'path LTREE')
            flags_column: Name of the bitfield column (default: 'status_flags')
            
        Returns:
            True if table was created, False if already exists
        """
        cursor = self.conn.cursor()
        
        # Check if table already exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (self.table_name,))
        
        if cursor.fetchone()[0]:
            cursor.close()
            print(f"Table '{self.table_name}' already exists")
            return False
        
        # Build column definitions
        columns = [primary_key + ' PRIMARY KEY']
        columns.append(f'{flags_column} BIGINT DEFAULT 0')
        
        if additional_columns:
            for col_name, col_def in additional_columns.items():
                columns.append(f'{col_name} {col_def}')
        
        columns.append('created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        
        # Create table
        create_sql = f"""
            CREATE TABLE {sql.Identifier(self.table_name).as_string(cursor)} (
                {', '.join(columns)}
            )
        """
        
        cursor.execute(create_sql)
        self.conn.commit()
        cursor.close()
        
        print(f"✓ Created table '{self.table_name}' with {flags_column} column")
        return True
    
    def ensure_table_exists(self, additional_columns: Optional[Dict[str, str]] = None,
                           primary_key: str = 'path LTREE',
                           flags_column: str = 'status_flags') -> bool:
        """
        Ensure the table exists, create it if it doesn't.
        
        Same args as create_table().
        
        Returns:
            True if table was created, False if already existed
        """
        return self.create_table(additional_columns, primary_key, flags_column)
    
    def table_exists(self) -> bool:
        """
        Check if the actual database table exists.
        
        Returns:
            True if table exists, False otherwise
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (self.table_name,))
        
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    def define_flags(self, flags: Dict[str, Dict[str, Any]], 
                    mode: str = 'merge') -> None:
        """
        Define bitfield flags for this table and generate SQL functions.
        
        Args:
            flags: Dictionary with structure:
                {
                    'FLAG_NAME': {
                        'bit': 0,
                        'description': 'Flag description'
                    },
                    ...
                }
            mode: How to handle existing flags:
                - 'merge' (default): Add new flags, update existing, keep others
                - 'replace': Remove all existing flags and use only these
                - 'strict': Fail if any conflicts with existing flags
        """
        cursor = self.conn.cursor()
        
        # Validate bit positions within this call
        bit_positions = [info['bit'] for info in flags.values()]
        if len(bit_positions) != len(set(bit_positions)):
            raise ValueError("Duplicate bit positions detected in new flags")
        
        if any(b < 0 or b >= 64 for b in bit_positions):
            raise ValueError("Bit positions must be 0-63")
        
        # Get existing flags for validation
        cursor.execute("""
            SELECT flag_name, bit_position
            FROM bitfield_definitions
            WHERE field_name = %s
        """, (self.field_name,))
        
        existing_flags = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Handle different modes
        if mode == 'replace':
            # Delete all existing flags first
            cursor.execute("""
                DELETE FROM bitfield_definitions
                WHERE field_name = %s
            """, (self.field_name,))
            print(f"  Removed {len(existing_flags)} existing flags (replace mode)")
            existing_flags = {}
        
        elif mode == 'strict':
            # Check for conflicts
            conflicts = []
            
            # Check for bit position conflicts
            for new_flag, new_info in flags.items():
                new_bit = new_info['bit']
                for exist_flag, exist_bit in existing_flags.items():
                    if exist_flag not in flags and exist_bit == new_bit:
                        conflicts.append(
                            f"Bit {new_bit}: new flag '{new_flag}' conflicts with existing '{exist_flag}'"
                        )
            
            if conflicts:
                raise ValueError(
                    f"Flag conflicts detected in strict mode:\n  " + 
                    "\n  ".join(conflicts)
                )
        
        elif mode == 'merge':
            # Validate: new flags shouldn't use bit positions of OTHER existing flags
            for new_flag, new_info in flags.items():
                new_bit = new_info['bit']
                for exist_flag, exist_bit in existing_flags.items():
                    # If it's not updating the same flag, check for bit conflicts
                    if exist_flag != new_flag.upper() and exist_bit == new_bit:
                        raise ValueError(
                            f"Bit position conflict: new flag '{new_flag}' (bit {new_bit}) "
                            f"conflicts with existing flag '{exist_flag}' (bit {exist_bit}). "
                            f"Use mode='replace' to replace all flags, or choose a different bit position."
                        )
        
        else:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'merge', 'replace', or 'strict'")
        
        # Insert/update definitions
        added = 0
        updated = 0
        
        for flag_name, info in flags.items():
            flag_upper = flag_name.upper()
            
            if flag_upper in existing_flags:
                updated += 1
            else:
                added += 1
            
            cursor.execute("""
                INSERT INTO bitfield_definitions 
                (field_name, flag_name, bit_position, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (field_name, flag_name) 
                DO UPDATE SET 
                    bit_position = EXCLUDED.bit_position,
                    description = EXCLUDED.description
            """, (
                self.field_name,
                flag_upper,
                info['bit'],
                info.get('description', '')
            ))
        
        self.conn.commit()
        
        # Update flag count in tracker
        self._update_flag_count()
        
        # Generate SQL helper functions specific to this table
        self._generate_sql_functions()
        
        cursor.close()
        
        # Detailed feedback
        if mode == 'replace':
            print(f"✓ Replaced flags for table '{self.table_name}': {len(flags)} flags defined")
        else:
            print(f"✓ Updated flags for table '{self.table_name}': {added} added, {updated} updated")
            
    
    
    def add_flags(self, flags: Dict[str, Dict[str, Any]]) -> None:
        """
        Convenience method: Add new flags (strict mode - fails on conflicts)
        
        Args:
            flags: Dictionary of flags to add
        """
        self.define_flags(flags, mode='strict')
    
    def update_flags(self, flags: Dict[str, Dict[str, Any]]) -> None:
        """
        Convenience method: Update existing flags or add new ones (merge mode)
        
        Args:
            flags: Dictionary of flags to add or update
        """
        self.define_flags(flags, mode='merge')
    
    def replace_flags(self, flags: Dict[str, Dict[str, Any]]) -> None:
        """
        Convenience method: Replace all flags (removes old ones first)
        
        Args:
            flags: Dictionary of flags to define (removes all existing)
        """
        self.define_flags(flags, mode='replace')
    
    def remove_flag(self, flag_name: str) -> bool:
        """
        Remove a single flag definition.
        
        Args:
            flag_name: Name of the flag to remove
            
        Returns:
            True if flag was removed, False if not found
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            DELETE FROM bitfield_definitions
            WHERE field_name = %s AND flag_name = %s
        """, (self.field_name, flag_name.upper()))
        
        removed = cursor.rowcount > 0
        self.conn.commit()
        cursor.close()
        
        if removed:
            self._update_flag_count()
            self._generate_sql_functions()
            print(f"✓ Removed flag '{flag_name}' from table '{self.table_name}'")
        
        return removed
    
    
    def _generate_sql_functions(self):
        """Generate SQL helper functions specific to this field_name/table"""
        cursor = self.conn.cursor()
        
        print(f"  Generating SQL functions for '{self.field_name}'...")
        
        # has_{field}: test if ALL flags are set
        cursor.execute(f"""
            CREATE OR REPLACE FUNCTION has_{self.field_name}(flags BIGINT, flag_mask BIGINT) 
            RETURNS BOOLEAN AS $f$
                SELECT (flags & flag_mask) = flag_mask;
            $f$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
        """)
        
        # has_any_{field}: test if ANY flag is set
        cursor.execute(f"""
            CREATE OR REPLACE FUNCTION has_any_{self.field_name}(flags BIGINT, flag_mask BIGINT) 
            RETURNS BOOLEAN AS $f$
                SELECT (flags & flag_mask) != 0;
            $f$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
        """)
        
        # set_{field}: set flags
        cursor.execute(f"""
            CREATE OR REPLACE FUNCTION set_{self.field_name}(flags BIGINT, flag_mask BIGINT) 
            RETURNS BIGINT AS $f$
                SELECT flags | flag_mask;
            $f$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
        """)
        
        # clear_{field}: clear flags
        cursor.execute(f"""
            CREATE OR REPLACE FUNCTION clear_{self.field_name}(flags BIGINT, flag_mask BIGINT) 
            RETURNS BIGINT AS $f$
                SELECT flags & ~flag_mask;
            $f$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
        """)
        
        # toggle_{field}: toggle flags
        cursor.execute(f"""
            CREATE OR REPLACE FUNCTION toggle_{self.field_name}(flags BIGINT, flag_mask BIGINT) 
            RETURNS BIGINT AS $f$
                SELECT flags # flag_mask;
            $f$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
        """)
        
        # format_{field}: human readable (table-specific)
        # Note: We DO use sql.Literal for the field_name string value in the WHERE clause
        cursor.execute(f"""
            CREATE OR REPLACE FUNCTION format_{self.field_name}(flags BIGINT) 
            RETURNS TEXT AS $f$
                SELECT string_agg(flag_name, ', ' ORDER BY bit_position)
                FROM bitfield_definitions
                WHERE field_name = '{self.field_name}'
                AND (flags & (1::BIGINT << bit_position)) != 0;
            $f$ LANGUAGE SQL STABLE;
        """)
        
        self.conn.commit()
        cursor.close()
        
        print(f"  ✓ Created functions: has_{self.field_name}(), set_{self.field_name}(), clear_{self.field_name}(), toggle_{self.field_name}(), format_{self.field_name}()")    
        def list_flags(self) -> List[Tuple[str, int, str]]:
            """Get all defined flags for this specific table"""
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT flag_name, bit_position, description
                FROM bitfield_definitions
                WHERE field_name = %s
                ORDER BY bit_position
            """, (self.field_name,))
            
            results = cursor.fetchall()
            cursor.close()
            return results
        
    def delete_flags(self):
        """Delete all flag definitions for this table (cleanup)"""
        cursor = self.conn.cursor()
        cursor.execute("""
            DELETE FROM bitfield_definitions
            WHERE field_name = %s
        """, (self.field_name,))
        self.conn.commit()
        cursor.close()
        
        # Update flag count
        self._update_flag_count()
        
        print(f"✓ Deleted all flags for field '{self.field_name}'")
    
    def cleanup_table(self, drop_table: bool = False, confirm: bool = False) -> Dict[str, Any]:
        """
        Convenience method to cleanup this table's bitfield configuration.
        
        Args:
            drop_table: If True, drop the actual database table
            confirm: Must be True for destructive operations
            
        Returns:
            Dictionary with cleanup results
        """
        tracker = BitfieldTableTracker(self.conn)
        return tracker.unregister_table(
            table_name=self.table_name,
            drop_table=drop_table,
            drop_definitions=True,
            drop_functions=True,
            confirm=confirm
        )
    
    @staticmethod
    def list_all_fields(conn) -> List[Tuple[str, int]]:
        """List all field_names and their flag counts (legacy method)"""
        cursor = conn.cursor()
        cursor.execute("""
            SELECT field_name, COUNT(*) as flag_count
            FROM bitfield_definitions
            GROUP BY field_name
            ORDER BY field_name
        """)
        results = cursor.fetchall()
        cursor.close()
        return results


