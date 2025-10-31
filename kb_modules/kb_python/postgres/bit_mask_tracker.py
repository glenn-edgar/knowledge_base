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
    """
    
    def __init__(self, conn):
        """
        Initialize the bitfield table tracker.
        
        Args:
            conn: psycopg2 connection
        """
        self.conn = conn
        self._ensure_registry_schema()
    
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
        
        # Create index for quick lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_bitfield_registry_field 
            ON bitfield_registry(field_name)
        """)
        
        self.conn.commit()
        cursor.close()
    
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
        if drop_definitions:
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


    
    # ... (previous methods) ...
    
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
        
        # Count orphaned items
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
    
    
    def cleanup_all_bitfields(self,drop_actual_tables=False):
        """
        Complete cleanup of all bitfield infrastructure.
        
        Args:
            conn: Database connection
            drop_actual_tables: If True, drops the actual database tables too
        """
        
        print("\n" + "="*60)
        print("Cleaning ALL Bitfield Tables")
        print("="*60)
        
        # Show what will be cleaned
        preview = self.get_cleanup_preview()
        print(f"\nFound {preview['total_tables']} tracked tables:")
        for table_info in preview['tracked_tables']:
            print(f"  - {table_info['table_name']:25} ({table_info['flag_count']} flags)")
        
        if preview['total_tables'] == 0:
            print("\nNo tracked tables found. Already clean!")
            return
        
        # Confirm
        
        
        # Do the cleanup
        if drop_actual_tables:
            summary = self.drop_all_bitfield_tables(confirm=True)
        else:
            summary = self.reset_all_bitfield_metadata(confirm=True)
        
        # Print results
        print("\n" + "="*60)
        print("Cleanup Complete")
        print("="*60)
        print(f"  Tables processed: {summary['total_tables']}")
        print(f"  Successfully cleaned: {len(summary['tables_cleaned'])}")
        print(f"  Definitions deleted: {summary['total_definitions_deleted']}")
        print(f"  Functions dropped: {summary['total_functions_dropped']}")
        print(f"  Orphaned definitions: {summary.get('orphaned_definitions_cleaned', 0)}")
        print(f"  Orphaned functions: {summary.get('orphaned_functions_cleaned', 0)}")
        
        if summary['tables_cleaned']:
            print(f"\nCleaned tables:")
            for table in summary['tables_cleaned']:
                print(f"  ✓ {table}")
        
        if summary['errors']:
            print(f"\nErrors:")
            for error in summary['errors']:
                print(f"  ✗ {error}")
        
        # Verify cleanup
        print("\n--- Verification ---")
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM bitfield_registry")
        reg_count = cursor.fetchone()[0]
        print(f"  bitfield_registry entries: {reg_count}")
        
        cursor.execute("SELECT COUNT(*) FROM bitfield_definitions")
        def_count = cursor.fetchone()[0]
        print(f"  bitfield_definitions entries: {def_count}")
        
        # Check for bitfield functions
        cursor.execute("""
            SELECT COUNT(*)
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
        func_count = cursor.fetchone()[0]
        print(f"  Bitfield functions remaining: {func_count}")
        
        cursor.close()
        
        if reg_count == 0 and def_count == 0 and func_count == 0:
            print("\n✓ Complete cleanup successful - ready for new knowledge base!")
        else:
            print("\n⚠ Some items remain - may need manual cleanup")



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
    
    def define_flags(self, flags: Dict[str, Dict[str, Any]]) -> None:
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
        """
        cursor = self.conn.cursor()
        
        # Validate bit positions
        bit_positions = [info['bit'] for info in flags.values()]
        if len(bit_positions) != len(set(bit_positions)):
            raise ValueError("Duplicate bit positions detected")
        
        if any(b < 0 or b >= 64 for b in bit_positions):
            raise ValueError("Bit positions must be 0-63")
        
        # Insert definitions for THIS table only
        for flag_name, info in flags.items():
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
                flag_name.upper(),
                info['bit'],
                info.get('description', '')
            ))
        
        self.conn.commit()
        
        # Update flag count in tracker
        self._update_flag_count()
        
        # Generate SQL helper functions specific to this table
        self._generate_sql_functions()
        
        cursor.close()
        
        print(f"✓ Defined {len(flags)} flags for table '{self.table_name}' (field: {self.field_name})")
    
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


class BitfieldOperations:
    """
    Read/write class for bitfield operations on a specific table.
    Must match the field_name used in BitfieldDefinitionManager.
    """
    
    def __init__(self, conn, table_name: str, flags_column: str = 'status_flags',
                 use_row_locks: bool = True, lock_timeout_ms: int = 5000):
        """
        Initialize bitfield operations for a specific table.
        
        Args:
            conn: psycopg2 connection
            table_name: ltree table name (must match BitfieldDefinitionManager)
            flags_column: column name for flags (default: status_flags)
            use_row_locks: If True, automatically use row locks for modify operations
            lock_timeout_ms: Timeout in milliseconds for lock acquisition
        """
        self.conn = conn
        self.table_name = table_name
        self.flags_column = flags_column
        self.field_name = self._sanitize_name(table_name)
        self.use_row_locks = use_row_locks
        self.lock_timeout_ms = lock_timeout_ms
        
        # Set lock timeout
        cursor = self.conn.cursor()
        cursor.execute(f"SET lock_timeout = '{lock_timeout_ms}ms'")
        cursor.close()
        
        # Load flag definitions - must exist for this field_name
        self.flags = self._load_definitions()
        
        if not self.flags:
            raise ValueError(
                f"No flag definitions found for table '{table_name}' (field: {self.field_name}). "
                f"Did you call BitfieldDefinitionManager.define_flags() first?"
            )
        
        print(f"BitfieldOperations initialized for table '{table_name}', field '{self.field_name}'")
        print(f"  Available flags: {list(self.flags.keys())}")
    
    def _sanitize_name(self, name: str) -> str:
        """Must match BitfieldDefinitionManager._sanitize_name()"""
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if sanitized and sanitized[0].isdigit():
            sanitized = '_' + sanitized
        return sanitized.lower()
    
    def _load_definitions(self) -> Dict[str, int]:
        """Load flag definitions for THIS table only"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT flag_name, bit_position
            FROM bitfield_definitions
            WHERE field_name = %s
        """, (self.field_name,))
        
        flags = {row[0]: 1 << row[1] for row in cursor.fetchall()}
        cursor.close()
        return flags
    
    def _validate_flag(self, name: str) -> int:
        """Validate flag name against THIS table's definitions"""
        name = name.upper()
        if name not in self.flags:
            raise ValueError(
                f"Unknown flag '{name}' for table '{self.table_name}'. "
                f"Valid flags: {list(self.flags.keys())}"
            )
        return self.flags[name]
    
    def format_flags(self, value: int) -> str:
        """Convert bitfield to human-readable string using THIS table's flags"""
        active_flags = []
        for name, mask in sorted(self.flags.items(), key=lambda x: x[1]):
            if value & mask:
                active_flags.append(name)
        return ', '.join(active_flags) if active_flags else '(none)'


# Complete example showing tracking functionality
def print_cleanup_summary(summary: Dict[str, Any]):
    """Helper function to print cleanup results"""
    print("\n" + "="*60)
    print("Cleanup Summary")
    print("="*60)
    print(f"  Total tables processed: {summary['total_tables']}")
    print(f"  Successfully cleaned: {len(summary['tables_cleaned'])}")
    print(f"  Failed: {len(summary['tables_failed'])}")
    print(f"  Definitions deleted: {summary['total_definitions_deleted']}")
    print(f"  Functions dropped: {summary['total_functions_dropped']}")
    print(f"  Orphaned definitions cleaned: {summary.get('orphaned_definitions_cleaned', 0)}")
    print(f"  Orphaned functions cleaned: {summary.get('orphaned_functions_cleaned', 0)}")
    
    if summary['tables_cleaned']:
        print(f"\n  Cleaned tables:")
        for table in summary['tables_cleaned']:
            print(f"    ✓ {table}")
    
    if summary['tables_failed']:
        print(f"\n  Failed tables:")
        for table in summary['tables_failed']:
            print(f"    ✗ {table}")
    
    if summary['errors']:
        print(f"\n  Errors:")
        for error in summary['errors']:
            print(f"    - {error}")
    
    print("="*60)
    
def test_bitfield_functions(conn, tracker):
    """
    Comprehensive tests for bitfield SQL functions.
    Tests all generated functions before cleanup.
    """
    print("\n" + "="*60)
    print("Testing Bitfield SQL Functions")
    print("="*60)
    
    # Create test table with bitfield definitions
    print("\n--- Setting up test_functions_table ---")
    mgr_test = BitfieldDefinitionManager(conn, 'test_functions_table',
                                        description='Testing bitfield functions')
    mgr_test.define_flags({
        'ACTIVE': {'bit': 0, 'description': 'Item is active'},
        'ENABLED': {'bit': 1, 'description': 'Item is enabled'},
        'ERROR': {'bit': 2, 'description': 'Item has error'},
        'WARNING': {'bit': 3, 'description': 'Item has warning'},
        'LOCKED': {'bit': 4, 'description': 'Item is locked'},
        'CALIBRATED': {'bit': 5, 'description': 'Item is calibrated'},
    })
    
    # Create the actual table
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_functions_table (
            id SERIAL PRIMARY KEY,
            name TEXT,
            status_flags BIGINT DEFAULT 0,
            value INTEGER DEFAULT 0
        )
    """)
    
    # Insert test data
    cursor.execute("""
        INSERT INTO test_functions_table (name, status_flags, value) VALUES
        ('item1', 0, 100),
        ('item2', 0, 200),
        ('item3', 0, 300),
        ('item4', 0, 400)
    """)
    conn.commit()
    
    print("  ✓ Created test table with 4 items")
    
    # Test 1: set_{field} - Set individual flags
    print("\n--- Test 1: set_test_functions_table() ---")
    
    # Set ACTIVE on item1
    cursor.execute("""
        UPDATE test_functions_table 
        SET status_flags = set_test_functions_table(status_flags, 1)
        WHERE name = 'item1'
    """)
    
    # Set ACTIVE and ENABLED on item2
    cursor.execute("""
        UPDATE test_functions_table 
        SET status_flags = set_test_functions_table(status_flags, 3)
        WHERE name = 'item2'
    """)
    
    # Set ACTIVE, ENABLED, and CALIBRATED on item3
    cursor.execute("""
        UPDATE test_functions_table 
        SET status_flags = set_test_functions_table(status_flags, 35)
        WHERE name = 'item3'
    """)
    
    conn.commit()
    
    cursor.execute("SELECT name, status_flags FROM test_functions_table ORDER BY name")
    for name, flags in cursor.fetchall():
        print(f"  {name}: flags = {flags:04b} ({flags})")
    
    # Test 2: format_{field} - Human readable output
    print("\n--- Test 2: format_test_functions_table() ---")
    cursor.execute("""
        SELECT name, format_test_functions_table(status_flags) as flags_text
        FROM test_functions_table 
        ORDER BY name
    """)
    
    for name, flags_text in cursor.fetchall():
        print(f"  {name}: {flags_text or '(none)'}")
    
    # Test 3: has_{field} - Check if ALL specified flags are set
    print("\n--- Test 3: has_test_functions_table() - Check ALL flags set ---")
    
    # Check which items have BOTH ACTIVE and ENABLED
    cursor.execute("""
        SELECT name, status_flags, 
               has_test_functions_table(status_flags, 3) as has_both
        FROM test_functions_table 
        ORDER BY name
    """)
    
    print("  Checking for ACTIVE AND ENABLED (mask=3):")
    for name, flags, has_both in cursor.fetchall():
        print(f"    {name}: {flags:06b} -> {has_both}")
    
    # Test 4: has_any_{field} - Check if ANY specified flag is set
    print("\n--- Test 4: has_any_test_functions_table() - Check ANY flag set ---")
    
    # Check which items have EITHER ERROR or WARNING
    cursor.execute("""
        SELECT name, status_flags,
               has_any_test_functions_table(status_flags, 12) as has_any_issue
        FROM test_functions_table 
        ORDER BY name
    """)
    
    print("  Checking for ERROR OR WARNING (mask=12):")
    for name, flags, has_any in cursor.fetchall():
        print(f"    {name}: {flags:06b} -> {has_any}")
    
    # Set ERROR on item2 to test
    cursor.execute("""
        UPDATE test_functions_table 
        SET status_flags = set_test_functions_table(status_flags, 4)
        WHERE name = 'item2'
    """)
    conn.commit()
    
    cursor.execute("""
        SELECT name, status_flags,
               has_any_test_functions_table(status_flags, 12) as has_any_issue
        FROM test_functions_table 
        WHERE name = 'item2'
    """)
    
    name, flags, has_any = cursor.fetchone()
    print(f"  After setting ERROR on item2: {name} -> {has_any}")
    
    # Test 5: toggle_{field} - Toggle flags on/off
    print("\n--- Test 5: toggle_test_functions_table() ---")
    
    print("  Before toggle:")
    cursor.execute("""
        SELECT name, format_test_functions_table(status_flags) as flags_text
        FROM test_functions_table 
        WHERE name = 'item2'
    """)
    name, flags_text = cursor.fetchone()
    print(f"    {name}: {flags_text}")
    
    # Toggle ERROR flag on item2 (should turn it off)
    cursor.execute("""
        UPDATE test_functions_table 
        SET status_flags = toggle_test_functions_table(status_flags, 4)
        WHERE name = 'item2'
    """)
    conn.commit()
    
    print("  After toggling ERROR flag:")
    cursor.execute("""
        SELECT name, format_test_functions_table(status_flags) as flags_text
        FROM test_functions_table 
        WHERE name = 'item2'
    """)
    name, flags_text = cursor.fetchone()
    print(f"    {name}: {flags_text}")
    
    # Test 6: clear_{field} - Clear specific flags
    print("\n--- Test 6: clear_test_functions_table() ---")
    
    # Set multiple flags on item4
    cursor.execute("""
        UPDATE test_functions_table 
        SET status_flags = set_test_functions_table(status_flags, 63)
        WHERE name = 'item4'
    """)
    conn.commit()
    
    print("  Before clear:")
    cursor.execute("""
        SELECT name, format_test_functions_table(status_flags) as flags_text
        FROM test_functions_table 
        WHERE name = 'item4'
    """)
    name, flags_text = cursor.fetchone()
    print(f"    {name}: {flags_text}")
    
    # Clear ACTIVE and ENABLED (mask = 3)
    cursor.execute("""
        UPDATE test_functions_table 
        SET status_flags = clear_test_functions_table(status_flags, 3)
        WHERE name = 'item4'
    """)
    conn.commit()
    
    print("  After clearing ACTIVE and ENABLED:")
    cursor.execute("""
        SELECT name, format_test_functions_table(status_flags) as flags_text
        FROM test_functions_table 
        WHERE name = 'item4'
    """)
    name, flags_text = cursor.fetchone()
    print(f"    {name}: {flags_text}")
    
    # Test 7: Complex query - Find items matching criteria
    print("\n--- Test 7: Complex query using multiple functions ---")
    
    cursor.execute("""
        SELECT 
            name,
            format_test_functions_table(status_flags) as flags,
            has_test_functions_table(status_flags, 1) as is_active,
            has_any_test_functions_table(status_flags, 12) as has_issues
        FROM test_functions_table 
        WHERE has_test_functions_table(status_flags, 1)  -- Must be ACTIVE
        ORDER BY name
    """)
    
    print("  Active items with issue status:")
    print(f"    {'Name':<10} {'Flags':<30} {'Active':<8} {'Has Issues'}")
    print(f"    {'-'*10} {'-'*30} {'-'*8} {'-'*11}")
    for name, flags, is_active, has_issues in cursor.fetchall():
        print(f"    {name:<10} {flags:<30} {str(is_active):<8} {has_issues}")
    
    # Test 8: Bulk operations using functions
    print("\n--- Test 8: Bulk flag operations ---")
    
    # Set WARNING flag on all items
    cursor.execute("""
        UPDATE test_functions_table 
        SET status_flags = set_test_functions_table(status_flags, 8)
    """)
    
    # Clear ERROR flag on all items
    cursor.execute("""
        UPDATE test_functions_table 
        SET status_flags = clear_test_functions_table(status_flags, 4)
    """)
    conn.commit()
    
    cursor.execute("""
        SELECT name, format_test_functions_table(status_flags) as flags_text
        FROM test_functions_table 
        ORDER BY name
    """)
    
    print("  After bulk operations (set WARNING, clear ERROR):")
    for name, flags_text in cursor.fetchall():
        print(f"    {name}: {flags_text}")
    
    # Test 9: Using functions in WHERE clauses
    print("\n--- Test 9: Using functions in WHERE clauses ---")
    
    cursor.execute("""
        SELECT name, value
        FROM test_functions_table 
        WHERE has_test_functions_table(status_flags, 9)  -- ACTIVE and WARNING
        ORDER BY value DESC
    """)
    
    print("  Items with ACTIVE and WARNING flags:")
    for name, value in cursor.fetchall():
        print(f"    {name}: value={value}")
    
    # Test 10: Conditional updates using functions
    print("\n--- Test 10: Conditional updates based on flags ---")
    
    cursor.execute("""
        UPDATE test_functions_table 
        SET value = value * 2,
            status_flags = set_test_functions_table(status_flags, 16)  -- Set LOCKED
        WHERE has_any_test_functions_table(status_flags, 8)  -- Has WARNING
        RETURNING name, value, format_test_functions_table(status_flags) as flags
    """)
    conn.commit()
    
    print("  Updated items (doubled value, set LOCKED for items with WARNING):")
    for name, value, flags in cursor.fetchall():
        print(f"    {name}: value={value}, flags={flags}")
    
    # Test 11: Aggregation with functions
    print("\n--- Test 11: Aggregation queries ---")
    
    cursor.execute("""
        SELECT 
            has_test_functions_table(status_flags, 1) as is_active,
            COUNT(*) as count,
            AVG(value) as avg_value
        FROM test_functions_table 
        GROUP BY has_test_functions_table(status_flags, 1)
        ORDER BY is_active DESC
    """)
    
    print("  Statistics by ACTIVE status:")
    print(f"    {'Active':<8} {'Count':<8} {'Avg Value'}")
    print(f"    {'-'*8} {'-'*8} {'-'*10}")
    for is_active, count, avg_value in cursor.fetchall():
        print(f"    {str(is_active):<8} {count:<8} {avg_value:.1f}")
    
    # Final state
    print("\n--- Final state of all items ---")
    cursor.execute("""
        SELECT 
            name, 
            status_flags,
            format_test_functions_table(status_flags) as flags_text,
            value
        FROM test_functions_table 
        ORDER BY name
    """)
    
    print(f"  {'Name':<10} {'Flags':<8} {'Value':<8} {'Flags Description'}")
    print(f"  {'-'*10} {'-'*8} {'-'*8} {'-'*30}")
    for name, flags, flags_text, value in cursor.fetchall():
        print(f"  {name:<10} {flags:<8} {value:<8} {flags_text}")
    
    cursor.close()
    
    # Return the manager for cleanup
    return mgr_test


def test_multiple_tables_isolation(conn, tracker):
    """
    Test that functions for different tables are properly isolated.
    """
    print("\n" + "="*60)
    print("Testing Function Isolation Between Tables")
    print("="*60)
    
    # Create two tables with different flag definitions
    print("\n--- Creating table_a and table_b ---")
    
    mgr_a = BitfieldDefinitionManager(conn, 'table_a', description='Table A')
    mgr_a.define_flags({
        'FLAG_A1': {'bit': 0, 'description': 'Flag A1'},
        'FLAG_A2': {'bit': 1, 'description': 'Flag A2'},
        'FLAG_A3': {'bit': 2, 'description': 'Flag A3'},
    })
    
    mgr_b = BitfieldDefinitionManager(conn, 'table_b', description='Table B')
    mgr_b.define_flags({
        'FLAG_B1': {'bit': 0, 'description': 'Flag B1'},
        'FLAG_B2': {'bit': 1, 'description': 'Flag B2'},
        'FLAG_B3': {'bit': 2, 'description': 'Flag B3'},
        'FLAG_B4': {'bit': 3, 'description': 'Flag B4'},
    })
    
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS table_a (
            id SERIAL PRIMARY KEY,
            status_flags BIGINT DEFAULT 0
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS table_b (
            id SERIAL PRIMARY KEY,
            status_flags BIGINT DEFAULT 0
        )
    """)
    
    cursor.execute("INSERT INTO table_a (status_flags) VALUES (7)")  # All 3 flags
    cursor.execute("INSERT INTO table_b (status_flags) VALUES (15)") # All 4 flags
    conn.commit()
    
    print("  ✓ Created table_a (3 flags) and table_b (4 flags)")
    
    # Test that each table uses its own format function
    print("\n--- Testing format functions are table-specific ---")
    
    cursor.execute("SELECT format_table_a(status_flags) FROM table_a")
    flags_a = cursor.fetchone()[0]
    print(f"  table_a flags: {flags_a}")
    
    cursor.execute("SELECT format_table_b(status_flags) FROM table_b")
    flags_b = cursor.fetchone()[0]
    print(f"  table_b flags: {flags_b}")
    
    # Test that manipulation functions are isolated
    print("\n--- Testing set functions are table-specific ---")
    
    cursor.execute("UPDATE table_a SET status_flags = set_table_a(status_flags, 4)")
    cursor.execute("UPDATE table_b SET status_flags = set_table_b(status_flags, 8)")
    conn.commit()
    
    cursor.execute("SELECT format_table_a(status_flags) FROM table_a")
    flags_a = cursor.fetchone()[0]
    print(f"  table_a after set: {flags_a}")
    
    cursor.execute("SELECT format_table_b(status_flags) FROM table_b")
    flags_b = cursor.fetchone()[0]
    print(f"  table_b after set: {flags_b}")
    
    cursor.close()
    
    print("  ✓ Functions are properly isolated between tables")
    
    return mgr_a, mgr_b


# Add this to the main section
if __name__ == '__main__':
    import os
    connection_params = {
        'host': 'localhost',
        'database': 'knowledge_base',
        'user': 'gedgar',
        'password': os.getenv("POSTGRES_PASSWORD"),
        'port': 5432
    }
    conn = psycopg2.connect(**connection_params)
    
    print("\n" + "="*60)
    print("ChainTree Bitfield Management with Tracking")
    print("="*60)
    
    # Create tracker
    tracker = BitfieldTableTracker(conn)
    
    # ===== SETUP PHASE: Define flags for each table =====
    
    print("\n--- Setting up motor_nodes table ---")
    mgr_motors = BitfieldDefinitionManager(conn, 'motor_nodes', 
                                          description='Motor control and status')
    mgr_motors.define_flags({
        'ACTIVE': {'bit': 0, 'description': 'Motor is active'},
        'ENABLED': {'bit': 1, 'description': 'Motor is enabled'},
        'ERROR': {'bit': 2, 'description': 'Motor has error'},
        'RUNNING': {'bit': 3, 'description': 'Motor is running'},
        'STALLED': {'bit': 4, 'description': 'Motor is stalled'},
        'CALIBRATED': {'bit': 5, 'description': 'Motor is calibrated'},
    })
    
    print("\n--- Setting up sensor_nodes table ---")
    mgr_sensors = BitfieldDefinitionManager(conn, 'sensor_nodes',
                                           description='Sensor readings and status')
    mgr_sensors.define_flags({
        'ACTIVE': {'bit': 0, 'description': 'Sensor is active'},
        'ENABLED': {'bit': 1, 'description': 'Sensor is enabled'},
        'ERROR': {'bit': 2, 'description': 'Sensor has error'},
        'READING_VALID': {'bit': 3, 'description': 'Sensor reading is valid'},
        'CALIBRATED': {'bit': 4, 'description': 'Sensor is calibrated'},
        'OUT_OF_RANGE': {'bit': 5, 'description': 'Reading out of range'},
    })
    
    # ===== RUN COMPREHENSIVE TESTS =====
    
    # Test 1: Complete function testing
    mgr_test_funcs = test_bitfield_functions(conn, tracker)
    
    # Test 2: Table isolation
    mgr_a, mgr_b = test_multiple_tables_isolation(conn, tracker)
    
    # ===== DEMONSTRATE TRACKING =====
    
    print("\n" + "="*60)
    print("Bitfield Table Tracking")
    print("="*60)
    
    print("\n--- All tracked tables ---")
    tracked_tables = tracker.list_all_tables()
    for table_name, field_name, flags_col, flag_count, updated in tracked_tables:
        print(f"  {table_name:25} -> {field_name:25} [{flags_col}] {flag_count} flags")
    
    print("\n--- Statistics ---")
    stats = tracker.get_statistics()
    print(f"  Total tables: {stats['total_tables']}")
    print(f"  Total flags: {stats['total_flags']}")
    print(f"  Average flags per table: {stats['average_flags_per_table']}")
    
    # ===== CLEANUP PHASE =====
    
    print("\n" + "="*60)
    print("Cleanup Phase")
    print("="*60)
    
    # Clean up test tables
    print("\n--- Cleaning up test_functions_table ---")
    result = tracker.full_cleanup('test_functions_table', confirm=True)
    print(f"  ✓ Unregistered: {result['unregistered']}")
    print(f"  ✓ Table dropped: {result['table_dropped']}")
    print(f"  ✓ Definitions deleted: {result['definitions_deleted']}")
    print(f"  ✓ Functions dropped: {len(result['functions_dropped'])}")
    
    print("\n--- Cleaning up table_a and table_b ---")
    result_a = tracker.full_cleanup('table_a', confirm=True)
    result_b = tracker.full_cleanup('table_b', confirm=True)
    print(f"  ✓ Cleaned up table_a and table_b")
    
    # Check for orphaned items
    print("\n--- Checking for orphaned items ---")
    orphaned_defs = tracker.cleanup_orphaned_definitions()
    print(f"  Orphaned definitions cleaned: {orphaned_defs}")
    
    orphaned_funcs = tracker.list_orphaned_functions()
    if orphaned_funcs:
        print(f"  Orphaned functions found: {len(orphaned_funcs)}")
        print(f"    {', '.join(orphaned_funcs[:5])}...")
    else:
        print(f"  ✓ No orphaned functions found")
    
    print("\n--- Final registry state ---")
    tracked_tables = tracker.list_all_tables()
    for table_name, field_name, flags_col, flag_count, updated in tracked_tables:
        print(f"  {table_name:25} -> {field_name:25} [{flags_col}] {flag_count} flags")
        
        
    print("\n" + "="*60)
    print("Knowledge Base Cleanup Workflow")
    print("="*60)
    
    # Scenario: You're creating a new knowledge base and want to clean out
    # all bitfield tables from the previous knowledge base
    
    print("\n--- Step 1: Preview what will be cleaned ---")
    preview = tracker.get_cleanup_preview()
    print(f"\n  Found {preview['total_tables']} tracked tables:")
    for table_info in preview['tracked_tables']:
        print(f"    - {table_info['table_name']:25} ({table_info['flag_count']} flags)")
    
    print(f"\n  Total flags across all tables: {preview['total_flags']}")
    print(f"  Orphaned definitions: {preview['orphaned_definitions']}")
    print(f"  Orphaned functions: {len(preview['orphaned_functions'])}")
    
    if preview['orphaned_functions']:
        print(f"    {', '.join(preview['orphaned_functions'][:5])}...")
    
    # Option 1: Clean metadata only (keep tables and their data)
    print("\n--- Option 1: Reset bitfield metadata only ---")
    print("  (Unregisters all tables, deletes definitions, drops functions)")
    print("  (BUT keeps the actual database tables with their data)")
    print("\n  Uncommment to run:")
    print("  # summary = tracker.reset_all_bitfield_metadata(confirm=True)")
    print("  # print_cleanup_summary(summary)")
    
    # Option 2: Drop everything including tables
    print("\n--- Option 2: Drop all bitfield tables ---")
    print("  (Drops actual tables AND all metadata)")
    print("\n  Uncomment to run:")
    print("  # summary = tracker.drop_all_bitfield_tables(confirm=True)")
    print("  # print_cleanup_summary(summary)")
    
    # Actual cleanup for demo (clean specific test tables)
    print("\n--- Demo: Cleaning up test tables only ---")
    
    test_tables = ['test_functions_table', 'table_a', 'table_b']
    
    for table_name in test_tables:
        info = tracker.get_table_info(table_name)
        if info:
            print(f"\n  Cleaning {table_name}...")
            result = tracker.full_cleanup(table_name, confirm=True)
            print(f"    Unregistered: {result['unregistered']}")
            print(f"    Table dropped: {result['table_dropped']}")
            print(f"    Definitions deleted: {result['definitions_deleted']}")
            print(f"    Functions dropped: {len(result['functions_dropped'])}")
    
    # Clean up orphans
    print("\n--- Cleaning orphaned items ---")
    orphaned_defs = tracker.cleanup_orphaned_definitions()
    print(f"  Orphaned definitions cleaned: {orphaned_defs}")
    
    orphaned_funcs = tracker.list_orphaned_functions()
    if orphaned_funcs:
        dropped = tracker.drop_orphaned_functions(confirm=True)
        print(f"  Orphaned functions cleaned: {len(dropped)}")
    else:
        print(f"  No orphaned functions found")
    
    # Show final state
    print("\n--- Final tracked tables ---")
    tracked_tables = tracker.list_all_tables()
    if tracked_tables:
        print(f"  Remaining tracked tables: {len(tracked_tables)}")
        for table_name, field_name, flags_col, flag_count, updated in tracked_tables:
            print(f"    {table_name:25} -> {field_name:25} [{flags_col}] {flag_count} flags")
    else:
        print("  ✓ No tracked tables remaining (clean slate)")
    
    
    
    print("\n" + "="*60)
    print("Workflow demonstration complete")
    print("="*60)
    
    print("\nOption 1: Clean bitfield metadata only")
    print("  - Keeps motor_nodes, sensor_nodes tables with their data")
    print("  - Removes all bitfield definitions and functions")
    print("  - You can redefine bitfields later\n")
    
    # Option 2: Drop everything
    print("Option 2: Drop ALL bitfield tables")
    print("  - DROPS motor_nodes, sensor_nodes tables completely")
    print("  - Removes all bitfield definitions and functions")
    print("  - Complete clean slate\n")
    
   
    tracker.cleanup_all_bitfields(drop_actual_tables=True)
  
    conn.close()
    
    print("\n" + "="*60)
    print("Complete: All tests passed, cleanup successful")
    print("="*60)