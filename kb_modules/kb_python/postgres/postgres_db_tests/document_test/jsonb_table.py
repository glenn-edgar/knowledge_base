#!/usr/bin/env python3
"""
LTree JSONB Database Operations
JSONB operations including queues on ltree-based document database.
Provides mainstream PostgreSQL JSONB operators and queue abstractions.
"""

import json
from typing import Optional, List, Dict, Any, Union
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from psycopg2 import sql


class QueueOperationError(Exception):
    """Raised when a queue operation fails."""
    pass


class LTreeJsonDB:
    """
    JSONB operations on ltree document database with queue support.
    Assumes records already exist and are managed by another class.
    
    Provides:
    - Core JSONB operations (get, set, delete, contains, etc.)
    - Array operations (append, prepend, remove, etc.)
    - Queue/Stack abstractions (enqueue, dequeue, push, pop)
    - Path-based queries using standard PostgreSQL JSONB operators
    """
    
    def __init__(self, 
                 conn, 
                 table_name: str = "knowledge_base_documents"):
        """
        Initialize the JSONB database operations.
        
        Args:
            conn: psycopg2 connection object
            table_name: Name of the table to operate on
        """
        self.conn = conn
        self.table_name = table_name
    
    # ===== Core JSONB Operations =====
    
    def jsonb_get(self,
                  ltree_path: str,
                  json_path: str,
                  as_text: bool = False,
                  doc_type: Optional[str] = None) -> Any:
        """
        Get a value from JSONB field using -> or ->> operators.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path in format "field" or "field.subfield"
            as_text: If True, use ->> (text), else use -> (JSON)
            doc_type: Optional document type filter
            
        Returns:
            The value at the JSON path, or None if not found
        """
        # Convert dot notation to PostgreSQL path operators
        path_parts = json_path.split('.')
        
        # Build the accessor chain and params list
        # SQL format: SELECT {accessor} FROM table WHERE ltree = %s
        # Params order: [accessor_params..., ltree_path, doc_type?]
        if len(path_parts) == 1:
            # Single key: use -> or ->>
            operator = "->>" if as_text else "->"
            accessor = f"data {operator} %s"
            params = [path_parts[0], ltree_path]
        else:
            # Nested path: use #> or #>>
            operator = "#>>" if as_text else "#>"
            accessor = f"data {operator} %s::text[]"
            params = [path_parts, ltree_path]
        
        type_filter = "AND type = %s" if doc_type else ""
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT {accessor} as value
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else None
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to get JSONB value: {e}")
    
    def jsonb_set(self,
                  ltree_path: str,
                  json_path: str,
                  value: Any,
                  doc_type: Optional[str] = None,
                  create_missing: bool = True) -> bool:
        """
        Set a value in JSONB field using jsonb_set().
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path in format "field" or "field.subfield"
            value: Value to set (will be JSON encoded)
            doc_type: Optional document type filter
            create_missing: Create path if it doesn't exist
            
        Returns:
            True if successful, False if document not found
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, Json(value), ltree_path]
        if doc_type:
            params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = jsonb_set(
            data,
            %s::text[],
            %s::jsonb,
            {str(create_missing).lower()}
        ),
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter}
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to set JSONB value: {e}")
    
    def jsonb_delete_key(self,
                        ltree_path: str,
                        key: str,
                        doc_type: Optional[str] = None) -> bool:
        """
        Delete a key from JSONB using - operator.
        
        Args:
            ltree_path: The document ltree path
            key: Key to delete
            doc_type: Optional document type filter
            
        Returns:
            True if successful
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [key, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = data - %s,
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter}
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to delete JSONB key: {e}")
    
    def jsonb_delete_path(self,
                         ltree_path: str,
                         json_path: str,
                         doc_type: Optional[str] = None) -> bool:
        """
        Delete a nested path from JSONB using #- operator.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to delete (e.g., "address.city")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = data #- %s::text[],
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter}
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to delete JSONB path: {e}")
    
    # ===== Existence & Search Operations =====
    
    def jsonb_has_key(self,
                     ltree_path: str,
                     key: str,
                     doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB has a key using ? operator.
        
        Args:
            ltree_path: The document ltree path
            key: Key to check
            doc_type: Optional document type filter
            
        Returns:
            True if key exists
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [key, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data ? %s as has_key
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB key: {e}")
    
    def jsonb_has_any_keys(self,
                          ltree_path: str,
                          keys: List[str],
                          doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB has any of the specified keys using ?| operator.
        
        Args:
            ltree_path: The document ltree path
            keys: List of keys to check
            doc_type: Optional document type filter
            
        Returns:
            True if any key exists
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [keys, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data ?| %s::text[] as has_any
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB keys: {e}")
    
    def jsonb_has_all_keys(self,
                          ltree_path: str,
                          keys: List[str],
                          doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB has all of the specified keys using ?& operator.
        
        Args:
            ltree_path: The document ltree path
            keys: List of keys to check
            doc_type: Optional document type filter
            
        Returns:
            True if all keys exist
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [keys, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data ?& %s::text[] as has_all
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB keys: {e}")
    
    def jsonb_contains(self,
                      ltree_path: str,
                      contained: Dict[str, Any],
                      doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB contains an object using @> operator.
        
        Args:
            ltree_path: The document ltree path
            contained: Object that should be contained
            doc_type: Optional document type filter
            
        Returns:
            True if contained
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [Json(contained), ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data @> %s::jsonb as contains
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB containment: {e}")
    
    def jsonb_contained_by(self,
                          ltree_path: str,
                          container: Dict[str, Any],
                          doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB is contained by an object using <@ operator.
        
        Args:
            ltree_path: The document ltree path
            container: Object that should contain the data
            doc_type: Optional document type filter
            
        Returns:
            True if contained by
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [Json(container), ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data <@ %s::jsonb as contained_by
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB containment: {e}")
    
    # ===== Path Query Operations =====
    
    def jsonb_path_exists(self,
                         ltree_path: str,
                         json_path_query: str,
                         doc_type: Optional[str] = None) -> bool:
        """
        Check if a JSON path exists using jsonb_path_exists().
        
        Example: '$.address.city ? (@ == "LA")'
        
        Args:
            ltree_path: The document ltree path
            json_path_query: JSON path query
            doc_type: Optional document type filter
            
        Returns:
            True if path exists
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [json_path_query, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT jsonb_path_exists(data, %s::jsonpath) as exists
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSON path: {e}")
    
    def jsonb_path_query(self,
                        ltree_path: str,
                        json_path_query: str,
                        doc_type: Optional[str] = None) -> List[Any]:
        """
        Query JSON path using jsonb_path_query_array().
        
        Example: '$.items[*].price'
        
        Args:
            ltree_path: The document ltree path
            json_path_query: JSON path query
            doc_type: Optional document type filter
            
        Returns:
            Array of matching values
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [json_path_query, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT jsonb_path_query_array(data, %s::jsonpath) as results
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else []
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to query JSON path: {e}")
    
    def jsonb_query(self,
                   ltree_path: str,
                   jsonb_filter: Dict[str, Any],
                   doc_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Query a document with JSONB containment filter using @>.
        
        Args:
            ltree_path: The document ltree path
            jsonb_filter: JSONB filter using @> containment operator
            doc_type: Optional document type filter
            
        Returns:
            The document data if found and matches filter, None otherwise
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [ltree_path, Json(jsonb_filter)]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT id, ltree::text as ltree, type, data
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        AND data @> %s::jsonb
        {type_filter};
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return dict(result) if result else None
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to query JSONB: {e}")
    
    # ===== Array Operations =====
    
    def jsonb_array_append(self,
                          ltree_path: str,
                          json_path: str,
                          item: Any,
                          doc_type: Optional[str] = None) -> bool:
        """
        Append an item to a JSONB array using || operator.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            item: Item to append
            doc_type: Optional document type filter
            
        Returns:
            True if successful, False if document not found
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, path_parts, Json(item), ltree_path]
        if doc_type:
            params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = jsonb_set(
            data,
            %s::text[],
            COALESCE(data #> %s::text[], '[]'::jsonb) || %s::jsonb,
            true
        ),
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter}
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to append to JSONB array: {e}")
    
    def jsonb_array_prepend(self,
                           ltree_path: str,
                           json_path: str,
                           item: Any,
                           doc_type: Optional[str] = None) -> bool:
        """
        Prepend an item to a JSONB array using || operator.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            item: Item to prepend
            doc_type: Optional document type filter
            
        Returns:
            True if successful, False if document not found
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, Json(item), path_parts, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = jsonb_set(
            data,
            %s::text[],
            %s::jsonb || COALESCE(data #> %s::text[], '[]'::jsonb),
            true
        ),
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter}
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to prepend to JSONB array: {e}")
    
    def jsonb_array_remove_index(self,
                                 ltree_path: str,
                                 json_path: str,
                                 index: int,
                                 doc_type: Optional[str] = None) -> Optional[Any]:
        """
        Remove an item from a JSONB array by index and return it.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            index: Index to remove (0-based)
            doc_type: Optional document type filter
            
        Returns:
            The removed item, or None if not found
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        
        # First get the item
        select_params = [path_parts, index, ltree_path]
        if doc_type:
            select_params.append(doc_type)
        
        select_sql = f"""
        SELECT (data #> %s::text[]) -> %s as item
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        # Then remove it
        update_params = [path_parts, path_parts, index, ltree_path]
        if doc_type:
            update_params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = jsonb_set(
            data,
            %s::text[],
            (data #> %s::text[]) - %s,
            true
        ),
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                # Get the item first
                cur.execute(select_sql, select_params)
                result = cur.fetchone()
                removed_item = result[0] if result else None
                
                # Then remove it if it exists
                if removed_item is not None:
                    cur.execute(update_sql, update_params)
                
                self.conn.commit()
                return removed_item
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to remove from JSONB array: {e}")
    
    def jsonb_array_contains(self,
                            ltree_path: str,
                            json_path: str,
                            item: Any,
                            doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB array contains an item using @> operator.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            item: Item to check for
            doc_type: Optional document type filter
            
        Returns:
            True if array contains item
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, Json([item]), ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT (data #> %s::text[]) @> %s::jsonb as contains
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check array containment: {e}")
    
    def jsonb_array_elements(self,
                            ltree_path: str,
                            json_path: str,
                            doc_type: Optional[str] = None) -> List[Any]:
        """
        Expand JSONB array elements using jsonb_array_elements().
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            doc_type: Optional document type filter
            
        Returns:
            List of array elements
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT jsonb_array_elements(data #> %s::text[]) as element
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                results = cur.fetchall()
                return [row[0] for row in results]
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to expand array elements: {e}")
    
    # ===== Queue Operations (High-Level Abstractions) =====
    
    def enqueue(self,
                ltree_path: str,
                item: Any,
                queue_path: str = "items",
                doc_type: Optional[str] = None) -> bool:
        """
        Add an item to the end of the queue (append) - FIFO.
        
        Args:
            ltree_path: The document ltree path
            item: Item to add to queue
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
            
        Raises:
            QueueOperationError: If operation fails
        """
        try:
            result = self.jsonb_array_append(ltree_path, queue_path, item, doc_type)
            if not result:
                raise QueueOperationError(f"Document not found: {ltree_path}")
            return True
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to enqueue: {e}")
    
    def dequeue(self,
                ltree_path: str,
                queue_path: str = "items",
                doc_type: Optional[str] = None) -> Optional[Any]:
        """
        Remove and return the first item from the queue (FIFO).
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            The dequeued item, or None if queue is empty
            
        Raises:
            QueueOperationError: If operation fails
        """
        try:
            item = self.jsonb_array_remove_index(ltree_path, queue_path, 0, doc_type)
            return item
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to dequeue: {e}")
    
    def peek(self,
             ltree_path: str,
             queue_path: str = "items",
             doc_type: Optional[str] = None,
             index: int = 0) -> Optional[Any]:
        """
        View an item in the queue without removing it.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            index: Index to peek at (default: 0 for first item)
            
        Returns:
            The item at the specified index, or None if not found
        """
        queue = self.jsonb_get(ltree_path, queue_path, as_text=False, doc_type=doc_type)
        if queue and isinstance(queue, list) and 0 <= index < len(queue):
            return queue[index]
        return None
    
    def size(self,
             ltree_path: str,
             queue_path: str = "items",
             doc_type: Optional[str] = None) -> int:
        """
        Get the number of items in the queue.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            Number of items in queue (0 if queue doesn't exist)
        """
        queue = self.jsonb_get(ltree_path, queue_path, as_text=False, doc_type=doc_type)
        if queue and isinstance(queue, list):
            return len(queue)
        return 0
    
    def is_empty(self,
                 ltree_path: str,
                 queue_path: str = "items",
                 doc_type: Optional[str] = None) -> bool:
        """
        Check if the queue is empty.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            True if queue is empty or doesn't exist
        """
        return self.size(ltree_path, queue_path, doc_type) == 0
    
    def clear(self,
              ltree_path: str,
              queue_path: str = "items",
              doc_type: Optional[str] = None) -> bool:
        """
        Remove all items from the queue.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
            
        Raises:
            QueueOperationError: If operation fails
        """
        try:
            result = self.jsonb_set(ltree_path, queue_path, [], doc_type, create_missing=True)
            if not result:
                raise QueueOperationError(f"Document not found: {ltree_path}")
            return True
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to clear queue: {e}")
    
    def get_all(self,
                ltree_path: str,
                queue_path: str = "items",
                doc_type: Optional[str] = None) -> List[Any]:
        """
        Get all items in the queue without modifying it.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            List of all items in queue (empty list if queue doesn't exist)
        """
        queue = self.jsonb_get(ltree_path, queue_path, as_text=False, doc_type=doc_type)
        if queue and isinstance(queue, list):
            return queue
        return []
    
    def push(self,
             ltree_path: str,
             item: Any,
             queue_path: str = "items",
             doc_type: Optional[str] = None) -> bool:
        """
        Push an item to the front of the queue - for stack/priority operations (LIFO).
        
        Args:
            ltree_path: The document ltree path
            item: Item to push
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
            
        Raises:
            QueueOperationError: If operation fails
        """
        try:
            result = self.jsonb_array_prepend(ltree_path, queue_path, item, doc_type)
            if not result:
                raise QueueOperationError(f"Document not found: {ltree_path}")
            return True
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to push: {e}")
    
    def pop(self,
            ltree_path: str,
            queue_path: str = "items",
            doc_type: Optional[str] = None) -> Optional[Any]:
        """
        Remove and return the last item from the queue - for stack operations (LIFO).
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            The popped item, or None if queue is empty
            
        Raises:
            QueueOperationError: If operation fails
        """
        size = self.size(ltree_path, queue_path, doc_type)
        if size == 0:
            return None
        
        try:
            item = self.jsonb_array_remove_index(ltree_path, queue_path, size - 1, doc_type)
            return item
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to pop: {e}")
    
    def get_metadata(self,
                     ltree_path: str,
                     metadata_path: str = "metadata",
                     doc_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get queue metadata.
        
        Args:
            ltree_path: The document ltree path
            metadata_path: JSON path to metadata (default: "metadata")
            doc_type: Optional document type filter
            
        Returns:
            Metadata dict or None if not found
        """
        return self.jsonb_get(ltree_path, metadata_path, as_text=False, doc_type=doc_type)
    
    def set_metadata(self,
                     ltree_path: str,
                     metadata: Dict[str, Any],
                     metadata_path: str = "metadata",
                     doc_type: Optional[str] = None) -> bool:
        """
        Set queue metadata.
        
        Args:
            ltree_path: The document ltree path
            metadata: Metadata to set
            metadata_path: JSON path to metadata (default: "metadata")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
        """
        return self.jsonb_set(ltree_path, metadata_path, metadata, doc_type)


# ===== Test Suite =====

if __name__ == "__main__":
    """Comprehensive test suite for LTreeJsonDB."""
    import os
    
    print("=" * 70)
    print("LTree JSONB Database - Test Suite")
    print("=" * 70)
    print()
    
    conn = None
    try:
        # Setup connection
        password = os.environ.get('POSTGRES_PASSWORD', '')
        conn = psycopg2.connect(
            dbname="knowledge_base",
            user="gedgar",
            password=password,
            host="localhost",
            port=5432
        )
        conn.autocommit = False
        print("✓ Connected to PostgreSQL")
        
        # Create test table and records
        table_name = "knowledge_base_documents"
        
        print("✓ Setting up test environment...")
        with conn.cursor() as cur:
            # Enable ltree
            cur.execute("CREATE EXTENSION IF NOT EXISTS ltree;")
            
            # Insert test records
            cur.execute(f"""
            INSERT INTO {table_name} (ltree, type, data) VALUES
            ('root.queues.tasks', 'queue', '{{"items": [], "metadata": {{"name": "Task Queue"}}}}'::jsonb),
            ('root.queues.messages', 'queue', '{{"items": [], "metadata": {{"name": "Message Queue"}}}}'::jsonb),
            ('root.queues.priority', 'priority_queue', '{{"items": []}}'::jsonb),
            ('root.test.operators', 'test', '{{"name": "Test", "role": "admin", "tags": ["python", "postgres"], "address": {{"city": "LA", "zip": "90001"}}}}'::jsonb)
            ON CONFLICT (ltree) DO UPDATE SET 
                data = EXCLUDED.data,
                updated_at = CURRENT_TIMESTAMP;
            """)
            
            conn.commit()
        
        print("✓ Test environment ready")
        print()
        
        # Initialize database
        db = LTreeJsonDB(conn=conn, table_name=table_name)
        print("✓ JSONB database initialized")
        print()
        
        # Test 1: Basic JSONB Get Operations
        print("Test 1: Basic JSONB Get Operations (-> and ->>)")
        print("-" * 70)
        
        test_path = "root.test.operators"
        
        # Get as JSON
        name = db.jsonb_get(test_path, "name", as_text=False)
        print(f"✓ Get name (JSON): {name} (type: {type(name)})")
        
        # Get as text
        name_text = db.jsonb_get(test_path, "name", as_text=True)
        print(f"✓ Get name (text): {name_text} (type: {type(name_text)})")
        
        # Get nested value
        city = db.jsonb_get(test_path, "address.city", as_text=True)
        print(f"✓ Get nested city: {city}")
        print()
        
        # Test 2: Key Existence Checks
        print("Test 2: Key Existence Checks (?, ?|, ?&)")
        print("-" * 70)
        
        has_role = db.jsonb_has_key(test_path, "role")
        print(f"✓ Has 'role' key: {has_role}")
        
        has_any = db.jsonb_has_any_keys(test_path, ["role", "nonexistent"])
        print(f"✓ Has any of ['role', 'nonexistent']: {has_any}")
        
        has_all = db.jsonb_has_all_keys(test_path, ["name", "role"])
        print(f"✓ Has all of ['name', 'role']: {has_all}")
        
        has_all_fail = db.jsonb_has_all_keys(test_path, ["name", "nonexistent"])
        print(f"✓ Has all of ['name', 'nonexistent']: {has_all_fail}")
        print()
        
        # Test 3: Containment Operators
        print("Test 3: Containment Operators (@>, <@)")
        print("-" * 70)
        
        contains_admin = db.jsonb_contains(test_path, {"role": "admin"})
        print(f"✓ Contains {{'role': 'admin'}}: {contains_admin}")
        
        contains_wrong = db.jsonb_contains(test_path, {"role": "user"})
        print(f"✓ Contains {{'role': 'user'}}: {contains_wrong}")
        
        contained_by = db.jsonb_contained_by(test_path, {
            "name": "Test", 
            "role": "admin",
            "tags": ["python", "postgres"],
            "address": {"city": "LA", "zip": "90001"},
            "extra": "field"
        })
        print(f"✓ Is contained by larger object: {contained_by}")
        print()
        
        # Test 4: Array Contains
        print("Test 4: Array Contains (@>)")
        print("-" * 70)
        
        has_python = db.jsonb_array_contains(test_path, "tags", "python")
        print(f"✓ Tags contain 'python': {has_python}")
        
        has_ruby = db.jsonb_array_contains(test_path, "tags", "ruby")
        print(f"✓ Tags contain 'ruby': {has_ruby}")
        print()
        
        # Test 5: JSON Path Queries
        print("Test 5: JSON Path Queries (jsonb_path_*)")
        print("-" * 70)
        
        # Path exists
        has_admin_role = db.jsonb_path_exists(test_path, '$.role ? (@ == "admin")')
        print(f"✓ Path exists (role == admin): {has_admin_role}")
        
        # Query array elements
        tags = db.jsonb_path_query(test_path, '$.tags[*]')
        print(f"✓ Query tags array: {tags}")
        print()
        
        # Test 6: Set and Delete Operations
        print("Test 6: Set and Delete Operations")
        print("-" * 70)
        
        # Set a value
        db.jsonb_set(test_path, "status", "active")
        status = db.jsonb_get(test_path, "status", as_text=True)
        print(f"✓ Set status: {status}")
        
        # Delete a key
        db.jsonb_delete_key(test_path, "status")
        status_after = db.jsonb_get(test_path, "status")
        print(f"✓ Delete status, value after: {status_after}")
        
        # Delete nested path
        db.jsonb_delete_path(test_path, "address.zip")
        zip_after = db.jsonb_get(test_path, "address.zip")
        print(f"✓ Delete address.zip, value after: {zip_after}")
        print()
        
        # Test 7: Array Elements Expansion
        print("Test 7: Array Elements Expansion (jsonb_array_elements)")
        print("-" * 70)
        
        elements = db.jsonb_array_elements(test_path, "tags")
        print(f"✓ Expanded tag elements: {elements}")
        print()
        
        # Test 8: Basic Queue Operations (FIFO)
        print("Test 8: Basic Queue Operations (FIFO)")
        print("-" * 70)
        
        queue_path = "root.queues.tasks"
        
        db.enqueue(queue_path, {"task": "Task 1", "priority": 1})
        print("✓ Enqueued Task 1")
        
        db.enqueue(queue_path, {"task": "Task 2", "priority": 2})
        print("✓ Enqueued Task 2")
        
        db.enqueue(queue_path, {"task": "Task 3", "priority": 3})
        print("✓ Enqueued Task 3")
        
        size = db.size(queue_path)
        print(f"✓ Queue size: {size}")
        
        item = db.dequeue(queue_path)
        print(f"✓ Dequeued: {item}")
        
        item = db.peek(queue_path)
        print(f"✓ Peeked (without removing): {item}")
        
        size = db.size(queue_path)
        print(f"✓ Queue size after dequeue: {size}")
        print()
        
        # Test 9: Stack Operations (LIFO)
        print("Test 9: Stack Operations (LIFO)")
        print("-" * 70)
        
        stack_path = "root.queues.messages"
        
        db.push(stack_path, {"message": "First"})
        print("✓ Pushed 'First'")
        
        db.push(stack_path, {"message": "Second"})
        print("✓ Pushed 'Second'")
        
        db.push(stack_path, {"message": "Third"})
        print("✓ Pushed 'Third'")
        
        item = db.pop(stack_path)
        print(f"✓ Popped (LIFO): {item}")
        
        item = db.pop(stack_path)
        print(f"✓ Popped (LIFO): {item}")
        
        size = db.size(stack_path)
        print(f"✓ Stack size: {size}")
        print()
        
        # Test 10: Edge Cases
        print("Test 10: Edge Cases")
        print("-" * 70)
        
        # Dequeue from empty queue
        db.clear(queue_path)
        item = db.dequeue(queue_path)
        print(f"✓ Dequeue from empty queue: {item}")
        
        # Pop from empty queue
        item = db.pop(queue_path)
        print(f"✓ Pop from empty queue: {item}")
        
        # Peek at invalid index
        db.enqueue(queue_path, {"data": "test"})
        item = db.peek(queue_path, index=10)
        print(f"✓ Peek at invalid index: {item}")
        
        # Peek at negative index
        item = db.peek(queue_path, index=-1)
        print(f"✓ Peek at negative index: {item}")
        print()
        
        print("=" * 70)
        print("All tests completed successfully!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\n✓ Database connection closed")