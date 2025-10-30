import types
from typing import Optional, Any, List, Dict
from dataclasses import dataclass
from datetime import datetime
import copy
import re



@dataclass



class TreeNode:
    """Represents a node in the tree with metadata."""

    def __init__(
        self,
        ltree_name: str,
        data: Any,
    
    ):
        self.ltree_name = ltree_name
        list_ltree_name = ltree_name.split(".")
        self.node_name = list_ltree_name[-1]
        self.label_name = list_ltree_name[-2]
        self.data = data
    

class KB_Ltree_Search:

    def __init__(self,data):
        self.data = data
    
      # Utility methods
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive tree statistics."""
        if not self.data:
            return {
                'total_nodes': 0,
                'max_depth': 0,
                'avg_depth': 0.0,
                'root_nodes': 0,
                'leaf_nodes': 0
            }
        
        depths = [self.nlevel(path) for path in self.data.keys()]
        root_nodes = sum(1 for path in self.data.keys() if self.nlevel(path) == 1)
        
        # Count leaf nodes (nodes with no children)
        leaf_nodes = 0
        for path in self.data.keys():
            has_children = any(self.ltree_ancestor(path, other_path) for other_path in self.data.keys())
            if not has_children:
                leaf_nodes += 1
        
        return {
            'total_nodes': len(self.data),
            'max_depth': max(depths),
            'avg_depth': sum(depths) / len(depths),
            'root_nodes': root_nodes,
            'leaf_nodes': leaf_nodes
        }
    
    def clear(self) -> None:
        """Clear all data."""
        self.data.clear()
    
    def size(self) -> int:
        """Get the number of nodes."""
        return len(self.data)
    
    def get_all_paths(self) -> List[str]:
        """Get all paths sorted."""
        return sorted(self.data.keys())
    
    @classmethod
    def validate_path(cls, path: str) -> bool:
        """
        Validate that a path conforms to ltree format.
        
        Args:
            path: The path to validate
            
        Returns:
            True if valid, False otherwise
        """
        
        if not path:
            return False
        
        # ltree labels must start with letter or underscore, then alphanumeric and underscores
        # Each label can be 1-256 characters
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$'
        if not re.match(pattern, path):
            return False
        
        # Check each label length
        labels = path.split('.')
        return all(1 <= len(label) <= 256 for label in labels)

    def _path_labels(self, path: str) -> List[str]:
        """Get the labels of a path as a list."""
        return path.split('.')
    
    def _subpath(self, path: str, start: int, length: Optional[int] = None) -> str:
        """
        Extract a subpath from a path.
        
        Args:
            path: The source path
            start: Starting position (0-based)
            length: Number of labels to extract (None for all remaining)
        """
        labels = self._path_labels(path)
        if start < 0:
            start = len(labels) + start
        
        if length is None:
            return '.'.join(labels[start:])
        else:
            return '.'.join(labels[start:start + length])
    
    def _convert_ltree_query_to_regex(self, query: str) -> str:
        """
        Convert full ltree query syntax to regex.
        
        Supports all ltree operators:
        - @ - ltxtquery word separation  
        - @@ - ltxtquery full-text search
        - ~ - match lquery pattern
        - ? - match lquery with case-insensitive
        - @> - ancestor (handled separately)
        - <@ - descendant (handled separately)
        - && - overlap (for arrays)
        - Regular expressions with *{n}, *{n,m}, *{,m} quantifiers
        """
        # Handle ltxtquery format (word1@word2@word3)
        if '@' in query and not query.startswith('@') and not query.endswith('@'):
            # This is an ltxtquery format, convert @ to . for path matching
            return self._convert_simple_pattern(query.replace('@', '.'))
        
        # Convert lquery format
        return self._convert_lquery_pattern(query)
    
    def _convert_lquery_pattern(self, pattern: str) -> str:
        """Convert lquery pattern to regex."""
        # Escape special regex characters first
        result = re.escape(pattern)
        
        # Convert ltree-specific patterns
        # *{n,m} - between n and m levels
        result = re.sub(r'\\*\\\{(\d+),(\d+)\\\}', lambda m: f'([^.]+\\.){{{m.group(1)},{m.group(2)}}}', result)
        
        # *{n,} - n or more levels  
        result = re.sub(r'\\*\\\{(\d+),\\\}', lambda m: f'([^.]+\\.){{{m.group(1)},}}', result)
        
        # *{,m} - up to m levels
        result = re.sub(r'\\*\\\{,(\d+)\\\}', lambda m: f'([^.]+\\.){{0,{m.group(1)}}}', result)
        
        # *{n} - exactly n levels
        result = re.sub(r'\\*\\\{(\d+)\\\}', lambda m: f'([^.]+\\.){{{m.group(1)}}}', result)
        
        # ** - any number of levels (including zero)
        result = result.replace('\\*\\*', '.*')
        
        # * - exactly one level
        result = result.replace('\\*', '[^.]+')
        
        # {a,b,c} - choice between alternatives
        result = re.sub(r'\\{([^}]+)\\}', lambda m: f"({m.group(1).replace(',', '|')})", result)
        
        # Remove trailing dots from quantified patterns
        result = re.sub(r'\\\.\)\{([^}]+)\}', r'){{\1}}[^.]*', result)
        
        return f'^{result}$'
    
    def _convert_simple_pattern(self, pattern: str) -> str:
        """Convert simple wildcard pattern to regex."""
        
        # First handle special sequences before escaping
        parts = pattern.split('.*')
        escaped_parts = [re.escape(part) for part in parts]
        result = '.*'.join(escaped_parts)
        
        # Now handle other wildcards
        result = result.replace('\\*\\*', '.*')
        result = result.replace('\\*', '[^.]+')
        result = re.sub(r'\\{([^}]+)\\}', lambda m: f"({m.group(1).replace(',', '|')})", result)
        return f'^{result}$'
    # Core ltree operators implementation
    def ltree_match(self, path: str, query: str) -> bool:
        """
        Check if path matches ltree query using ~ operator.
        
        Args:
            path: The path to test
            query: The ltree query pattern
        """
        try:
            
            regex_pattern = self._convert_ltree_query_to_regex(query)
            return bool(re.match(regex_pattern, path))
        except Exception:
            return False
    
    def ltxtquery_match(self, path: str, ltxtquery: str) -> bool:
        """
        Check if path matches ltxtquery using @@ operator.
        ltxtquery supports word-based matching with boolean operators.
        
        Args:
            path: The path to test
            ltxtquery: The ltxtquery expression (e.g., "word1 & word2", "word1 | word2")
        """
        # Split path into words (labels)
        path_words = set(path.split('.'))
        
        # Simple ltxtquery parser - supports &, |, !, ()
        query = ltxtquery.strip()
        
        # Handle simple cases first
        if '&' not in query and '|' not in query and '!' not in query:
            # Single word query
            return query.strip() in path_words
        
        # Replace logical operators with Python equivalents
        # This is a simplified implementation
        query = query.replace('&', ' and ')
        query = query.replace('|', ' or ')
        query = query.replace('!', ' not ')
        
        # Replace words with boolean checks
        for word in re.findall(r'\b\w+\b', ltxtquery):
            if word not in ['and', 'or', 'not']:
                query = query.replace(word, f"'{word}' in path_words")
        
        try:
            return eval(query)
        except:
            return False
    
    def ltree_ancestor(self, ancestor: str, descendant: str) -> bool:
        """
        Check if ancestor @> descendant (ancestor-of relationship).
        
        Args:
            ancestor: The potential ancestor path
            descendant: The potential descendant path
        """
        if ancestor == descendant:
            return False
        return descendant.startswith(ancestor + '.')
    
    def ltree_descendant(self, descendant: str, ancestor: str) -> bool:
        """
        Check if descendant <@ ancestor (descendant-of relationship).
        
        Args:
            descendant: The potential descendant path
            ancestor: The potential ancestor path
        """
        return self.ltree_ancestor(ancestor, descendant)
    
    def ltree_ancestor_or_equal(self, ancestor: str, descendant: str) -> bool:
        """
        Check if ancestor @> descendant or ancestor = descendant.
        Equivalent to PostgreSQL's path1 @> path2 when including equality.
        """
        return ancestor == descendant or self.ltree_ancestor(ancestor, descendant)
    
    def ltree_descendant_or_equal(self, descendant: str, ancestor: str) -> bool:
        """
        Check if descendant <@ ancestor or descendant = ancestor.
        Equivalent to PostgreSQL's path1 <@ path2 when including equality.
        """
        return descendant == ancestor or self.ltree_descendant(descendant, ancestor)
    
    def ltree_concatenate(self, path1: str, path2: str) -> str:
        """
        Concatenate two ltree paths using || operator.
        
        Args:
            path1: First path
            path2: Second path
            
        Returns:
            Concatenated path
        """
        if not path1:
            return path2
        if not path2:
            return path1
        return f"{path1}.{path2}"
    
    def nlevel(self, path: str) -> int:
        """Return the number of labels in the path (ltree nlevel function)."""
        return len(path.split('.'))
    
    def subltree(self, path: str, start: int, end: int) -> str:
        """
        Extract a subtree from start to end position (ltree subltree function).
        
        Args:
            path: The source path
            start: Starting position (0-based)
            end: Ending position (exclusive)
        """
        labels = path.split('.')
        return '.'.join(labels[start:end])
    
    def subpath_func(self, path: str, offset: int, length: Optional[int] = None) -> str:
        """Extract subpath (ltree subpath function)."""
        return self._subpath(path, offset, length)
    
    def index_func(self, path: str, subpath: str, offset: int = 0) -> int:
        """
        Find the position of subpath in path (ltree index function).
        Returns -1 if not found.
        """
        labels = path.split('.')
        sub_labels = subpath.split('.')
        
        for i in range(offset, len(labels) - len(sub_labels) + 1):
            if labels[i:i + len(sub_labels)] == sub_labels:
                return i
        return -1
    
    def text2ltree(self, text: str) -> str:
        """Convert text to ltree format (basic validation and normalization)."""
        if self.validate_path(text):
            return text
        raise ValueError(f"Cannot convert '{text}' to valid ltree format")
    
    def ltree2text(self, ltree_path: str) -> str:
        """Convert ltree to text (identity function for valid paths)."""
        return ltree_path
    
    def lca(self, *paths: str) -> Optional[str]:
        """
        Find the longest common ancestor of multiple paths (ltree lca function).
        
        Args:
            *paths: Variable number of paths
            
        Returns:
            The longest common ancestor path, or None if no common ancestor
        """
        if not paths:
            return None
        
        if len(paths) == 1:
            return paths[0]
        
        # Split all paths into labels
        all_labels = [path.split('.') for path in paths]
        
        # Find the minimum length
        min_length = min(len(labels) for labels in all_labels)
        
        # Find common prefix
        common_labels = []
        for i in range(min_length):
            current_label = all_labels[0][i]
            if all(labels[i] == current_label for labels in all_labels):
                common_labels.append(current_label)
            else:
                break
        
        return '.'.join(common_labels) if common_labels else None
    
    
    
    def get(self, path: str) -> Optional[Any]:
        """Retrieve data from a specific path."""
        if not self.validate_path(path):
            raise ValueError(f"Invalid ltree path: {path}")
        
        node = self.data.get(path)
        return copy.deepcopy(node.data) if node else None
    
    def get_node(self, path: str) -> Optional[TreeNode]:
        """Retrieve the full node (with metadata) from a specific path."""
        if not self.validate_path(path):
            raise ValueError(f"Invalid ltree path: {path}")
        
        node = self.data.get(path)
        return copy.deepcopy(node) if node else None
    
    # Advanced querying with full ltree support
    def query(self, pattern: str) -> List[Dict[str, Any]]:
        """Query using ltree pattern matching (~)."""
        results = []
        
        for path, node in self.data.items():
            if self.ltree_match(path, pattern):
                results.append(node)
        
        results.sort(key=lambda x: x['ltree_name'])
        return results
    
    def query_ltxtquery(self, ltxtquery: str) -> List[Dict[str, Any]]:
        """Query using ltxtquery pattern matching (@@)."""
        results = []
        for path, node in self.data.items():
            if self.ltxtquery_match(path, ltxtquery):
                results.append(node)
        results.sort(key=lambda x: x['ltree_name'])
        return results
    
    def query_by_operator(self, operator: str, path1: str, path2: str = None) -> List[Dict[str, Any]]:
        """
        Query using specific ltree operators.
        
        Args:
            operator: '@>', '<@', '~', '@@', '||'
            path1: First operand (for @>, <@ this is the reference path)
            path2: Second operand (for operators that need it)
        """
        
        results = []
        
        if operator == '@>':  # ancestor-of
            for path, node in self.data.items():
                if self.ltree_ancestor(path1, path):
                    results.append(node)
        
        elif operator == '<@':  # descendant-of  
            for path, node in self.data.items():
                if self.ltree_descendant(path, path1):
                    results.append(node)
        
        elif operator == '~':  # lquery match
            
            return self.query(path1)
        
        elif operator == '@@':  # ltxtquery match
            return self.query_ltxtquery(path1)
        
        results.sort(key=lambda x: x['ltree_name'])
        return results
    
    def query_ancestors(self, path: str) -> List[Dict[str, Any]]:
        """Get all ancestors using @> operator."""
        if not self.validate_path(path):
            raise ValueError(f"Invalid ltree path: {path}")
        
        results = []
        for stored_path, node in self.data.items():
            if self.ltree_ancestor(stored_path, path):
                results.append(node)
        
        results.sort(key=lambda x: len(x['ltree_name'].split('.')))
        return results
    
    def query_descendants(self, path: str) -> List[Dict[str, Any]]:
        """Get all descendants using <@ operator."""
        if not self.validate_path(path):
            raise ValueError(f"Invalid ltree path: {path}")
        
        results = []
        for stored_path, node in self.data.items():
            if self.ltree_descendant(stored_path, path):
                results.append(node)
        
        results.sort(key=lambda x: x['ltree_name'])
        
        return results
    
    def query_subtree(self, path: str) -> List[Dict[str, Any]]:
        """Get node and all its descendants."""
        results = []
        
        # Add the node itself if it exists
        if self.exists(path):
            node = self.data[path]
            results.append(node)
        
        # Add all descendants
        results.extend(self.query_descendants(path))
        results.sort(key=lambda x: x['ltree_name'])
        return results
    
    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        return path in self.data and self.validate_path(path)
    '''
    should not be used
    def delete(self, path: str) -> bool:
        """Delete a specific node."""
        if path in self.data:
            del self.data[path]
            return True
        return False
    
    def add_subtree(self, path: str,subtree: List[Dict[str, Any]]):
        """Add a subtree to a specific path."""
        if not self.validate_path(path):
            raise ValueError(f"Invalid ltree path: {path}")
        if self.exists(path) == False:
            raise ValueError(f"Path {path} does not exist")

        for node in subtree:
            self.store(path + '.' + node['path'], node['data'])
        return  True
    
    def delete_subtree(self, path: str) -> int:
        """Delete a node and all its descendants."""
        to_delete = [path] if path in self.data else []
        
        # Find all descendants
        for stored_path in self.data.keys():
            if self.ltree_descendant(stored_path, path):
                to_delete.append(stored_path)
        
        # Delete them
        for delete_path in to_delete:
            if delete_path in self.data:
                del self.data[delete_path]
        
        return len(to_delete)
    '''