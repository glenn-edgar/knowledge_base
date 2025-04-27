"""
Knowledge Base Management Module

This module provides functionality for constructing, querying, and managing a knowledge base
using PostgreSQL as the backend database.

Main Components:
- Construct_KB: Class for building and managing the knowledge base structure
- KB_Search: Class for querying and filtering knowledge base entries
- PostgresConnector: Class for managing PostgreSQL database connections
- select_and_print_table: Function for dumping knowledge base contents
"""

from .construct_kb import Construct_KB
from .kb_query_support import KB_Search

__all__ = [
    'Construct_KB',
    'KB_Search',
   
]

__version__ = '1.0.0' 