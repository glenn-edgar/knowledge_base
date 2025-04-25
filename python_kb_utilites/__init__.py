"""
Python Knowledge Base Utilities

This package provides utilities for working with PostgreSQL databases, particularly
for knowledge base operations.
"""

from .sql_script import PostgresConnector
from .dump_kb import select_and_print_table

__all__ = ['PostgresConnector', 'select_and_print_table'] 