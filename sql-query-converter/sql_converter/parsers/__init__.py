"""
Parser package for SQL Converter with AST-based parsing.

This package provides SQL parsing capabilities using Abstract Syntax Trees (AST)
for more robust and accurate SQL transformations.
"""
from typing import Dict, Any, Optional, List, Type

# Import the main parser class
from sql_converter.parsers.sql_parser import SQLParser

# Map of supported dialects
# This maps our dialect names to internal implementation classes or modules
SUPPORTED_DIALECTS = {
    'ansi': 'ansi',
    'tsql': 'tsql',
    'mysql': 'mysql',
    'postgresql': 'postgres',
    'oracle': 'oracle',
    'snowflake': 'snowflake',
    'redshift': 'redshift',
    'spark': 'spark',
    'bigquery': 'bigquery',
}

# Default dialect to use if not specified
DEFAULT_DIALECT = 'ansi'


def create_parser(
    dialect: str = DEFAULT_DIALECT, 
    config: Optional[Dict[str, Any]] = None
) -> SQLParser:
    """
    Create a SQLParser instance for the specified dialect.
    
    Args:
        dialect: SQL dialect to use ('ansi', 'tsql', 'mysql', etc.)
        config: Additional configuration options for the parser
        
    Returns:
        SQLParser instance configured for the dialect
        
    Raises:
        ValueError: If the dialect is not supported
    """
    if dialect not in SUPPORTED_DIALECTS:
        # Fall back to default dialect with warning
        import logging
        logging.getLogger(__name__).warning(
            f"Unsupported dialect '{dialect}', falling back to '{DEFAULT_DIALECT}'"
        )
        dialect = DEFAULT_DIALECT
    
    # Create and return a parser instance
    return SQLParser(dialect=dialect, config=config)


def get_supported_dialects() -> List[str]:
    """
    Get a list of supported SQL dialects.
    
    Returns:
        List of dialect names supported by the parser
    """
    return list(SUPPORTED_DIALECTS.keys())


def is_dialect_supported(dialect: str) -> bool:
    """
    Check if a SQL dialect is supported.
    
    Args:
        dialect: Dialect name to check
        
    Returns:
        True if the dialect is supported, False otherwise
    """
    return dialect in SUPPORTED_DIALECTS


# Parse SQL string into AST
def parse_sql(sql: str, dialect: str = DEFAULT_DIALECT, **options) -> List[Any]:
    """
    Parse SQL string into AST expressions.
    
    This is a convenience function that creates a parser and parses the SQL.
    
    Args:
        sql: SQL string to parse
        dialect: SQL dialect to use
        **options: Additional parsing options
        
    Returns:
        List of AST expressions
        
    Raises:
        SQLSyntaxError: When SQL contains syntax errors
        ParserError: When parsing fails
    """
    parser = create_parser(dialect=dialect)
    return parser.parse(sql)


# Export public interface
__all__ = [
    'SQLParser',
    'create_parser',
    'get_supported_dialects',
    'is_dialect_supported',
    'parse_sql',
    'SUPPORTED_DIALECTS',
    'DEFAULT_DIALECT',
]