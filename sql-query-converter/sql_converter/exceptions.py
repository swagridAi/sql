"""
Custom exception classes for SQL Converter.

This module contains all exception types used throughout the SQL Converter application,
providing a consistent error handling approach and meaningful error messages.
"""
from typing import Optional


class SQLConverterError(Exception):
    """Base exception for all SQL Converter errors."""
    
    def __init__(self, message: str, source: Optional[str] = None):
        self.source = source
        self.message = message
        super().__init__(f"{message} {f'[Source: {source}]' if source else ''}")


class ConfigError(SQLConverterError):
    """Raised when there's an issue with configuration."""
    pass


class ValidationError(SQLConverterError):
    """Raised when validation of inputs fails."""
    pass


class SQLSyntaxError(ValidationError):
    """Raised when SQL syntax is invalid."""
    
    def __init__(self, message: str, source: Optional[str] = None, 
                 position: Optional[int] = None, line: Optional[int] = None):
        self.position = position
        self.line = line
        location_info = ""
        if line is not None:
            location_info += f" at line {line}"
        if position is not None:
            location_info += f" at position {position}"
        
        super().__init__(f"SQL syntax error{location_info}: {message}", source)


class ParserError(SQLConverterError):
    """Raised when there's an error during SQL parsing."""
    pass


class ConverterError(SQLConverterError):
    """Raised when there's an error during SQL conversion."""
    pass


class FileError(SQLConverterError):
    """Raised when there's an issue with file operations."""
    
    def __init__(self, message: str, filepath: Optional[str] = None):
        self.filepath = filepath
        super().__init__(f"{message} {f'[File: {filepath}]' if filepath else ''}")


class PluginError(SQLConverterError):
    """Raised when there's an issue with a plugin or extension."""
    pass