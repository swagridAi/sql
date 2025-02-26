"""
Enhanced exception classes for SQL Converter with AST-based parsing support.

This module contains all exception types used throughout the SQL Converter application,
providing a consistent error handling approach with additional support for AST-based
parsing operations and more detailed error information.
"""
from typing import Optional, Any, Dict, List


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
    """
    Raised when SQL syntax is invalid, with enhanced AST information.
    
    Attributes:
        position: Character position where the error occurred
        line: Line number where the error occurred
        column: Column number where the error occurred
        ast_node: AST node where the error occurred (if available)
    """
    
    def __init__(
        self, 
        message: str, 
        source: Optional[str] = None, 
        position: Optional[int] = None, 
        line: Optional[int] = None,
        column: Optional[int] = None,
        ast_node: Optional[Any] = None
    ):
        self.position = position
        self.line = line
        self.column = column
        self.ast_node = ast_node
        
        # Build location information for the error message
        location_info = ""
        if line is not None:
            location_info += f" at line {line}"
            if column is not None:
                location_info += f", column {column}"
        elif position is not None:
            location_info += f" at position {position}"
        
        # Include AST node type if available
        node_info = ""
        if ast_node is not None:
            try:
                node_type = type(ast_node).__name__
                node_info = f" (node type: {node_type})"
            except:
                pass
        
        super().__init__(
            f"SQL syntax error{location_info}{node_info}: {message}", 
            source
        )


class ParserError(SQLConverterError):
    """
    Raised when there's an error during SQL parsing.
    
    Attributes:
        position: Character position where the error occurred
        line: Line number where the error occurred
        column: Column number where the error occurred
        token: Token information where the error occurred
    """
    
    def __init__(
        self, 
        message: str, 
        source: Optional[str] = None,
        position: Optional[int] = None,
        line: Optional[int] = None,
        column: Optional[int] = None,
        token: Optional[str] = None
    ):
        self.position = position
        self.line = line
        self.column = column
        self.token = token
        
        # Build location information
        location_info = ""
        if line is not None:
            location_info += f" at line {line}"
            if column is not None:
                location_info += f", column {column}"
        elif position is not None:
            location_info += f" at position {position}"
            
        # Include token information if available
        token_info = f" near '{token}'" if token else ""
        
        super().__init__(
            f"Parser error{location_info}{token_info}: {message}", 
            source
        )


class ASTError(SQLConverterError):
    """
    Raised when there's an error manipulating the SQL Abstract Syntax Tree.
    
    Attributes:
        node_type: Type of the AST node where the error occurred
        operation: Operation being performed when the error occurred
    """
    
    def __init__(
        self, 
        message: str, 
        source: Optional[str] = None,
        node_type: Optional[str] = None,
        operation: Optional[str] = None
    ):
        self.node_type = node_type
        self.operation = operation
        
        # Build context information
        context_info = ""
        if operation:
            context_info += f" during {operation}"
        if node_type:
            context_info += f" on {node_type} node"
            
        super().__init__(
            f"AST error{context_info}: {message}", 
            source
        )


class ConverterError(SQLConverterError):
    """
    Raised when there's an error during SQL conversion.
    
    Attributes:
        converter: Name of the converter that encountered the error
        stage: Conversion stage where the error occurred
        expressions: SQL expressions being converted (if applicable)
    """
    
    def __init__(
        self, 
        message: str, 
        source: Optional[str] = None,
        converter: Optional[str] = None,
        stage: Optional[str] = None,
        expressions: Optional[List[Any]] = None
    ):
        self.converter = converter
        self.stage = stage
        self.expressions = expressions
        
        # Build context information
        context_info = ""
        if converter:
            context_info += f" in {converter} converter"
        if stage:
            context_info += f" during {stage} stage"
            
        super().__init__(
            f"Converter error{context_info}: {message}", 
            source
        )


class FileError(SQLConverterError):
    """
    Raised when there's an issue with file operations.
    
    Attributes:
        filepath: Path to the file that caused the error
        operation: File operation that failed (read, write, etc.)
    """
    
    def __init__(
        self, 
        message: str, 
        filepath: Optional[str] = None,
        operation: Optional[str] = None
    ):
        self.filepath = filepath
        self.operation = operation
        
        # Build context information
        context_info = ""
        if operation:
            context_info += f" during {operation}"
        file_info = f" [File: {filepath}]" if filepath else ""
        
        super().__init__(f"{message}{context_info}{file_info}")


class TransformationError(SQLConverterError):
    """
    Raised when an AST transformation fails.
    
    Attributes:
        transformation: Name of the transformation that failed
        node_type: Type of the AST node being transformed
        details: Additional details about the transformation
    """
    
    def __init__(
        self, 
        message: str, 
        source: Optional[str] = None,
        transformation: Optional[str] = None,
        node_type: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.transformation = transformation
        self.node_type = node_type
        self.details = details or {}
        
        # Build context information
        context_info = ""
        if transformation:
            context_info += f" during {transformation} transformation"
        if node_type:
            context_info += f" of {node_type} node"
            
        super().__init__(
            f"Transformation error{context_info}: {message}", 
            source
        )


class CTEError(TransformationError):
    """
    Raised when there's an error specific to CTE transformations.
    
    Attributes:
        temp_table: Name of the temporary table involved
        cte_name: Name of the CTE being created
    """
    
    def __init__(
        self, 
        message: str, 
        source: Optional[str] = None,
        temp_table: Optional[str] = None,
        cte_name: Optional[str] = None,
        **kwargs
    ):
        self.temp_table = temp_table
        self.cte_name = cte_name
        
        # Add additional details
        details = kwargs.pop('details', {})
        if temp_table:
            details['temp_table'] = temp_table
        if cte_name:
            details['cte_name'] = cte_name
            
        # Build table information
        table_info = ""
        if temp_table:
            table_info += f" for temp table '{temp_table}'"
            if cte_name:
                table_info += f" (CTE: '{cte_name}')"
                
        super().__init__(
            f"{message}{table_info}", 
            source,
            transformation="CTE conversion",
            details=details,
            **kwargs
        )


class PluginError(SQLConverterError):
    """Raised when there's an issue with a plugin or extension."""
    pass


class CircularDependencyError(ValidationError):
    """
    Raised when a circular dependency is detected in temp table references.
    
    Attributes:
        tables: List of tables involved in the circular dependency
    """
    
    def __init__(
        self, 
        message: str, 
        source: Optional[str] = None,
        tables: Optional[List[str]] = None
    ):
        self.tables = tables or []
        
        # Format the cycle for display
        cycle_display = ""
        if tables:
            cycle_display = " -> ".join(tables)
            if len(tables) > 1:
                cycle_display += f" -> {tables[0]}"  # Complete the cycle
            cycle_display = f" ({cycle_display})"
            
        super().__init__(
            f"Circular dependency detected{cycle_display}: {message}", 
            source
        )


class DialectError(ParserError):
    """
    Raised when there's an issue specific to SQL dialect handling.
    
    Attributes:
        dialect: SQL dialect being used
        feature: SQL feature that caused the error
    """
    
    def __init__(
        self, 
        message: str, 
        source: Optional[str] = None,
        dialect: Optional[str] = None,
        feature: Optional[str] = None,
        **kwargs
    ):
        self.dialect = dialect
        self.feature = feature
        
        # Build dialect information
        dialect_info = ""
        if dialect:
            dialect_info += f" in {dialect} dialect"
        if feature:
            dialect_info += f" ({feature} feature)"
            
        super().__init__(
            f"Dialect error{dialect_info}: {message}", 
            source,
            **kwargs
        )