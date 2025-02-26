"""
Enhanced logging configuration for SQL Converter with AST-based parsing support.

This module provides a more comprehensive logging system supporting
additional diagnostic information about AST operations.
"""
import logging
import sys
from typing import Optional, Dict, Any, List


class SQLNodeAdapter(logging.LoggerAdapter):
    """
    Adapter to include AST node information in logging messages.
    This enriches log messages with AST context when available.
    """
    
    def process(self, msg, kwargs):
        """Process the log message to include AST node information."""
        extra = kwargs.get('extra', {})
        node_info = ""
        
        # Extract node information if available
        if 'node_type' in extra:
            node_info = f" [Node: {extra['node_type']}"
            if 'node_name' in extra:
                node_info += f", {extra['node_name']}"
            node_info += "]"
            
        # Extract operation information if available
        operation_info = ""
        if 'operation' in extra:
            operation_info = f" [Operation: {extra['operation']}]"
            
        # Combine all information
        enhanced_msg = f"{msg}{node_info}{operation_info}"
        
        return enhanced_msg, kwargs


def setup_logging(
    level: str = 'INFO', 
    log_file: Optional[str] = None,
    log_format: Optional[str] = None,
    console: bool = True
) -> None:
    """
    Configure the logging system with enhanced AST-related options.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (None for console only)
        log_format: Custom log format string
        console: Whether to output logs to console
    """
    # Convert level string to logging constant
    try:
        numeric_level = getattr(logging, level.upper(), logging.INFO)
    except AttributeError:
        print(f"Invalid logging level: {level}, defaulting to INFO")
        numeric_level = logging.INFO
    
    # Set default format if not provided
    if not log_format:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create and add handlers
    handlers = []
    
    # Add console handler if requested
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(console_handler)
    
    # Add file handler if requested
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(numeric_level)
            file_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(file_handler)
        except Exception as e:
            print(f"Error setting up log file: {e}")
            # Fall back to console logging
            if not console:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(numeric_level)
                console_handler.setFormatter(logging.Formatter(log_format))
                handlers.append(console_handler)
    
    # Add all handlers to root logger
    for handler in handlers:
        root_logger.addHandler(handler)
    
    # Log the setup
    logging.info(f"Logging initialized. Level: {level}, File: {log_file if log_file else 'None'}")


def get_ast_logger(name: str) -> logging.LoggerAdapter:
    """
    Get a logger adapter for AST operations.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        LoggerAdapter with AST node context support
    """
    logger = logging.getLogger(name)
    return SQLNodeAdapter(logger, {})


def log_ast_operation(
    logger: logging.LoggerAdapter,
    message: str,
    operation: str,
    node_type: Optional[str] = None,
    node_name: Optional[str] = None,
    level: str = 'DEBUG',
    **extra
):
    """
    Log an AST operation with relevant context.
    
    Args:
        logger: Logger adapter to use
        message: Log message
        operation: Operation being performed
        node_type: Type of AST node involved
        node_name: Name or identifier of the node
        level: Logging level
        **extra: Additional context information
    """
    # Combine context
    context = {
        'operation': operation,
        **extra
    }
    
    if node_type:
        context['node_type'] = node_type
    if node_name:
        context['node_name'] = node_name
    
    # Log with appropriate level
    level_method = getattr(logger, level.lower(), logger.debug)
    level_method(message, extra=context)


def log_ast_transformation(
    logger: logging.LoggerAdapter,
    source_type: str,
    target_type: str,
    transformation: str,
    success: bool,
    details: Optional[Dict[str, Any]] = None
):
    """
    Log an AST transformation operation.
    
    Args:
        logger: Logger adapter to use
        source_type: Source node type
        target_type: Target node type
        transformation: Transformation being performed
        success: Whether the transformation succeeded
        details: Additional details about the transformation
    """
    status = "succeeded" if success else "failed"
    message = f"Transformation {transformation} {status}: {source_type} → {target_type}"
    
    level = "INFO" if success else "WARNING"
    
    # Log with transformation details
    log_ast_operation(
        logger,
        message,
        operation=transformation,
        node_type=source_type,
        level=level,
        target_type=target_type,
        details=details or {}
    )


def format_node_path(path: List[Any]) -> str:
    """
    Format a node path for logging.
    
    Args:
        path: List of nodes in the path
        
    Returns:
        Formatted string representation of the path
    """
    if not path:
        return "[]"
    
    formatted = []
    for node in path:
        try:
            node_type = type(node).__name__
            formatted.append(node_type)
        except:
            formatted.append("Unknown")
    
    return " → ".join(formatted)