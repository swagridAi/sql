# sql_converter/utils/logging.py
import logging
from typing import Optional

def setup_logging(level: str = 'INFO', log_file: Optional[str] = None) -> None:
    """Configure logging system"""
    level = getattr(logging, level.upper(), logging.INFO)
    
    handlers = [
        logging.StreamHandler()
    ]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )