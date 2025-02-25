# File: sql-query-converter/sql_converter/converters/base.py
from abc import ABC, abstractmethod
from typing import Dict, Any
import logging

class BaseConverter(ABC):

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def convert(self, sql: str) -> str:
        """Convert SQL using this converter's logic"""
        pass

__all__ = ['BaseConverter']  # Add this at the bottom