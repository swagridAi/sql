# sql_converter/cli.py
import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any
from .converters import get_converter
from .utils.config import ConfigManager
from .utils.logging import setup_logging
from .converters.base import BaseConverter

class SQLConverterApp:
    def __init__(self, converters: Dict[str, BaseConverter], config: Dict[str, Any]):
        self.converters = converters
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def process_file(self, input_path: Path, output_path: Path, conversions: List[str]) -> None:
        """Process a single SQL file"""
        try:
            self.logger.info(f"Processing file: {input_path}")
            sql = input_path.read_text(encoding='utf-8')

            converted_sql = sql
            for conversion in conversions:
                if conversion not in self.converters:
                    raise ValueError(f"Unknown converter: {conversion}")
                
                converter = self.converters[conversion]
                try:
                    converted_sql = converter.convert(converted_sql)
                except ValueError as e:
                    self.logger.error(f"Validation error in {input_path}: {str(e)}")
                    raise ValueError(f"SQL validation failed: {str(e)}")

            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(converted_sql, encoding='utf-8')
            self.logger.info(f"Saved converted SQL to: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to process {input_path}: {str(e)}")
            raise

    def process_directory(self, input_dir: Path, output_dir: Path, conversions: List[str]) -> None:
        """Process all SQL files in a directory"""
        for input_path in input_dir.glob("**/*.sql"):
            if input_path.is_file():
                relative_path = input_path.relative_to(input_dir)
                output_path = output_dir / relative_path
                self.process_file(input_path, output_path, conversions)