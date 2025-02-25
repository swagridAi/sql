# sql_converter/cli.py
import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List
from .converters import get_converter  # Replace existing converter imports
from .converters.cte import CTEConverter
from .utils.config import ConfigManager
from .utils.logging import setup_logging
from .converters.base import BaseConverter
from typing import Any

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
                converted_sql = converter.convert(converted_sql)

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


def main():
    # Initialize configuration
    config_manager = ConfigManager()
    config_manager.load_config()

    # Setup logging
    setup_logging(
        level=config_manager.get('logging.level', 'INFO'),
        log_file=config_manager.get('logging.file')
    )

    # Initialize converters with config
    converters = {
        name: get_converter(name, config_manager.get(f"{name}_converter", {}))
        for name in config_manager.get('converters', ['cte'])
    }

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='SQL Query Conversion Tool',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-i', '--input',
        type=Path,
        required=True,
        help='Input file or directory'
    )
    parser.add_argument(
        '-o', '--output',
        type=Path,
        required=True,
        help='Output file or directory'
    )
    parser.add_argument(
        '-c', '--convert',
        nargs='+',
        choices=list(converters.keys()),
        default=config_manager.get('converters', ['cte']),
        help='Conversion operations to apply'
    )

    args = parser.parse_args()

    # Update config with CLI arguments
    config_manager.update_from_cli(vars(args))

    # Initialize application
    app = SQLConverterApp(converters, config_manager.config)

    try:
        input_path = config_manager.get('input_path', args.input)
        output_path = config_manager.get('output_path', args.output)

        if input_path.is_file():
            if output_path.exists() and output_path.is_dir():
                output_path = output_path / input_path.name
            app.process_file(input_path, output_path, args.convert)
        elif input_path.is_dir():
            app.process_directory(input_path, output_path, args.convert)
        else:
            raise ValueError(f"Invalid input path: {input_path}")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()