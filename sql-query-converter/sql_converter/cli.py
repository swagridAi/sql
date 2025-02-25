import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
import traceback

from sql_converter.converters import get_converter
from sql_converter.utils.config import ConfigManager
from sql_converter.utils.logging import setup_logging
from sql_converter.converters.base import BaseConverter
from sql_converter.exceptions import (
    SQLConverterError, ConfigError, ValidationError, 
    SQLSyntaxError, FileError, ConverterError
)


class SQLConverterApp:
    """
    Main application for SQL conversion, handling workflow orchestration.
    """
    
    def __init__(self, converters: Dict[str, BaseConverter], config: Dict[str, Any]):
        """
        Initialize the SQL Converter Application.
        
        Args:
            converters: Dictionary of converter name to converter instance
            config: Configuration dictionary
            
        Raises:
            ConfigError: When initialization fails due to config issues
        """
        if not converters:
            raise ConfigError("No converters provided")
            
        self.converters = converters
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Track processed files for reporting
        self.processed_files: Set[Path] = set()
        self.failed_files: Set[Tuple[Path, str]] = set()  # (path, error_message)

    def process_file(self, input_path: Path, output_path: Path, conversions: List[str]) -> None:
        """
        Process a single SQL file.
        
        Args:
            input_path: Path to input SQL file
            output_path: Path to output SQL file
            conversions: List of converter names to apply
            
        Raises:
            FileError: When file operations fail
            ConverterError: When conversion fails
            ValidationError: When SQL validation fails
        """
        if not input_path.exists():
            raise FileError(f"Input file does not exist", filepath=str(input_path))
            
        if not input_path.is_file():
            raise FileError(f"Input path is not a file", filepath=str(input_path))
            
        # Check file access
        if not os.access(input_path, os.R_OK):
            raise FileError(f"No read permission for input file", filepath=str(input_path))
            
        # Ensure output directory exists and is writable
        output_dir = output_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if output_dir.exists() and not os.access(output_dir, os.W_OK):
            raise FileError(f"No write permission for output directory", 
                           filepath=str(output_dir))
            
        try:
            self.logger.info(f"Processing file: {input_path}")
            
            # Read input file with proper error handling
            try:
                sql = input_path.read_text(encoding='utf-8')
            except UnicodeDecodeError:
                # Try with a different encoding
                try:
                    sql = input_path.read_text(encoding='latin-1')
                    self.logger.warning(f"File {input_path} was not UTF-8, read as Latin-1")
                except Exception as e:
                    raise FileError(f"Failed to read file: {str(e)}", 
                                   filepath=str(input_path)) from e
            except Exception as e:
                raise FileError(f"Failed to read file: {str(e)}", 
                               filepath=str(input_path)) from e

            # Apply conversions
            converted_sql = sql
            for conversion in conversions:
                if conversion not in self.converters:
                    raise ConverterError(f"Unknown converter: {conversion}")
                
                converter = self.converters[conversion]
                self.logger.debug(f"Applying converter '{conversion}' to {input_path}")
                
                # Apply the conversion with proper error handling
                try:
                    converted_sql = converter.convert(converted_sql)
                except Exception as e:
                    # Preserve error type if it's a known one, otherwise wrap
                    if isinstance(e, (SQLSyntaxError, ValidationError, ConverterError)):
                        raise
                    raise ConverterError(
                        f"Error in {conversion} converter: {str(e)}",
                        source=input_path.name
                    ) from e

            # Write output file with proper error handling
            try:
                output_path.write_text(converted_sql, encoding='utf-8')
                self.logger.info(f"Saved converted SQL to: {output_path}")
                self.processed_files.add(input_path)
            except Exception as e:
                raise FileError(f"Failed to write output file: {str(e)}", 
                               filepath=str(output_path)) from e

        except Exception as e:
            self.logger.error(f"Failed to process {input_path}: {str(e)}")
            self.failed_files.add((input_path, str(e)))
            raise

    def process_directory(self, input_dir: Path, output_dir: Path, conversions: List[str]) -> None:
        """
        Process all SQL files in a directory, preserving the directory structure.
        
        Args:
            input_dir: Directory containing SQL files to process
            output_dir: Directory to write converted SQL files to
            conversions: List of converter names to apply
            
        Raises:
            FileError: When directory operations fail
        """
        if not input_dir.exists():
            raise FileError(f"Input directory does not exist", filepath=str(input_dir))
            
        if not input_dir.is_dir():
            raise FileError(f"Input path is not a directory", filepath=str(input_dir))
            
        # Process sql files while preserving directory structure
        try:
            for input_path in input_dir.glob("**/*.sql"):
                if input_path.is_file():
                    # Calculate relative path to preserve directory structure
                    relative_path = input_path.relative_to(input_dir)
                    output_path = output_dir / relative_path
                    
                    try:
                        self.process_file(input_path, output_path, conversions)
                    except Exception as e:
                        self.logger.error(f"Error processing {input_path}: {e}")
                        # Continue processing other files
                        continue
                        
            # Check if we processed any files
            if not self.processed_files and not self.failed_files:
                self.logger.warning(f"No SQL files found in {input_dir}")
                
        except Exception as e:
            if isinstance(e, FileError):
                raise
            raise FileError(f"Error processing directory: {str(e)}", 
                           filepath=str(input_dir)) from e

    def get_summary(self) -> Dict[str, Any]:
        """
        Generate a summary of processing results.
        
        Returns:
            Dictionary with processing statistics
        """
        return {
            'processed_files': len(self.processed_files),
            'failed_files': len(self.failed_files),
            'success_rate': (
                len(self.processed_files) / 
                (len(self.processed_files) + len(self.failed_files))
                if (len(self.processed_files) + len(self.failed_files)) > 0 
                else 0
            ) * 100,
            'failures': [
                {'file': str(path), 'error': error} 
                for path, error in self.failed_files
            ]
        }


def main():
    """
    Main entry point for SQL converter CLI application.
    
    This function parses command-line arguments, initializes the application,
    and orchestrates the conversion process with comprehensive error handling.
    """
    # Initialize base logging before config
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger()
    
    try:
        # Initialize configuration
        config_manager = ConfigManager()
        
        try:
            config_manager.load_config()
        except ConfigError as e:
            logger.error(f"Configuration error: {e}")
            sys.exit(1)
            
        # Validate configuration
        validation_errors = config_manager.validate_config()
        if validation_errors:
            logger.warning("Configuration validation issues:")
            for error in validation_errors:
                logger.warning(f"  - {error}")

        # Setup logging with config
        try:
            setup_logging(
                level=config_manager.get('logging.level', 'INFO'),
                log_file=config_manager.get('logging.file')
            )
        except Exception as e:
            logger.error(f"Failed to configure logging: {e}")
            # Continue with basic logging

        # Parse command line arguments with better error handling
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
        
        # Get available converters before adding CLI arguments
        try:
            available_converters = [name for name in config_manager.get('converters', ['cte'])]
            
            parser.add_argument(
                '-c', '--convert',
                nargs='+',
                choices=available_converters,
                default=available_converters,
                help='Conversion operations to apply'
            )
        except Exception as e:
            logger.error(f"Failed to initialize converters list: {e}")
            available_converters = ['cte']  # Fallback
            
            parser.add_argument(
                '-c', '--convert',
                nargs='+',
                default=['cte'],
                help='Conversion operations to apply (failed to load converter list)'
            )

        # Add verbosity control
        parser.add_argument(
            '-v', '--verbose',
            action='store_true',
            help='Enable verbose output'
        )
        
        # Parse arguments
        try:
            args = parser.parse_args()
        except Exception as e:
            logger.error(f"Argument parsing error: {e}")
            parser.print_help()
            sys.exit(1)

        # Update verbosity
        if args.verbose:
            logger.setLevel(logging.DEBUG)
            for handler in logger.handlers:
                handler.setLevel(logging.DEBUG)

        # Update config with CLI arguments
        config_manager.update_from_cli(vars(args))

        # Initialize converters with config
        try:
            converters = {
                name: get_converter(name, config_manager.get(f"{name}_converter", {}))
                for name in config_manager.get('converters', ['cte'])
            }
        except Exception as e:
            logger.error(f"Failed to initialize converters: {e}")
            sys.exit(1)

        # Initialize application
        try:
            app = SQLConverterApp(converters, config_manager.config)
        except ConfigError as e:
            logger.error(f"Application initialization error: {e}")
            sys.exit(1)

        # Process input
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
                raise FileError(f"Invalid input path: {input_path}")
                
            # Print summary
            summary = app.get_summary()
            logger.info(f"Processing complete: {summary['processed_files']} files processed, "
                      f"{summary['failed_files']} files failed "
                      f"({summary['success_rate']:.1f}% success rate)")
                      
            if summary['failed_files'] > 0:
                logger.warning("Failed files:")
                for failure in summary['failures'][:5]:  # Show the first 5 failures
                    logger.warning(f"  {failure['file']}: {failure['error']}")
                    
                if len(summary['failures']) > 5:
                    logger.warning(f"  ... and {len(summary['failures']) - 5} more failures")
                    
                # Exit with error code if there were failures
                sys.exit(1)

        except FileError as e:
            logger.error(f"File error: {e}")
            sys.exit(1)
        except ConverterError as e:
            logger.error(f"Converter error: {e}")
            sys.exit(1)
        except SQLSyntaxError as e:
            logger.error(f"SQL syntax error: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if args.verbose:
                logger.error(traceback.format_exc())
            sys.exit(1)

    except Exception as e:
        # Last resort error handling
        logger.error(f"Critical error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()