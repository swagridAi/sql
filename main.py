from pathlib import Path
import logging
import argparse
from typing import Optional, Dict, Any
import sys

from sql_transformer.utils.config_loader import load_config, get_enabled_transformations
from sql_transformer.core.pipeline import TransformationPipeline
from sql_transformer.transformers import (
    TempTableToCteTransformer,
    TableRenamerTransformer,
    PivotConverterTransformer
)

# Constants
DEFAULT_CONFIG_PATH = Path("config/config.yaml")
REQUIRED_CONFIG_KEYS = ['input_dir', 'output_dir']

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_directory(prompt: str, default: Optional[Path] = None) -> Path:
    """Interactive directory prompt with validation"""
    while True:
        path_str = input(f"{prompt} [{str(default) if default else 'required'}]: ") or str(default)
        if not path_str:
            continue
        path = Path(path_str)
        if path.exists():
            return path
        logger.error(f"Path does not exist: {path}")

def validate_and_prompt(config: Dict[str, Any], required_keys: list) -> Dict[str, Any]:
    """Ensure required config values exist, prompt if missing"""
    for key in required_keys:
        if not config.get(key):
            config[key] = get_directory(f"Enter {key.replace('_', ' ')}", 
                                      Path(config.get(key, ''))).as_posix()
    return config

def configure_cli() -> argparse.ArgumentParser:
    """Configure command line interface with adaptive requirements"""
    parser = argparse.ArgumentParser(
        description="SQL Transformation Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('-c', '--config', default=DEFAULT_CONFIG_PATH,
                      help="Path to configuration file")
    parser.add_argument('-i', '--input', 
                      help="Input path (file or directory)")
    parser.add_argument('-o', '--output',
                      help="Output directory path")
    parser.add_argument('-r', '--recursive', action='store_true',
                      help="Process SQL files recursively in subdirectories")
    parser.add_argument('-p', '--preserve-structure', action='store_true',
                      help="Maintain original directory structure in output")
    parser.add_argument('-f', '--overwrite', action='store_true',
                      help="Overwrite existing files in output directory")
    
    return parser

def load_adaptive_config(args: argparse.Namespace) -> Dict[str, Any]:
    """Load config with fallback to defaults and prompts"""
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        if args.config == DEFAULT_CONFIG_PATH:
            logger.warning(f"Default config not found at {DEFAULT_CONFIG_PATH}")
            config = {}
        else:
            logger.error(f"Specified config file not found: {args.config}")
            sys.exit(1)
    
    # Merge CLI args into config with priority
    if args.input: config['input_dir'] = args.input
    if args.output: config['output_dir'] = args.output
    if args.recursive: config['recursive'] = True
    if args.preserve_structure: config['preserve_structure'] = True
    if args.overwrite: config['overwrite'] = True
    
    # Validate and prompt for missing required values
    return validate_and_prompt(config, REQUIRED_CONFIG_KEYS)


def configure_pipeline(config_path: str) -> TransformationPipeline:
    """Create transformation pipeline from config"""
    config = load_config(config_path)
    pipeline = TransformationPipeline()
    
    for transformation in get_enabled_transformations(config):
        match transformation['name']:
            case 'temp_table_to_cte':
                pipeline.add_transformer(
                    TempTableToCteTransformer(**transformation['params'])
                )
            case 'table_renamer':
                pipeline.add_transformer(
                    TableRenamerTransformer(**transformation['params'])
                )
            case 'pivot_converter':
                pipeline.add_transformer(
                    PivotConverterTransformer(**transformation['params'])
                )
    
    return pipeline

class SqlFileProcessor:
    """Handles file system operations for SQL transformations"""
    
    def __init__(self, pipeline: TransformationPipeline, preserve_structure: bool = True):
        self.pipeline = pipeline
        self.preserve_structure = preserve_structure
        self.logger = logging.getLogger('FileProcessor')

    def process_file(self, input_file: Path, output_dir: Path):
        """Process a single SQL file"""
        try:
            self.logger.info(f"Processing file: {input_file}")
            with open(input_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            converted = self.pipeline.process(sql_content)
            output_file = output_dir / input_file.name
            
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(converted)
                
        except Exception as e:
            self.logger.error(f"Error processing {input_file}: {str(e)}")
            raise

    def process_directory(self, input_dir: Path, output_dir: Path, 
                        recursive: bool = False, overwrite: bool = False):
        """Process directory of SQL files"""
        input_dir = input_dir.resolve()
        output_dir = output_dir.resolve()

        self.logger.info(f"Processing directory: {input_dir} -> {output_dir}")
        self.logger.info(f"Recursive: {recursive}, Preserve structure: {self.preserve_structure}")

        pattern = "**/*.sql" if recursive else "*.sql"
        
        for sql_file in input_dir.glob(pattern):
            if not sql_file.is_file():
                continue

            # Create output path
            if self.preserve_structure:
                relative_path = sql_file.relative_to(input_dir)
                output_file = output_dir / relative_path
            else:
                output_file = output_dir / sql_file.name

            # Skip existing files unless overwrite
            if output_file.exists() and not overwrite:
                self.logger.debug(f"Skipping existing file: {output_file}")
                continue

            try:
                self.process_file(sql_file, output_file.parent)
            except Exception as e:
                self.logger.error(f"Failed to process {sql_file}: {str(e)}")
                if not overwrite:
                    raise

def main():
    """Main entry point"""
    # Parse command line arguments
    parser = configure_cli()
    args = parser.parse_args()
    
    # Verify input path
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input path does not exist: {input_path}")
        if input_path.is_file():
            input_path = get_directory("Enter input file directory", input_path.parent)
        else:
            input_path = get_directory("Enter input directory", input_path)

    # Create output directory if needed
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize pipeline and processor
    pipeline = configure_pipeline(args.config)
    processor = SqlFileProcessor(
        pipeline=pipeline,
        preserve_structure=args.preserve_structure
    )

    # Process files
    try:
        if input_path.is_file():
            processor.process_file(input_path, output_dir)
        else:
            processor.process_directory(
                input_dir=input_path,
                output_dir=output_dir,
                recursive=args.recursive,
                overwrite=args.overwrite
            )
        logger.info("Processing completed successfully")
    except Exception as e:
        logger.error(f"Fatal error during processing: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()