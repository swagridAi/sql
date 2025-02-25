# sql_converter/utils/config.py
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
from dotenv import load_dotenv

class ConfigManager:
    def __init__(self):
        self.config: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)
        load_dotenv()  # Load environment variables
        
        # Default search paths for config files
        self.config_paths = [
            Path("sql_converter/config/default.yml"),
            Path(os.getenv("SQL_CONVERTER_CONFIG", "")),
            Path("~/.config/sql_converter/config.yml").expanduser(),
            Path("./sql_converter.yml"),
        ]

    def load_config(self) -> None:
        """Load configuration from first found valid config file"""
        for path in self.config_paths:
            if path and path.exists() and path.is_file():  # Add is_file() check
                try:
                    with open(path, 'r') as f:
                        self.config = yaml.safe_load(f)
                    self.logger.info(f"Loaded config from {path}")
                    return
                except Exception as e:
                    self.logger.warning(f"Failed to load config from {path}: {str(e)}")
        
        self.logger.warning("No valid config file found, using defaults")
        self.config = {
            'converters': ['cte'],
            'logging': {'level': 'INFO', 'file': 'conversions.log'}
        }

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """Get config value using dot notation (e.g. 'logging.level')"""
        keys = key.split('.')
        value = self.config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    def update_from_cli(self, cli_args: Dict[str, Any]) -> None:
        """Merge CLI arguments into config"""
        if 'convert' in cli_args:
            self.config['converters'] = cli_args['convert']
        if 'input' in cli_args:
            self.config['input_path'] = cli_args['input']
        if 'output' in cli_args:
            self.config['output_path'] = cli_args['output']