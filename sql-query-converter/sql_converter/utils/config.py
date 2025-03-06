import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
import yaml
from dotenv import load_dotenv

from sql_converter.exceptions import ConfigError


class ConfigManager:
    """
    Manages configuration from multiple sources with precedence rules.
    """
    
    def __init__(self):
        """Initialize the configuration manager."""
        self.config: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)
        
        # Try to load environment variables
        try:
            load_dotenv()  # Load environment variables
        except Exception as e:
            self.logger.warning(f"Failed to load environment variables: {str(e)}")
        
        # Default search paths for config files
        self.config_paths = [
            Path("sql_converter/config/default.yml"),
            Path(os.getenv("SQL_CONVERTER_CONFIG", "")),
            Path("~/.config/sql_converter/config.yml").expanduser(),
            Path("./sql_converter.yml"),
        ]

    def load_config(self) -> None:
        """
        Load configuration from first found valid config file.
        
        Raises:
            ConfigError: When config loading fails critically
        """
        loaded = False
        errors = []
        
        # Try each path in order
        for path in self.config_paths:
            if not path or not path.exists() or not path.is_file():
                continue
                
            try:
                with open(path, 'r') as f:
                    loaded_config = yaml.safe_load(f)
                    
                # Validate config structure
                if not isinstance(loaded_config, dict):
                    self.logger.warning(f"Invalid config format in {path}: not a dictionary")
                    errors.append(f"Config at {path} is not a dictionary")
                    continue
                
                self.config = loaded_config
                self.logger.info(f"Loaded config from {path}")
                loaded = True
                break
                
            except Exception as e:
                error_msg = f"Failed to load config from {path}: {str(e)}"
                self.logger.warning(error_msg)
                errors.append(error_msg)
        
        # If no config loaded, use defaults but raise warning
        if not loaded:
            self.logger.warning("No valid config file found, using defaults")
            self.config = {
                'converters': ['cte'],
                'logging': {'level': 'INFO', 'file': 'conversions.log'}
            }
            
            # If there were critical errors in configuration loading, raise exception
            if any("Permission denied" in err for err in errors):
                raise ConfigError(
                    "Cannot access configuration files due to permission issues",
                    "\n".join(errors)
                )

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Get config value using dot notation (e.g. 'logging.level').
        
        Args:
            key: Config key using dot notation
            default: Default value if key not found
            
        Returns:
            Config value or default
            
        Raises:
            ConfigError: When key is invalid
        """
        if not key:
            raise ConfigError("Empty configuration key provided")
            
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                if not isinstance(value, dict):
                    self.logger.debug(f"Config path '{key}' traversal failed at '{k}': not a dictionary")
                    return default
                value = value.get(k)
                if value is None:
                    return default
            return value
        except Exception as e:
            self.logger.debug(f"Error retrieving config value for '{key}': {str(e)}")
            return default

    def update_from_cli(self, cli_args: Dict[str, Any]) -> None:
        """
        Merge CLI arguments into config.
        
        Args:
            cli_args: CLI arguments dictionary
            
        Raises:
            ConfigError: When CLI arguments are invalid
        """
        if not isinstance(cli_args, dict):
            raise ConfigError(f"CLI arguments must be a dictionary, got {type(cli_args).__name__}")
            
        try:
            # Apply CLI arguments with proper validation
            if 'convert' in cli_args:
                converters = cli_args['convert']
                if not isinstance(converters, list):
                    raise ConfigError(f"'convert' must be a list, got {type(converters).__name__}")
                self.config['converters'] = converters
                
            if 'input' in cli_args:
                input_path = cli_args['input']
                if not isinstance(input_path, (str, Path)):
                    raise ConfigError(f"'input' must be a string or Path, got {type(input_path).__name__}")
                self.config['input_path'] = input_path
                
            if 'output' in cli_args:
                output_path = cli_args['output']
                if not isinstance(output_path, (str, Path)):
                    raise ConfigError(f"'output' must be a string or Path, got {type(output_path).__name__}")
                self.config['output_path'] = output_path
                
        except Exception as e:
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"Error updating configuration from CLI: {str(e)}")
            
    def validate_config(self) -> List[str]:
        """
        Validate the loaded configuration.
        
        Returns:
            List of validation errors (empty if valid)
            
        Raises:
            ConfigError: When validation fails critically
        """
        errors = []
        
        # Check for required sections
        if 'converters' not in self.config:
            errors.append("Missing 'converters' section in configuration")
            
        # Validate converters
        converters = self.config.get('converters', [])
        if not isinstance(converters, list):
            errors.append(f"'converters' must be a list, got {type(converters).__name__}")
        elif not converters:
            errors.append("No converters specified in configuration")
            
        # Validate converter-specific configs
        for converter in converters:
            converter_config_key = f"{converter}_converter"
            converter_config = self.config.get(converter_config_key)
            
            if converter_config is not None and not isinstance(converter_config, dict):
                errors.append(f"'{converter_config_key}' must be a dictionary")
                
        # Validate logging config
        logging_config = self.config.get('logging', {})
        if not isinstance(logging_config, dict):
            errors.append(f"'logging' must be a dictionary, got {type(logging_config).__name__}")
        else:
            # Check log level
            log_level = logging_config.get('level')
            if log_level and log_level not in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
                errors.append(f"Invalid log level: '{log_level}'")
                
            # Check log file
            log_file = logging_config.get('file')
            if log_file and not isinstance(log_file, (str, Path)):
                errors.append(f"'logging.file' must be a string or Path, got {type(log_file).__name__}")
                
        # Return all validation errors
        return errors
            
    def merge_configs(self, other_config: Dict[str, Any]) -> None:
        """
        Merge another config dictionary into this one.
        
        Args:
            other_config: Config dictionary to merge
            
        Raises:
            ConfigError: When merging fails
        """
        if not isinstance(other_config, dict):
            raise ConfigError(f"Cannot merge non-dictionary config: {type(other_config).__name__}")
            
        try:
            self._recursive_merge(self.config, other_config)
        except Exception as e:
            raise ConfigError(f"Error merging configurations: {str(e)}")
            
    def _recursive_merge(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> None:
        """
        Recursively merge overlay dictionary into base dictionary.
        
        Args:
            base: Base dictionary to merge into
            overlay: Overlay dictionary with values to merge
        """
        for key, value in overlay.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                # Recursively merge nested dictionaries
                self._recursive_merge(base[key], value)
            else:
                # Otherwise replace or add the value
                base[key] = value