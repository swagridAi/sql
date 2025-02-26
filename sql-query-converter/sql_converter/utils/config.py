"""
Enhanced configuration management for SQL Converter with AST-based parsing support.

This module provides a comprehensive configuration system for the SQL Converter
application, with support for AST-based parsing and transformation options.
"""
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, Set
import yaml
from dotenv import load_dotenv

from sql_converter.exceptions import ConfigError


class ConfigManager:
    """
    Manages configuration from multiple sources with precedence rules,
    including enhanced support for AST-based parsing options.
    """
    
    # Default configuration
    DEFAULT_CONFIG = {
        'converters': ['cte'],
        'logging': {
            'level': 'INFO',
            'file': 'conversions.log',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'console': True
        },
        'parser': {
            'dialect': 'ansi',
            'optimization_level': 1,
            'schema_aware': False,
            'error_handling': 'strict',
            'pretty_print': {
                'enabled': True,
                'indent_spaces': 2,
                'uppercase_keywords': True,
                'max_line_length': 100
            }
        },
        'cte_converter': {
            'indent_spaces': 2,
            'temp_table_patterns': ['#?temp_*', '#?tmp_*', '#.*'],
            'cte_naming': {
                'strip_prefix': True,
                'style': 'original'
            },
            'dependency_handling': {
                'detect_cycles': True,
                'auto_break_cycles': False
            },
            'ast': {
                'preserve_comments': True,
                'preserve_formatting': False
            }
        },
        'output': {
            'default_output_dir': './converted_sql',
            'overwrite': True,
            'backup': True,
            'format': True,
            'formatting': {
                'indent_spaces': 2,
                'uppercase_keywords': True,
                'max_line_length': 80,
                'comma_style': 'end',
                'align_columns': True
            }
        },
        'advanced': {
            'parallelism': 0,
            'max_memory_mb': 0,
            'timeout_seconds': 0
        }
    }
    
    # Valid configuration values
    VALID_DIALECTS = {'ansi', 'tsql', 'mysql', 'postgresql', 'oracle', 'snowflake', 'redshift'}
    VALID_OPTIMIZATION_LEVELS = {0, 1, 2}
    VALID_ERROR_HANDLING = {'strict', 'relaxed', 'recovery'}
    VALID_LOG_LEVELS = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
    VALID_NAMING_STYLES = {'original', 'snake_case', 'camelCase'}
    VALID_COMMA_STYLES = {'end', 'start'}
    
    def __init__(self):
        """Initialize the configuration manager with enhanced AST options."""
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
        
        # Initialize with default configuration
        self.config = self.DEFAULT_CONFIG.copy()

    def load_config(self) -> None:
        """
        Load configuration from first found valid config file,
        with support for AST-based parsing options.
        
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
                
                # Merge with defaults, with file config taking precedence
                self._recursive_merge(self.config, loaded_config)
                
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
            
            # If there were critical errors in configuration loading, raise exception
            if any("Permission denied" in err for err in errors):
                raise ConfigError(
                    "Cannot access configuration files due to permission issues",
                    "\n".join(errors)
                )
        
        # Process environment variable overrides for key settings
        self._apply_env_overrides()

    def _apply_env_overrides(self) -> None:
        """Apply configuration overrides from environment variables."""
        # Map of environment variables to config paths
        env_mappings = {
            'SQL_CONVERTER_DIALECT': 'parser.dialect',
            'SQL_CONVERTER_LOG_LEVEL': 'logging.level',
            'SQL_CONVERTER_LOG_FILE': 'logging.file',
            'SQL_CONVERTER_CONVERTERS': 'converters',
            'SQL_CONVERTER_OUTPUT_DIR': 'output.default_output_dir',
            'SQL_CONVERTER_OPTIMIZATION': 'parser.optimization_level',
        }
        
        # Apply each override if environment variable exists
        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Special handling for lists
                if config_path == 'converters':
                    converters = [c.strip() for c in env_value.split(',')]
                    self.set(config_path, converters)
                # Special handling for numeric values
                elif config_path == 'parser.optimization_level':
                    try:
                        opt_level = int(env_value)
                        if opt_level in self.VALID_OPTIMIZATION_LEVELS:
                            self.set(config_path, opt_level)
                        else:
                            self.logger.warning(
                                f"Invalid optimization level in {env_var}: {env_value}. "
                                f"Must be one of {self.VALID_OPTIMIZATION_LEVELS}"
                            )
                    except ValueError:
                        self.logger.warning(
                            f"Invalid numeric value in {env_var}: {env_value}"
                        )
                else:
                    self.set(config_path, env_value)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Get config value using dot notation (e.g. 'parser.dialect').
        
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

    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value using dot notation.
        
        Args:
            key: Config key using dot notation
            value: Value to set
            
        Raises:
            ConfigError: When key is invalid
        """
        if not key:
            raise ConfigError("Empty configuration key provided")
            
        keys = key.split('.')
        config = self.config
        
        # Traverse to the second-to-last key
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            elif not isinstance(config[k], dict):
                config[k] = {}
            config = config[k]
            
        # Set the value
        config[keys[-1]] = value

    def update_from_cli(self, cli_args: Dict[str, Any]) -> None:
        """
        Merge CLI arguments into config, with support for AST-specific options.
        
        Args:
            cli_args: CLI arguments dictionary
            
        Raises:
            ConfigError: When CLI arguments are invalid
        """
        if not isinstance(cli_args, dict):
            raise ConfigError(f"CLI arguments must be a dictionary, got {type(cli_args).__name__}")
            
        try:
            # Apply CLI arguments with proper validation
            
            # Converters
            if 'convert' in cli_args:
                converters = cli_args['convert']
                if not isinstance(converters, list):
                    raise ConfigError(f"'convert' must be a list, got {type(converters).__name__}")
                self.config['converters'] = converters
            
            # Input/output paths
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
            
            # AST-specific options
            if 'dialect' in cli_args:
                dialect = cli_args['dialect']
                if dialect not in self.VALID_DIALECTS:
                    raise ConfigError(
                        f"Invalid SQL dialect: {dialect}. "
                        f"Must be one of {', '.join(self.VALID_DIALECTS)}"
                    )
                self.set('parser.dialect', dialect)
                
            if 'optimize' in cli_args:
                optimize = cli_args['optimize']
                if not isinstance(optimize, int) or optimize not in self.VALID_OPTIMIZATION_LEVELS:
                    raise ConfigError(
                        f"Invalid optimization level: {optimize}. "
                        f"Must be one of {self.VALID_OPTIMIZATION_LEVELS}"
                    )
                self.set('parser.optimization_level', optimize)
                
            # Logging options
            if 'verbose' in cli_args and cli_args['verbose']:
                self.set('logging.level', 'DEBUG')
                
        except Exception as e:
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"Error updating configuration from CLI: {str(e)}")

    def validate_config(self) -> List[str]:
        """
        Validate the loaded configuration, including AST-based options.
        
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
        
        # Validate parser config
        parser_config = self.config.get('parser', {})
        if not isinstance(parser_config, dict):
            errors.append(f"'parser' must be a dictionary, got {type(parser_config).__name__}")
        else:
            # Check dialect
            dialect = parser_config.get('dialect')
            if dialect and dialect not in self.VALID_DIALECTS:
                errors.append(
                    f"Invalid SQL dialect: {dialect}. "
                    f"Must be one of {', '.join(self.VALID_DIALECTS)}"
                )
                
            # Check optimization level
            opt_level = parser_config.get('optimization_level')
            if opt_level is not None:
                if not isinstance(opt_level, int) or opt_level not in self.VALID_OPTIMIZATION_LEVELS:
                    errors.append(
                        f"Invalid optimization level: {opt_level}. "
                        f"Must be one of {self.VALID_OPTIMIZATION_LEVELS}"
                    )
                    
            # Check error handling
            error_handling = parser_config.get('error_handling')
            if error_handling and error_handling not in self.VALID_ERROR_HANDLING:
                errors.append(
                    f"Invalid error handling mode: {error_handling}. "
                    f"Must be one of {', '.join(self.VALID_ERROR_HANDLING)}"
                )
                
        # Validate logging config
        logging_config = self.config.get('logging', {})
        if not isinstance(logging_config, dict):
            errors.append(f"'logging' must be a dictionary, got {type(logging_config).__name__}")
        else:
            # Check log level
            log_level = logging_config.get('level')
            if log_level and log_level not in self.VALID_LOG_LEVELS:
                errors.append(
                    f"Invalid log level: '{log_level}'. "
                    f"Must be one of {', '.join(self.VALID_LOG_LEVELS)}"
                )
                
            # Check log file
            log_file = logging_config.get('file')
            if log_file and not isinstance(log_file, (str, Path)):
                errors.append(
                    f"'logging.file' must be a string or Path, got {type(log_file).__name__}"
                )
                
        # Validate CTE converter config
        cte_config = self.config.get('cte_converter', {})
        if not isinstance(cte_config, dict):
            errors.append(f"'cte_converter' must be a dictionary, got {type(cte_config).__name__}")
        else:
            # Check temp table patterns
            patterns = cte_config.get('temp_table_patterns')
            if patterns is not None:
                if not isinstance(patterns, list):
                    errors.append(
                        f"'cte_converter.temp_table_patterns' must be a list, "
                        f"got {type(patterns).__name__}"
                    )
                elif not patterns:
                    errors.append("No temp table patterns specified")
                    
            # Check naming style
            naming = cte_config.get('cte_naming', {})
            if naming and isinstance(naming, dict):
                style = naming.get('style')
                if style and style not in self.VALID_NAMING_STYLES:
                    errors.append(
                        f"Invalid naming style: {style}. "
                        f"Must be one of {', '.join(self.VALID_NAMING_STYLES)}"
                    )
                
        # Validate output config
        output_config = self.config.get('output', {})
        if not isinstance(output_config, dict):
            errors.append(f"'output' must be a dictionary, got {type(output_config).__name__}")
        else:
            # Check output directory
            output_dir = output_config.get('default_output_dir')
            if output_dir and not isinstance(output_dir, (str, Path)):
                errors.append(
                    f"'output.default_output_dir' must be a string or Path, "
                    f"got {type(output_dir).__name__}"
                )
                
            # Check formatting options
            formatting = output_config.get('formatting', {})
            if formatting and isinstance(formatting, dict):
                comma_style = formatting.get('comma_style')
                if comma_style and comma_style not in self.VALID_COMMA_STYLES:
                    errors.append(
                        f"Invalid comma style: {comma_style}. "
                        f"Must be one of {', '.join(self.VALID_COMMA_STYLES)}"
                    )
                
        # Return all validation errors
        return errors
            
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
                
    def get_for_converter(self, converter_name: str) -> Dict[str, Any]:
        """
        Get configuration specific to a converter, including AST options.
        
        Args:
            converter_name: Name of the converter
            
        Returns:
            Configuration dictionary for the converter
        """
        config_key = f"{converter_name}_converter"
        converter_config = self.config.get(config_key, {})
        
        # Add parser-related AST options that converters might need
        parser_config = self.config.get('parser', {})
        ast_config = {
            'dialect': parser_config.get('dialect', 'ansi'),
            'optimization_level': parser_config.get('optimization_level', 1),
            'pretty_print': parser_config.get('pretty_print', {}),
        }
        
        # Merge configurations
        result = {**converter_config, 'ast': {**ast_config, **converter_config.get('ast', {})}}
        return result

    def get_all_sections(self) -> Dict[str, Any]:
        """
        Get all configuration sections (for debugging).
        
        Returns:
            Dictionary with all configuration sections
        """
        return self.config.copy()