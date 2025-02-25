import pytest
from unittest.mock import mock_open, patch
from sql_converter.utils.config import ConfigManager

def test_config_loading():
    yaml_content = """
    converters:
      - cte
      - pivot
    logging:
      level: DEBUG
    """
    # Ensure the mock correctly simulates a config file
    with patch("builtins.open", mock_open(read_data=yaml_content)):
        # Patch Path.exists and Path.is_file to return True for any path
        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.is_file", return_value=True):
                manager = ConfigManager()
                # Force a specific path that will be mocked
                manager.config_paths = [manager.config_paths[0]]  # Just use the first path
                manager.load_config()
                
                # Verify the config was loaded correctly
                assert 'pivot' in manager.get('converters')
                assert manager.get('logging.level') == 'DEBUG'

def test_config_priority():
    manager = ConfigManager()
    manager.config = {'converters': ['base']}
    manager.update_from_cli({'convert': ['cte']})
    assert manager.config['converters'] == ['cte']