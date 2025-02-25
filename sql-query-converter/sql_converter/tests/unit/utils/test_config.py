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
    with patch("builtins.open", mock_open(read_data=yaml_content)):
        manager = ConfigManager()
        manager.load_config()
        assert 'pivot' in manager.get('converters')
        assert manager.get('logging.level') == 'DEBUG'

def test_config_priority():
    manager = ConfigManager()
    manager.config = {'converters': ['base']}
    manager.update_from_cli({'convert': ['cte']})
    assert manager.config['converters'] == ['cte']