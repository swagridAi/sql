import pytest
from pathlib import Path
from sql_converter import ConfigManager

@pytest.fixture
def config_manager():
    manager = ConfigManager()
    manager.config = {
        'converters': ['cte'],
        'cte_converter': {
            'indent_spaces': 2,
            'temp_table_patterns': ['#.*']
        }
    }
    return manager

@pytest.fixture
def sample_sql_file(tmp_path):
    test_sql = """
    SELECT * INTO #temp FROM users;
    SELECT name FROM #temp;
    """
    file_path = tmp_path / "test.sql"
    file_path.write_text(test_sql)
    return file_path