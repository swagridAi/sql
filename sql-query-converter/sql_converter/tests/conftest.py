"""
Test fixtures and configuration for SQL Converter tests.
Updated to support AST-based parsing and conversion.
"""
import pytest
from pathlib import Path
import tempfile
import os
import sys

# Add path to root of project
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sql_converter.utils.config import ConfigManager
from sql_converter.parsers.sql_parser import SQLParser
from sql_converter.converters.cte import CTEConverter
from sql_converter.cli import SQLConverterApp

@pytest.fixture
def config_manager():
    """Create a ConfigManager with test configuration."""
    manager = ConfigManager()
    manager.config = {
        'converters': ['cte'],
        'cte_converter': {
            'indent_spaces': 2,
            'temp_table_patterns': ['#.*']
        },
        'parser': {
            'dialect': 'ansi',
            'optimization_level': 0
        }
    }
    return manager

@pytest.fixture
def sql_parser():
    """Create a SQLParser instance for testing."""
    return SQLParser(dialect='ansi')

@pytest.fixture
def tsql_parser():
    """Create a T-SQL dialect parser for testing."""
    return SQLParser(dialect='tsql')

@pytest.fixture
def mysql_parser():
    """Create a MySQL dialect parser for testing."""
    return SQLParser(dialect='mysql')

@pytest.fixture
def cte_converter():
    """Create a CTEConverter instance with default config."""
    return CTEConverter()

@pytest.fixture
def configured_converter():
    """Create a CTEConverter with custom configuration."""
    config = {
        'indent_spaces': 2,
        'temp_table_patterns': ['#temp_.*', '#tmp_.*', '#.*']
    }
    return CTEConverter(config=config)

@pytest.fixture
def converter_app(config_manager, cte_converter):
    """Create a SQLConverterApp instance for testing."""
    return SQLConverterApp(
        converters={'cte': cte_converter},
        config=config_manager.config
    )

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)

@pytest.fixture
def fixtures_path():
    """Return the path to test fixtures."""
    return Path(__file__).parent / 'fixtures'

@pytest.fixture
def sample_sql_file(temp_dir):
    """Create a sample SQL file for testing."""
    content = """
    -- Sample SQL with temp tables
    SELECT * INTO #temp FROM users WHERE status = 'active';
    SELECT name FROM #temp;
    """
    file_path = temp_dir / "test.sql"
    file_path.write_text(content)
    return file_path

@pytest.fixture
def complex_sql_file(temp_dir):
    """Create a more complex SQL file with multiple temp tables."""
    content = """
    SELECT * INTO #temp1 FROM users;
    SELECT * INTO #temp2 FROM orders;
    SELECT u.*, o.* FROM #temp1 u JOIN #temp2 o ON u.id = o.user_id;
    """
    file_path = temp_dir / "complex.sql"
    file_path.write_text(content)
    return file_path

@pytest.fixture
def load_fixture_sql():
    """
    Helper to load SQL from a fixture file.
    Usage: sql = load_fixture_sql('input/simple_select.sql')
    """
    fixtures_dir = Path(__file__).parent / 'fixtures'
    
    def _load(relative_path):
        path = fixtures_dir / relative_path
        if not path.exists():
            raise FileNotFoundError(f"Fixture not found: {relative_path}")
        return path.read_text()
        
    return _load

@pytest.fixture
def normalize_sql():
    """
    Helper to normalize SQL for comparison (removes whitespace differences).
    """
    import re
    
    def _normalize(sql):
        # Remove comments
        sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
        # Replace newlines and multiple spaces with a single space
        sql = re.sub(r'\s+', ' ', sql)
        # Remove leading/trailing whitespace
        sql = sql.strip()
        return sql.lower()  # Case insensitive comparison
        
    return _normalize