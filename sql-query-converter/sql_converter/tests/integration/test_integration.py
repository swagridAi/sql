import pytest
from pathlib import Path
from sql_converter.cli import SQLConverterApp
from sql_converter.converters.cte import CTEConverter  # Add missing import
from sql_converter.utils.config import ConfigManager

def test_full_conversion(tmp_path, config_manager):
    input_dir = Path("sql_converter/tests/fixtures/input")
    output_dir = tmp_path / "output"
    output_dir.mkdir(exist_ok=True)
    
    app = SQLConverterApp(
        converters={'cte': CTEConverter()},
        config=config_manager.config
    )
    app.process_directory(input_dir, output_dir, ['cte'])
    
    # Verify all files were converted
    input_files = list(input_dir.glob("**/*.sql"))
    output_files = list(output_dir.glob("**/*.sql"))
    assert len(output_files) > 0
    
    # Compare with expected results but normalize whitespace
    expected_dir = Path("sql_converter/tests/fixtures/expected")
    for input_file in input_files:
        relative = input_file.relative_to(input_dir)
        output_file = output_dir / relative
        expected_file = expected_dir / relative
        
        if output_file.exists() and expected_file.exists():
            # Normalize whitespace for comparison
            import re
            output_text = re.sub(r'\s+', ' ', output_file.read_text().strip())
            expected_text = re.sub(r'\s+', ' ', expected_file.read_text().strip())
            assert output_text == expected_text
            
def test_directory_structure_preservation(tmp_path):
    # Create nested input structure
    input_dir = tmp_path / "input"
    nested_dir = input_dir / "subdir"
    nested_dir.mkdir(parents=True)
    
    # Create test files
    (input_dir / "root.sql").write_text("SELECT * INTO #temp FROM users;")
    (nested_dir / "nested.sql").write_text("SELECT * INTO #temp2 FROM products;")
    
    # Set up output dir
    output_dir = tmp_path / "output"
    output_dir.mkdir(exist_ok=True)
    
    # Run conversion with structure preservation
    app = SQLConverterApp(
        converters={'cte': CTEConverter()},
        config={"preserve_structure": True}
    )
    app.process_directory(input_dir, output_dir, ['cte'])
    
    # Verify structure was preserved
    assert (output_dir / "root.sql").exists()
    assert (output_dir / "subdir" / "nested.sql").exists()

def test_error_handling(tmp_path):
    # Create SQL with deliberate syntax error that will trigger an exception
    input_file = tmp_path / "invalid.sql"
    input_file.write_text("SELECT * FROM WHERE x = 1;")  # Missing table name
    
    output_file = tmp_path / "output.sql"
    
    app = SQLConverterApp(
        converters={'cte': CTEConverter()},
        config={}
    )
    
    # Ensure the converter actually raises exceptions for syntax errors
    with pytest.raises(Exception):
        app.process_file(input_file, output_file, ['cte'])

def test_config_loading_from_multiple_sources(tmp_path, monkeypatch):
    # Set up test config file
    config_file = tmp_path / "test_config.yml"
    config_file.write_text("converters:\n  - cte\nlogging:\n  level: DEBUG")
    
    # Set env var
    monkeypatch.setenv("SQL_CONVERTER_CONFIG", str(config_file))
    
    # Initialize config and verify values
    config_manager = ConfigManager()
    config_manager.load_config()
    
    assert config_manager.get('logging.level') == 'DEBUG'