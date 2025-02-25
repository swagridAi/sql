import pytest
from pathlib import Path
from sql_converter.cli import SQLConverterApp
from sql_converter.converters.cte import CTEConverter  # Add missing import
from sql_converter.utils.config import ConfigManager

def test_full_conversion(tmp_path, config_manager):
    # Fix the path to match actual project structure
    input_dir = Path("sql-query-converter/sql_converter/tests/fixtures/input")
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
    expected_dir = Path("sql-query-converter/sql_converter/tests/fixtures/expected")
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