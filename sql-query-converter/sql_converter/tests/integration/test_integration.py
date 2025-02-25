import pytest
from pathlib import Path
from sql_converter.cli import SQLConverterApp

def test_full_conversion(tmp_path, config_manager):
    input_dir = Path("tests/fixtures/input")
    output_dir = tmp_path / "output"
    
    app = SQLConverterApp(
        converters={'cte': CTEConverter()},
        config=config_manager.config
    )
    app.process_directory(input_dir, output_dir, ['cte'])
    
    # Verify all files were converted
    input_files = list(input_dir.glob("**/*.sql"))
    output_files = list(output_dir.glob("**/*.sql"))
    assert len(input_files) == len(output_files)
    
    # Compare with expected results
    for input_file in input_files:
        relative = input_file.relative_to(input_dir)
        output_file = output_dir / relative
        expected_file = Path("tests/fixtures/expected") / relative
        
        assert output_file.read_text() == expected_file.read_text()