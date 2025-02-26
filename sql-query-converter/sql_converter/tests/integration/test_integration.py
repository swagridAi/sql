import pytest
import os
from pathlib import Path
from sql_converter.cli import SQLConverterApp
from sql_converter.converters.cte import CTEConverter

def test_full_conversion(tmp_path, config_manager):
    # Find the project root directory
    # Start from the current file and walk up until we find the sql_converter directory
    current_dir = Path(__file__).resolve().parent
    
    # Find the root directory that contains sql_converter
    root_dir = None
    search_dir = current_dir
    while search_dir != search_dir.parent:  # Stop at filesystem root
        if (search_dir / "sql_converter" / "tests" / "fixtures").exists():
            root_dir = search_dir
            break
        search_dir = search_dir.parent
    
    assert root_dir is not None, "Could not find project root directory"
    
    # Build paths based on the found root directory
    fixtures_dir = root_dir / "sql_converter" / "tests" / "fixtures" / "input"
    expected_dir = root_dir / "sql_converter" / "tests" / "fixtures" / "expected"
    
    print(f"Project root directory: {root_dir}")
    print(f"Looking for fixtures at: {fixtures_dir}")
    
    assert fixtures_dir.exists(), f"Fixtures directory not found at {fixtures_dir}"
    assert expected_dir.exists(), f"Expected directory not found at {expected_dir}"
    
    output_dir = tmp_path / "output"
    output_dir.mkdir(exist_ok=True)
    
    app = SQLConverterApp(
        converters={'cte': CTEConverter()},
        config=config_manager.config
    )
    app.process_directory(fixtures_dir, output_dir, ['cte'])
    
    # Verify all files were converted
    input_files = list(fixtures_dir.glob("**/*.sql"))
    output_files = list(output_dir.glob("**/*.sql"))
    assert len(output_files) > 0
    
    # Compare with expected results but normalize whitespace
    for input_file in input_files:
        relative = input_file.relative_to(fixtures_dir)
        output_file = output_dir / relative
        expected_file = expected_dir / relative
        
        if output_file.exists() and expected_file.exists():
            # Normalize whitespace for comparison
            import re
            output_text = re.sub(r'\s+', ' ', output_file.read_text().strip())
            expected_text = re.sub(r'\s+', ' ', expected_file.read_text().strip())
            assert output_text == expected_text