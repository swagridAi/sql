"""
Integration tests for the command-line interface.
Updated to work with the AST-based implementation.
"""
import pytest
from unittest.mock import patch
import sys
from pathlib import Path

from sql_converter.cli import main, SQLConverterApp
from sql_converter.parsers.sql_parser import SQLParser
from sql_converter.converters.cte import CTEConverter


class TestCLI:
    """Test suite for the command-line interface."""
    
    def test_cli_help(self):
        """Test the CLI help output."""
        # Patch sys.argv to simulate CLI arguments
        with patch('sys.argv', ['cli.py', '--help']):
            # Patch sys.exit to avoid exiting the test
            with patch('sys.exit') as mock_exit:
                # Patch print_help to avoid printing during tests
                with patch('argparse.ArgumentParser.print_help') as mock_print_help:
                    main()
                    mock_print_help.assert_called_once()

    def test_cli_file_conversion(self, sample_sql_file, temp_dir):
        """Test converting a file via CLI."""
        output_file = temp_dir / "output.sql"
        
        # Patch sys.argv to simulate CLI arguments
        with patch('sys.argv', [
            'cli.py',
            '-i', str(sample_sql_file),
            '-o', str(output_file),
            '-c', 'cte'
        ]):
            # Patch sys.exit to avoid exiting the test
            with patch('sys.exit'):
                main()
        
        # Verify output file was created
        assert output_file.exists()
        
        # Verify content uses CTEs
        content = output_file.read_text()
        assert "WITH temp AS" in content
        assert "FROM users" in content
        assert "SELECT name FROM temp" in content

    def test_cli_directory_conversion(self, temp_dir, sample_sql_file, complex_sql_file):
        """Test converting a directory of files via CLI."""
        input_dir = temp_dir
        output_dir = temp_dir / "output"
        
        # Patch sys.argv to simulate CLI arguments
        with patch('sys.argv', [
            'cli.py',
            '-i', str(input_dir),
            '-o', str(output_dir),
            '-c', 'cte'
        ]):
            # Patch sys.exit to avoid exiting the test
            with patch('sys.exit'):
                main()
        
        # Verify output directory was created
        assert output_dir.exists()
        
        # Verify output files were created
        assert (output_dir / "test.sql").exists()
        assert (output_dir / "complex.sql").exists()
        
        # Verify content uses CTEs
        test_content = (output_dir / "test.sql").read_text()
        assert "WITH temp AS" in test_content
        
        complex_content = (output_dir / "complex.sql").read_text()
        assert "WITH temp1 AS" in complex_content
        assert "temp2 AS" in complex_content

    def test_cli_dialect_selection(self, sample_sql_file, temp_dir):
        """Test selecting a specific SQL dialect via CLI."""
        output_file = temp_dir / "tsql_output.sql"
        
        # Patch sys.argv to simulate CLI arguments with dialect selection
        with patch('sys.argv', [
            'cli.py',
            '-i', str(sample_sql_file),
            '-o', str(output_file),
            '-c', 'cte',
            '-d', 'tsql'  # Specify T-SQL dialect
        ]):
            # Patch sys.exit to avoid exiting the test
            with patch('sys.exit'):
                main()
        
        # Verify output file was created
        assert output_file.exists()
        
        # Content should be valid - exact format might vary by dialect
        content = output_file.read_text()
        assert "WITH" in content
        assert "FROM" in content

    def test_cli_error_handling(self, temp_dir):
        """Test CLI error handling for invalid input."""
        # Create an invalid SQL file
        invalid_file = temp_dir / "invalid.sql"
        invalid_file.write_text("SELECT FROM WHERE;")  # Invalid SQL
        
        output_file = temp_dir / "output.sql"
        
        # Patch sys.argv to simulate CLI arguments
        with patch('sys.argv', [
            'cli.py',
            '-i', str(invalid_file),
            '-o', str(output_file),
            '-c', 'cte'
        ]):
            # Patch sys.exit to catch the exit code
            with patch('sys.exit') as mock_exit:
                main()
                
                # Should exit with error code
                mock_exit.assert_called()
                args = mock_exit.call_args[0]
                assert args[0] != 0  # Non-zero exit code

    def test_app_initialization(self, config_manager):
        """Test SQLConverterApp initialization."""
        # Create converter
        converter = CTEConverter()
        
        # Create app with the converter
        app = SQLConverterApp(
            converters={'cte': converter},
            config=config_manager.config
        )
        
        # Verify app has a parser instance
        assert hasattr(app, 'parser')
        assert isinstance(app.parser, SQLParser)
        
        # Verify app has the converter
        assert 'cte' in app.converters
        assert app.converters['cte'] is converter

    def test_app_summary_generation(self, converter_app, sample_sql_file, temp_dir):
        """Test generation of processing summary."""
        # Set up output paths
        output_file = temp_dir / "output.sql"
        
        # Process a file
        converter_app.process_file(sample_sql_file, output_file, ['cte'])
        
        # Get summary
        summary = converter_app.get_summary()
        
        # Verify summary statistics
        assert summary['processed_files'] == 1
        assert summary['failed_files'] == 0
        assert summary['success_rate'] == 100.0
        assert len(summary['failures']) == 0