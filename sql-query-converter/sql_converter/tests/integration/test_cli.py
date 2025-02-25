import pytest
from unittest.mock import patch
import argparse
import sys
from pathlib import Path
from sql_converter.cli import main, SQLConverterApp

def test_cli_help():
    # Skip using CliRunner and test more directly
    with patch('sys.argv', ['cli.py', '--help']):
        with patch('argparse.ArgumentParser.print_help') as mock_print_help:
            try:
                main()
            except SystemExit:
                pass
            mock_print_help.assert_called_once()

def test_cli_file_conversion(tmp_path, sample_sql_file):
    output_file = tmp_path / "output.sql"
    
    # Patch sys.argv directly
    with patch('sys.argv', [
        'cli.py',
        '-i', str(sample_sql_file),
        '-o', str(output_file),
        '-c', 'cte'
    ]):
        try:
            main()
        except SystemExit:
            pass  # Catch potential system exit
    
    # Verify the output
    assert output_file.exists()
    content = output_file.read_text()
    assert "WITH temp AS" in content
    assert "SELECT" in content