import pytest
from click.testing import CliRunner
from sql_converter.cli import main

def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    assert result.exit_code == 0
    assert "Input file or directory" in result.output

def test_cli_file_conversion(tmp_path, sample_sql_file):
    output_file = tmp_path / "output.sql"
    runner = CliRunner()
    result = runner.invoke(main, [
        '-i', str(sample_sql_file),
        '-o', str(output_file),
        '-c', 'cte'
    ])
    assert result.exit_code == 0
    assert output_file.exists()
    assert "WITH temp AS" in output_file.read_text()