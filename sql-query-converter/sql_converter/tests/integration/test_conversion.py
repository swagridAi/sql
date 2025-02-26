"""
Integration tests for the SQL conversion process.
Consolidated from existing integration tests and updated for AST-based implementation.
"""
import pytest
import re
from pathlib import Path

from sql_converter.cli import SQLConverterApp
from sql_converter.converters.cte import CTEConverter
from sql_converter.parsers.sql_parser import SQLParser


class TestSQLConversion:
    """Integration tests for SQL conversion."""
    
    @pytest.fixture
    def converter_app(self, config_manager):
        """Create a SQLConverterApp instance for testing."""
        # Create a parser
        parser = SQLParser(dialect=config_manager.config['parser']['dialect'])
        
        # Create a converter
        converter = CTEConverter(config=config_manager.config['cte_converter'])
        
        # Create the app
        app = SQLConverterApp(
            converters={'cte': converter},
            config=config_manager.config
        )
        return app

    def test_basic_conversion(self, converter_app, temp_dir):
        """Test basic conversion of a simple SQL file."""
        # Create input file
        input_content = """
        -- Simple SQL with a temp table
        SELECT * INTO #temp FROM users;
        SELECT name FROM #temp WHERE status = 'active';
        """
        
        input_file = temp_dir / "simple.sql"
        input_file.write_text(input_content)
        
        # Set up output file
        output_file = temp_dir / "simple_output.sql"
        
        # Process the file
        converter_app.process_file(input_file, output_file, ['cte'])
        
        # Verify the output file exists
        assert output_file.exists()
        
        # Read the output
        output_content = output_file.read_text()
        
        # Verify the conversion
        assert "WITH temp AS" in output_content
        assert "SELECT * FROM users" in output_content
        assert "SELECT name FROM temp WHERE status = 'active'" in output_content
        
        # Verify no temp tables remain
        assert "#temp" not in output_content

    def test_multiple_temp_tables(self, converter_app, temp_dir):
        """Test conversion with multiple temp tables."""
        # Create input file
        input_content = """
        -- Multiple temp tables
        SELECT * INTO #temp1 FROM users WHERE type = 'customer';
        SELECT * INTO #temp2 FROM products WHERE status = 'active';
        
        -- Query joins both temp tables
        SELECT 
            u.name,
            p.product_name,
            p.price
        FROM 
            #temp1 u
        JOIN
            #temp2 p ON u.preferred_product = p.product_id;
        """
        
        input_file = temp_dir / "multiple.sql"
        input_file.write_text(input_content)
        
        # Set up output file
        output_file = temp_dir / "multiple_output.sql"
        
        # Process the file
        converter_app.process_file(input_file, output_file, ['cte'])
        
        # Verify the output file exists
        assert output_file.exists()
        
        # Read the output
        output_content = output_file.read_text()
        
        # Verify the conversion
        assert "WITH temp1 AS" in output_content
        assert "temp2 AS" in output_content
        assert "FROM temp1 u" in output_content
        assert "JOIN temp2 p" in output_content
        
        # Verify no temp tables remain
        assert "#temp1" not in output_content
        assert "#temp2" not in output_content

    def test_fixture_based_conversion(self, converter_app, fixtures_path, temp_dir):
        """Test conversion using the fixture files."""
        # Get all input fixtures
        input_dir = fixtures_path / "input"
        fixture_files = list(input_dir.glob("*.sql"))
        
        # Skip if no fixtures found
        if not fixture_files:
            pytest.skip("No fixture files found")
            
        # Create output directory
        output_dir = temp_dir / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Process each fixture
        for input_file in fixture_files:
            # Skip non-temp table fixtures
            if input_file.name == "permanent_table.sql":
                continue
                
            # Calculate output path
            output_file = output_dir / input_file.name
            
            # Process the file
            converter_app.process_file(input_file, output_file, ['cte'])
            
            # Verify output file exists
            assert output_file.exists(), f"Output file not created for {input_file.name}"
            
            # Find corresponding expected file
            expected_file = fixtures_path / "expected" / input_file.name
            
            # Skip comparison if expected file doesn't exist
            if not expected_file.exists():
                continue
                
            # Read output and expected content
            output_content = output_file.read_text()
            expected_content = expected_file.read_text()
            
            # Normalize both for comparison
            def normalize(text):
                # Remove whitespace differences
                return re.sub(r'\s+', ' ', text).strip().lower()
                
            normalized_output = normalize(output_content)
            normalized_expected = normalize(expected_content)
            
            # Verify the output matches expected
            assert normalized_output == normalized_expected, \
                f"Output doesn't match expected for {input_file.name}"

    def test_complex_nested_query(self, converter_app, temp_dir):
        """Test conversion of a complex nested query with temp tables."""
        # Create input file with complex nested queries
        input_content = """
        -- Complex nested query with CTEs and temp tables
        WITH base_data AS (
            SELECT * FROM users WHERE status = 'active'
        )
        SELECT 
            bd.id,
            bd.name,
            bd.email,
            o.order_count,
            o.total_spent
        INTO #user_stats
        FROM 
            base_data bd
        LEFT JOIN (
            SELECT 
                user_id,
                COUNT(*) as order_count,
                SUM(amount) as total_spent
            FROM orders
            WHERE order_date > '2023-01-01'
            GROUP BY user_id
        ) o ON bd.id = o.user_id;
        
        -- Query using the temp table
        SELECT * FROM #user_stats
        WHERE order_count > 5
        ORDER BY total_spent DESC;
        """
        
        input_file = temp_dir / "complex.sql"
        input_file.write_text(input_content)
        
        # Set up output file
        output_file = temp_dir / "complex_output.sql"
        
        # Process the file
        converter_app.process_file(input_file, output_file, ['cte'])
        
        # Verify the output file exists
        assert output_file.exists()
        
        # Read the output
        output_content = output_file.read_text()
        
        # Verify the original CTE is preserved
        assert "WITH base_data AS" in output_content
        
        # Verify the temp table is converted to a CTE
        assert "user_stats AS" in output_content
        
        # Verify the query references the CTE
        assert "FROM user_stats" in output_content
        
        # Verify no temp tables remain
        assert "#user_stats" not in output_content

    def test_edge_case_handling(self, converter_app, temp_dir):
        """Test handling of edge cases."""
        # Create tests for different edge cases
        edge_cases = [
            # Case 1: Multiple statements on one line
            {
                'name': 'oneline',
                'content': "SELECT * INTO #temp FROM users; SELECT * FROM #temp;",
                'check': lambda c: "WITH temp AS" in c and "SELECT * FROM temp" in c
            },
            
            # Case 2: Comments alongside code
            {
                'name': 'comments',
                'content': """
                -- Create temp table
                SELECT * INTO #temp FROM users; -- End of first statement
                /* Multi-line comment
                   spans multiple lines */
                SELECT * FROM #temp; -- Use temp table
                """,
                'check': lambda c: "WITH temp AS" in c and "SELECT * FROM temp" in c
            },
            
            # Case 3: Quoted identifiers with special characters
            {
                'name': 'quoted',
                'content': """
                SELECT * INTO #temp FROM "user.table"."column.name";
                SELECT "a.b" FROM #temp;
                """,
                'check': lambda c: "WITH temp AS" in c and 'FROM "user.table"."column.name"' in c
            }
        ]
        
        # Test each edge case
        for case in edge_cases:
            # Create input file
            input_file = temp_dir / f"{case['name']}.sql"
            input_file.write_text(case['content'])
            
            # Set up output file
            output_file = temp_dir / f"{case['name']}_output.sql"
            
            # Process the file
            converter_app.process_file(input_file, output_file, ['cte'])
            
            # Verify the output file exists
            assert output_file.exists()
            
            # Read the output and verify
            output_content = output_file.read_text()
            assert case['check'](output_content), f"Edge case '{case['name']}' failed verification"

    def test_directory_structure_preservation(self, converter_app, temp_dir):
        """Test that directory structure is preserved when processing directories."""
        # Create a nested directory structure with SQL files
        nested_dir = temp_dir / "nested"
        nested_dir.mkdir()
        
        deeper_dir = nested_dir / "deeper"
        deeper_dir.mkdir()
        
        # Create SQL files at different levels
        root_file = temp_dir / "root.sql"
        root_file.write_text("SELECT * INTO #temp FROM users; SELECT * FROM #temp;")
        
        nested_file = nested_dir / "nested.sql"
        nested_file.write_text("SELECT * INTO #temp FROM products; SELECT * FROM #temp;")
        
        deeper_file = deeper_dir / "deeper.sql"
        deeper_file.write_text("SELECT * INTO #temp FROM orders; SELECT * FROM #temp;")
        
        # Set up output directory
        output_dir = temp_dir / "output"
        
        # Process the directory
        converter_app.process_directory(temp_dir, output_dir, ['cte'])
        
        # Verify output directory structure matches input
        assert (output_dir / "root.sql").exists()
        assert (output_dir / "nested").exists()
        assert (output_dir / "nested" / "nested.sql").exists()
        assert (output_dir / "nested" / "deeper").exists()
        assert (output_dir / "nested" / "deeper" / "deeper.sql").exists()
        
        # Verify all files were converted correctly
        for output_file in [
            output_dir / "root.sql",
            output_dir / "nested" / "nested.sql",
            output_dir / "nested" / "deeper" / "deeper.sql"
        ]:
            content = output_file.read_text()
            assert "WITH temp AS" in content
            assert "#temp" not in content