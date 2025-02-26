"""
Unit tests for the updated AST-based CTEConverter.
Focuses on functionality rather than implementation details.
"""
import pytest
from sqlglot import exp
import re

from sql_converter.converters.cte import CTEConverter
from sql_converter.parsers.sql_parser import SQLParser
from sql_converter.exceptions import ValidationError, ConverterError, CircularDependencyError


class TestCTEConverter:
    """Test suite for the CTEConverter using AST-based implementation."""
    
    def test_basic_conversion(self, cte_converter, sql_parser, normalize_sql):
        """Test basic conversion of a temporary table to CTE."""
        sql = "SELECT * INTO #temp FROM users; SELECT * FROM #temp;"
        
        # Convert the SQL
        result = cte_converter.convert(sql)
        
        # Verify the conversion worked
        assert "WITH temp AS" in result
        assert "SELECT * FROM users" in result
        assert "SELECT * FROM temp" in result
        
        # The result should be valid SQL
        parsed_result = sql_parser.parse(result)
        assert len(parsed_result) == 1  # Should be a single WITH statement
        assert isinstance(parsed_result[0], exp.With)

    def test_ast_conversion_api(self, cte_converter, sql_parser):
        """Test the AST-based conversion API."""
        sql = "SELECT * INTO #temp FROM users; SELECT * FROM #temp;"
        
        # Parse into AST
        expressions = sql_parser.parse(sql)
        
        # Convert using the AST API
        result_expr = cte_converter.convert_ast(expressions, sql_parser)
        
        # Verify the result is a list containing a WITH expression
        assert isinstance(result_expr, list)
        assert len(result_expr) == 1
        assert isinstance(result_expr[0], exp.With)
        
        # Convert back to SQL for verification
        result_sql = sql_parser.to_sql(result_expr[0])
        
        # Verify the conversion worked
        assert "WITH temp AS" in result_sql
        assert "SELECT * FROM temp" in result_sql

    def test_multiple_temp_tables(self, cte_converter, sql_parser, load_fixture_sql):
        """Test converting multiple temp tables in a single query."""
        # Load the fixture
        sql = load_fixture_sql('input/multiple_temps.sql')
        
        # Convert using AST-based converter
        result = cte_converter.convert(sql)
        
        # Verify both temp tables are converted to CTEs
        assert "WITH temp1 AS" in result
        assert "temp2 AS" in result
        
        # Verify the references are updated
        assert "FROM temp1 t1 JOIN temp2 t2" in result
        
        # Verify this is valid SQL
        parsed_result = sql_parser.parse(result)
        assert len(parsed_result) == 1

    def test_nested_temp_tables(self, cte_converter, sql_parser, load_fixture_sql):
        """Test converting nested temp tables (one temp table references another)."""
        # Load the fixture
        sql = load_fixture_sql('input/nested_temps.sql')
        
        # Convert using AST-based converter
        result = cte_converter.convert(sql)
        
        # Verify both temp tables are converted to CTEs
        assert "WITH inner_temp AS" in result
        assert "outer_temp AS" in result
        
        # Verify the dependency order
        inner_pos = result.find("inner_temp AS")
        outer_pos = result.find("outer_temp AS")
        assert inner_pos < outer_pos  # inner_temp should be defined first
        
        # Verify references are updated
        assert "FROM inner_temp" in result

    def test_pattern_matching(self, configured_converter, sql_parser, load_fixture_sql):
        """Test custom patterns for temporary table identification."""
        # Load the fixture
        sql = load_fixture_sql('input/pattern_matching.sql')
        
        # Convert using configured converter with custom patterns
        result = configured_converter.convert(sql)
        
        # Verify conversion
        assert "WITH tmp_users AS" in result
        assert "FROM tmp_users" in result
        
        # No temp tables should remain
        assert "#tmp_users" not in result

    def test_permanent_table_preservation(self, cte_converter, sql_parser, load_fixture_sql):
        """Test that permanent tables are not converted."""
        # Load the fixture
        sql = load_fixture_sql('input/permanent_table.sql')
        
        # Convert the SQL
        result = cte_converter.convert(sql)
        
        # Verify permanent table is not converted
        assert "INTO permanent_table" in result
        assert "WITH" not in result  # No CTEs should be created

    def test_with_comments(self, cte_converter, sql_parser, load_fixture_sql):
        """Test conversion of SQL with comments."""
        # Load the fixture
        sql = load_fixture_sql('input/with_comments.sql')
        
        # Convert the SQL
        result = cte_converter.convert(sql)
        
        # Verify conversion worked
        assert "WITH commented_temp AS" in result
        assert "FROM commented_temp" in result
        
        # Comments should not interfere with the conversion
        assert "#commented_temp" not in result
        assert "#ignored_temp" not in result  # From commented-out SQL

    def test_circular_dependency_detection(self, cte_converter, sql_parser):
        """Test that circular dependencies are detected and reported."""
        # Create a circular dependency between temp tables
        sql = """
        SELECT * INTO #temp1 FROM #temp2;
        SELECT * INTO #temp2 FROM #temp1;
        SELECT * FROM #temp1;
        """
        
        # This should raise a validation error
        with pytest.raises((ValidationError, CircularDependencyError)):
            cte_converter.convert(sql)

    def test_complex_query_structure(self, cte_converter, sql_parser):
        """Test handling complex query structures."""
        sql = """
        WITH existing_cte AS (
            SELECT * FROM users WHERE status = 'active'
        )
        SELECT * INTO #temp FROM existing_cte;
        
        SELECT * FROM #temp;
        """
        
        # Convert the SQL
        result = cte_converter.convert(sql)
        
        # Verify the existing CTE is preserved
        assert "WITH existing_cte AS" in result
        
        # Verify the temp table is converted
        assert "temp AS" in result
        
        # Verify the reference is updated
        assert "FROM temp" in result

    def test_subquery_handling(self, cte_converter, sql_parser):
        """Test handling temp tables in subqueries."""
        sql = """
        SELECT * INTO #temp FROM (
            SELECT id, name 
            FROM users 
            WHERE status = 'active'
        ) active_users;
        
        SELECT * FROM #temp;
        """
        
        # Convert the SQL
        result = cte_converter.convert(sql)
        
        # Verify conversion
        assert "WITH temp AS" in result
        assert "FROM (SELECT id, name FROM users WHERE status = 'active')" in result.replace('\n', ' ')
        assert "FROM temp" in result
        
        # No temp tables should remain
        assert "#temp" not in result

    def test_dialects_support(self, cte_converter, tsql_parser):
        """Test conversion with different SQL dialects."""
        # T-SQL specific SQL
        sql = """
        SELECT [col1], [col2] INTO #temp FROM [schema].[table];
        SELECT [col1] FROM #temp WHERE [col2] > 10;
        """
        
        # Parse with TSQL parser
        expressions = tsql_parser.parse(sql)
        
        # Convert using AST-based method
        result_expr = cte_converter.convert_ast(expressions, tsql_parser)
        
        # Convert back to SQL
        result = tsql_parser.to_sql(result_expr[0])
        
        # Verify conversion worked with dialect specifics preserved
        assert "WITH temp AS" in result
        assert "[col1]" in result  # Brackets should be preserved
        assert "[schema].[table]" in result  # Schema notation should be preserved