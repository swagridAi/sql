"""
Tests for error handling in the AST-based SQL parser and converter.
Focuses on verifying clear and helpful error messages.
"""
import pytest
import re

from sql_converter.parsers.sql_parser import SQLParser
from sql_converter.converters.cte import CTEConverter
from sql_converter.exceptions import (
    SQLSyntaxError, ParserError, CircularDependencyError, 
    ValidationError, DialectError, ASTError
)


class TestErrorHandling:
    """Test suite for error handling in AST-based parsing and conversion."""
    
    @pytest.fixture
    def parser(self):
        """Create a SQLParser instance."""
        return SQLParser()
    
    @pytest.fixture
    def converter(self):
        """Create a CTEConverter instance."""
        return CTEConverter()
    
    def test_syntax_error_reporting(self, parser):
        """Test that syntax errors are reported with useful information."""
        # SQL with syntax errors
        invalid_sql_samples = [
            # Missing FROM clause
            "SELECT id, name WHERE status = 'active';",
            
            # Unbalanced parentheses
            "SELECT * FROM (SELECT id FROM users WHERE id > 100;",
            
            # Invalid JOIN syntax
            "SELECT * FROM users JOIN orders;",
            
            # Invalid GROUP BY position
            "SELECT * FROM users GROUP BY name WHERE status = 'active';"
        ]
        
        for invalid_sql in invalid_sql_samples:
            # Should raise a SQLSyntaxError
            with pytest.raises(SQLSyntaxError) as excinfo:
                parser.parse(invalid_sql)
            
            # Error message should be clear and helpful
            error_msg = str(excinfo.value).lower()
            assert "syntax error" in error_msg
            
            # Should either have line/position info or a clear description
            has_position = re.search(r'(at line|at position)', error_msg) is not None
            has_description = any(term in error_msg for term in ['missing', 'invalid', 'expected', 'unbalanced'])
            
            assert has_position or has_description, f"Error message lacks position or description: {error_msg}"

    def test_validation_error_reporting(self, parser, converter):
        """Test validation errors are reported clearly."""
        # SQL with circular dependency
        circular_sql = """
        SELECT * INTO #temp1 FROM #temp2;
        SELECT * INTO #temp2 FROM #temp1;
        """
        
        # Should raise a ValidationError or CircularDependencyError
        with pytest.raises((ValidationError, CircularDependencyError)) as excinfo:
            expressions = parser.parse(circular_sql)
            converter.convert_ast(expressions, parser)
            
        # Error message should mention circular dependency
        error_msg = str(excinfo.value).lower()
        assert "circular" in error_msg
        assert "dependency" in error_msg
        
        # Should mention the tables involved
        assert "#temp1" in error_msg or "temp1" in error_msg
        assert "#temp2" in error_msg or "temp2" in error_msg

    def test_empty_sql_handling(self, parser):
        """Test handling of empty SQL input."""
        # Empty SQL should be rejected with a clear error
        with pytest.raises(SQLSyntaxError) as excinfo:
            parser.parse("")
            
        # Error should indicate empty input
        assert "empty sql statement" in str(excinfo.value).lower()
        
        # Just whitespace should also be rejected
        with pytest.raises(SQLSyntaxError):
            parser.parse("   \n   ")

    def test_dialect_specific_errors(self):
        """Test errors for dialect-specific features used with wrong dialect."""
        # T-SQL specific syntax
        tsql = "SELECT [Column Name] FROM [dbo].[Table];"
        
        # Parse with a MySQL parser (which doesn't support bracket identifiers)
        mysql_parser = SQLParser(dialect='mysql')
        
        # This might raise an error or silently handle it
        # We're just ensuring it doesn't crash unexpectedly
        try:
            mysql_parser.parse(tsql)
        except (SQLSyntaxError, DialectError):
            # Expected behavior - dialect error
            pass
        except Exception as e:
            # Unexpected error type
            pytest.fail(f"Unexpected error type: {type(e).__name__}: {str(e)}")

    def test_helpful_error_messages(self, parser, converter):
        """Test that error messages are helpful and actionable."""
        # Various problematic SQL inputs
        test_cases = [
            # Invalid join condition
            {
                'sql': "SELECT * FROM users u JOIN orders o;",
                'expected_terms': ['join', 'missing', 'condition', 'on']
            },
            
            # Unmatched quotes
            {
                'sql': "SELECT * FROM users WHERE name = 'John;",
                'expected_terms': ['quote', 'unbalanced', 'unmatched']
            },
            
            # Invalid table name
            {
                'sql': "SELECT * FROM ;",
                'expected_terms': ['table', 'missing', 'name']
            }
        ]
        
        for case in test_cases:
            with pytest.raises(SQLSyntaxError) as excinfo:
                parser.parse(case['sql'])
                
            # Error message should contain at least one expected term
            error_msg = str(excinfo.value).lower()
            assert any(term in error_msg for term in case['expected_terms']), \
                f"Error message '{error_msg}' should contain one of {case['expected_terms']}"

    def test_error_location_information(self, parser):
        """Test that error messages include location information when possible."""
        # SQL with a syntax error at a specific position
        sql = "SELECT * FROM users WHERE id = ;"  # Missing value after =
        
        with pytest.raises(SQLSyntaxError) as excinfo:
            parser.parse(sql)
            
        error = excinfo.value
        
        # Error should ideally have position or line information
        has_location = (
            hasattr(error, 'position') and error.position is not None or
            hasattr(error, 'line') and error.line is not None
        )
        
        if has_location:
            # If we have location info, it should be in the message
            if hasattr(error, 'position') and error.position is not None:
                assert f"position {error.position}" in str(error).lower()
            if hasattr(error, 'line') and error.line is not None:
                assert f"line {error.line}" in str(error).lower()
        else:
            # If not, the message should still be clear about what's wrong
            assert any(term in str(error).lower() for term in ['expected', 'missing', 'after'])

    def test_graceful_degradation(self, parser, converter):
        """Test that the system degrades gracefully for partially valid SQL."""
        # SQL with one valid statement and one invalid statement
        mixed_sql = """
        -- This part is valid
        SELECT * INTO #temp FROM users;
        
        -- This part has a syntax error
        SELECT * FROM #temp WHERE;
        """
        
        # Parsing the whole thing should fail
        with pytest.raises(SQLSyntaxError):
            parser.parse(mixed_sql)
        
        # But if we split it, we should get at least the valid statement
        statements = mixed_sql.split(';')
        valid_stmt = statements[0].strip()
        
        # This should parse successfully
        expressions = parser.parse(valid_stmt)
        
        # And we should be able to convert it
        converted = converter.convert_ast(expressions, parser)
        
        # Verify the conversion worked
        result = parser.to_sql(converted[0])
        assert "WITH temp AS" in result
        assert "FROM users" in result

    def test_unicode_handling(self, parser):
        """Test handling of Unicode characters in SQL."""
        # SQL with Unicode characters
        unicode_sql = """
        SELECT * FROM users WHERE name = '日本語';
        """
        
        # This should parse without error
        expressions = parser.parse(unicode_sql)
        
        # Verify the Unicode characters are preserved
        result = parser.to_sql(expressions[0])
        assert '日本語' in result