"""
Unit tests for the AST-based SQLParser.
Replaces the original regex-based parser tests.
"""
import pytest
from sqlglot import exp
import re

from sql_converter.parsers.sql_parser import SQLParser
from sql_converter.exceptions import SQLSyntaxError, ParserError, DialectError


class TestSQLParser:
    """Test suite for the AST-based SQLParser."""
    
    def test_parser_initialization(self):
        """Test that the parser initializes with different dialects."""
        # Default initialization
        parser = SQLParser()
        assert parser.dialect_name == 'ansi'
        assert parser.dialect == 'ansi'
        
        # Explicit dialect initialization
        parser = SQLParser(dialect='tsql')
        assert parser.dialect_name == 'tsql'
        assert parser.dialect == 'tsql'
        
        # Unknown dialect falls back to ANSI
        parser = SQLParser(dialect='unknown')
        assert parser.dialect_name == 'unknown'
        assert parser.dialect == 'ansi'  # Falls back to ANSI

    def test_basic_parsing(self, sql_parser):
        """Test basic SQL parsing capabilities."""
        sql = "SELECT * FROM users WHERE id = 1;"
        
        # Parse the SQL into AST expressions
        expressions = sql_parser.parse(sql)
        
        # Verify the result is a list of expressions
        assert isinstance(expressions, list)
        assert len(expressions) == 1
        
        # Verify the expression is a Select
        assert isinstance(expressions[0], exp.Select)
        
        # Verify the structure
        select_expr = expressions[0]
        assert select_expr.find(exp.Star)  # Has a * selection
        
        # Check the table reference
        table = select_expr.find(exp.Table)
        assert table.name == 'users'
        
        # Check the WHERE clause
        where = select_expr.find(exp.Where)
        assert where is not None

    def test_multi_statement_parsing(self, sql_parser):
        """Test parsing multiple SQL statements."""
        sql = """
        SELECT * FROM users;
        UPDATE users SET status = 'active' WHERE id = 1;
        """
        
        # Parse multiple statements
        expressions = sql_parser.parse(sql)
        
        # Verify we got two expressions
        assert len(expressions) == 2
        assert isinstance(expressions[0], exp.Select)
        assert isinstance(expressions[1], exp.Update)

    def test_statement_splitting(self, sql_parser):
        """Test splitting SQL into individual statements."""
        sql = """
        SELECT * FROM table; 
        -- Comment
        INSERT INTO #temp VALUES (1);
        """
        
        # Split statements using the parser
        statements = sql_parser.split_statements(sql)
        
        # Verify the results
        assert len(statements) == 2
        assert "SELECT" in statements[0]
        assert "INSERT" in statements[1]
        
        # Verify comments are handled correctly
        assert "Comment" not in statements[0]
        assert "Comment" not in statements[1]

    def test_dialect_specific_parsing(self, tsql_parser, mysql_parser):
        """Test dialect-specific SQL features."""
        # Test T-SQL specific features
        tsql = "SELECT [col.name] FROM [dbo].[table];"
        
        tsql_expressions = tsql_parser.parse(tsql)
        assert len(tsql_expressions) == 1
        
        # Test MySQL specific features
        mysql = "SELECT * FROM `users` LIMIT 10;"
        
        mysql_expressions = mysql_parser.parse(mysql)
        assert len(mysql_expressions) == 1
        
        # Limit should be preserved
        mysql_sql = mysql_parser.to_sql(mysql_expressions[0])
        assert "LIMIT" in mysql_sql.upper()

    def test_validate_sql(self, sql_parser):
        """Test SQL validation capabilities."""
        # Test valid SQL
        valid_sql = "SELECT * FROM users WHERE id = 1;"
        sql_parser.validate_sql(valid_sql)  # Should not raise
        
        # Test invalid SQL
        invalid_sql = "SELECT FROM WHERE;"
        with pytest.raises(SQLSyntaxError):
            sql_parser.validate_sql(invalid_sql)
        
        # Test empty SQL
        with pytest.raises(SQLSyntaxError) as excinfo:
            sql_parser.validate_sql("")
        assert "empty sql statement" in str(excinfo.value).lower()

    def test_syntax_error_detection(self, sql_parser):
        """Test that syntax errors are properly detected and reported."""
        invalid_sql = "SELECT FROM users;"
        
        # This should raise a SQLSyntaxError
        with pytest.raises(SQLSyntaxError) as excinfo:
            sql_parser.parse(invalid_sql)
        
        # Verify error contains position information
        error_msg = str(excinfo.value).lower()
        assert "syntax error" in error_msg

    def test_find_table_references(self, sql_parser):
        """Test finding table references in SQL."""
        sql = """
        SELECT u.id, o.order_id 
        FROM users u
        JOIN orders o ON u.id = o.user_id
        LEFT JOIN profiles p ON u.id = p.user_id;
        """
        
        # Find table references
        table_refs = sql_parser.find_table_references(sql)
        
        # Verify we found all tables
        table_names = [ref['table'] for ref in table_refs]
        assert 'users' in table_names
        assert 'orders' in table_names
        assert 'profiles' in table_names
        
        # Verify aliases
        aliases = [ref['alias'] for ref in table_refs]
        assert 'u' in aliases
        assert 'o' in aliases
        assert 'p' in aliases
        
        # Verify contexts
        contexts = [ref['context'] for ref in table_refs]
        assert 'FROM' in contexts
        assert any('JOIN' in ctx for ctx in contexts)

    def test_find_temp_tables(self, sql_parser):
        """Test finding temporary tables in SQL."""
        sql = """
        SELECT * INTO #temp1 FROM users;
        CREATE TEMP TABLE #temp2 AS SELECT * FROM orders;
        SELECT t1.id, t2.order_id FROM #temp1 t1 JOIN #temp2 t2 ON t1.id = t2.user_id;
        """
        
        # Find temp tables with patterns
        temp_tables = sql_parser.find_temp_tables(sql, ['#.*'])
        
        # Verify we found both temp tables
        temp_names = [info['name'] for info in temp_tables]
        assert '#temp1' in temp_names
        assert '#temp2' in temp_names
        
        # Verify types are correct
        types = {info['name']: info['type'] for info in temp_tables}
        assert types['#temp1'] == 'SELECT_INTO'
        
        # At least one of the temp tables should be recognized as CREATE_TEMP type
        assert any(t['type'] in ('CREATE_TEMP', 'CREATE_TEMP_AS') for t in temp_tables)

    def test_replace_references(self, sql_parser):
        """Test replacing table references in an AST."""
        sql = "SELECT * FROM #temp WHERE id = 1;"
        
        # Parse SQL
        expressions = sql_parser.parse(sql)
        
        # Replace references
        replacements = {'#temp': 'cte_temp'}
        modified = sql_parser.replace_references(expressions[0], replacements)
        
        # Convert back to SQL and verify the replacement
        modified_sql = sql_parser.to_sql(modified)
        assert '#temp' not in modified_sql
        assert 'cte_temp' in modified_sql
        
        # Original expression should be unchanged
        original_sql = sql_parser.to_sql(expressions[0])
        assert '#temp' in original_sql

    def test_generate_cte(self, sql_parser):
        """Test generating a CTE from a subquery."""
        sql = "SELECT * FROM users WHERE id = 1;"
        
        # Parse SQL
        expressions = sql_parser.parse(sql)
        select_expr = expressions[0]
        
        # Generate a CTE
        cte = sql_parser.generate_cte('user_cte', select_expr)
        
        # Verify result is a With expression
        assert isinstance(cte, exp.With)
        
        # Verify CTE name and definition is in the SQL
        cte_sql = sql_parser.to_sql(cte)
        assert 'WITH user_cte AS' in cte_sql.upper()
        assert 'SELECT * FROM users WHERE id = 1' in cte_sql

    def test_comment_handling(self, sql_parser):
        """Test that comments are properly handled."""
        sql = """
        -- This is a comment
        SELECT * FROM users; /* This is another comment */
        -- This is a final comment
        """
        
        # Parse SQL
        expressions = sql_parser.parse(sql)
        
        # Verify the comment doesn't interfere with the parse
        assert len(expressions) == 1
        assert isinstance(expressions[0], exp.Select)
        
        # Convert back to SQL
        result_sql = sql_parser.to_sql(expressions[0])
        
        # Comments should be removed in the parsed output
        assert "This is a comment" not in result_sql
        assert "This is another comment" not in result_sql

    def test_to_sql_format_consistency(self, sql_parser):
        """Test that to_sql produces consistent SQL format."""
        sql = "SELECT id, name FROM users WHERE status = 'active' ORDER BY name;"
        
        # Parse and convert back to SQL
        expressions = sql_parser.parse(sql)
        result_sql = sql_parser.to_sql(expressions[0])
        
        # Normalize for comparison (different formatters may have whitespace differences)
        def normalize(s):
            return re.sub(r'\s+', ' ', s).strip().lower()
        
        # Core elements should be preserved
        normalized_original = normalize(sql)
        normalized_result = normalize(result_sql)
        
        assert "select" in normalized_result
        assert "from users" in normalized_result
        assert "where status = 'active'" in normalized_result
        assert "order by" in normalized_result