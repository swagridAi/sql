import pytest
from sql_converter.parsers.sql_parser import SQLParser

def test_statement_splitting():
    sql = """
    SELECT * FROM table; 
    -- Comment
    INSERT INTO #temp VALUES (1);
    """
    parser = SQLParser()
    statements = parser.split_statements(sql)
    assert len(statements) == 2
    assert "SELECT" in statements[0]
    assert "INSERT" in statements[1]

def test_tsql_bracket_handling():
    sql = "SELECT [col.name] FROM [dbo.table];"
    parser = SQLParser(dialect='tsql')
    statements = parser.split_statements(sql)
    assert len(statements) == 1
    assert "[col.name]" in statements[0]

def test_comment_handling():
    sql = """
    /* Multi-line 
       comment */
    SELECT 1; -- Line comment
    """
    parser = SQLParser()
    statements = parser.split_statements(sql)
    assert len(statements) == 1
    assert "SELECT" in statements[0]