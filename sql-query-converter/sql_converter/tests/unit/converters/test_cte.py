import pytest
from sql_converter.converters.cte import CTEConverter

def test_basic_cte_conversion():
    sql = "SELECT * INTO #temp FROM users; SELECT * FROM #temp;"
    converter = CTEConverter()
    converted = converter.convert(sql)
    assert "WITH temp AS" in converted
    assert "SELECT * FROM temp" in converted

def test_multiple_temp_tables():
    sql = """
    CREATE TEMP TABLE #table1 AS SELECT * FROM a;
    SELECT * INTO #table2 FROM b;
    SELECT * FROM #table1 JOIN #table2;
    """
    converter = CTEConverter()
    converted = converter.convert(sql)
    assert "table1 AS" in converted
    assert "table2 AS" in converted
    assert "FROM table1 JOIN table2" in converted

def test_temp_table_pattern_matching():
    sql = "SELECT * INTO #my_temp FROM table;"
    converter = CTEConverter(config={'temp_table_patterns': ['#my_*']})
    converted = converter.convert(sql)
    assert "my_temp AS" in converted

def test_nested_temp_tables():
    sql = """
    SELECT * INTO #outer FROM (
        SELECT * FROM #inner
    );
    """
    converter = CTEConverter()
    converted = converter.convert(sql)
    assert "WITH outer AS" in converted
    assert "inner AS" in converted