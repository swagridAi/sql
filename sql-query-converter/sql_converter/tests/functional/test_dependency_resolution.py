"""
Tests for dependency resolution between temporary tables.
Tests the ordering and dependency handling in CTEConverter.
"""
import pytest
from sqlglot import exp

from sql_converter.parsers.sql_parser import SQLParser
from sql_converter.converters.cte import CTEConverter
from sql_converter.exceptions import ValidationError, CircularDependencyError


class TestDependencyResolution:
    """Test suite for temporary table dependency resolution."""
    
    @pytest.fixture
    def parser(self):
        """Create a SQLParser instance."""
        return SQLParser()
    
    @pytest.fixture
    def converter(self):
        """Create a CTEConverter instance."""
        return CTEConverter()
    
    def test_simple_dependency(self, parser, converter):
        """Test simple dependency between two temp tables."""
        sql = """
        -- First temp table
        SELECT * INTO #temp1 FROM users WHERE status = 'active';
        
        -- Second temp table depends on first
        SELECT * INTO #temp2 FROM #temp1 WHERE registration_date > '2023-01-01';
        
        -- Query uses both temp tables
        SELECT 
            t1.id, 
            t1.name,
            t2.registration_date
        FROM 
            #temp1 t1
        LEFT JOIN
            #temp2 t2 ON t1.id = t2.id;
        """
        
        # Parse the SQL into AST
        expressions = parser.parse(sql)
        
        # Convert using AST method
        converted = converter.convert_ast(expressions, parser)
        
        # Convert back to SQL
        result = parser.to_sql(converted[0])
        
        # Verify dependency order in the result
        # #temp1 should be defined before #temp2
        temp1_pos = result.find("temp1 AS")
        temp2_pos = result.find("temp2 AS")
        
        assert temp1_pos >= 0
        assert temp2_pos >= 0
        assert temp1_pos < temp2_pos  # temp1 should be defined first

    def test_multiple_dependencies(self, parser, converter):
        """Test with multiple dependencies between temp tables."""
        sql = """
        -- Base temp tables
        SELECT * INTO #customers FROM users WHERE type = 'customer';
        SELECT * INTO #products FROM items WHERE category = 'product';
        
        -- Intermediate temp table depends on both
        SELECT 
            c.id AS customer_id,
            p.id AS product_id,
            o.order_date
        INTO #orders 
        FROM orders o
        JOIN #customers c ON o.customer_id = c.id
        JOIN #products p ON o.product_id = p.id;
        
        -- Final temp table depends on intermediate
        SELECT
            customer_id,
            COUNT(product_id) AS product_count,
            MAX(order_date) AS latest_order
        INTO #customer_stats
        FROM #orders
        GROUP BY customer_id;
        
        -- Final query
        SELECT 
            c.name,
            c.email,
            s.product_count,
            s.latest_order
        FROM 
            #customers c
        JOIN
            #customer_stats s ON c.id = s.customer_id
        ORDER BY
            s.product_count DESC;
        """
        
        # Parse the SQL into AST
        expressions = parser.parse(sql)
        
        # Convert using AST method
        converted = converter.convert_ast(expressions, parser)
        
        # Convert back to SQL
        result = parser.to_sql(converted[0])
        
        # Verify dependency order in the result
        # This order should be followed:
        # 1. #customers and #products (no dependencies)
        # 2. #orders (depends on #customers and #products)
        # 3. #customer_stats (depends on #orders)
        customers_pos = result.find("customers AS")
        products_pos = result.find("products AS")
        orders_pos = result.find("orders AS")
        stats_pos = result.find("customer_stats AS")
        
        # Base tables should come before tables that depend on them
        assert min(customers_pos, products_pos) < orders_pos
        assert orders_pos < stats_pos

    def test_circular_dependency_detection(self, parser, converter):
        """Test detection of circular dependencies."""
        sql = """
        -- Circular dependency between temp tables
        SELECT * INTO #temp1 FROM #temp2;
        SELECT * INTO #temp2 FROM #temp1;
        
        SELECT * FROM #temp1;
        """
        
        # Parse the SQL into AST
        expressions = parser.parse(sql)
        
        # This should raise a validation error
        with pytest.raises((ValidationError, CircularDependencyError)) as excinfo:
            converter.convert_ast(expressions, parser)
            
        # Error should mention circular dependency
        assert "circular" in str(excinfo.value).lower()
        assert "dependency" in str(excinfo.value).lower()

    def test_self_reference_handling(self, parser, converter):
        """Test handling of self-references in temp tables."""
        sql = """
        -- Create initial temp table
        SELECT * INTO #temp FROM users;
        
        -- Update the temp table by self-reference
        -- (This is normally invalid in SQL, but testing how the parser handles it)
        SELECT * INTO #temp FROM #temp WHERE status = 'active';
        
        SELECT * FROM #temp;
        """
        
        # Parse the SQL into AST
        expressions = parser.parse(sql)
        
        # This might raise an error, or produce a specific result
        # We're just testing that it's handled consistently
        try:
            converted = converter.convert_ast(expressions, parser)
            
            # If no error, verify the output makes sense
            result = parser.to_sql(converted[0])
            
            # We should still have a temp definition
            assert "WITH temp AS" in result
            
        except ValidationError as e:
            # If it raises an error for invalid SQL, that's fine too
            assert "reference" in str(e).lower() or "dependency" in str(e).lower()

    def test_complex_dependency_graph(self, parser, converter):
        """Test complex dependency graph with multiple paths."""
        sql = """
        -- Base tables
        SELECT * INTO #A FROM table1;
        SELECT * INTO #B FROM table2;
        
        -- Mid-level dependencies
        SELECT * INTO #C FROM #A;
        SELECT * INTO #D FROM #A JOIN #B ON #A.id = #B.id;
        SELECT * INTO #E FROM #B;
        
        -- Top-level dependencies
        SELECT * INTO #F FROM #C JOIN #D ON #C.id = #D.id;
        SELECT * INTO #G FROM #D JOIN #E ON #D.id = #E.id;
        
        -- Final query
        SELECT * 
        FROM #F f
        JOIN #G g ON f.id = g.id;
        """
        
        # Parse the SQL into AST
        expressions = parser.parse(sql)
        
        # Convert using AST method
        converted = converter.convert_ast(expressions, parser)
        
        # Convert back to SQL
        result = parser.to_sql(converted[0])
        
        # Verify dependency order in the result
        # Dependency graph: 
        # Level 1: A, B
        # Level 2: C (depends on A), D (depends on A,B), E (depends on B)
        # Level 3: F (depends on C,D), G (depends on D,E)
        # 
        # These conditions should hold:
        # - A and B before C, D, E
        # - C, D, E before F, G
        
        pos = {name: result.find(f"{name.lower()} AS") for name in ['A', 'B', 'C', 'D', 'E', 'F', 'G']}
        
        # Level 1 before Level 2
        assert max(pos['A'], pos['B']) < min(pos['C'], pos['D'], pos['E'])
        
        # Level 2 before Level 3
        assert max(pos['C'], pos['D'], pos['E']) < min(pos['F'], pos['G'])
        
        # Dependent tables must come after all their dependencies
        assert pos['C'] > pos['A']  # C depends on A
        assert pos['D'] > max(pos['A'], pos['B'])  # D depends on A and B
        assert pos['E'] > pos['B']  # E depends on B
        assert pos['F'] > max(pos['C'], pos['D'])  # F depends on C and D
        assert pos['G'] > max(pos['D'], pos['E'])  # G depends on D and E

    def test_dependency_extraction(self, parser, converter):
        """Test the correct extraction of dependencies from SQL."""
        sql = """
        SELECT * INTO #temp1 FROM users;
        
        -- This has a dependency in a subquery
        SELECT * INTO #temp2 FROM (
            SELECT t1.*, p.profile_data 
            FROM #temp1 t1
            JOIN profiles p ON t1.id = p.user_id
        ) enriched_data;
        
        -- This has a dependency in a JOIN
        SELECT * INTO #temp3 
        FROM orders o
        JOIN #temp1 t1 ON o.user_id = t1.id
        JOIN #temp2 t2 ON o.user_id = t2.id;
        
        SELECT * FROM #temp3;
        """
        
        # Parse the SQL into AST
        expressions = parser.parse(sql)
        
        # Convert using AST method
        converted = converter.convert_ast(expressions, parser)
        
        # Convert back to SQL
        result = parser.to_sql(converted[0])
        
        # Verify the dependencies were correctly extracted and ordered
        temp1_pos = result.find("temp1 AS")
        temp2_pos = result.find("temp2 AS")
        temp3_pos = result.find("temp3 AS")
        
        # Check the ordering
        assert temp1_pos < temp2_pos < temp3_pos