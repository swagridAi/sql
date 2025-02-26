"""
AST-based CTEConverter implementation for converting temporary tables to CTEs.

This module handles the transformation of SQL queries with temporary tables
into equivalent queries using Common Table Expressions (CTEs) by manipulating
the Abstract Syntax Tree rather than using regex pattern matching.
"""
import logging
import re
from typing import List, Dict, Any, Optional, Set, Tuple, Union

import sqlglot
from sqlglot import exp

from sql_converter.converters.base import BaseConverter
from sql_converter.parsers.sql_parser import SQLParser
from sql_converter.exceptions import ConverterError, ValidationError, ConfigError


class CTEConverter(BaseConverter):
    """
    Converts SQL queries with temporary tables to Common Table Expressions (CTEs)
    using AST-based analysis and transformation.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize CTEConverter with configuration.
        
        Args:
            config: Configuration dictionary for converter settings
        """
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Configuration with defaults
        self.indent_spaces = self.config.get('indent_spaces', 4)
        temp_table_patterns = self.config.get('temp_table_patterns', ['#.*'])
        
        # Compile temporary table regex patterns
        try:
            self.temp_patterns = self._process_patterns(temp_table_patterns)
        except Exception as e:
            raise ConfigError(f"Failed to process temp table patterns: {str(e)}")
        
    def _process_patterns(self, patterns: List[str]) -> List:
        """
        Convert configuration patterns to compiled regex patterns.
        
        Args:
            patterns: List of pattern strings
            
        Returns:
            List of compiled regex pattern objects
            
        Raises:
            ConfigError: When pattern processing fails
        """
        if not patterns:
            raise ConfigError("No temp table patterns provided")
            
        compiled_patterns = []
        for i, pattern in enumerate(patterns):
            try:
                # Convert simplified pattern to regex
                processed = (
                    pattern.replace('?', '.?')
                           .replace('*', '.*')
                           .replace('#', r'\#')
                )
                compiled_patterns.append(re.compile(processed))
            except Exception as e:
                self.logger.warning(f"Invalid pattern '{pattern}' at index {i}: {str(e)}")
        
        if not compiled_patterns:
            self.logger.warning("No valid patterns found, using default pattern '#.*'")
            return [re.compile(r'\#.*')]
            
        return compiled_patterns

    def convert(self, sql: str) -> str:
        """
        Legacy method to maintain backward compatibility.
        Converts SQL with temp tables to use CTEs using the new AST-based approach.
        
        Args:
            sql: SQL query text to convert
            
        Returns:
            Converted SQL using CTEs
            
        Raises:
            ConverterError: For general conversion errors
            ValidationError: For validation errors
        """
        try:
            # Create a parser instance
            parser = SQLParser()
            
            # Parse the SQL into AST expressions
            expressions = parser.parse(sql)
            
            # Use the AST-based conversion method
            converted_expressions = self.convert_ast(expressions, parser)
            
            # Convert back to SQL text
            converted_sql = "\n".join([parser.to_sql(expr) for expr in converted_expressions])
            
            return converted_sql
            
        except Exception as e:
            error_msg = f"Failed to convert SQL: {str(e)}"
            self.logger.error(error_msg)
            raise ConverterError(error_msg) from e

    def convert_ast(self, expressions: List[exp.Expression], parser: SQLParser) -> List[exp.Expression]:
        """
        Convert SQL expressions with temp tables to use CTEs using AST manipulation.
        
        Args:
            expressions: List of AST expressions to convert
            parser: SQLParser instance for AST operations
            
        Returns:
            List of converted AST expressions
            
        Raises:
            ConverterError: For general conversion errors
            ValidationError: For validation errors
        """
        try:
            # Extract regex pattern strings for the parser
            pattern_strings = [p.pattern for p in self.temp_patterns]
            
            # Find temporary tables in the expressions
            temp_tables = self._identify_temp_tables(expressions, parser, pattern_strings)
            
            if not temp_tables:
                # No temp tables found, return the original expressions
                return expressions
                
            # Build dependency graph between temp tables
            dependency_graph = self._build_dependency_graph(temp_tables)
            
            # Order temp tables based on dependencies
            ordered_temp_tables = self._topological_sort(dependency_graph, temp_tables)
            
            # Separate main query from temp table definitions
            main_expressions, definition_expressions = self._separate_expressions(
                expressions, temp_tables
            )
            
            if not main_expressions:
                # If all expressions are temp table definitions, use the last one
                # as the main query (with temp references replaced)
                main_expressions = [self._deep_copy_expression(definition_expressions[-1])]
            
            # Create CTEs and apply transformations
            result = self._create_ctes_and_transform(
                main_expressions, ordered_temp_tables, parser
            )
            
            return result
            
        except Exception as e:
            if isinstance(e, (ValidationError, ConverterError)):
                raise
            error_msg = f"Failed to convert AST: {str(e)}"
            self.logger.error(error_msg)
            raise ConverterError(error_msg) from e

    def _identify_temp_tables(
        self, 
        expressions: List[exp.Expression],
        parser: SQLParser,
        pattern_strings: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Identify temporary tables and their definitions in AST expressions.
        
        Args:
            expressions: List of AST expressions to analyze
            parser: SQLParser instance for AST operations
            pattern_strings: List of regex pattern strings for temp tables
            
        Returns:
            Dictionary mapping temp table names to their definition info
        """
        # Use the parser to find temp tables
        temp_tables_info = parser.find_temp_tables("\n".join([parser.to_sql(expr) for expr in expressions]), pattern_strings)
        
        # Convert to our internal format
        temp_tables = {}
        for temp_info in temp_tables_info:
            name = temp_info['name']
            temp_tables[name] = {
                'name': name,
                'cte_name': self._get_cte_name(name),
                'type': temp_info['type'],
                'definition': temp_info['definition'],
                'defined_expr': temp_info['defined_expr'],
                'dependencies': temp_info['dependencies']
            }
            
        return temp_tables

    def _get_cte_name(self, temp_name: str) -> str:
        """
        Generate a CTE name from a temp table name.
        
        Args:
            temp_name: Original temp table name
            
        Returns:
            Cleaned name suitable for a CTE
        """
        return temp_name.lstrip('#').replace('.', '_')

    def _build_dependency_graph(self, temp_tables: Dict[str, Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Build a dependency graph between temp tables.
        
        Args:
            temp_tables: Dictionary of temp tables and their info
            
        Returns:
            Dictionary mapping temp tables to their dependencies
        """
        # Initialize the graph with empty dependency lists
        graph = {name: [] for name in temp_tables}
        
        # Add dependencies from the temp_tables info
        for name, info in temp_tables.items():
            for dep in info['dependencies']:
                if dep in temp_tables and dep != name:  # Avoid self-references
                    if dep not in graph[name]:
                        graph[name].append(dep)
        
        return graph

    def _topological_sort(
        self, 
        graph: Dict[str, List[str]], 
        temp_tables: Dict[str, Dict[str, Any]]
    ) -> List[str]:
        """
        Sort temp tables in dependency order using topological sort.
        
        Args:
            graph: Dependency graph of temp tables
            temp_tables: Dictionary of temp tables and their info
            
        Returns:
            List of temp table names in dependency order
            
        Raises:
            ValidationError: If a circular dependency is detected
        """
        # Track visited nodes for cycle detection
        permanent_mark = set()
        temporary_mark = set()
        result = []
        
        def visit(node):
            if node in permanent_mark:
                return
            if node in temporary_mark:
                raise ValidationError(f"Circular dependency detected involving {node}")
                
            temporary_mark.add(node)
            
            # Visit dependencies first
            for dependency in graph.get(node, []):
                visit(dependency)
                
            temporary_mark.remove(node)
            permanent_mark.add(node)
            result.append(node)
            
        # Visit all nodes
        for node in graph:
            if node not in permanent_mark:
                visit(node)
                
        # Return in reverse order (dependencies first)
        return list(reversed(result))

    def _separate_expressions(
        self, 
        expressions: List[exp.Expression],
        temp_tables: Dict[str, Dict[str, Any]]
    ) -> Tuple[List[exp.Expression], List[exp.Expression]]:
        """
        Separate main query expressions from temp table definition expressions.
        
        Args:
            expressions: List of all AST expressions
            temp_tables: Dictionary of temp tables and their info
            
        Returns:
            Tuple of (main_expressions, definition_expressions)
        """
        # Find expression objects that define temp tables
        definition_expr_set = set()
        for info in temp_tables.values():
            definition_expr_set.add(id(info['defined_expr']))
        
        # Separate main and definition expressions
        main_expressions = []
        definition_expressions = []
        
        for expr in expressions:
            if id(expr) in definition_expr_set:
                definition_expressions.append(expr)
            else:
                main_expressions.append(expr)
        
        return main_expressions, definition_expressions

    def _deep_copy_expression(self, expr: exp.Expression) -> exp.Expression:
        """
        Create a deep copy of an AST expression.
        
        Args:
            expr: AST expression to copy
            
        Returns:
            Deep copy of the expression
        """
        return expr.copy()

    def _create_ctes_and_transform(
        self,
        main_expressions: List[exp.Expression],
        ordered_temp_tables: List[str],
        parser: SQLParser
    ) -> List[exp.Expression]:
        """
        Create CTEs and transform main expressions to use them.
        
        Args:
            main_expressions: List of main query expressions
            ordered_temp_tables: List of temp table names in dependency order
            parser: SQLParser instance for AST operations
            
        Returns:
            List of transformed expressions using CTEs
        """
        if not ordered_temp_tables:
            return main_expressions
        
        # Create a map of original table names to CTE names
        replacements = {
            name: self._get_cte_name(name)
            for name in ordered_temp_tables
        }
        
        # Transform each main expression
        result = []
        for expr in main_expressions:
            # Replace table references in the main expression
            transformed_expr = parser.replace_references(expr, replacements)
            
            # If it's already a WITH expression, we need to add our CTEs to it
            if isinstance(transformed_expr, exp.With):
                # Extract the existing WITH expression's query
                with_query = transformed_expr.expression
                
                # Add our CTEs to the existing CTEs
                for name in ordered_temp_tables:
                    cte_name = replacements[name]
                    cte_def = self._temp_tables[name]['definition']
                    
                    # Replace references in the CTE definition
                    cte_def = parser.replace_references(cte_def, replacements)
                    
                    # Add to existing WITH expressions
                    transformed_expr.expressions.append(
                        exp.CTE(
                            this=exp.to_identifier(cte_name),
                            expression=cte_def
                        )
                    )
                
                result.append(transformed_expr)
            else:
                # Create a new WITH expression with our CTEs
                ctes = []
                for name in ordered_temp_tables:
                    cte_name = replacements[name]
                    cte_def = self._temp_tables[name]['definition']
                    
                    # Replace references in the CTE definition
                    cte_def = parser.replace_references(cte_def, replacements)
                    
                    ctes.append(
                        exp.CTE(
                            this=exp.to_identifier(cte_name),
                            expression=cte_def
                        )
                    )
                
                # Create the WITH expression with the transformed main query
                with_expr = exp.With(
                    expressions=ctes,
                    expression=transformed_expr
                )
                
                result.append(with_expr)
        
        return result