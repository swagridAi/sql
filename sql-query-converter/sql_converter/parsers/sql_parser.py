"""
Advanced SQL Parser implementation using sqlglot for proper AST parsing.

This module replaces the regex-based parser with a robust SQL parser that
creates a proper Abstract Syntax Tree (AST) representation of SQL code.
"""
import logging
from typing import List, Dict, Optional, Generator, Tuple, Union, Any
from pathlib import Path

import sqlglot
from sqlglot import parse, ParseError, TokenError, exp
from sqlglot.optimizer import optimize

from sql_converter.exceptions import SQLSyntaxError, ParserError


class SQLParser:
    """
    Parser for SQL statements using sqlglot for proper AST parsing.
    This class maintains the same interface as the original parser
    but adds AST-based capabilities.
    """
    
    # Map our dialect names to sqlglot dialects
    DIALECT_MAP = {
        'ansi': None,  # Use SQLGlot default dialect (no specific dialect)
        'tsql': 'tsql',
        'mysql': 'mysql',
        'postgresql': 'postgres',
        'oracle': 'oracle',
        'bigquery': 'bigquery',
        'snowflake': 'snowflake',
        'redshift': 'redshift',
        'spark': 'spark',
    }
    
    def __init__(self, dialect: str = 'ansi'):
        """
        Initialize the SQL parser with the specified dialect.
        
        Args:
            dialect: SQL dialect to use ('ansi', 'tsql', 'mysql', etc.)
        """
        self.dialect_name = dialect.lower()
        self.logger = logging.getLogger(__name__)
        
        # Map to sqlglot dialect (can be None for default dialect)
        self.dialect = self.DIALECT_MAP.get(self.dialect_name, None)
        self.logger.debug(f"Initialized parser with dialect: {self.dialect or 'default'}")

    def parse(self, sql: str) -> List[exp.Expression]:
        """
        Parse SQL into AST expressions.
        
        Args:
            sql: SQL code to parse
            
        Returns:
            List of sqlglot Expression objects representing the parsed SQL
            
        Raises:
            SQLSyntaxError: When SQL contains syntax errors
            ParserError: When the parser encounters an error
        """
        try:
            # Parse the SQL into a list of expression trees - pass dialect only if not None
            if self.dialect:
                expressions = parse(sql, dialect=self.dialect, error_level='raise')
            else:
                expressions = parse(sql, error_level='raise')  # Use SQLGlot default dialect
            return expressions
        except ParseError as e:
            # Extract position information if available
            position = getattr(e, 'position', None)
            line = None
            if position:
                # Calculate line number from position
                line = sql[:position].count('\n') + 1
            
            # Raise our custom exception with detailed info
            raise SQLSyntaxError(
                str(e),
                source=sql[:100] + '...' if len(sql) > 100 else sql,
                position=position,
                line=line
            ) from e
        except TokenError as e:
            # Handle tokenization errors
            raise SQLSyntaxError(
                f"SQL tokenization error: {str(e)}",
                source=sql[:100] + '...' if len(sql) > 100 else sql
            ) from e
        except Exception as e:
            # Handle any other unexpected errors
            raise ParserError(
                f"Error parsing SQL: {str(e)}",
                source=sql[:100] + '...' if len(sql) > 100 else sql
            ) from e

    def validate_sql(self, sql: str) -> None:
        """
        Validates SQL syntax and raises specific SQLSyntaxError exceptions.
        
        Args:
            sql: The SQL statement to validate
            
        Raises:
            SQLSyntaxError: When SQL contains syntax errors
        """
        # Check for empty SQL
        if not sql or not sql.strip():
            raise SQLSyntaxError("Empty SQL statement", position=0, line=1)
        
        # Use the parser to validate syntax - this will raise appropriate exceptions
        self.parse(sql)
        
        # If we get here, the SQL is syntactically valid according to the parser

    def split_statements(self, sql: str, skip_validation: bool = False) -> List[str]:
        """
        Split SQL into individual statements using the parser.
        
        Args:
            sql: SQL code potentially containing multiple statements
            skip_validation: If True, skip initial SQL validation
            
        Returns:
            List of individual SQL statements
            
        Raises:
            SQLSyntaxError: When SQL contains syntax errors
            ParserError: When the parser encounters an error
        """
        # Don't validate the whole SQL if skip_validation is True
        # The parser will validate each statement separately
        if not skip_validation:
            try:
                self.validate_sql(sql)
            except SQLSyntaxError as e:
                self.logger.error(f"SQL validation error: {e}")
                raise

        try:
            # Parse SQL into AST expressions
            expressions = self.parse(sql)
            
            # Convert expressions back to SQL strings
            statements = [expr.sql(dialect=self.dialect) for expr in expressions]
            
            # Return non-empty statements
            return [stmt for stmt in statements if stmt.strip()]
            
        except Exception as e:
            # If parsing fails but we need to split anyway (skip_validation=True),
            # fall back to semicolon splitting as a best effort
            if skip_validation:
                self.logger.warning(f"AST parsing failed, falling back to semicolon splitting: {e}")
                return self._fallback_split_statements(sql)
            raise

    def _fallback_split_statements(self, sql: str) -> List[str]:
        """
        Fallback method to split SQL statements by semicolons.
        Used when AST parsing fails but we still need a best-effort split.
        
        Args:
            sql: SQL code to split
            
        Returns:
            List of SQL statements (best effort)
        """
        # Simple semicolon splitting - this won't handle quoted semicolons correctly
        raw_statements = sql.split(';')
        
        # Filter and clean statements
        statements = [stmt.strip() for stmt in raw_statements]
        return [stmt for stmt in statements if stmt]

    def tokenize(self, sql: str) -> Generator[Tuple[str, str], None, None]:
        """
        Tokenize SQL into meaningful components using the parser's tokenizer.
        
        Args:
            sql: SQL statement to tokenize
            
        Returns:
            Generator yielding (token_type, token_value) tuples
            
        Raises:
            ParserError: When tokenization fails
        """
        try:
            # Use sqlglot's tokenizer with appropriate dialect handling
            if self.dialect:
                tokens = sqlglot.tokenize(sql, dialect=self.dialect)
            else:
                tokens = sqlglot.tokenize(sql)  # Use default dialect
            
            for token in tokens:
                # Map sqlglot token types to our expected types
                token_type = self._map_token_type(token.token_type)
                token_value = token.text
                
                # Skip whitespace tokens
                if token_type == 'WHITESPACE':
                    continue
                    
                yield (token_type, token_value)
                
        except Exception as e:
            # Convert any unexpected errors to ParserError
            raise ParserError(
                f"Error during SQL tokenization: {str(e)}",
                source=sql[:100] + '...' if len(sql) > 100 else sql
            ) from e

    def _map_token_type(self, sqlglot_token_type: str) -> str:
        """
        Map sqlglot token types to our expected token types.
        
        Args:
            sqlglot_token_type: Token type from sqlglot
            
        Returns:
            Mapped token type string
        """
        # Map from sqlglot token types to our types
        type_map = {
            'STRING': 'STRING',
            'NUMBER': 'NUMBER',
            'IDENTIFIER': 'IDENTIFIER',
            'KEYWORD': 'KEYWORD',
            'OPERATOR': 'OPERATOR',
            'L_PAREN': 'PAREN',
            'R_PAREN': 'PAREN',
            'SEMICOLON': 'SEMICOLON',
            'WHITESPACE': 'WHITESPACE',
            'COMMENT': 'COMMENT',
        }
        
        # Return mapped type or the original if not in map
        return type_map.get(sqlglot_token_type, sqlglot_token_type)

    def parse_identifiers(self, sql: str) -> List[str]:
        """
        Extract all identifiers from SQL query using the AST.
        
        Args:
            sql: SQL statement to extract identifiers from
            
        Returns:
            List of SQL identifiers found
            
        Raises:
            ParserError: When identifier extraction fails
        """
        try:
            # Parse the SQL into an AST
            expressions = self.parse(sql)
            
            # Extract identifiers from the AST
            identifiers = []
            for expr in expressions:
                # Use sqlglot's built-in traversal to find all identifiers
                for node in expr.find_all(exp.Identifier):
                    # Get the identifier name, handling qualified names
                    identifier = node.name
                    identifiers.append(identifier)
                    
            return identifiers
            
        except Exception as e:
            if isinstance(e, (SQLSyntaxError, ParserError)):
                raise
            # Convert other errors to ParserError
            raise ParserError(f"Error extracting identifiers: {str(e)}")

    def find_table_references(self, sql: str) -> List[Dict[str, Any]]:
        """
        Find all table references in the SQL using the AST.
        
        Args:
            sql: SQL statement to analyze
            
        Returns:
            List of dictionaries with table reference information
            
        Raises:
            ParserError: When table extraction fails
        """
        try:
            # Parse the SQL into an AST
            expressions = self.parse(sql)
            
            # Initialize results
            table_refs = []
            
            # Extract table references from all expressions
            for expr in expressions:
                # Find all table references (FROM, JOIN, etc.)
                for table in expr.find_all(exp.Table):
                    # Get reference information
                    ref_info = {
                        'table': table.name,
                        'alias': table.alias_or_name,
                        'schema': table.db,
                        'catalog': table.catalog,
                        'is_cte': isinstance(table.parent, exp.CTE),
                        'context': self._get_reference_context(table)
                    }
                    table_refs.append(ref_info)
                    
            return table_refs
            
        except Exception as e:
            if isinstance(e, (SQLSyntaxError, ParserError)):
                raise
            # Convert other errors to ParserError
            raise ParserError(f"Error finding table references: {str(e)}")

    def _get_reference_context(self, node: exp.Expression) -> str:
        """
        Determine the context in which a table is referenced.
        
        Args:
            node: AST node to examine
            
        Returns:
            Context string (FROM, JOIN, etc.)
        """
        # Walk up the tree to find the context
        parent = node.parent
        
        while parent:
            if isinstance(parent, exp.From):
                return 'FROM'
            elif isinstance(parent, exp.Join):
                return f'{parent.kind} JOIN'
            elif isinstance(parent, exp.Into):
                return 'INTO'
            elif isinstance(parent, exp.With):
                return 'WITH'
                
            parent = parent.parent
            
        return 'UNKNOWN'

    def find_temp_tables(self, sql: str, patterns: List[str]) -> List[Dict[str, Any]]:
        """
        Find temporary table definitions and references in the SQL.
        
        Args:
            sql: SQL statement to analyze
            patterns: List of patterns to identify temp tables
            
        Returns:
            List of dictionaries with temp table information
            
        Raises:
            ParserError: When extraction fails
        """
        try:
            # First compile patterns for temp table identification
            import re
            temp_patterns = [re.compile(pattern) for pattern in patterns]
            
            # Parse the SQL into an AST
            expressions = self.parse(sql)
            
            # Find all table definitions and references
            temp_tables = []
            
            for expr in expressions:
                # Look for SELECT INTO statements
                for select in expr.find_all(exp.Select):
                    if hasattr(select, 'into') and select.into:
                        table_name = select.into.name
                        
                        # Check if this is a temp table
                        if any(pattern.search(table_name) for pattern in temp_patterns):
                            # Create a definition record
                            temp_info = {
                                'name': table_name,
                                'type': 'SELECT_INTO',
                                'definition': select,
                                'defined_expr': expr,
                                'dependencies': self._find_dependencies(select, temp_patterns)
                            }
                            temp_tables.append(temp_info)
                
                # Look for CREATE TEMP TABLE statements
                for create in expr.find_all(exp.Create):
                    if hasattr(create, 'this') and create.this:
                        table_name = create.this.name
                        
                        # Check if this is a temp table
                        if any(pattern.search(table_name) for pattern in temp_patterns):
                            # Get definition type
                            if hasattr(create, 'expression') and create.expression:
                                definition_type = 'CREATE_TEMP_AS'
                            else:
                                definition_type = 'CREATE_TEMP'
                                
                            # Create a definition record
                            temp_info = {
                                'name': table_name,
                                'type': definition_type,
                                'definition': create,
                                'defined_expr': expr,
                                'dependencies': self._find_dependencies(create, temp_patterns)
                            }
                            temp_tables.append(temp_info)
            
            return temp_tables
            
        except Exception as e:
            if isinstance(e, (SQLSyntaxError, ParserError)):
                raise
            # Convert other errors to ParserError
            raise ParserError(f"Error finding temp tables: {str(e)}")

    def _find_dependencies(self, node: exp.Expression, temp_patterns: List) -> List[str]:
        """
        Find dependencies on other temp tables in a definition.
        
        Args:
            node: AST node to examine
            temp_patterns: List of compiled patterns for temp tables
            
        Returns:
            List of temp table names this definition depends on
        """
        dependencies = []
        
        # Find all table references in this node
        for table in node.find_all(exp.Table):
            table_name = table.name
            
            # Check if this is a reference to a temp table
            if any(pattern.search(table_name) for pattern in temp_patterns):
                dependencies.append(table_name)
                
        return dependencies

    def replace_references(self, expr: exp.Expression, replacements: Dict[str, str]) -> exp.Expression:
        """
        Replace table references in an AST expression.
        
        Args:
            expr: AST expression to modify
            replacements: Dictionary mapping original names to replacements
            
        Returns:
            Modified AST expression
        """
        # Make a copy of the expression to avoid modifying the original
        new_expr = expr.copy()
        
        # Replace all table references
        for table in new_expr.find_all(exp.Table):
            table_name = table.name
            if table_name in replacements:
                # Replace the table name with the new name
                table.set('this', exp.to_identifier(replacements[table_name]))
                
        return new_expr

    def to_sql(self, expr: exp.Expression) -> str:
        """
        Convert an AST expression back to SQL text.
        
        Args:
            expr: AST expression to convert
            
        Returns:
            SQL string
        """
        if self.dialect:
            return expr.sql(dialect=self.dialect)
        else:
            return expr.sql()  # Use default dialect

    def generate_cte(self, name: str, definition: exp.Expression) -> exp.With:
        """
        Generate a CTE expression from a subquery definition.
        
        Args:
            name: Name for the CTE
            definition: AST expression defining the CTE
            
        Returns:
            With expression representing the CTE
        """
        # Create a CTE node with the given name and definition
        if isinstance(definition, exp.Select):
            # For SELECT statements, use directly
            cte = exp.With(
                expressions=[
                    exp.CTE(
                        this=exp.to_identifier(name),
                        expression=definition
                    )
                ]
            )
        else:
            # For other statements, extract the SELECT part if possible
            select_part = definition.find(exp.Select)
            if select_part:
                cte = exp.With(
                    expressions=[
                        exp.CTE(
                            this=exp.to_identifier(name),
                            expression=select_part
                        )
                    ]
                )
            else:
                # Fallback - convert to SQL and parse as a subquery
                sql = f"SELECT * FROM ({self.to_sql(definition)}) AS subquery"
                parsed = self.parse(sql)[0]
                
                cte = exp.With(
                    expressions=[
                        exp.CTE(
                            this=exp.to_identifier(name),
                            expression=parsed
                        )
                    ]
                )
                
        return cte