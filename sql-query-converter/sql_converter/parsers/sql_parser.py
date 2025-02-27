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
    
    def __init__(self, dialect: str = 'tsql'):
        """Initialize with T-SQL as the default dialect"""
        self.dialect_name = dialect.lower()
        self.logger = logging.getLogger(__name__)
        # Always use tsql dialect for our converter
        self.dialect = 'tsql'

    def parse(self, sql: str) -> List[exp.Expression]:
        """
        Parse SQL into AST expressions, optimized for T-SQL.
        
        Args:
            sql: SQL code to parse
            
        Returns:
            List of sqlglot Expression objects representing the parsed SQL
            
        Raises:
            SQLSyntaxError: When SQL contains syntax errors
            ParserError: When the parser encounters an error
        """
        if not sql or not sql.strip():
            raise SQLSyntaxError("Empty SQL statement", position=0, line=1)
            
        try:
            # Parse the SQL into a list of expression trees using T-SQL dialect
            expressions = sqlglot.parse(
                sql, 
                dialect='tsql',  # Always use T-SQL dialect
                error_level='raise',  # Ensure errors are raised for invalid syntax
                # T-SQL specific options
                handle_brackets=True,  # [schema].[table] style identifiers
                identify_variables=True,  # @variable style variables
                #detect_additional_semicolons=True  # Handle extra semicolons (if supported by sqlglot version)
            )
            
            # Filter out expression types that should not be treated as separate statements
            # Semicolons are sometimes parsed as separate expressions but aren't meaningful statements
            filtered_expressions = []
            for expr in expressions:
                if isinstance(expr, exp.Semicolon):
                    # Skip standalone semicolons
                    continue
                    
                # Keep track of potential comments
                if hasattr(expr, '_comments') and expr._comments:
                    # Leave comments attached to expressions but don't count them as separate expressions
                    pass
                    
                filtered_expressions.append(expr)
                
            # Some versions of sqlglot might parse T-SQL GO batch separator - filter if present
            filtered_expressions = [expr for expr in filtered_expressions 
                                if not (hasattr(expr, 'this') and 
                                        getattr(expr, 'this', None) == 'GO')]
            
            # Post-process expressions if needed
            for expr in filtered_expressions:
                # Handle any T-SQL specific post-processing
                # For example, resolving table variables or temp tables
                pass
                
            self.logger.debug(f"Successfully parsed {len(filtered_expressions)} T-SQL statements")
            
            # Perform validation on the parsed expressions
            for expr in filtered_expressions:
                # Verify essential components for SQL statements
                if isinstance(expr, exp.Select) and not expr.find(exp.From):
                    # SELECT statements should have FROM clause in T-SQL (except in specific cases)
                    # This helps catch syntax errors that sqlglot might miss
                    if not expr.find(exp.Where) and not expr.expressions:
                        raise SQLSyntaxError("SELECT statement missing FROM clause", 
                                            ast_node=expr)
                
            return filtered_expressions
            
        except sqlglot.ParseError as e:
            # Extract position information if available
            position = getattr(e, 'position', None)
            line = None
            column = None
            
            if position:
                # Calculate line and column from position
                lines = sql[:position].split('\n')
                line = len(lines)
                column = len(lines[-1]) + 1 if lines else position
            
            # Raise our custom exception with detailed info
            raise SQLSyntaxError(
                f"T-SQL syntax error: {str(e)}",
                source=sql[:100] + '...' if len(sql) > 100 else sql,
                position=position,
                line=line,
                column=column
            ) from e
            
        except sqlglot.TokenError as e:
            # Handle tokenization errors
            raise SQLSyntaxError(
                f"T-SQL tokenization error: {str(e)}",
                source=sql[:100] + '...' if len(sql) > 100 else sql
            ) from e
            
        except Exception as e:
            if isinstance(e, SQLSyntaxError):
                # Re-raise existing SQLSyntaxError
                raise
                
            # Handle any other unexpected errors
            error_type = type(e).__name__
            error_msg = str(e)
            
            self.logger.error(f"Error parsing T-SQL ({error_type}): {error_msg}")
            
            # Handle specific error types with more helpful messages
            if "bracket" in error_msg.lower() or "identifier" in error_msg.lower():
                # Could be an issue with T-SQL bracket identifiers
                context = f"Check for unbalanced brackets [] in table or column names"
                raise ParserError(
                    f"Error parsing T-SQL identifiers: {error_msg}. {context}",
                    source=sql[:100] + '...' if len(sql) > 100 else sql
                ) from e
                
            elif "temporary" in error_msg.lower() or "temp" in error_msg.lower():
                # Could be an issue with temporary tables
                context = f"Check temporary table syntax (tables should use # prefix in T-SQL)"
                raise ParserError(
                    f"Error parsing temporary tables: {error_msg}. {context}",
                    source=sql[:100] + '...' if len(sql) > 100 else sql
                ) from e
                
            # Generic error
            raise ParserError(
                f"Error parsing T-SQL: {error_msg}",
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
        try:
            # Use sqlglot with error_level='raise' to ensure exceptions
            sqlglot.parse(sql, dialect='tsql', error_level='raise')
        except sqlglot.ParseError as e:
            raise SQLSyntaxError(f"SQL syntax error: {str(e)}")
        
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
        Find temporary table definitions and references in T-SQL.
        
        Args:
            sql: SQL statement to analyze
            patterns: List of patterns to identify temp tables (e.g., '#.*')
                
        Returns:
            List of dictionaries with temp table information
                
        Raises:
            ParserError: When extraction fails
        """
        try:
            # Compile patterns for temp table identification
            import re
            temp_patterns = [re.compile(pattern) for pattern in patterns]
            
            # Parse the SQL into AST to help with dependency analysis
            try:
                expressions = self.parse(sql)
            except Exception as e:
                self.logger.warning(f"Parsing error during temp table search: {e}")
                # Fall back to regex-only approach if parsing fails
                expressions = []
            
            # Initialize results
            temp_tables = []
            found_temp_names = set()
            
            # Step 1: Find table definitions using regex (more reliable for T-SQL temp tables)
            # Pattern for SELECT INTO #temp
            select_into_pattern = re.compile(r'SELECT\s+.+?\s+INTO\s+([#][a-zA-Z0-9_]+)', re.IGNORECASE | re.DOTALL)
            # Pattern for CREATE TABLE #temp
            create_table_pattern = re.compile(r'CREATE\s+(TEMP(?:ORARY)?\s+)?TABLE\s+([#][a-zA-Z0-9_]+)', re.IGNORECASE)
            
            # Find all SELECT INTO temp table definitions
            for match in select_into_pattern.finditer(sql):
                temp_name = match.group(1)
                # Check if it matches any of our patterns
                if any(pattern.search(temp_name) for pattern in temp_patterns):
                    # Get the SQL fragment for this definition
                    # (This is approximate - for exact boundaries we'd need a proper parser)
                    stmt_start = sql[:match.start()].rfind(';') + 1
                    if stmt_start < 0:
                        stmt_start = 0
                    stmt_end = sql.find(';', match.end())
                    if stmt_end < 0:
                        stmt_end = len(sql)
                    definition_sql = sql[stmt_start:stmt_end].strip()
                    
                    # Add to results
                    temp_tables.append({
                        'name': temp_name,
                        'type': 'SELECT_INTO',
                        'definition': definition_sql,
                        'defined_expr': None,  # Will try to fill this from parsed expressions
                        'dependencies': []  # Will find dependencies in next step
                    })
                    found_temp_names.add(temp_name)
            
            # Find all CREATE TABLE temp table definitions
            for match in create_table_pattern.finditer(sql):
                temp_name = match.group(2)
                # Check if it matches any of our patterns
                if any(pattern.search(temp_name) for pattern in temp_patterns):
                    # Get the SQL fragment for this definition
                    stmt_start = sql[:match.start()].rfind(';') + 1
                    if stmt_start < 0:
                        stmt_start = 0
                    stmt_end = sql.find(';', match.end())
                    if stmt_end < 0:
                        stmt_end = len(sql)
                    definition_sql = sql[stmt_start:stmt_end].strip()
                    
                    # Add to results
                    temp_tables.append({
                        'name': temp_name,
                        'type': 'CREATE_TABLE',
                        'definition': definition_sql,
                        'defined_expr': None,  # Will try to fill this from parsed expressions
                        'dependencies': []  # Will find dependencies in next step
                    })
                    found_temp_names.add(temp_name)
            
            # Step 2: Connect with AST if possible
            if expressions:
                # Match definitions to AST expressions
                for temp_info in temp_tables:
                    temp_name = temp_info['name']
                    for expr in expressions:
                        # For SELECT INTO
                        if temp_info['type'] == 'SELECT_INTO':
                            select = expr.find(exp.Select)
                            if select and hasattr(select, 'into'):
                                into_expr = select.into
                                if into_expr and hasattr(into_expr, 'name') and into_expr.name == temp_name:
                                    temp_info['defined_expr'] = expr
                                    break
                        # For CREATE TABLE
                        elif temp_info['type'] == 'CREATE_TABLE':
                            create = expr.find(exp.Create)
                            if create and hasattr(create, 'this') and create.this:
                                create_name = create.this.name
                                if create_name == temp_name:
                                    temp_info['defined_expr'] = expr
                                    break
            
            # Step 3: Find dependencies between temp tables
            for temp_info in temp_tables:
                definition_sql = temp_info['definition']
                dependencies = []
                
                # Look for references to other temp tables in this definition
                for other_name in found_temp_names:
                    if other_name != temp_info['name'] and re.search(r'\b' + re.escape(other_name) + r'\b', definition_sql):
                        dependencies.append(other_name)
                
                # If we have an AST, use it to verify dependencies
                if temp_info['defined_expr']:
                    ast_dependencies = []
                    for table in temp_info['defined_expr'].find_all(exp.Table):
                        table_name = table.name
                        if table_name in found_temp_names and table_name != temp_info['name']:
                            ast_dependencies.append(table_name)
                    
                    # If AST found dependencies, use those (more accurate)
                    if ast_dependencies:
                        dependencies = ast_dependencies
                
                # Store dependencies
                temp_info['dependencies'] = dependencies
            
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
            Modified AST expression with table references replaced
        """
        if not replacements:
            return expr  # No replacements to make
        
        # Make a deep copy to avoid modifying the original expression
        new_expr = expr.copy()
        
        # Keep track of changes for logging
        changes_made = 0
        
        # Find all table references (direct table nodes)
        for table in new_expr.find_all(exp.Table):
            # Get the table name
            if hasattr(table, 'this') and table.this and hasattr(table.this, 'this'):
                # Get raw table name
                table_name = table.this.this
                
                # Check if it's in our replacements dictionary
                if table_name in replacements:
                    # Get the replacement name
                    new_name = replacements[table_name]
                    
                    # Replace the table identifier
                    table.this = exp.Identifier(this=new_name)
                    changes_made += 1
                    self.logger.debug(f"Replaced table reference: {table_name} → {new_name}")
                    
            # Alternative approach if the above doesn't work - try using table.name
            elif hasattr(table, 'name') and table.name in replacements:
                table_name = table.name
                new_name = replacements[table_name]
                
                # Create a new identifier and set it
                table.set('this', exp.to_identifier(new_name))
                changes_made += 1
                self.logger.debug(f"Replaced table reference: {table_name} → {new_name}")
        
        # Find all column references that might refer to tables (t.column format)
        for column in new_expr.find_all(exp.Column):
            if hasattr(column, 'table') and column.table:
                table_ref = column.table
                
                # Get the table name from the table reference
                if hasattr(table_ref, 'this'):
                    table_name = table_ref.this
                else:
                    # Try direct access if 'this' attribute is not available
                    table_name = table_ref
                    
                # Check if it's a string or object
                if isinstance(table_name, str) and table_name in replacements:
                    # Replace with new name
                    new_name = replacements[table_name]
                    column.set('table', exp.to_identifier(new_name))
                    changes_made += 1
                    self.logger.debug(f"Replaced column table reference: {table_name} → {new_name}")
                elif hasattr(table_name, 'this') and table_name.this in replacements:
                    # Handle nested structure
                    original_name = table_name.this
                    new_name = replacements[original_name]
                    column.set('table', exp.to_identifier(new_name))
                    changes_made += 1
                    self.logger.debug(f"Replaced nested column table reference: {original_name} → {new_name}")
        
        # Find all CTE references (WITH ... AS)
        for cte in new_expr.find_all(exp.CTE):
            if hasattr(cte, 'this') and hasattr(cte.this, 'this'):
                cte_name = cte.this.this
                if cte_name in replacements:
                    new_name = replacements[cte_name]
                    cte.this = exp.Identifier(this=new_name)
                    changes_made += 1
                    self.logger.debug(f"Replaced CTE reference: {cte_name} → {new_name}")
        
        # Handle table aliases (FROM table AS alias)
        for alias in new_expr.find_all(exp.TableAlias):
            if hasattr(alias, 'this') and hasattr(alias.this, 'this'):
                # Check the underlying table
                underlying_table = alias.this
                if isinstance(underlying_table, exp.Table) and hasattr(underlying_table, 'this') and hasattr(underlying_table.this, 'this'):
                    table_name = underlying_table.this.this
                    if table_name in replacements:
                        new_name = replacements[table_name]
                        underlying_table.this = exp.Identifier(this=new_name)
                        changes_made += 1
                        self.logger.debug(f"Replaced aliased table: {table_name} → {new_name}")
        
        # Handle INTO references (SELECT ... INTO #temp)
        select_nodes = new_expr.find_all(exp.Select)
        for select in select_nodes:
            if hasattr(select, 'into') and select.into:
                into_ref = select.into
                if hasattr(into_ref, 'this') and hasattr(into_ref.this, 'this'):
                    table_name = into_ref.this.this
                    if table_name in replacements:
                        new_name = replacements[table_name]
                        into_ref.this = exp.Identifier(this=new_name)
                        changes_made += 1
                        self.logger.debug(f"Replaced INTO reference: {table_name} → {new_name}")
                elif hasattr(into_ref, 'name') and into_ref.name in replacements:
                    table_name = into_ref.name
                    new_name = replacements[table_name]
                    into_ref.set('this', exp.to_identifier(new_name))
                    changes_made += 1
                    self.logger.debug(f"Replaced INTO reference: {table_name} → {new_name}")
        
        # Log summary of changes
        if changes_made > 0:
            self.logger.debug(f"Made {changes_made} replacements in expression")
        else:
            self.logger.debug("No replacements made in expression")
        
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
            With expression representing the CTE in T-SQL format
        """
        self.logger.debug(f"Generating CTE '{name}' from definition")
        
        try:
            # Ensure definition is appropriate for a CTE
            if not isinstance(definition, (exp.Select, exp.Union, exp.Except, exp.Intersect)):
                # If it's not a query expression, try to extract a query
                select_expr = definition.find(exp.Select)
                if select_expr:
                    definition = select_expr
                    self.logger.debug(f"Extracted SELECT from definition for CTE '{name}'")
                else:
                    # If we can't find a SELECT, wrap the expression in a subquery
                    self.logger.debug(f"Wrapping definition in subquery for CTE '{name}'")
                    
                    # Convert to SQL and back to ensure it works as a subquery
                    sql_text = self.to_sql(definition)
                    definition = exp.Select(
                        expressions=[exp.Star()],
                        from_=exp.From(
                            this=exp.Subquery(
                                this=definition
                            )
                        )
                    )
                    self.logger.debug(f"Created wrapper subquery for CTE '{name}'")
            
            # Create the CTE identifier with the provided name
            cte_identifier = exp.to_identifier(name)
            
            # Create the CTE node with the name and definition
            cte = exp.CTE(
                this=cte_identifier,
                expression=definition
            )
            
            # Create the WITH expression with the CTE
            with_expr = exp.With(
                expressions=[cte]
            )
            
            # Verify the output SQL looks correct
            sql_output = self.to_sql(with_expr)
            
            # Basic validation check to help troubleshoot
            if f"WITH {name}" not in sql_output:
                self.logger.warning(f"Generated CTE SQL doesn't contain expected 'WITH {name}': {sql_output}")
                
                # Try alternate explicit construction method if the first approach failed
                # Sometimes sqlglot requires different construction methods depending on version
                self.logger.debug("Trying alternate CTE construction method")
                
                # Construct a raw SQL string and re-parse it
                cte_sql = f"WITH {name} AS ({self.to_sql(definition)}) SELECT * FROM {name}"
                try:
                    alternate_expr = self.parse(cte_sql)[0]
                    with_expr = alternate_expr.find(exp.With)
                    self.logger.debug("Alternate CTE construction successful")
                except Exception as e:
                    self.logger.warning(f"Alternate CTE construction failed: {e}")
            
            return with_expr
            
        except Exception as e:
            self.logger.error(f"Error generating CTE '{name}': {e}")
            # Create a basic WITH expression as a fallback
            # This might not be perfect but should be better than failing completely
            try:
                # Template-based fallback
                cte_sql = f"WITH {name} AS (SELECT 1 AS dummy)"
                template = self.parse(cte_sql)[0]
                with_expr = template.find(exp.With)
                
                # Replace the template definition with the actual definition
                if with_expr and hasattr(with_expr, 'expressions') and with_expr.expressions:
                    cte_node = with_expr.expressions[0]
                    if hasattr(cte_node, 'expression'):
                        cte_node.expression = definition
                
                return with_expr
            except Exception as fallback_error:
                self.logger.error(f"CTE generation fallback also failed: {fallback_error}")
                # Last resort: return a minimal WITH that may not be perfect
                return exp.With(
                    expressions=[
                        exp.CTE(
                            this=exp.Identifier(this=name),
                            expression=definition
                        )
                    ]
                )