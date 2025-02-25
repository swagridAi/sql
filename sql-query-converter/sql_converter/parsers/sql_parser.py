import re
import logging
from pathlib import Path
from typing import List, Dict, Optional, Generator, Tuple, Union, Match

from sql_converter.exceptions import SQLSyntaxError, ParserError


class SQLParser:
    """Parser for SQL statements with comprehensive error handling."""
    
    def __init__(self, dialect: str = 'ansi'):
        self.dialect = dialect.lower()
        self.logger = logging.getLogger(__name__)
        self.comment_handlers = {
            'ansi': self._handle_ansi_comments,
            'tsql': self._handle_tsql_comments,
            'mysql': self._handle_mysql_comments,
        }

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
        
        # Split into statements for statement-level validation
        try:
            statements = self.split_statements(sql, skip_validation=True)
        except Exception:
            # Fall back to whole script validation if splitting fails
            statements = [sql]
        
        # Validate each statement separately
        for stmt in statements:
            self._validate_statement(stmt)
    
    def _validate_statement(self, stmt: str) -> None:
        """
        Validates a single SQL statement.
        
        Args:
            stmt: The SQL statement to validate
            
        Raises:
            SQLSyntaxError: When SQL contains syntax errors
        """
        # Find line number for error messages
        def get_line_number(position: int) -> int:
            """Get line number for a position in the SQL string."""
            return stmt[:position].count('\n') + 1
        
        # Check for basic syntax errors with more precise error messages
        if "FROM WHERE" in stmt.upper():
            match = re.search(r'FROM\s+WHERE', stmt, re.IGNORECASE)
            if match:
                position = match.start()
                raise SQLSyntaxError(
                    "Missing table name between FROM and WHERE clauses",
                    position=position,
                    line=get_line_number(position)
                )
        
        # Check for unbalanced parentheses with position tracking
        if stmt.count('(') != stmt.count(')'):
            # Find the position where parentheses become unbalanced
            balance = 0
            for i, char in enumerate(stmt):
                if char == '(':
                    balance += 1
                elif char == ')':
                    balance -= 1
                    if balance < 0:
                        # Too many closing parentheses
                        raise SQLSyntaxError(
                            "Unbalanced parentheses: unexpected ')'",
                            position=i,
                            line=get_line_number(i)
                        )
            # If we get here with positive balance, there are too many opening parentheses
            if balance > 0:
                raise SQLSyntaxError(
                    f"Unbalanced parentheses: missing {balance} closing parentheses",
                    position=len(stmt),
                    line=get_line_number(len(stmt))
                )
        
        # Check for unbalanced quotes with detailed error messages
        try:
            self._check_balanced_quotes(stmt)
        except SQLSyntaxError as e:
            # Re-raise with line number information
            position = getattr(e, 'position', None)
            if position is not None:
                line = get_line_number(position)
                raise SQLSyntaxError(
                    e.message,
                    position=position,
                    line=line
                ) from None
            raise
        
        # Check for JOIN without ON clause
        join_without_on = re.search(r'\bJOIN\b(?:(?!\bON\b).)*?(?:\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|$)', 
                                   stmt, re.IGNORECASE | re.DOTALL)
        if join_without_on and not re.search(r'\bCROSS\s+JOIN\b', stmt, re.IGNORECASE):
            # Exclude CROSS JOIN which doesn't need ON
            match_text = join_without_on.group(0)
            if not re.search(r'\bUSING\b', match_text, re.IGNORECASE):  # Also exclude JOIN USING
                position = join_without_on.start()
                raise SQLSyntaxError(
                    "JOIN clause missing ON condition",
                    position=position,
                    line=get_line_number(position)
                )
        
        # Check for invalid GROUP BY syntax - within a single statement
        group_where = re.search(r'\bGROUP\s+BY\b.*?\bWHERE\b', stmt, re.IGNORECASE | re.DOTALL)
        if group_where:
            position = group_where.start()
            raise SQLSyntaxError(
                "WHERE clause must come before GROUP BY",
                position=position,
                line=get_line_number(position)
            )

    def _check_balanced_quotes(self, sql: str) -> None:
        """
        Check for balanced single and double quotes in SQL.
        
        Args:
            sql: The SQL statement to check
            
        Raises:
            SQLSyntaxError: When quotes are unbalanced
        """
        # Track quotation state
        in_single_quote = False
        in_double_quote = False
        escaped = False
        
        for i, char in enumerate(sql):
            # Handle escape sequences
            if escaped:
                escaped = False
                continue
                
            if char == '\\':
                escaped = True
                continue
                
            # Toggle quote state
            if char == "'":
                # Handle escaped single quotes ('') in SQL
                if in_single_quote and i + 1 < len(sql) and sql[i + 1] == "'":
                    # This is an escaped quote, skip the next one
                    escaped = True
                    continue
                in_single_quote = not in_single_quote
                    
            elif char == '"':
                # Handle escaped double quotes ("") in SQL
                if in_double_quote and i + 1 < len(sql) and sql[i + 1] == '"':
                    # This is an escaped quote, skip the next one
                    escaped = True
                    continue
                in_double_quote = not in_double_quote
                
        # Check final state
        if in_single_quote:
            raise SQLSyntaxError("Unbalanced single quotes", position=len(sql) - 1)
        if in_double_quote:
            raise SQLSyntaxError("Unbalanced double quotes", position=len(sql) - 1)

    def split_statements(self, sql: str, skip_validation: bool = False) -> List[str]:
        """
        Split SQL into individual statements while handling comments, strings, and nesting.
        
        Args:
            sql: SQL code potentially containing multiple statements
            skip_validation: If True, skip initial SQL validation
            
        Returns:
            List of individual SQL statements
            
        Raises:
            ParserError: When the parser encounters an unrecoverable error
            SQLSyntaxError: When SQL contains syntax errors
        """
        # Validate the overall SQL first (unless skipped)
        if not skip_validation:
            try:
                self.validate_sql(sql)
            except SQLSyntaxError as e:
                self.logger.error(f"SQL validation error: {e}")
                raise
            
        statements = []
        current = []
        state = {
            'in_string': False,
            'string_char': None,
            'in_comment': False,
            'comment_type': None,
            'paren_depth': 0,
            'bracket_depth': 0,
            'escape_next': False,
        }

        # Use regex to replace comments with spaces
        # First, remove block comments (/* ... */)
        sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
        
        # Then handle line comments (--) by replacing until end of line
        # Make sure to preserve newlines
        sql = re.sub(r'--.*?(\n|$)', '\n', sql, flags=re.DOTALL)

        try:
            for i, char in enumerate(sql):
                # Handle string literals
                if state['in_string']:
                    if state['escape_next']:
                        state['escape_next'] = False
                    elif char == '\\':
                        state['escape_next'] = True
                    elif char == state['string_char']:
                        state['in_string'] = False
                        state['string_char'] = None
                elif char in ("'", '"'):
                    state['in_string'] = True
                    state['string_char'] = char
                
                # Handle parentheses and brackets (only when not in a string)
                if not state['in_string']:
                    if char == '(':
                        state['paren_depth'] += 1
                    elif char == ')':
                        state['paren_depth'] = max(0, state['paren_depth'] - 1)
                    
                    # Handle brackets for TSQL
                    if self.dialect == 'tsql':
                        if char == '[':
                            state['bracket_depth'] += 1
                        elif char == ']':
                            state['bracket_depth'] = max(0, state['bracket_depth'] - 1)
                
                # Add character to current statement
                current.append(char)
                
                # Check for statement termination
                if (char == ';' and 
                    not state['in_string'] and 
                    state['paren_depth'] == 0 and 
                    state['bracket_depth'] == 0):
                    
                    statement = ''.join(current).strip()
                    if statement:
                        statements.append(statement)
                    current = []
                    state.update({
                        'in_string': False,
                        'string_char': None,
                        'paren_depth': 0,
                        'bracket_depth': 0,
                        'escape_next': False,
                    })
                
        except Exception as e:
            # Convert any unexpected errors to ParserError with context
            position = i if 'i' in locals() else 0
            raise ParserError(
                f"Error while parsing SQL: {str(e)}",
                source=sql[:100] + '...' if len(sql) > 100 else sql
            ) from e

        # Add remaining content if not empty
        final_statement = ''.join(current).strip()
        if final_statement:
            statements.append(final_statement)

        # Filter out any empty statements
        return [stmt for stmt in statements if stmt]

    def _handle_ansi_comments(self, char: str, state: Dict, position: int) -> None:
        """
        Handle standard SQL comments (-- and /* */ style).
        
        Args:
            char: Current character being processed
            state: Current parser state dictionary
            position: Current position in the SQL string
        """
        # This method is deprecated as we now handle comments directly in split_statements
        pass

    def _handle_tsql_comments(self, char: str, state: Dict, position: int) -> None:
        """
        Handle T-SQL specific comments.
        
        Args:
            char: Current character being processed
            state: Current parser state dictionary
            position: Current position in the SQL string
        """
        # This method is deprecated as we now handle comments directly in split_statements
        pass

    def _handle_mysql_comments(self, char: str, state: Dict, position: int) -> None:
        """
        Handle MySQL specific comments (# style).
        
        Args:
            char: Current character being processed
            state: Current parser state dictionary
            position: Current position in the SQL string
        """
        # This method is deprecated as we now handle comments directly in split_statements
        pass

    def tokenize(self, sql: str) -> Generator[Tuple[str, str], None, None]:
        """
        Tokenize SQL into meaningful components.
        
        Args:
            sql: SQL statement to tokenize
            
        Returns:
            Generator yielding (token_type, token_value) tuples
            
        Raises:
            ParserError: When tokenization fails
        """
        try:
            # First, preprocess to remove comments
            clean_sql = self._remove_comments(sql)
            
            token_spec = [
                ('STRING',      r"'(''|[^'])*'"),     # Single-quoted strings
                ('STRING',      r'"([^"]|"")*"'),     # Double-quoted strings
                ('NUMBER',      r'\d+(\.\d+)?([eE][+-]?\d+)?'),  # Numbers
                ('KEYWORD',     r'\b(SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|'
                                r'JOIN|INTO|CREATE|TEMP|TABLE|AS|AND|OR|'
                                r'GROUP BY|ORDER BY|HAVING|LIMIT)\b', re.IGNORECASE),
                ('IDENTIFIER',  r'[a-zA-Z_][a-zA-Z0-9_#@$]*'),  # Identifiers
                ('OPERATOR',    r'[+\-*/%=<>!~&|^]'),  # Operators
                ('PAREN',       r'[()]'),              # Parentheses
                ('BRACKET',     r'[\[\]]'),            # Brackets
                ('SEMICOLON',   r';'),                 # Statement terminator
                ('WHITESPACE',  r'\s+'),               # Whitespace
            ]

            tok_regex = '|'.join(
                f'(?P<{name}>{pattern})' for name, pattern in token_spec
            )
            flags = re.DOTALL | re.IGNORECASE
            
            for match in re.finditer(tok_regex, clean_sql, flags):
                kind = match.lastgroup
                value = match.group().strip()
                if kind == 'WHITESPACE':
                    continue
                yield (kind, value)
                
        except Exception as e:
            # Convert any unexpected errors to ParserError
            raise ParserError(
                f"Error during SQL tokenization: {str(e)}",
                source=sql[:100] + '...' if len(sql) > 100 else sql
            ) from e

    def _remove_comments(self, sql: str) -> str:
        """
        Remove SQL comments to simplify tokenization.
        
        Args:
            sql: SQL statement containing comments
            
        Returns:
            SQL with comments removed
        """
        try:
            # First, remove /* */ block comments
            pattern = r'/\*[\s\S]*?\*/'
            sql = re.sub(pattern, ' ', sql)
            
            # Then, remove -- line comments (up to end of line)
            pattern = r'--.*?$'
            sql = re.sub(pattern, ' ', sql, flags=re.MULTILINE)
            
            # Finally, remove # MySQL style comments
            pattern = r'#.*?$'
            sql = re.sub(pattern, ' ', sql, flags=re.MULTILINE)
            
            return sql
        except Exception as e:
            # Convert regex errors to ParserError
            raise ParserError(f"Error removing comments: {str(e)}")

    def parse_identifiers(self, sql: str) -> List[str]:
        """
        Extract all identifiers from SQL query.
        
        Args:
            sql: SQL statement to extract identifiers from
            
        Returns:
            List of SQL identifiers found
            
        Raises:
            ParserError: When identifier extraction fails
        """
        try:
            identifiers = []
            for kind, value in self.tokenize(sql):
                if kind == 'IDENTIFIER':
                    # Handle quoted identifiers
                    clean_value = value.strip('[]"\'`')
                    identifiers.append(clean_value)
            return identifiers
        except Exception as e:
            if isinstance(e, ParserError):
                raise
            # Convert other errors to ParserError
            raise ParserError(f"Error extracting identifiers: {str(e)}")