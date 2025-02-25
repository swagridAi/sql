# sql_converter/parsers/sql_parser.py
import re
import logging
from pathlib import Path
from typing import List, Dict, Optional, Generator, Tuple

class SQLParser:
    def __init__(self, dialect: str = 'ansi'):
        self.dialect = dialect.lower()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.comment_handlers = {
            'ansi': self._handle_ansi_comments,
            'tsql': self._handle_tsql_comments,
            'mysql': self._handle_mysql_comments,
        }

    def validate_sql(self, sql: str) -> None:
        """
        Performs validation on SQL syntax and raises ValueError for invalid SQL.
        """
        # Check for basic syntax errors
        if "FROM WHERE" in sql.upper():
            raise ValueError("Invalid SQL syntax: FROM clause missing table name")
        
        # Check for unbalanced parentheses
        if sql.count('(') != sql.count(')'):
            raise ValueError("Invalid SQL syntax: Unbalanced parentheses")
        
        # Check for unbalanced quotes
        single_quotes = sql.count("'") - sql.count("''")  # Account for escaped quotes
        if single_quotes % 2 != 0:
            raise ValueError("Invalid SQL syntax: Unbalanced single quotes")
        
        double_quotes = sql.count('"') - sql.count('""')  # Account for escaped quotes
        if double_quotes % 2 != 0:
            raise ValueError("Invalid SQL syntax: Unbalanced double quotes")
        
        # Check for common SQL syntax errors
        if re.search(r'\bSELECT\b.*\bFROM\b.*\bJOIN\b.*\bWHERE\b', sql, re.IGNORECASE) and \
           not re.search(r'\bJOIN\b.*\bON\b', sql, re.IGNORECASE):
            raise ValueError("Invalid SQL syntax: JOIN missing ON clause")
        
        # Check for invalid GROUP BY syntax
        if re.search(r'\bGROUP\s+BY\b.*\bWHERE\b', sql, re.IGNORECASE):
            raise ValueError("Invalid SQL syntax: WHERE clause must come before GROUP BY")
        
        # Check for missing FROM clause in SELECT
        if re.search(r'\bSELECT\b.*\bWHERE\b', sql, re.IGNORECASE) and \
           not re.search(r'\bFROM\b', sql, re.IGNORECASE):
            raise ValueError("Invalid SQL syntax: SELECT with WHERE but missing FROM clause")

    def split_statements(self, sql: str) -> List[str]:
        """
        Split SQL into individual statements while handling:
        - Nested parentheses/brackets
        - String literals
        - Different comment types
        - Dialect-specific syntax
        """
        # Validate the overall SQL first
        try:
            self.validate_sql(sql)
        except ValueError as e:
            self.logger.error(f"SQL validation error: {str(e)}")
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
            'prev_char': None,
        }

        for char in sql:
            # Update state before processing current character
            self._update_state(char, state)

            # Skip adding character if it's part of a comment
            if not state['in_comment']:
                current.append(char)
            
            # Check for statement termination
            if (char == ';' and 
                not state['in_string'] and 
                not state['in_comment'] and 
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
                # Don't reset comment state or prev_char as they might continue

        # Add remaining content if not empty
        final_statement = ''.join(current).strip()
        if final_statement:
            statements.append(final_statement)

        # Filter out any empty statements
        return [stmt for stmt in statements if stmt]

    def _update_state(self, char: str, state: Dict) -> None:
        """Update parsing state based on current character"""
        # First check if we're in a comment
        if state['in_comment']:
            # Handle comment state
            handler = self.comment_handlers.get(self.dialect, self._handle_ansi_comments)
            handler(char, state)
            if state['in_comment'] == False:
                # If we just exited a comment, reset prev_char to avoid
                # misinterpreting the end of a comment as the start of something else
                state['prev_char'] = None
            else:
                # Update prev_char only if still in comment
                state['prev_char'] = char
            return
        
        # Handle string literals and escaping
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
        
        # Only check for comments if we're not in a string
        if not state['in_string']:
            handler = self.comment_handlers.get(self.dialect, self._handle_ansi_comments)
            handler(char, state)
            
            # Handle brackets (TSQL)
            if self.dialect == 'tsql' and not state['in_comment']:
                if char == '[':
                    state['bracket_depth'] += 1
                elif char == ']':
                    state['bracket_depth'] = max(0, state['bracket_depth'] - 1)
            
            # Handle parentheses
            if not state['in_comment']:
                if char == '(':
                    state['paren_depth'] += 1
                elif char == ')':
                    state['paren_depth'] = max(0, state['paren_depth'] - 1)
        
        # Update previous character for next iteration if not in comment
        if not state['in_comment']:
            state['prev_char'] = char

    def _handle_ansi_comments(self, char: str, state: Dict) -> None:
        """Handle standard SQL comments (-- and /* */ style)"""
        if state['in_comment']:
            # Handle end of comments
            if state['comment_type'] == 'block':
                if state['prev_char'] == '*' and char == '/':
                    state['in_comment'] = False
                    state['comment_type'] = None
            elif state['comment_type'] == 'line' and char == '\n':
                state['in_comment'] = False
                state['comment_type'] = None
        elif not state['in_string']:  # Only check for comments when not in a string
            # Handle start of comments
            if char == '-' and state['prev_char'] == '-':
                state['in_comment'] = True
                state['comment_type'] = 'line'
            elif char == '/' and state['prev_char'] == '*':
                # This is actually an end of block comment marker 
                # that wasn't caught in the first part, ignore it
                pass
            elif char == '*' and state['prev_char'] == '/':
                state['in_comment'] = True
                state['comment_type'] = 'block'

    def _handle_tsql_comments(self, char: str, state: Dict) -> None:
        """Handle T-SQL specific comments (same as ANSI plus GO statements)"""
        self._handle_ansi_comments(char, state)
        
        # Handle GO statements outside of comments and strings
        # (handled in split_statements as they terminate statements)

    def _handle_mysql_comments(self, char: str, state: Dict) -> None:
        """Handle MySQL specific comments (# style)"""
        if state['in_comment']:
            # Let ANSI handler manage comment endings
            self._handle_ansi_comments(char, state)
        elif not state['in_string'] and char == '#':
            state['in_comment'] = True
            state['comment_type'] = 'line'
        else:
            self._handle_ansi_comments(char, state)

    def tokenize(self, sql: str) -> Generator[Tuple[str, str], None, None]:
        """
        Tokenize SQL into meaningful components
        Yields (token_type, token_value) tuples
        """
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

    def _remove_comments(self, sql: str) -> str:
        """
        Remove SQL comments to simplify tokenization.
        Handles both -- line comments and /* */ block comments.
        """
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

    def parse_identifiers(self, sql: str) -> List[str]:
        """Extract all identifiers from SQL query"""
        identifiers = []
        for kind, value in self.tokenize(sql):
            if kind == 'IDENTIFIER':
                # Handle quoted identifiers
                clean_value = value.strip('[]"\'`')
                identifiers.append(clean_value)
        return identifiers