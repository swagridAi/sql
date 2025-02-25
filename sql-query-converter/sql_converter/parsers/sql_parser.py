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

    def split_statements(self, sql: str) -> List[str]:
        """
        Split SQL into individual statements while handling:
        - Nested parentheses/brackets
        - String literals
        - Different comment types
        - Dialect-specific syntax
        """
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
            prev_state = state.copy()
            state['prev_char'] = prev_state['prev_char']
            
            # Update state before processing current character
            self._update_state(char, state)

            if not state['in_comment']:
                current.append(char)

            # Check for statement termination
            if self._is_statement_end(char, state, prev_state):
                statement = ''.join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
                state.update({
                    'in_string': False,
                    'string_char': None,
                    'in_comment': False,
                    'comment_type': None,
                    'paren_depth': 0,
                    'bracket_depth': 0,
                    'escape_next': False,
                    'prev_char': None,
                })

            state['prev_char'] = char

        # Add remaining content
        final_statement = ''.join(current).strip()
        if final_statement:
            statements.append(final_statement)

        return statements

    def _update_state(self, char: str, state: Dict) -> None:
        """Update parsing state based on current character"""
        handler = self.comment_handlers.get(self.dialect, self._handle_ansi_comments)
        handler(char, state)

        # Handle string literals
        if not state['in_comment'] and not state['escape_next']:
            if char in ("'", '"'):
                if state['in_string'] and char == state['string_char']:
                    state['in_string'] = False
                    state['string_char'] = None
                else:
                    state['in_string'] = True
                    state['string_char'] = char
            elif char == '\\' and state['in_string']:
                state['escape_next'] = True
                return

        # Handle brackets (TSQL)
        if self.dialect == 'tsql' and not state['in_comment']:
            if char == '[':
                state['bracket_depth'] += 1
            elif char == ']':
                state['bracket_depth'] = max(0, state['bracket_depth'] - 1)

        # Handle parentheses
        if not state['in_comment'] and not state['in_string']:
            if char == '(':
                state['paren_depth'] += 1
            elif char == ')':
                state['paren_depth'] = max(0, state['paren_depth'] - 1)

        state['escape_next'] = False

    def _is_statement_end(self, char: str, state: Dict, prev_state: Dict) -> bool:
        """Determine if current character ends a statement"""
        # Handle T-SQL GO statements
        if self.dialect == 'tsql' and state['prev_char'] == 'G' and char == 'O':
            return True
        
        # Handle standard semicolon termination
        return (
            char == ';' 
            and not state['in_string']
            and not state['in_comment']
            and state['paren_depth'] == 0
            and state['bracket_depth'] == 0
        )

    def _handle_ansi_comments(self, char: str, state: Dict) -> None:
        """Handle standard SQL comments (- and /* */ style)"""
        if state['in_comment']:
            if state['comment_type'] == 'block':
                if state['prev_char'] == '*' and char == '/':
                    state['in_comment'] = False
                    state['comment_type'] = None
            elif state['comment_type'] == 'line' and char == '\n':
                state['in_comment'] = False
                state['comment_type'] = None
        else:
            if char == '-' and state['prev_char'] == '-':
                state['in_comment'] = True
                state['comment_type'] = 'line'
                state['prev_char'] = None  # Reset to avoid double detection
            elif char == '*' and state['prev_char'] == '/':
                state['in_comment'] = True
                state['comment_type'] = 'block'
                state['prev_char'] = None

    def _handle_tsql_comments(self, char: str, state: Dict) -> None:
        """Handle T-SQL specific comments (same as ANSI plus GO statements)"""
        self._handle_ansi_comments(char, state)

    def _handle_mysql_comments(self, char: str, state: Dict) -> None:
        """Handle MySQL specific comments (# style)"""
        if char == '#' and not state['in_comment']:
            state['in_comment'] = True
            state['comment_type'] = 'line'
        else:
            self._handle_ansi_comments(char, state)

    def tokenize(self, sql: str) -> Generator[Tuple[str, str], None, None]:
        """
        Tokenize SQL into meaningful components
        Yields (token_type, token_value) tuples
        """
        token_spec = [
            ('STRING',      r"'(''|[^'])*'"),     # Single-quoted strings
            ('STRING',      r'"([^"]|"")*"'),     # Double-quoted strings
            ('COMMENT',     r'--[^\n]*'),         # Line comments
            ('COMMENT',     r'/\*.*?\*/', re.DOTALL),  # Block comments
            ('NUMBER',      r'\d+(\.\d+)?([eE][+-]?\d+)?'),  # Numbers
            ('KEYWORD',     r'\b(SELECT|INSERT|UPDATE[|DELETE|FROM|WHERE|'
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
            f'(?P<{name}>{pattern})' for name, pattern, *flags in token_spec
        )
        flags = re.DOTALL | re.IGNORECASE
        for match in re.finditer(tok_regex, sql, flags):
            kind = match.lastgroup
            value = match.group().strip()
            if kind == 'WHITESPACE':
                continue
            if kind == 'COMMENT':
                value = value.replace('\n', ' ')
            yield (kind, value)

    def parse_identifiers(self, sql: str) -> List[str]:
        """Extract all identifiers from SQL query"""
        identifiers = []
        for kind, value in self.tokenize(sql):
            if kind == 'IDENTIFIER':
                # Handle quoted identifiers
                clean_value = value.strip('[]"\'`')
                identifiers.append(clean_value)
        return identifiers

if __name__ == '__main__':
    # Test implementation
    logging.basicConfig(level=logging.DEBUG)
    
    test_sql = """
    -- ANSI style comment
    SELECT * FROM users WHERE name = 'John; Doe' AND age > 25;
    
    /* Multi-line
       comment */
    CREATE TEMP TABLE #temp_results AS (
        SELECT department, AVG(salary) AS avg_salary
        FROM employees
        WHERE hiredate > '2020-01-01'
        GROUP BY department
    );
    
    INSERT INTO #temp_results VALUES ('HR', 65000);
    """
    
    print("Testing ANSI SQL parsing:")
    parser = SQLParser(dialect='ansi')
    statements = parser.split_statements(test_sql)
    for i, stmt in enumerate(statements, 1):
        print(f"\nStatement {i}:")
        print(stmt)
    
    print("\nTokens:")
    for token in parser.tokenize(test_sql):
        print(token)
    
    print("\nIdentifiers:")
    print(parser.parse_identifiers(test_sql))