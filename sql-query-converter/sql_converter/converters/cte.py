# sql_converter/converters/cte.py
import re
import logging
from typing import List, Tuple, Dict, Optional, Any  # Added Any
from sql_converter.converters.base import BaseConverter  # Absolute import
from sql_converter.parsers.sql_parser import SQLParser  # Absolute import


class CTEConverter(BaseConverter):
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Configuration with defaults
        self.indent_spaces = self.config.get('indent_spaces', 4)
        temp_table_patterns = self.config.get('temp_table_patterns', ['#.*'])
        
        # Initialize components
        self.parser = SQLParser()
        self.temp_table_regex = self._process_patterns(temp_table_patterns)
        
        # Conversion state
        self.cte_definitions: List[Tuple[str, str]] = []
        self.main_queries: List[str] = []
        self.temp_table_map: Dict[str, str] = {}
        self.current_temp_table: Optional[str] = None

    def _process_patterns(self, patterns: List[str]) -> str:
        """Convert config patterns to regex pattern"""
        regex_fragments = []
        for pattern in patterns:
            try:
                # Convert simplified pattern to regex
                processed = (
                    pattern.replace('?', '.?')
                           .replace('*', '.*')
                           .replace('#', r'\#')
                )
                regex_fragments.append(processed)
            except Exception as e:
                self.logger.warning(f"Invalid pattern '{pattern}': {str(e)}")
        
        return '|'.join(regex_fragments) or r'\#.*'

    def convert(self, sql: str) -> str:
        """Main conversion entry point"""
        self._reset_state()
        statements = self.parser.split_statements(sql)
        
        for stmt in statements:
            self._process_statement(stmt)
        
        return self._build_final_query()

    def _reset_state(self):
        """Reset converter state between conversions"""
        self.cte_definitions.clear()
        self.main_queries.clear()
        self.temp_table_map.clear()
        self.current_temp_table = None

    def _process_statement(self, stmt: str) -> None:
        """Process a single SQL statement"""
        if self._handle_temp_creation(stmt):
            return

        if self.current_temp_table:
            self._handle_temp_insert(stmt)
        else:
            self._add_main_query(stmt)

    def _handle_temp_creation(self, stmt: str) -> bool:
        """Handle temp table creation using configured patterns"""
        return any([
            self._handle_select_into(stmt),
            self._handle_create_temp_as(stmt),
            self._handle_create_temp(stmt)
        ])

    def _handle_select_into(self, stmt: str) -> bool:
        """Handle SELECT INTO pattern"""
        pattern = re.compile(
            r'^\s*SELECT\s+(?P<select_clause>.+?)\s+INTO\s+(?P<table>\S+)\s+(?P<remainder>FROM.*)',
            re.IGNORECASE | re.DOTALL
        )

        if not (match := pattern.match(stmt)):
            return False

        try:
            raw_table = match.group('table')
            if not re.search(self.temp_table_regex, raw_table):
                return False

            clean_name = raw_table.lstrip('#').replace('.', '_')
            full_query = f"SELECT {match.group('select_clause')}\n{match.group('remainder')}"
            
            self.cte_definitions.append((clean_name, full_query))
            self.temp_table_map[raw_table] = clean_name
            return True

        except Exception as e:
            self.logger.error(f"SELECT INTO conversion failed: {str(e)}")
            return False

    def _handle_create_temp_as(self, stmt: str) -> bool:
        """Handle CREATE TEMP TABLE AS SELECT"""
        pattern = re.compile(
            r'^\s*CREATE\s+TEMP\s+TABLE\s+(?P<table>\S+)\s+AS\s+(?P<query>SELECT.*)',
            re.IGNORECASE | re.DOTALL
        )

        if not (match := pattern.match(stmt)):
            return False

        raw_table = match.group('table')
        if not re.search(self.temp_table_regex, raw_table):
            return False

        clean_name = raw_table.lstrip('#').replace('.', '_')
        self.cte_definitions.append((clean_name, match.group('query')))
        self.temp_table_map[raw_table] = clean_name
        return True

    def _handle_create_temp(self, stmt: str) -> bool:
        """Handle CREATE TEMP TABLE without AS SELECT"""
        pattern = re.compile(
            r'^\s*CREATE\s+TEMP\s+TABLE\s+(?P<table>\S+)',
            re.IGNORECASE
        )

        if not (match := pattern.match(stmt)):
            return False

        raw_table = match.group('table')
        if not re.search(self.temp_table_regex, raw_table):
            return False

        self.current_temp_table = raw_table
        return True

    def _handle_temp_insert(self, stmt: str) -> None:
        """Handle INSERT INTO temp table"""
        pattern = re.compile(
            rf'^\s*INSERT\s+INTO\s+{re.escape(self.current_temp_table)}\s+(?P<query>SELECT.*)',
            re.IGNORECASE | re.DOTALL
        )

        if (match := pattern.match(stmt)):
            clean_name = self.current_temp_table.lstrip('#').replace('.', '_')
            self.cte_definitions.append((clean_name, match.group('query')))
            self.temp_table_map[self.current_temp_table] = clean_name
            self.current_temp_table = None
        else:
            self._add_main_query(stmt)

    def _add_main_query(self, stmt: str) -> None:
        """Replace temp references in final queries"""
        processed = stmt
        for temp, cte in self.temp_table_map.items():
            processed = re.sub(
                rf'\b{re.escape(temp)}\b', 
                cte, 
                processed, 
                flags=re.IGNORECASE
            )
        self.main_queries.append(processed)

    def _build_final_query(self) -> str:
        """Construct final CTE query"""
        if not self.cte_definitions:
            return ';\n'.join(self.main_queries)

        cte_clauses = [
            f"{name} AS (\n{self._indent(query)}\n)"
            for name, query in self.cte_definitions
        ]
        return (
            f"WITH {',\n'.join(cte_clauses)}\n"
            f"{';\n'.join(self.main_queries)}"
        )

    def _indent(self, sql: str) -> str:
        """Apply configured indentation"""
        indent = ' ' * self.indent_spaces
        return '\n'.join(f"{indent}{line}" for line in sql.split('\n'))
    
converter = CTEConverter(config={
    'indent_spaces': 4,
    'temp_table_patterns': ['#.*']
})

sql = "SELECT * INTO #temp FROM users;"
print(converter.convert(sql))