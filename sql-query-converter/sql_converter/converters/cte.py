# sql_converter/converters/cte.py
import re
import logging
from typing import List, Tuple, Dict, Optional, Any
from sql_converter.converters.base import BaseConverter
from sql_converter.parsers.sql_parser import SQLParser


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
        """Main conversion entry point with multi-pass handling for nested temps"""
        self._reset_state()
        
        try:
            # Initial validation of complete SQL
            self.parser.validate_sql(sql)
            
            # Split into statements
            statements = self.parser.split_statements(sql)
            
            # First pass: Identify all temp tables and their definitions
            for stmt in statements:
                self._process_statement(stmt)
            
            # Second pass: Resolve nested references between temp tables
            self._resolve_nested_references()
            
            # Build the final query with properly ordered CTEs
            return self._build_final_query()
        
        except ValueError as e:
            self.logger.error(f"SQL conversion failed: {str(e)}")
            raise

    def _reset_state(self):
        """Reset converter state between conversions"""
        self.cte_definitions.clear()
        self.main_queries.clear()
        self.temp_table_map.clear()
        self.current_temp_table = None

    def _process_statement(self, stmt: str) -> None:
        """Process a single SQL statement"""
        # Validate the statement before processing
        try:
            self.parser.validate_sql(stmt)
        except ValueError as e:
            self.logger.error(f"Invalid SQL statement: {str(e)}")
            raise ValueError(f"Failed to process SQL: {str(e)}")
            
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
            
            # Build and clean the full query
            full_query = f"SELECT {match.group('select_clause')}\n{match.group('remainder')}"
            if full_query.endswith(';'):
                full_query = full_query[:-1]
            
            self.cte_definitions.append((clean_name, full_query))
            self.temp_table_map[raw_table] = clean_name
            return True

        except Exception as e:
            self.logger.error(f"SELECT INTO conversion failed: {str(e)}")
            return False

    def _handle_create_temp_as(self, stmt: str) -> bool:
        """Handle CREATE TEMP TABLE AS SELECT with or without parentheses"""
        # Pattern for: CREATE TEMP TABLE #name AS SELECT...
        pattern1 = re.compile(
            r'^\s*CREATE\s+TEMP\s+TABLE\s+(?P<table>\S+)\s+AS\s*(?P<query>SELECT.*)',
            re.IGNORECASE | re.DOTALL
        )
        
        # Pattern for: CREATE TEMP TABLE #name AS (SELECT...)
        pattern2 = re.compile(
            r'^\s*CREATE\s+TEMP\s+TABLE\s+(?P<table>\S+)\s+AS\s*\((?P<query>SELECT.*?)(?:\)|;|\s*$)',
            re.IGNORECASE | re.DOTALL
        )

        match = pattern1.match(stmt) or pattern2.match(stmt)
        if not match:
            return False

        raw_table = match.group('table')
        if not re.search(self.temp_table_regex, raw_table):
            return False

        clean_name = raw_table.lstrip('#').replace('.', '_')
        
        # Clean up the query - remove trailing semicolons
        query = match.group('query').strip()
        if query.endswith(';'):
            query = query[:-1]
            
        self.cte_definitions.append((clean_name, query))
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
            
            # Clean the query
            query = match.group('query').strip()
            if query.endswith(';'):
                query = query[:-1]
                
            self.cte_definitions.append((clean_name, query))
            self.temp_table_map[self.current_temp_table] = clean_name
            self.current_temp_table = None
        else:
            self._add_main_query(stmt)

    def _resolve_nested_references(self):
        """Resolve references between temp tables in CTE definitions"""
        # Maximum number of passes to prevent infinite loops
        max_passes = 10
        changes_made = True
        pass_count = 0
        
        # Continue making passes until no more changes or max passes reached
        while changes_made and pass_count < max_passes:
            changes_made = False
            pass_count += 1
            
            # Update all CTE definitions in each pass
            updated_definitions = []
            for name, query in self.cte_definitions:
                original_query = query
                processed_query = query
                
                # Try to replace all temp table references in this CTE definition
                for temp, cte in self.temp_table_map.items():
                    # Use improved pattern to match temp table names correctly
                    if temp.startswith('#'):
                        pattern = rf'(?<![a-zA-Z0-9_]){re.escape(temp)}(?![a-zA-Z0-9_])'
                    else:
                        pattern = rf'\b{re.escape(temp)}\b'
                    
                    processed_query = re.sub(
                        pattern, 
                        cte, 
                        processed_query, 
                        flags=re.IGNORECASE
                    )
                
                # Check if we made any changes in this pass
                if processed_query != original_query:
                    changes_made = True
                    self.logger.debug(f"Resolved nested reference in CTE '{name}'")
                
                updated_definitions.append((name, processed_query))
            
            # Update CTE definitions for next pass
            self.cte_definitions = updated_definitions
            
        if pass_count == max_passes and changes_made:
            self.logger.warning("Reached maximum passes for resolving nested references")

    def _add_main_query(self, stmt: str) -> None:
        """Replace temp references in final queries"""
        processed = stmt
        for temp, cte in self.temp_table_map.items():
            # Use improved pattern to match temp table names correctly
            if temp.startswith('#'):
                pattern = rf'(?<![a-zA-Z0-9_]){re.escape(temp)}(?![a-zA-Z0-9_])'
            else:
                pattern = rf'\b{re.escape(temp)}\b'
                
            processed = re.sub(
                pattern, 
                cte, 
                processed, 
                flags=re.IGNORECASE
            )
        self.main_queries.append(processed)

    def _build_final_query(self) -> str:
        """Construct final CTE query with proper formatting"""
        if not self.cte_definitions:
            return ';\n'.join(query.rstrip(';') for query in self.main_queries)

        # Format each CTE definition
        cte_clauses = []
        for name, query in self.cte_definitions:
            # Ensure query doesn't have trailing semicolons
            clean_query = query.rstrip(';')
            cte_clauses.append(f"{name} AS (\n{self._indent(clean_query)}\n)")
        
        # Format each main query - ensure they don't have trailing semicolons except the last one
        formatted_main_queries = []
        for i, query in enumerate(self.main_queries):
            # Remove any trailing semicolons
            clean_query = query.rstrip(';')
            # Add back semicolon for all but potentially the last query
            if i < len(self.main_queries) - 1:
                formatted_main_queries.append(f"{clean_query};")
            else:
                # For the last query, keep it as is (without trailing semicolon)
                formatted_main_queries.append(clean_query)
        
        # Build the final query
        return f"WITH {',\n'.join(cte_clauses)}\n{' '.join(formatted_main_queries)}"

    def _indent(self, sql: str) -> str:
        """Apply configured indentation"""
        indent = ' ' * self.indent_spaces
        return '\n'.join(f"{indent}{line}" for line in sql.split('\n'))