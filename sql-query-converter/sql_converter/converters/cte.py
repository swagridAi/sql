import re
import logging
from typing import List, Tuple, Dict, Optional, Any, Match, Pattern, Set

from sql_converter.converters.base import BaseConverter
from sql_converter.parsers.sql_parser import SQLParser
from sql_converter.exceptions import ConverterError, ValidationError, SQLSyntaxError, ConfigError


class CTEConverter(BaseConverter):
    """Converts SQL queries with temporary tables to Common Table Expressions (CTEs)."""
    
    # Precompile regex patterns for performance
    _SELECT_INTO_PATTERN = re.compile(
        r'^\s*SELECT\s+(?P<select_clause>.+?)\s+INTO\s+(?P<table>\S+)\s+(?P<remainder>FROM.*)',
        re.IGNORECASE | re.DOTALL
    )
    
    _CREATE_TEMP_AS_PATTERN1 = re.compile(
        r'^\s*CREATE\s+TEMP\s+TABLE\s+(?P<table>\S+)\s+AS\s*(?P<query>SELECT.*)',
        re.IGNORECASE | re.DOTALL
    )
    
    _CREATE_TEMP_AS_PATTERN2 = re.compile(
        r'^\s*CREATE\s+TEMP\s+TABLE\s+(?P<table>\S+)\s+AS\s*\((?P<query>SELECT.*?)(?:\)|;|\s*$)',
        re.IGNORECASE | re.DOTALL
    )
    
    _CREATE_TEMP_PATTERN = re.compile(
        r'^\s*CREATE\s+TEMP\s+TABLE\s+(?P<table>\S+)',
        re.IGNORECASE
    )
    
    _INSERT_INTO_PATTERN = re.compile(
        r'^\s*INSERT\s+INTO\s+(?P<table>\S+)\s+(?P<query>SELECT.*)',
        re.IGNORECASE | re.DOTALL
    )
    
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
        
        # Initialize components
        self.parser = SQLParser()
        
        # Compile temp table regex from patterns
        try:
            self.temp_table_regex = self._process_patterns(temp_table_patterns)
        except Exception as e:
            raise ConfigError(f"Failed to process temp table patterns: {str(e)}")
        
        # Conversion state - will be reset for each conversion
        self.temp_tables = {}
        self.temp_table_order = []  # Track order of appearance
        self.current_temp_table = None

    def _process_patterns(self, patterns: List[str]) -> str:
        """
        Convert configuration patterns to regex pattern.
        
        Args:
            patterns: List of pattern strings
            
        Returns:
            Compiled regex pattern string
            
        Raises:
            ConfigError: When pattern processing fails
        """
        if not patterns:
            raise ConfigError("No temp table patterns provided")
            
        regex_fragments = []
        for i, pattern in enumerate(patterns):
            try:
                # Convert simplified pattern to regex
                processed = (
                    pattern.replace('?', '.?')
                           .replace('*', '.*')
                           .replace('#', r'\#')
                )
                regex_fragments.append(processed)
            except Exception as e:
                self.logger.warning(f"Invalid pattern '{pattern}' at index {i}: {str(e)}")
        
        if not regex_fragments:
            self.logger.warning("No valid patterns found, using default pattern '#.*'")
            return r'\#.*'
            
        return '|'.join(regex_fragments)

    def convert(self, sql: str) -> str:
        """
        Convert SQL with temp tables to use CTEs.
        
        Args:
            sql: SQL query text to convert
            
        Returns:
            Converted SQL using CTEs
            
        Raises:
            ConverterError: For general conversion errors
            ValidationError: For validation errors
            SQLSyntaxError: For SQL syntax errors
        """
        try:
            # Reset state for this conversion
            self.temp_tables = {}
            self.temp_table_order = []  # Reset temp table order
            self.current_temp_table = None
            
            # Phase 1: Split the SQL into statements
            statements = self.parser.split_statements(sql)
            
            # Phase 2: Analyze SQL and identify temp tables
            self._identify_temp_tables(statements)
            
            # Phase 3: Build dependency graph
            dependency_graph = self._build_dependency_graph(statements)
            
            # Phase 4: Generate CTEs in topological order
            ctes = self._generate_ctes(dependency_graph)
            
            # Phase 5: Transform remaining statements
            main_query = self._transform_main_query(statements)
            
            # Phase 6: Assemble the final query
            return self._assemble_final_query(ctes, main_query)
            
        except SQLSyntaxError as e:
            self.logger.error(f"SQL syntax error during conversion: {e}")
            raise
        except ValidationError as e:
            self.logger.error(f"Validation error during conversion: {e}")
            raise
        except Exception as e:
            error_msg = f"Failed to convert SQL: {str(e)}"
            self.logger.error(error_msg)
            snippet = sql[:100] + "..." if len(sql) > 100 else sql
            raise ConverterError(error_msg, source=snippet) from e

    def _identify_temp_tables(self, statements: List[str]) -> None:
        """
        Identify temporary tables and their definitions in SQL statements.
        
        Args:
            statements: List of SQL statements
        """
        # Initialize/reset the temp table order tracking
        self.temp_table_order = []
        
        for stmt in statements:
            # Check for "SELECT ... INTO #temp"
            select_into_match = self._SELECT_INTO_PATTERN.match(stmt)
            if select_into_match:
                table_name = select_into_match.group('table')
                if self._is_temp_table(table_name):
                    # Only add to order list the first time we see it
                    if table_name not in self.temp_tables:
                        self.temp_table_order.append(table_name)
                        
                    select_clause = select_into_match.group('select_clause')
                    from_clause = select_into_match.group('remainder')
                    definition = f"SELECT {select_clause}\n{from_clause}"
                    
                    self.temp_tables[table_name] = {
                        'definition': definition,
                        'cte_name': self._get_cte_name(table_name),
                        'type': 'SELECT_INTO',
                        'statement': stmt
                    }
                    continue
                    
            # Check for "CREATE TEMP TABLE #temp AS SELECT ..."
            create_temp_match = (self._CREATE_TEMP_AS_PATTERN1.match(stmt) or 
                                self._CREATE_TEMP_AS_PATTERN2.match(stmt))
            if create_temp_match:
                table_name = create_temp_match.group('table')
                if self._is_temp_table(table_name):
                    # Only add to order list the first time we see it
                    if table_name not in self.temp_tables:
                        self.temp_table_order.append(table_name)
                        
                    definition = create_temp_match.group('query').strip()
                    if definition.endswith(';'):
                        definition = definition[:-1]
                    
                    self.temp_tables[table_name] = {
                        'definition': definition,
                        'cte_name': self._get_cte_name(table_name),
                        'type': 'CREATE_TEMP_AS',
                        'statement': stmt
                    }
                    continue
            
            # Check for "CREATE TEMP TABLE" followed by "INSERT INTO"
            create_temp_match = self._CREATE_TEMP_PATTERN.match(stmt)
            if create_temp_match:
                table_name = create_temp_match.group('table')
                if self._is_temp_table(table_name):
                    # Only add to order list the first time we see it
                    if table_name not in self.temp_tables:
                        self.temp_table_order.append(table_name)
                        
                    self.current_temp_table = table_name
                    continue
            
            # Check for "INSERT INTO #temp"
            if self.current_temp_table:
                insert_pattern = re.compile(
                    rf'^\s*INSERT\s+INTO\s+{re.escape(self.current_temp_table)}\s+(?P<query>SELECT.*)',
                    re.IGNORECASE | re.DOTALL
                )
                insert_match = insert_pattern.match(stmt)
                if insert_match:
                    definition = insert_match.group('query').strip()
                    if definition.endswith(';'):
                        definition = definition[:-1]
                    
                    self.temp_tables[self.current_temp_table] = {
                        'definition': definition,
                        'cte_name': self._get_cte_name(self.current_temp_table),
                        'type': 'INSERT_INTO',
                        'statement': stmt
                    }
                    self.current_temp_table = None
                    continue

    def _is_temp_table(self, table_name: str) -> bool:
        """
        Check if a table name matches temp table patterns.
        
        Args:
            table_name: Table name to check
            
        Returns:
            True if it's a temp table, False otherwise
        """
        return bool(re.search(self.temp_table_regex, table_name))

    def _get_cte_name(self, temp_name: str) -> str:
        """
        Generate a CTE name from a temp table name.
        
        Args:
            temp_name: Original temp table name
            
        Returns:
            Cleaned name suitable for a CTE
        """
        return temp_name.lstrip('#').replace('.', '_')

    def _extract_table_references(self, sql: str) -> Set[str]:
        """
        Extract all table references from SQL that match temp table patterns.
        
        Args:
            sql: SQL statement to analyze
            
        Returns:
            Set of referenced temp table names
        """
        references = set()
    
        # FIXED: Improved regex to better catch table references
        # Look for FROM/JOIN followed by anything that's not a space, comma, semicolon, or parenthesis
        pattern = re.compile(
            r'(?:FROM|JOIN)\s+(?:\w+\.)?([^\s,;()]+)',
            re.IGNORECASE
        )
        
        # Also specifically look for temp tables with # prefix
        temp_pattern = re.compile(r'#\w+', re.IGNORECASE)
        
        # Check regular FROM/JOIN references
        for match in pattern.finditer(sql):
            table_ref = match.group(1)
            if self._is_temp_table(table_ref) and table_ref in self.temp_tables:
                references.add(table_ref)
        
        # ADDED: Also look for direct # references anywhere
        for match in temp_pattern.finditer(sql):
            table_ref = match.group(0)
            if table_ref in self.temp_tables:
                references.add(table_ref)
        
        return references

    def _build_dependency_graph(self, statements: List[str]) -> Dict[str, List[str]]:
        """
        Build a dependency graph between temp tables.
        
        Args:
            statements: List of SQL statements
            
        Returns:
            Dictionary mapping temp tables to their dependencies
        """
        dependency_graph = {name: [] for name in self.temp_tables}
        
        # Process defined temp tables first
        for temp_name, temp_info in self.temp_tables.items():
            # Extract table references from the definition
            definition = temp_info['definition']
            references = self._extract_table_references(definition)
            
            for ref in references:
                if ref in self.temp_tables and ref != temp_name:  # Avoid self-references
                    dependency_graph[temp_name].append(ref)
        
        # Find any references in the main query
        for stmt in statements:
            # Skip statements that define temp tables
            if any(info['statement'] == stmt for info in self.temp_tables.values()):
                continue
                
            # Check for implicit dependencies between temp tables
            # that are referenced in the same statement
            references = self._extract_table_references(stmt)
            if len(references) > 1:
                # Multiple temp tables are referenced in this statement
                # Create implicit dependencies based on order of reference
                references_list = list(references)
                for i in range(len(references_list) - 1):
                    for j in range(i + 1, len(references_list)):
                        ref1 = references_list[i]
                        ref2 = references_list[j]
                        if ref2 not in dependency_graph[ref1]:
                            dependency_graph[ref1].append(ref2)
        
        return dependency_graph

    def _generate_ctes(self, dependency_graph: Dict[str, List[str]]) -> List[Tuple[str, str]]:
        """
        Generate CTEs in proper dependency order using topological sort while 
        preserving original order within same dependency level.
        
        Args:
            dependency_graph: Dependency graph of temp tables
            
        Returns:
            List of (cte_name, definition) tuples in proper order
        """
        # Track original order of appearance using our explicit tracking list
        original_order = {name: idx for idx, name in enumerate(self.temp_table_order)}
        
        # Helper function for topological sort
        def topological_sort():
            # Track permanent and temporary marks for cycle detection
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
                for dependency in dependency_graph.get(node, []):
                    visit(dependency)
                    
                temporary_mark.remove(node)
                permanent_mark.add(node)
                result.append(node)
                
            # Visit all nodes
            for node in dependency_graph:
                if node not in permanent_mark:
                    visit(node)
                    
            return result
        
        # Get the ordered list of temp table names
        ordered_temp_tables = topological_sort()
        
        # Calculate dependency level for each table
        levels = {}
        for node in ordered_temp_tables:
            # Calculate level (max level of dependencies + 1)
            max_dep_level = 0
            for dep in dependency_graph.get(node, []):
                if dep in levels:
                    max_dep_level = max(max_dep_level, levels[dep] + 1)
            levels[node] = max_dep_level
        
        # Group nodes by level
        level_groups = {}
        for node, level in levels.items():
            if level not in level_groups:
                level_groups[level] = []
            level_groups[level].append(node)
        
        # Sort each level by original order
        for level in level_groups:
            # Use get() with a default to handle any tables not in our order list
            level_groups[level].sort(key=lambda x: original_order.get(x, float('inf')))
        
        # Build final ordered list respecting both dependencies and original order
        final_ordered_tables = []
        for level in sorted(level_groups.keys()):
            final_ordered_tables.extend(level_groups[level])
        
        # Generate CTE definitions
        ctes = []
        for temp_name in final_ordered_tables:
            # Get the cleaned name and definition
            cte_name = self.temp_tables[temp_name]['cte_name']
            definition = self.temp_tables[temp_name]['definition']
            
            # FIXED: Use the same improved pattern for replacing references
            for ref_temp_name in self.temp_tables:
                if ref_temp_name != temp_name:  # Avoid self-references
                    ref_cte_name = self.temp_tables[ref_temp_name]['cte_name']
                    # Use the same improved pattern we used in _transform_main_query
                    pattern = r'(?<![a-zA-Z0-9_])' + re.escape(ref_temp_name) + r'(?![a-zA-Z0-9_])'
                    definition = re.sub(pattern, ref_cte_name, definition, flags=re.IGNORECASE)
            
            ctes.append((cte_name, definition))
        
        return ctes

    def _is_temp_definition(self, stmt: str) -> bool:
        """
        Check if a statement defines a temp table.
        
        Args:
            stmt: SQL statement to check
            
        Returns:
            True if it defines a temp table, False otherwise
        """
        # Check if this statement matches any of the temp table definition patterns
        for temp_info in self.temp_tables.values():
            if temp_info['statement'] == stmt:
                return True
        return False

    def _transform_main_query(self, statements: List[str]) -> str:
        """
        Transform the main query by replacing temp table references with CTE names.
        
        Args:
            statements: List of SQL statements
            
        Returns:
            Transformed main query
        """
        # Filter out statements that define temp tables
        main_statements = []
        for stmt in statements:
            if not self._is_temp_definition(stmt):
                main_statements.append(stmt)
        
        # Replace temp table references in remaining statements
        transformed_statements = []
        for stmt in main_statements:
            transformed = stmt
            for temp_name, info in self.temp_tables.items():
                cte_name = info['cte_name']
                pattern = r'(?<![a-zA-Z0-9_])' + re.escape(temp_name) + r'(?![a-zA-Z0-9_])'
                transformed = re.sub(pattern, cte_name, transformed, flags=re.IGNORECASE)
            transformed_statements.append(transformed)
        
        # Join statements WITHOUT stripping semicolons
        return "\n".join(transformed_statements)  # Removed the rstrip(';

    def _assemble_final_query(self, ctes: List[Tuple[str, str]], main_query: str) -> str:
        """
        Assemble the final query with CTEs and main query.
        
        Args:
            ctes: List of (cte_name, definition) tuples
            main_query: The main query string
            
        Returns:
            Complete SQL with CTEs
        """
        if not ctes:
            return main_query
        
        # Format each CTE with proper indentation
        cte_clauses = []
        for name, definition in ctes:
            # Clean and indent the definition
            clean_def = definition.rstrip(';')
            indented_def = self._indent(clean_def)
            cte_clauses.append(f"{name} AS (\n{indented_def}\n)")
        
        # Check if the original query had semicolons BEFORE stripping them
        had_semicolon = main_query.rstrip().endswith(';')
        
        # Now strip the semicolon for formatting
        main_query = main_query.rstrip(';')
        
        # Use the saved flag to determine whether to add a semicolon
        if had_semicolon:
            return f"WITH {',\n'.join(cte_clauses)}\n{main_query};"
        else:
            return f"WITH {',\n'.join(cte_clauses)}\n{main_query}"

    def _indent(self, sql: str) -> str:
        """
        Apply configured indentation to SQL.
        
        Args:
            sql: SQL string to indent
            
        Returns:
            Indented SQL
        """
        indent = ' ' * self.indent_spaces
        return '\n'.join(f"{indent}{line}" for line in sql.split('\n'))