"""
SQL Temp Table to CTE Converter (Final Working Version)
"""

import re
from typing import List, Tuple, Dict, Optional

DEBUG = True
LOG_FILE = "sql_conversion.log"

class TempTableConverter:
    def __init__(self):
        self.cte_definitions: List[Tuple[str, str]] = []
        self.main_queries: List[str] = []
        self.temp_table_map: Dict[str, str] = {}
        self.current_temp_table: Optional[str] = None
        self.log_file = open(LOG_FILE, 'w') if DEBUG else None

    def __del__(self):
        if self.log_file:
            self.log_file.close()

    def _log(self, *messages):
        """Log messages to file"""
        if DEBUG and self.log_file:
            msg = ' '.join(str(m) for m in messages)
            self.log_file.write(msg + '\n')

    def convert(self, sql: str) -> str:
        """Main conversion entry point"""
        self._log("=== Starting Conversion ===")
        statements = self._split_statements(sql)
        
        for stmt in statements:
            self._process_statement(stmt)
        
        return self._build_final_query()

    def _split_statements(self, sql: str) -> List[str]:
        """Split SQL into statements handling nested structures"""
        statements = []
        current = []
        in_string = False
        string_char = None
        paren_depth = 0

        for char in sql:
            if char in ("'", '"') and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
            elif char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1

            current.append(char)

            if not in_string and paren_depth == 0 and char == ';':
                statements.append(''.join(current).strip())
                current = []

        if current:
            statements.append(''.join(current).strip())
            
        self._log(f"Split into {len(statements)} statements")
        return statements

    def _process_statement(self, stmt: str) -> None:
        """Process a single SQL statement"""
        if self._handle_temp_creation(stmt):
            return

        if self.current_temp_table:
            self._handle_temp_insert(stmt)
        else:
            self._add_main_query(stmt)

    def _handle_temp_creation(self, stmt: str) -> bool:
        """Handle temp table creation patterns"""
        return any(
            handler(stmt)
            for handler in [
                self._handle_select_into,
                self._handle_create_temp_as,
                self._handle_create_temp
            ]
        )

    def _handle_select_into(self, stmt: str) -> bool:
        """Handle SELECT ... INTO with improved pattern matching"""
        pattern = re.compile(
            r'^\s*SELECT\s+(?P<select_clause>.+?)\s+INTO\s+(?P<table>#?\w+)\s+(?P<remainder>FROM.*)',
            re.IGNORECASE | re.DOTALL
        )

        match = pattern.match(stmt)
        if not match:
            return False

        try:
            table = match.group('table').lstrip('#')
            select = match.group('select_clause')
            from_clause = match.group('remainder')

            full_query = f"SELECT {select}\n{from_clause}"
            self._log(f"Reconstructed query for {table}:\n{full_query[:500]}...")

            self.cte_definitions.append((table, full_query))
            self.temp_table_map[match.group('table')] = table
            return True

        except Exception as e:
            self._log(f"Error processing SELECT INTO: {str(e)}")
            return False

    def _handle_create_temp_as(self, stmt: str) -> bool:
        """Handle CREATE TEMP TABLE ... AS SELECT"""
        match = re.match(
            r'^\s*CREATE\s+TEMP\s+TABLE\s+(\w+)\s+AS\s+(SELECT.*)',
            stmt, re.IGNORECASE | re.DOTALL
        )
        if not match:
            return False

        table, subquery = match.groups()
        self.cte_definitions.append((table, subquery))
        self.temp_table_map[table] = table
        return True

    def _handle_create_temp(self, stmt: str) -> bool:
        """Handle CREATE TEMP TABLE without AS SELECT"""
        match = re.match(r'^\s*CREATE\s+TEMP\s+TABLE\s+(\w+)', stmt, re.IGNORECASE)
        if not match:
            return False

        self.current_temp_table = match.group(1)
        return True

    def _handle_temp_insert(self, stmt: str) -> None:
        """Handle INSERT INTO temp table"""
        match = re.match(
            rf'^\s*INSERT\s+INTO\s+{re.escape(self.current_temp_table)}\s+(SELECT.*)',
            stmt, re.IGNORECASE | re.DOTALL
        )
        if match:
            self.cte_definitions.append((self.current_temp_table, match.group(1)))
            self.temp_table_map[self.current_temp_table] = self.current_temp_table
            self.current_temp_table = None
        else:
            self._add_main_query(stmt)

    def _add_main_query(self, stmt: str) -> None:
        """Add main query with temp references replaced"""
        processed = stmt
        for temp, cte in self.temp_table_map.items():
            processed = re.sub(rf'\b{re.escape(temp)}\b', cte, processed, flags=re.IGNORECASE)
        self.main_queries.append(processed)

    def _build_final_query(self) -> str:
        """Construct final CTE query with proper formatting"""
        if not self.cte_definitions:
            return ' '.join(self.main_queries)

        cte_clauses = [
            f"{name} AS (\n{self._indent(query)}\n)"
            for name, query in self.cte_definitions
        ]
        return f"WITH {',\n     '.join(cte_clauses)}\n" + '\n'.join(self.main_queries)

    def _indent(self, sql: str) -> str:
        """Indent SQL blocks consistently"""
        return '\n'.join(f"    {line}" for line in sql.split('\n'))


# Example usage
if __name__ == "__main__":
    test_sql = """
    SELECT DISTINCT 
        fdm.setsk, 
        fdm.dealsk, 
        buh.busunithlevel02name, 
        re.glreportingentitycode, 
        drea.glreportingentitycode AS affglreportingentitycode, 
        dmt.measuretypecode, 
        gla.glaccountcode, 
        dpt.productgroup 
    INTO #distinct_deal_measure
    FROM miris.dimdealset dds
    INNER JOIN (
        SELECT 
            setsk, dealsk, glbusinessunitsk, measuretypesk, glaccountsk, glreportingentitysk 
        FROM miris.factdealmeasure fdm 
        WHERE setsk = 15658
    ) fdm 
    ON dds.dealsk = fdm.dealsk AND fdm.setsk = 15658 AND dds.setsk = 15658
    LEFT JOIN dbo.gldimaccount gla 
    ON gla.glaccountsk = fdm.glaccountsk
    LEFT JOIN dbo.dimmeasuretype dmt 
    ON fdm.measuretypesk = dmt.measuretypesk
    LEFT JOIN dbo.dimproducttype dpt 
    ON dpt.producttypesk = dds.producttypesk
    LEFT JOIN dbo.gldimbusinessunithierarchy buh 
    ON fdm.glbusinessunitsk = buh.glbusinessunitsk 
    AND buh.busunithtrecode = 'bunit_stat' 
    AND buh.rowstatus = 'A'
    LEFT JOIN dbo.gldimreportingentity re 
    ON fdm.glreportingentitysk = re.glreportingentitysk
    LEFT JOIN dbo.gldimreportingentity drea 
    ON dds.glaffiliatereportingentitysk = drea.glreportingentitysk
    INNER JOIN (
        SELECT DISTINCT 
            reg.glreportingentitysk, 
            reg.glreportingentitycode, 
            cr.reportingconsumercode 
        FROM dbo.dimglreportingentitygroupmembership reg
        INNER JOIN dbo.dimglconsumerelevance cr
        ON reg.glreportingentitygroupcode = cr.glreportingentitygroupcode 
        AND cr.rowstatus = 'A'
        WHERE cr.reportingconsumercode LIKE '%EMEA%'
        AND reg.rowstatus = 'A'
    ) emea 
    ON emea.glreportingentitysk = dds.glreportingentitysk
    WHERE fdm.setsk = 15658
    AND NOT (
        (re.glreportingentitycode = drea.glreportingentitycode) 
        AND SUBSTRING(gla.glaccountcode, 5, 2) = '99'
    );
    """

    converter = TempTableConverter()
    converted_sql = converter.convert(test_sql)
    print("Converted SQL:")
    print(converted_sql)