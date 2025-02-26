-- Test handling of various SQL syntax edge cases

-- Edge case 1: Quoted identifiers with special characters
SELECT 
    "column.with.dots", 
    "column with spaces",
    "column""with""quotes"
INTO #quoted_identifiers
FROM "schema.name"."table name";

-- Edge case 2: Multiple statements on one line with comments
SELECT * INTO #oneliner FROM users; /* inline comment */ SELECT * FROM #oneliner;

-- Edge case 3: Unicode characters in identifiers and strings
SELECT * INTO #unicode FROM products WHERE category = N'家電製品' AND "説明書" IS NOT NULL;

-- Edge case 4: Empty result set with complex CASE expression
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM #oneliner) THEN 'Exists' 
        ELSE 'Empty' 
    END AS result,
    CASE
        WHEN COUNT(*) > 0 THEN COUNT(*)
        ELSE NULL
    END AS record_count
INTO #edge_case
FROM #quoted_identifiers
WHERE 1 = 0;

-- Edge case 5: Nested UNION and temp table references
SELECT * INTO #unions FROM (
    SELECT id, 'type1' AS source FROM #quoted_identifiers
    UNION ALL
    SELECT id, 'type2' AS source FROM #unicode
    UNION ALL
    SELECT NULL AS id, 'type3' AS source FROM #edge_case
) combined_data;

-- Final query using all temp tables
SELECT * FROM #unions
ORDER BY COALESCE(id, 0), source;