WITH quoted_identifiers AS (
  SELECT 
    "column.with.dots", 
    "column with spaces",
    "column""with""quotes"
  FROM "schema.name"."table name"
),
oneliner AS (
  SELECT * FROM users
),
unicode AS (
  SELECT * FROM products WHERE category = N'家電製品' AND "説明書" IS NOT NULL
),
edge_case AS (
  SELECT 
    CASE 
      WHEN EXISTS (SELECT 1 FROM oneliner) THEN 'Exists' 
      ELSE 'Empty' 
    END AS result,
    CASE
      WHEN COUNT(*) > 0 THEN COUNT(*)
      ELSE NULL
    END AS record_count
  FROM quoted_identifiers
  WHERE 1 = 0
),
unions AS (
  SELECT * FROM (
    SELECT id, 'type1' AS source FROM quoted_identifiers
    UNION ALL
    SELECT id, 'type2' AS source FROM unicode
    UNION ALL
    SELECT NULL AS id, 'type3' AS source FROM edge_case
  ) combined_data
)
SELECT * FROM unions
ORDER BY COALESCE(id, 0), source;