WITH inner_temp AS (
  SELECT * FROM source_table
),
outer_temp AS (
  SELECT * FROM inner_temp
)
SELECT * FROM outer_temp;