WITH temp1 AS (
  SELECT * FROM table1
),
temp2 AS (
  SELECT * FROM table2
)
SELECT t1.*, t2.* FROM temp1 t1 JOIN temp2 t2 ON t1.id = t2.id;