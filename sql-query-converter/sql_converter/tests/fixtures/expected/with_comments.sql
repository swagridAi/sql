WITH commented_temp AS (
  SELECT * FROM users /* important table */
)
SELECT * FROM commented_temp;