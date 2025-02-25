WITH temp AS (
  SELECT [col1] FROM [dbo].[table]
)
SELECT [col1] FROM temp;