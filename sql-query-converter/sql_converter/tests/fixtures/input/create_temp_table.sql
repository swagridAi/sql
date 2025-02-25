CREATE TEMP TABLE #orders_summary AS
SELECT customer_id, SUM(total) AS total_spent
FROM orders
GROUP BY customer_id;

SELECT * FROM #orders_summary WHERE total_spent > 1000;