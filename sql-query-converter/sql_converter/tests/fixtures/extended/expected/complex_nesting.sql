WITH base_data AS (
  SELECT 
    id,
    name,
    email,
    status
  FROM 
    users
  WHERE 
    created_date > '2023-01-01'
),
user_metrics AS (
  SELECT
    user_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_spent,
    MAX(order_date) AS last_order_date
  FROM (
    SELECT
      o.id AS order_id,
      b.id AS user_id,
      o.amount,
      o.order_date
    FROM
      orders o
    JOIN
      base_data b ON o.user_id = b.id
    WHERE
      o.status = 'completed'
  ) order_data
  GROUP BY
    user_id
),
final_report AS (
  SELECT
    b.id,
    b.name,
    b.email,
    m.order_count,
    m.total_spent,
    m.last_order_date,
    ROW_NUMBER() OVER (PARTITION BY b.status ORDER BY m.total_spent DESC) AS spending_rank
  FROM
    base_data b
  LEFT JOIN
    user_metrics m ON b.id = m.user_id
),
high_value AS (
  SELECT * FROM final_report WHERE total_spent > 1000
),
recent_order AS (
  SELECT * FROM final_report WHERE last_order_date > '2023-06-01'
)
SELECT
  h.id,
  h.name,
  h.email,
  h.order_count,
  h.total_spent,
  CASE WHEN r.id IS NOT NULL THEN 'Yes' ELSE 'No' END AS recent_customer
FROM
  high_value h
LEFT JOIN
  recent_order r ON h.id = r.id
ORDER BY
  h.total_spent DESC;