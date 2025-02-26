WITH pg_users AS (
  SELECT 
    t.id,
    t.name,
    t.email,
    t.created_at::date AS signup_date,
    COUNT(o.id) FILTER (WHERE o.status = 'completed') AS completed_orders,
    SUM(o.amount) FILTER (WHERE o.status = 'completed') AS total_spent
  FROM 
    users t
  LEFT JOIN
    orders o ON t.id = o.user_id
  WHERE
    t.created_at >= '2023-01-01'::date
  GROUP BY
    t.id, t.name, t.email, t.created_at
  HAVING
    COUNT(o.id) > 0
  ORDER BY
    total_spent DESC NULLS LAST
  LIMIT 100 OFFSET 0
),
recent_orders AS (
  SELECT 
    user_id,
    jsonb_agg(
      jsonb_build_object(
        'order_id', id,
        'date', created_at,
        'amount', amount
      )
    ) AS orders_json
  FROM
    orders
  WHERE
    created_at > now() - interval '30 days'
  GROUP BY
    user_id
)
SELECT
  u.*,
  COALESCE(ro.orders_json, '[]'::jsonb) AS recent_orders,
  CASE 
    WHEN EXTRACT(YEAR FROM AGE(NOW(), u.created_at::timestamp)) > 1 
    THEN 'loyal' 
    ELSE 'new' 
  END AS user_type
FROM
  pg_users u
LEFT JOIN
  recent_orders ro ON u.id = ro.user_id
WHERE
  u.completed_orders > 2;
