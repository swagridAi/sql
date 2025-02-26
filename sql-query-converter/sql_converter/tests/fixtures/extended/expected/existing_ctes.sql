WITH active_users AS (
  SELECT * FROM users WHERE status = 'active'
),
user_activity AS (
  SELECT
    au.id,
    au.name,
    au.email,
    COUNT(o.id) AS order_count
  FROM
    active_users au
  LEFT JOIN
    orders o ON au.id = o.user_id
  GROUP BY
    au.id, au.name, au.email
),
user_preferences AS (
  SELECT
    ua.id,
    ua.name,
    ua.order_count,
    p.preference_value
  FROM
    user_activity ua
  JOIN
    preferences p ON ua.id = p.user_id
  WHERE
    p.preference_type = 'email_frequency'
),
high_activity AS (
  SELECT * FROM user_activity WHERE order_count > 5
)
SELECT
  up.id,
  up.name,
  up.preference_value,
  ha.order_count
FROM
  user_preferences up
JOIN
  high_activity ha ON up.id = ha.id
ORDER BY
  ha.order_count DESC;