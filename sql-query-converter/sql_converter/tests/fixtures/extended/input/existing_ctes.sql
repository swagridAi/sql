-- Test handling SQL with existing CTEs and temp tables mixed together

-- Start with a CTE for active users
WITH active_users AS (
    SELECT * FROM users WHERE status = 'active'
)
-- Create a temp table from the CTE
SELECT
    au.id,
    au.name,
    au.email,
    COUNT(o.id) AS order_count
INTO #user_activity
FROM
    active_users au
LEFT JOIN
    orders o ON au.id = o.user_id
GROUP BY
    au.id, au.name, au.email;

-- Create another temp table from the first temp table
SELECT
    ua.id,
    ua.name,
    ua.order_count,
    p.preference_value
INTO #user_preferences
FROM
    #user_activity ua
JOIN
    preferences p ON ua.id = p.user_id
WHERE
    p.preference_type = 'email_frequency';

-- Final query using all CTEs and temp tables
WITH high_activity AS (
    SELECT * FROM #user_activity WHERE order_count > 5
)
SELECT
    up.id,
    up.name,
    up.preference_value,
    ha.order_count
FROM
    #user_preferences up
JOIN
    high_activity ha ON up.id = ha.id
ORDER BY
    ha.order_count DESC;