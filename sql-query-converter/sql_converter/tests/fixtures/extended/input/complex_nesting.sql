-- This file tests complex nested queries with multiple levels of temp tables

-- Create base temp table from subquery
SELECT * INTO #base_data FROM (
    SELECT 
        id,
        name,
        email,
        status
    FROM 
        users
    WHERE 
        created_date > '2023-01-01'
) recent_users;

-- Create intermediate temp table with aggregation from base table
SELECT
    user_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_spent,
    MAX(order_date) AS last_order_date
INTO #user_metrics
FROM (
    SELECT
        o.id AS order_id,
        b.id AS user_id,
        o.amount,
        o.order_date
    FROM
        orders o
    JOIN
        #base_data b ON o.user_id = b.id
    WHERE
        o.status = 'completed'
) order_data
GROUP BY
    user_id;

-- Create final temp table joining multiple sources with window functions
SELECT
    b.id,
    b.name,
    b.email,
    m.order_count,
    m.total_spent,
    m.last_order_date,
    ROW_NUMBER() OVER (PARTITION BY b.status ORDER BY m.total_spent DESC) AS spending_rank
INTO #final_report
FROM
    #base_data b
LEFT JOIN
    #user_metrics m ON b.id = m.user_id;

-- Final query with nested CTEs and multiple joins
WITH high_value AS (
    SELECT * FROM #final_report WHERE total_spent > 1000
),
recent_order AS (
    SELECT * FROM #final_report WHERE last_order_date > '2023-06-01'
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