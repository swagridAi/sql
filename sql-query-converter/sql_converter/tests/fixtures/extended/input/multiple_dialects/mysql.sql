-- MySQL dialect-specific features test

-- Using MySQL-specific syntax with backtick identifiers
CREATE TEMPORARY TABLE #temp_mysql AS
SELECT 
    `t`.`id`,
    `t`.`name`,
    COUNT(`o`.`order_id`) AS `order_count`,
    IFNULL(SUM(`o`.`amount`), 0) AS `total_amount`
FROM 
    `users` AS `t`
LEFT JOIN
    `orders` AS `o` ON `t`.`id` = `o`.`user_id`
WHERE
    `t`.`created_at` > '2023-01-01'
GROUP BY 
    `t`.`id`, `t`.`name`
HAVING 
    `order_count` > 0
ORDER BY 
    `total_amount` DESC
LIMIT 100;

-- Second query with more MySQL features
SELECT 
    m.*,
    JSON_EXTRACT(`m`.`preferences`, '$.email_frequency') AS `email_pref`,
    CONCAT(`m`.`name`, ' (', SUBSTRING(`m`.`email`, 1, 10), '...)') AS `display_name`,
    DATEDIFF(NOW(), `m`.`created_at`) AS `days_active`
FROM 
    #temp_mysql AS `m`
WHERE 
    `m`.`total_amount` > 500;