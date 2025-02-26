-- T-SQL dialect-specific features test

-- Using T-SQL specific syntax with bracket identifiers
SELECT 
    [u].[UserId],
    [u].[UserName],
    [u].[Email],
    CONVERT(DATE, [u].[CreatedAt]) AS [SignupDate],
    COUNT([o].[OrderId]) AS [OrderCount],
    SUM([o].[Amount]) AS [TotalSpent]
INTO #tsql_users
FROM 
    [dbo].[Users] [u]
LEFT JOIN
    [dbo].[Orders] [o] ON [u].[UserId] = [o].[UserId]
WHERE
    [u].[CreatedAt] >= '20230101' -- YYYYMMDD format
    AND [u].[IsActive] = 1
GROUP BY
    [u].[UserId], [u].[UserName], [u].[Email], [u].[CreatedAt]
HAVING
    COUNT([o].[OrderId]) > 0
ORDER BY
    [TotalSpent] DESC;

-- Query with more T-SQL features
DECLARE @cutoff_date DATETIME = DATEADD(MONTH, -3, GETDATE())

SELECT
    [u].*,
    [p].[PhoneNumber],
    [r].[RegionName],
    STUFF([u].[Email], 2, CHARINDEX('@', [u].[Email]) - 2, '***') AS [MaskedEmail],
    CASE 
        WHEN [u].[TotalSpent] > 1000 THEN 'High'
        WHEN [u].[TotalSpent] > 500 THEN 'Medium'
        ELSE 'Low'
    END AS [SpendingCategory],
    DATEDIFF(DAY, CONVERT(DATE, [u].[SignupDate]), GETDATE()) AS [DaysSinceSignup]
FROM
    #tsql_users [u]
LEFT JOIN
    [dbo].[UserProfiles] [p] ON [u].[UserId] = [p].[UserId]
LEFT JOIN
    [dbo].[Regions] [r] ON [p].[RegionId] = [r].[RegionId]
WHERE
    [u].[OrderCount] > 2
    AND EXISTS (
        SELECT 1 FROM [dbo].[Orders] [o]
        WHERE [o].[UserId] = [u].[UserId]
        AND [o].[OrderDate] > @cutoff_date
    )
ORDER BY
    [u].[TotalSpent] DESC
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY;