-- This file is used to test circular dependency detection
-- and should cause a validation error when processed

-- First temporary table depends on the second (not yet defined)
SELECT * INTO #temp1 FROM #temp2;

-- Second temporary table depends on the first (creating a circular dependency)
SELECT * INTO #temp2 FROM #temp1;

-- Query using both tables
SELECT 
    t1.id,
    t2.name
FROM 
    #temp1 t1
JOIN
    #temp2 t2 ON t1.id = t2.id;