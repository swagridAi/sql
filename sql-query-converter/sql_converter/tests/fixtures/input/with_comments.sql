-- Create temp table
SELECT * INTO #commented_temp FROM users /* important table */;
/*
Multi-line comment
SELECT * INTO #ignored_temp FROM logs;
*/
SELECT * FROM #commented_temp;