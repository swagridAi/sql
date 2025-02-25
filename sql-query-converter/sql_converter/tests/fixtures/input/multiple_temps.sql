SELECT * INTO #temp1 FROM table1;
SELECT * INTO #temp2 FROM table2;
SELECT t1.*, t2.* FROM #temp1 t1 JOIN #temp2 t2 ON t1.id = t2.id;