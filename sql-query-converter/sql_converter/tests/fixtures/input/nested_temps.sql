SELECT * INTO #inner_temp FROM source_table;
SELECT * INTO #outer_temp FROM #inner_temp;
SELECT * FROM #outer_temp;