-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;

-- Query complexity analysis
SELECT 
    warehouse_name,
    query_type,
    CASE 
        WHEN execution_time < 10000 THEN 'Quick (<10s)'
        WHEN execution_time < 60000 THEN 'Medium (10s-1m)'
        WHEN execution_time < 300000 THEN 'Long (1-5m)'
        ELSE 'Very Long (>5m)'
    END as duration_category,
    COUNT(*) as query_count,
    AVG(bytes_scanned) as avg_data_scanned
FROM query_history 
WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') 
GROUP BY 1,2,3;