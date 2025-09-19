-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;

-- User concurrency analysis
SELECT 
    warehouse_name,
    user_name,
    COUNT(DISTINCT DATE_TRUNC('hour', start_time)) as active_hours,
    COUNT(*) as total_queries,
    AVG(execution_time) as avg_execution_time
FROM query_history
WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') 
GROUP BY 1,2
ORDER BY active_hours DESC;