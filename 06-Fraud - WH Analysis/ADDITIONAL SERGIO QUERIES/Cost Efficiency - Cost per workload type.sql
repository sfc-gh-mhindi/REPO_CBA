-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;

-- 
SELECT 
    warehouse_name,
    query_type,
    SUM(credits_used) as total_credits,
    COUNT(*) as query_count,
    AVG(credits_used) as avg_credits_per_query,
    SUM(credits_used)/SUM(execution_time) * 3600000 as credits_per_hour
FROM query_history
WHERE warehouse_name IN (
        'WH_USR_PRD_P01_FRAUMD_001', 
        'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 
        'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 
        'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003'
    )
GROUP BY 1,2;
/*
 */