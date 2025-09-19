-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;

-- Spillage analysis
SELECT 
    warehouse_name,
    warehouse_size,
    COUNT(*) as total_queries,
    SUM(CASE WHEN bytes_spilled_to_local_storage > 0 THEN 1 ELSE 0 END) as local_spill_queries,
    SUM(CASE WHEN bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END) as remote_spill_queries,
    AVG(bytes_spilled_to_local_storage) as avg_local_spill
FROM query_history
WHERE warehouse_name IN (
        'WH_USR_PRD_P01_FRAUMD_001', 
        'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 
        'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 
        'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003'
    )
GROUP BY 1,2;

-- Memory pressure indicators
SELECT 
    warehouse_name,
    AVG(compilation_time) as avg_compile_time,
    AVG(queued_provisioning_time) as avg_queue_time,
    COUNT(*) filter (WHERE error_code IS NOT NULL) as failed_queries
FROM query_history
WHERE warehouse_name IN (
        'WH_USR_PRD_P01_FRAUMD_001', 
        'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 
        'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 
        'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003'
    )
GROUP BY 1;
/*
 */