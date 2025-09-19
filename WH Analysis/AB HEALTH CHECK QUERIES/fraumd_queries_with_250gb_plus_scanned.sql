-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;


SELECT 
total_elapsed_time/60000 total_elapsed_time_mins,
bytes_scanned/1024/1024/1024 gb_scanned,
bytes_spilled_to_local_storage,
bytes_spilled_to_remote_storage,
query_text,
user_name,
role_name,
warehouse_name,
warehouse_size,
execution_status,
start_time,
end_time
FROM
query_history
WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND
execution_status = 'SUCCESS'
AND warehouse_size IS NOT NULL
AND end_time > dateadd(MONTH,-1,CURRENT_DATE())
and bytes_scanned >= 250000000000;