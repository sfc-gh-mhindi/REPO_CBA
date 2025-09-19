-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;


select 
    NAME WAREHOUSE_NAME, 
    SIZE WAREHOUSE_SIZE, 
    WAREHOUSE_TYPE, 
    max_cluster_count, 
    parse_json(wh.management_policy)::variant:"maxIdleTime"::number auto_suspend,
    parse_json(wh.management_policy)::variant:"autoResume"::string auto_resume,
    enable_query_acceleration,
    query_acceleration_max_scale_factor,
    created_on,
    comment,
    server_count,
    resource_monitor_id
from SNOWHOUSE_IMPORT.@ACCOUNT_DEPLOYMENT@.warehouse_etl_v wh
where warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND wh.account_id = '@ACCOUNT_ID@'
and deleted_on is null
and temp_id = 0;