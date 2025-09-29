-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;


select name, frequency, credit_quota, credit_usage_double
from SNOWHOUSE_IMPORT.@ACCOUNT_DEPLOYMENT@.resource_monitor_etl_v
where warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND account_id = @ACCOUNT_ID@
  and deleted_on is null
  and purged_time is null
;