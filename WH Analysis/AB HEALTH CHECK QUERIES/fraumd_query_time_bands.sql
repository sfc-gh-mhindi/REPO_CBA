-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;


  SELECT 
  user_name,
   warehouse_size, 
   count(1) total_jobs, 
   sum(total_elapsed_time/1000/60) total_time_mins
  , count_if(total_elapsed_time/1000/60 < 1 ) count_jobs_0_1
  , count_if( total_elapsed_time/1000/60 between 1 and 5 )count_jobs_1_5
  , count_if( total_elapsed_time/1000/60 between 5 and 10 )count_jobs_5_10
  , count_if(total_elapsed_time/1000/60 between 10 and 60 )count_jobs_10_60
  , count_if(total_elapsed_time/1000/60 > 60 ) count_jobs_60
FROM pst.svcs.query_history
WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND 1=1
AND start_time >= dateadd(days, -30, current_date())
GROUP BY ALL
ORDER BY 4 DESC;