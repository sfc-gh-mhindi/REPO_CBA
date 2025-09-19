-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =
-- FRAUMD WAREHOUSE ANALYSIS - COLLATED QUERIES
-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =-- =
-- Filtered for warehouses:
--   WH_USR_PRD_P01_FRAUMD_001
--   WH_USR_PRD_P01_FRAUMD_LABMLFRD_001
--   WH_USR_PRD_P01_FRAUMD_LABMLFRD_002
--   WH_USR_PRD_P01_FRAUMD_LABMLFRD_003
-- ====================================================================================================

-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;


-- ================================================================================
-- QUERY: cost_per_user.sql
-- ================================================================================

--THIS IS APPROXIMATE CREDIT CONSUMPTION BY USER
WITH USER_HOUR_EXECUTION_CTE AS (
    SELECT  USER_NAME
    ,WAREHOUSE_NAME
    ,DATE_TRUNC('hour',START_TIME) as START_TIME_HOUR
    ,SUM(EXECUTION_TIME)  as USER_HOUR_EXECUTION_TIME
    FROM QUERY_HISTORY
    WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND WAREHOUSE_NAME IS NOT NULL
    AND EXECUTION_TIME > 0
  
 --Change the below filter if you want to look at a longer range than the last 1 month 
    AND START_TIME > DATEADD(Month,-1,CURRENT_TIMESTAMP())
    group by 1,2,3
    )
, HOUR_EXECUTION_CTE AS (
    SELECT  START_TIME_HOUR
    ,WAREHOUSE_NAME
    ,SUM(USER_HOUR_EXECUTION_TIME) AS HOUR_EXECUTION_TIME
    FROM USER_HOUR_EXECUTION_CTE
    group by 1,2
)
, APPROXIMATE_CREDITS AS (
    SELECT 
    A.USER_NAME
    ,C.WAREHOUSE_NAME
    ,(A.USER_HOUR_EXECUTION_TIME/B.HOUR_EXECUTION_TIME)*C.CREDITS_USED AS APPROXIMATE_CREDITS_USED

    FROM USER_HOUR_EXECUTION_CTE A
    JOIN HOUR_EXECUTION_CTE B  ON A.START_TIME_HOUR = B.START_TIME_HOUR and B.WAREHOUSE_NAME = A.WAREHOUSE_NAME
    JOIN WAREHOUSE_METERING_HISTORY C ON C.WAREHOUSE_NAME = A.WAREHOUSE_NAME AND C.START_TIME = A.START_TIME_HOUR
)

SELECT 
 USER_NAME
,WAREHOUSE_NAME
,SUM(APPROXIMATE_CREDITS_USED) AS APPROXIMATE_CREDITS_USED
FROM APPROXIMATE_CREDITS
GROUP BY 1,2
ORDER BY 3 DESC
;


-- ================================================================================
-- QUERY: monthly_credit_consumption.sql
-- ================================================================================

SELECT 
    TO_VARCHAR(START_TIME   , 'yyyy-MM') DATE, 
    WAREHOUSE_NAME,
    SUM(CREDITS_USED_COMPUTE) AS CREDITS
  FROM WAREHOUSE_METERING_HISTORY
 WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND START_TIME >= DATEADD(YEAR, -1, CURRENT_TIMESTAMP())  // Past 7 days
 GROUP BY ALL
 ORDER BY 1, 2 DESC
;


-- ================================================================================
-- QUERY: most_expensive_queries.sql
-- ================================================================================

WITH WAREHOUSE_SIZE AS
(
     SELECT WAREHOUSE_SIZE, NODES
       FROM (
              SELECT 'X-SMALL' AS WAREHOUSE_SIZE, 1 AS NODES
              UNION ALL
              SELECT 'SMALL' AS WAREHOUSE_SIZE, 2 AS NODES
              UNION ALL
              SELECT 'MEDIUM' AS WAREHOUSE_SIZE, 4 AS NODES
              UNION ALL
              SELECT 'LARGE' AS WAREHOUSE_SIZE, 8 AS NODES
              UNION ALL
              SELECT 'X-LARGE' AS WAREHOUSE_SIZE, 16 AS NODES
              UNION ALL
              SELECT '2X-LARGE' AS WAREHOUSE_SIZE, 32 AS NODES
              UNION ALL
              SELECT '3X-LARGE' AS WAREHOUSE_SIZE, 64 AS NODES
              UNION ALL
              SELECT '4X-LARGE' AS WAREHOUSE_SIZE, 128 AS NODES
            )
),
QH AS
(
     SELECT QH.QUERY_ID
           ,QH.QUERY_TEXT
           ,QH.USER_NAME
           ,QH.ROLE_NAME
           ,QH.EXECUTION_TIME
           ,QH.WAREHOUSE_SIZE
      FROM QUERY_HISTORY QH
     WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND START_TIME > DATEADD(month,-2,CURRENT_TIMESTAMP())
)

SELECT QH.QUERY_ID
      ,QH.QUERY_TEXT
      ,QH.USER_NAME
      ,QH.ROLE_NAME
      ,QH.EXECUTION_TIME as EXECUTION_TIME_MILLISECONDS
      ,(QH.EXECUTION_TIME/(1000)) as EXECUTION_TIME_SECONDS
      ,(QH.EXECUTION_TIME/(1000*60)) AS EXECUTION_TIME_MINUTES
      ,(QH.EXECUTION_TIME/(1000*60*60)) AS EXECUTION_TIME_HOURS
      ,WS.WAREHOUSE_SIZE
      ,WS.NODES
      ,(QH.EXECUTION_TIME/(1000*60*60))*WS.NODES as RELATIVE_PERFORMANCE_COST

FROM  QH
JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
ORDER BY RELATIVE_PERFORMANCE_COST DESC
LIMIT 200
;


-- ================================================================================
-- QUERY: other_credit_costs.sql
-- ================================================================================

WITH CTE_RAW
AS
(
SELECT 
  USAGE_DATE,
  TO_NUMBER(COMPUTE_CREDITS) AS WAREHOUSE_COMPUTE,
  TO_NUMBER(BILLABLE_CLOUD_SERVICES_CREDITS) AS CLOUD_SERVICES,
  TO_NUMBER(SNOWPIPE_CREDITS) AS SNOWPIPE,
  TO_NUMBER(RECLUSTERING_CREDITS) AS RECLUSTERING,
  TO_NUMBER(REPLICATION_CREDITS) AS REPLICATION,
  TO_NUMBER(READER_COMPUTE_CREDITS) AS READER_COMPUTE,
  TO_NUMBER(BILLABLE_CLOUD_SERVICES_READER_CREDITS) AS CLOUD_SERVICES_READER,
  TO_NUMBER(MATERIALIZED_VIEW_CREDITS) AS MATERIALIZED_VIEW,
  TO_NUMBER(SEARCH_OPTIMIZATION_CREDITS) AS SEARCH_OPTIMIZATION,
  TO_NUMBER(SERVERLESS_TASK_CREDITS) AS SERVERLESS_TASK,
  TO_NUMBER(query_acceleration_credits) AS QUERY_ACCELERATION

FROM FINANCE.CUSTOMER.USAGE_DAILY
WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND USAGE_DATE >= dateadd(days, -30, current_date())
AND snowflake_account_id = @ACCOUNT_ID@
and snowflake_deployment = '@ACCOUNT_DEPLOYMENT@'
),
CTE_MATURE
AS
(
SELECT * FROM CTE_RAW
UNPIVOT(CREDITS FOR SERVICE_TYPE IN (WAREHOUSE_COMPUTE,CLOUD_SERVICES,SNOWPIPE,RECLUSTERING,REPLICATION,READER_COMPUTE,CLOUD_SERVICES_READER,MATERIALIZED_VIEW,SEARCH_OPTIMIZATION,SERVERLESS_TASK))
)
SELECT 

SERVICE_TYPE,
SUM(CREDITS) CREDITS
FROM CTE_MATURE
GROUP BY ALL
ORDER BY CREDITS DESC
;


-- ================================================================================
-- QUERY: queries_longer_than_30_mins.sql
-- ================================================================================

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
AND total_elapsed_time/60000 > 30;  ;


-- ================================================================================
-- QUERY: queries_with_250gb_plus_scanned.sql
-- ================================================================================

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


-- ================================================================================
-- QUERY: query_time_bands.sql
-- ================================================================================

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


-- ================================================================================
-- QUERY: query_type_time_bands.sql
-- ================================================================================

SELECT

	QUERY_TYPE,

SUM(CASE WHEN TOTAL_ELAPSED_TIME < 1000 THEN 1 ELSE 0 END) AS BT_0_1,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 5000 AND TOTAL_ELAPSED_TIME >= 1000 THEN 1 ELSE 0 END) AS BT_1_5,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 10000 AND TOTAL_ELAPSED_TIME >= 5000 THEN 1 ELSE 0 END) AS BT_5_10,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 30000 AND TOTAL_ELAPSED_TIME >= 10000 THEN 1 ELSE 0 END) AS BT_10_30,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 60000 AND TOTAL_ELAPSED_TIME >= 30000 THEN 1 ELSE 0 END) AS BT_30_60,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 90000 AND TOTAL_ELAPSED_TIME >= 60000 THEN 1 ELSE 0 END) AS BT_60_90,
SUM(CASE WHEN TOTAL_ELAPSED_TIME >= 90000 THEN 1 ELSE 0 END) AS GTE_90

FROM
    query_history
WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND
    START_TIME >= dateadd(MONTH,-1,CURRENT_DATE())
GROUP BY
    ALL
ORDER BY
    GTE_90 DESC
;


-- ================================================================================
-- QUERY: resource_monitors.sql
-- ================================================================================

select name, frequency, credit_quota, credit_usage_double
from SNOWHOUSE_IMPORT.@ACCOUNT_DEPLOYMENT@.resource_monitor_etl_v
where warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND account_id = @ACCOUNT_ID@
  and deleted_on is null
  and purged_time is null
;


-- ================================================================================
-- QUERY: top_50_long_running_queries.sql
-- ================================================================================

select
          QUERY_ID
        ,ROW_NUMBER() OVER(ORDER BY PARTITIONS_SCANNED DESC) as QUERY_ID_INT
         ,QUERY_TEXT
         ,TOTAL_ELAPSED_TIME/1000 AS QUERY_EXECUTION_TIME_SECONDS
         ,PARTITIONS_SCANNED
         ,PARTITIONS_TOTAL

from QUERY_HISTORY Q
 where warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND 1=1
  and TO_DATE(Q.START_TIME) >     DATEADD(month,-1,TO_DATE(CURRENT_TIMESTAMP())) 
    and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
    and ERROR_CODE iS NULL
    and PARTITIONS_SCANNED is not null
   
  order by  TOTAL_ELAPSED_TIME desc
   
   LIMIT 50
   ;


-- ================================================================================
-- QUERY: warehouse_info.sql
-- ================================================================================

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


-- ================================================================================
-- QUERY: warehouse_utilisation.sql
-- ================================================================================

with credits as (
    select wmh.warehouse_name 
    ,      sum(credits_used) as credits_used
    from warehouse_metering_history wmh 
    where warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND wmh.start_time > dateadd(month, -1, current_date())
    group by wmh.warehouse_name
), queries as ( 
	SELECT -- Queries over 1Gb in size
		qu.warehouse_name ,
		warehouse_size ,
		AVG(CASE WHEN bytes_scanned   >= 1000000000  THEN bytes_scanned ELSE NULL END) AS avg_large ,
		COUNT(CASE WHEN bytes_scanned >= 1000000000  THEN 1 ELSE NULL END) AS count_large ,
		COUNT(CASE WHEN bytes_scanned <  1000000000  THEN 1 ELSE NULL END) AS count_small ,
        COUNT(
            CASE
                WHEN bytes_scanned < 1000000000 THEN 1
                ELSE NULL
            END
        ) AS count_below_one_gb,
        count(
            case
                when bytes_scanned >= 1000000000
                and bytes_scanned < 20000000000 then 1
                else null
            end
        ) as count_btw_one_and_twenty_gb,
        count(
            case
                when bytes_scanned >= 20000000000
                and bytes_scanned < 50000000000 then 1
                else null
            end
        ) as count_btw_twenty_and_fifty_gb,
        count(
            case
                when bytes_scanned >= 50000000000
                and bytes_scanned < 100000000000 then 1
                else null
            end
        ) as count_btw_fifty_and_one_hundred_gb,
        count(
            case
                when bytes_scanned >= 100000000000
                and bytes_scanned < 250000000000 then 1
                else null
            end
        ) as count_btw_one_hundred_and_twofifty_gb,
        count(
            case
                when bytes_scanned >= 250000000000 then 1
                else null
            end
        ) as count_over_twofifty_gb,

        AVG(
            CASE
                WHEN bytes_scanned < 1000000000 THEN total_elapsed_time/1000
                ELSE NULL
            END
        ) AS avg_exe_secs_below_one_gb,

        AVG(
            case
                when bytes_scanned >= 1000000000
                and bytes_scanned < 20000000000 then total_elapsed_time/1000
                else null
            end
        ) as avg_exe_secs_btw_one_and_twenty_gb,
        AVG(
            case
                when bytes_scanned >= 20000000000
                and bytes_scanned < 50000000000 then total_elapsed_time/1000
                else null
            end
        ) as avg_exe_secs_btw_twenty_and_fifty_gb,
        AVG(
            case
                when bytes_scanned >= 50000000000
                and bytes_scanned < 100000000000 then total_elapsed_time/1000
                else null
            end
        ) as avg_exe_secs_btw_fifty_and_one_hundred_gb,
        AVG(
            case
                when bytes_scanned >= 100000000000
                and bytes_scanned < 250000000000 then total_elapsed_time/1000
                else null
            end
        ) as avg_exe_secs_btw_one_hundred_and_twofifty_gb,
        AVG(
            case
                when bytes_scanned >= 250000000000 then total_elapsed_time/1000
                else null
            end
        ) as avg_exe_secs_over_twofifty_gb,

        
        
		AVG(CASE WHEN bytes_scanned   >= 1000000000  THEN total_elapsed_time / 1000 ELSE NULL END) AS avg_large_exe_time ,
		AVG(bytes_scanned) AS avg_bytes_scanned ,
		AVG(total_elapsed_time)/ 1000 AS avg_elapsed_time ,
		AVG(execution_time)/ 1000 AS avg_execution_time ,
		COUNT(*) AS count_queries
	FROM
		query_history qu
	WHERE
		execution_status = 'SUCCESS'
		AND warehouse_size IS NOT NULL
		AND end_time > dateadd(MONTH,-1,CURRENT_DATE())
		and bytes_scanned > 0
    GROUP BY
        qu.warehouse_name,
        warehouse_size
)
SELECT
	q.warehouse_name , -- Warehouse Name
	q.warehouse_size ,
	ROUND(count_large / count_queries * 100, 0) AS percent_large_more1GB,
	ROUND(count_small / count_queries * 100, 0) AS percent_small_less1GB ,
	CASE
		WHEN avg_large >= POWER(2, 40) THEN to_char(ROUND(avg_large / POWER(2, 40), 1)) || ' TB'
		WHEN avg_large >= POWER(2, 30) THEN to_char(ROUND(avg_large / POWER(2, 30), 1)) || ' GB'
		WHEN avg_large >= POWER(2, 20) THEN to_char(ROUND(avg_large / POWER(2, 20), 1)) || ' MB'
		WHEN avg_large >= POWER(2, 10) THEN to_char(ROUND(avg_large / POWER(2, 10), 1)) || ' K'
		ELSE to_char(avg_large)
	END AS avg_bytes_large ,
	ROUND(avg_large_exe_time) AS avg_large_exe_time ,
	ROUND(avg_execution_time) AS avg_all_exe_time_secs ,
    ROUND(avg_elapsed_time) avg_elapsed_time_secs,

    ROUND(count_below_one_gb / count_queries * 100, 0) AS percent_xs,
    ROUND(count_btw_one_and_twenty_gb / count_queries * 100, 0) AS percent_s,
    ROUND(count_btw_twenty_and_fifty_gb / count_queries * 100, 0) AS percent_m,
    ROUND(count_btw_fifty_and_one_hundred_gb / count_queries * 100, 0) AS percent_l,
    ROUND(count_btw_one_hundred_and_twofifty_gb / count_queries * 100, 0) AS percent_xl,
    ROUND(count_over_twofifty_gb / count_queries * 100, 0) AS percent_2xl,

    avg_exe_secs_below_one_gb,
    avg_exe_secs_btw_one_and_twenty_gb,
    avg_exe_secs_btw_twenty_and_fifty_gb,
    avg_exe_secs_btw_fifty_and_one_hundred_gb,
    avg_exe_secs_btw_one_hundred_and_twofifty_gb,
    avg_exe_secs_over_twofifty_gb,
    
    count_below_one_gb,
    count_btw_one_and_twenty_gb,
    count_btw_twenty_and_fifty_gb,
    count_btw_fifty_and_one_hundred_gb,
    count_btw_one_hundred_and_twofifty_gb,
    count_over_twofifty_gb,
        
	count_queries,
    ROUND(c.credits_used) as credits_used
FROM queries q,
     credits c
WHERE q.warehouse_name = c.warehouse_name
ORDER BY
    c.credits_used desc,
    case warehouse_size
       when 'X-Small' then 1
       when 'Small'   then 2
       when 'Medium'  then 3
       when 'Large'   then 4
       when 'X-Large' then 5
       when '2X-Large' then 6
       when '3X-Large' then 7
       when '4X-Large' then 8
       else 9
       end desc;


-- ================================================================================
-- QUERY: warehouse_utilisation_02.sql
-- ================================================================================

with timewindow as (
    -- adjust this start_marker to globally alter the "lookback" window for the rest of the CTE's
    select to_timestamp(dateadd(month, -1, current_date())) as start_marker
),
queries as (
    -- this is our base set of query data - executed queries
    -- that scanned at least 1 byte of data, and started after the start_marker
    -- defined in the prior CTE -- if running against large/high-volume accounts this is a
    -- good CTE to pull out and instantiate as a TEMP table...
    select query_id,
        warehouse_id,
        warehouse_name,
        warehouse_size,
        execution_status,
        query_type,
        user_name,
        role_name,
        start_time,
        end_time,
        bytes_scanned,
        total_elapsed_time,
        execution_time,
        partitions_total,
        partitions_scanned,
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage,
        bytes_sent_over_the_network,
        queued_provisioning_time,
        queued_repair_time,
        queued_overload_time
    from query_history
        join timewindow tw
    WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND
        -- Removing constraint on successful queries - failed queries burn credits too
        --execution_status = 'SUCCESS'
        --AND
        warehouse_size IS NOT NULL
        AND start_time > tw.start_marker
        and bytes_scanned > 0
        -- unnecessary condition, but maybe helps with pruning?
        and cluster_number is not null
) ,
credits as (
    -- summary of credit utilization by warehouse during the
    -- specified time period.  Note that credit utilization
    -- is tracked at the warehouse level, not by warehouse
    -- size.  Summation of credits_used in the output is not
    -- the same as account_level compute charges because any
    -- warehouse that was resized during the time period will
    -- have their credits represented multiple times
    select wmh.warehouse_id
        , sum(credits_used_compute) as credits_used
    from warehouse_metering_history wmh
        join timewindow tw
    where wmh.start_time > tw.start_marker
    group by wmh.warehouse_id
),
whstats as (
      -- warehouse statistics, average and stddev of bytes scanned,
    -- used in the P95 CTE to filter the outlier queries, plus success/fail
    -- counters that are used in credits/query calculations
    SELECT qu.warehouse_id 
        , qu.warehouse_name
        , qu.warehouse_size
        , count(*) as total_query_count
        , sum(case when execution_status = 'SUCCESS'
                   then 1
                   else 0 end) as successful_query_count
        , sum(case when execution_status IN ('FAIL', 'INCIDENT')
                   then 1
                   else 0 end) as failed_query_count
        , avg(bytes_scanned) as avg_bytes_scanned
        -- NOTE - stddev will throw an error if the bytes_scanned number is “very large” -
        --  to get around this error we divide by POWER(10,12) inside the function,
        --  then multiply by that same factor outside of the function:
        , stddev(bytes_scanned / POWER(10, 12)) * POWER(10, 12) as stdev_bytes_scanned
    FROM queries qu
    GROUP BY qu.warehouse_id 
        , qu.warehouse_name
        , qu.warehouse_size
),
all_queries as (
    -- basic stats against the full query population, including
    -- bucketing of query counts by bytes scanned so that we can
    -- produce the output histogram later.
    --
    -- this CTE is filtered to SUCCESSFUL queries only
    SELECT qu.warehouse_id, 
        qu.warehouse_size,
        count(*) as total_query_count,
        AVG(
            CASE
                WHEN bytes_scanned >= 1000000000 THEN bytes_scanned
                ELSE NULL
            END
        ) AS avg_large,
        COUNT(
            CASE
                WHEN bytes_scanned >= 1000000000 THEN 1
                ELSE NULL
            END
        ) AS count_large,
        COUNT(
            CASE
                WHEN bytes_scanned < 1000000000 THEN 1
                ELSE NULL
            END
        ) AS count_below_one_gb,
        count(
            case
                when bytes_scanned >= 1000000000
                and bytes_scanned < 20000000000 then 1
                else null
            end
        ) as count_btw_one_and_twenty_gb,
        count(
            case
                when bytes_scanned >= 20000000000
                and bytes_scanned < 50000000000 then 1
                else null
            end
        ) as count_btw_twenty_and_fifty_gb,
        count(
            case
               when bytes_scanned >= 50000000000
                and bytes_scanned < 100000000000 then 1
                else null
            end
        ) as count_btw_fifty_and_one_hundred_gb,
        count(
            case
                when bytes_scanned >= 100000000000
                and bytes_scanned < 250000000000 then 1
                else null
            end
        ) as count_btw_one_hundred_and_twofifty_gb,
        count(
            case
                when bytes_scanned >= 250000000000 then 1
                else null
            end
        ) as count_over_twofifty_gb,
        AVG(
            CASE
                WHEN bytes_scanned >= 1000000000 THEN total_elapsed_time / 1000
                ELSE NULL
            END
        ) AS avg_large_exe_time,
        AVG(bytes_scanned) AS avg_bytes_scanned,
        AVG(total_elapsed_time) AS avg_elapsed_time,
        AVG(execution_time) AS avg_execution_time,
        avg((partitions_total - partitions_scanned) / partitions_total) * 100 as avg_pruning_pct,
        sum(
            case
                when bytes_spilled_to_local_storage > 0 then 1
                else 0
            end
        ) as count_queries_with_spillage,
        avg(bytes_spilled_to_local_storage) as avg_spillage_local,
        avg(bytes_spilled_to_remote_storage) as avg_spillage_remote,
        avg(bytes_sent_over_the_network) as avg_bytes_sent_over_network,
        sum(
            case
                when queued_provisioning_time + queued_repair_time + queued_overload_time > 0 then 1
                else 0
            end
        ) as count_queries_with_queuing
    FROM queries qu
        join whstats whs on qu.warehouse_name = whs.warehouse_name
        and qu.warehouse_size = whs.warehouse_size
    WHERE qu.execution_status = 'SUCCESS'
    GROUP BY qu.warehouse_id, 
        qu.warehouse_size
),
p95_queries as (
    -- same stats as the prior CTE, but filtered to queries within 2 standard
    -- deviations of the mean based on bytes scanned, should represent 95% of
    -- the queries assuming normal distribution
    --
    -- this CTE is filtered to SUCCESSFUL queries only
    SELECT qu.warehouse_id, 
        qu.warehouse_size,
        count(*) as p95_query_count,
        AVG(
            CASE
                WHEN bytes_scanned >= 1000000000 THEN bytes_scanned
                ELSE NULL
            END
        ) AS avg_large,
        COUNT(
            CASE
                WHEN bytes_scanned >= 1000000000 THEN 1
                ELSE NULL
            END
        ) AS count_large,
        COUNT(
            CASE
                WHEN bytes_scanned < 1000000000 THEN 1
                ELSE NULL
            END
        ) AS count_below_one_gb,
        count(
            case
                when bytes_scanned >= 1000000000
                and bytes_scanned < 20000000000 then 1
                else null
            end
        ) as count_btw_one_and_twenty_gb,
        count(
            case
                when bytes_scanned >= 20000000000
                and bytes_scanned < 50000000000 then 1
                else null
            end
        ) as count_btw_twenty_and_fifty_gb,
        count(
            case
                when bytes_scanned >= 50000000000
                and bytes_scanned < 100000000000 then 1
                else null
            end
        ) as count_btw_fifty_and_one_hundred_gb,
        count(
            case
                when bytes_scanned >= 100000000000
                and bytes_scanned < 250000000000 then 1
                else null
            end
        ) as count_btw_one_hundred_and_twofifty_gb,
        count(
            case
                when bytes_scanned >= 250000000000 then 1
                else null
            end
        ) as count_over_twofifty_gb,
        AVG(
            CASE
                WHEN bytes_scanned >= 1000000000 THEN total_elapsed_time / 1000
                ELSE NULL
            END
        ) AS avg_large_exe_time,
        AVG(bytes_scanned) AS avg_bytes_scanned,
        AVG(total_elapsed_time) AS avg_elapsed_time,
        AVG(execution_time) AS avg_execution_time,
        avg(partitions_scanned) as avg_partitions_scanned,
        avg(
            (partitions_total - partitions_scanned) / partitions_total
        ) as avg_pruning_pct,
        sum(
            case
                when bytes_spilled_to_local_storage > 0 then 1
                else 0
            end
        ) as count_queries_with_spillage,
        avg(bytes_spilled_to_local_storage) as avg_spillage_local,
        avg(bytes_spilled_to_remote_storage) as avg_spillage_remote,
        avg(bytes_sent_over_the_network) as avg_bytes_sent_over_network,
        sum(
            case
                when queued_provisioning_time + queued_repair_time + queued_overload_time > 0 then 1
                else 0
            end
        ) as count_queries_with_queuing
    FROM queries qu
        join whstats whs 
            on qu.warehouse_name = whs.warehouse_name
            and qu.warehouse_size = whs.warehouse_size
            and qu.bytes_scanned between (whs.avg_bytes_scanned - (2 * whs.stdev_bytes_scanned)) and (whs.avg_bytes_scanned + (2 * whs.stdev_bytes_scanned))
    WHERE qu.execution_status = 'SUCCESS'
    GROUP BY qu.warehouse_id,
        qu.warehouse_size
),
wh_run_time as (
    -- calculation of total warehouse run time during the specified time period.
    -- this query needs some testing/validation - there are instances where
    -- two RESUME_WAREHOUSE events complete sequentially without intermediate
    -- SUSPEND_WAREHOUSE events, some other oddities...
    --
    -- note that these statistics are calculated solely at the warehouse level,
    -- not by warehouse and size
    select warehouse_id,
        sum(elapsed_time_in_ms) as total_wh_runtime_ms
    from (
            with wh_events as (
                select warehouse_id,
                    timestamp,
                    event_name,
                    row_number() over (
                        partition by warehouse_id
                        order by timestamp
                    ) as record_number
                from warehouse_events_history
                    join timewindow tw
                where 1=1
                and timestamp > tw.start_marker
                    and event_state = 'COMPLETED'
                    and event_name in ('SUSPEND_WAREHOUSE', 'RESUME_WAREHOUSE')
                order by warehouse_id,
                    timestamp
            )
            select wh.warehouse_id,
                case
                    when (
                        wh.record_number = 1
                        and wh.event_name = 'SUSPEND_WAREHOUSE'
                    ) then datediff(
                        millisecond,
                        dateadd(month, -1, current_date()),
                        wh.timestamp
                    )
                    else case
                        when wh.event_name = 'RESUME_WAREHOUSE' then case
                            when whnext.record_number is null then datediff(millisecond, wh.timestamp, current_timestamp())
                            when whnext.event_name = 'RESUME_WAREHOUSE' then 0
                            else datediff(millisecond, wh.timestamp, whnext.timestamp)
                        end
                        else 0
                    end
                end as elapsed_time_in_ms
            from wh_events wh
                left join wh_events whnext on wh.warehouse_id = whnext.warehouse_id
                and wh.record_number + 1 = whnext.record_number
            order by wh.warehouse_id,
                wh.record_number
        ) x
    group by warehouse_id
) ,
wh_query_time as (
    -- total time spent executing queries, by warehouse.  This query also needs
    -- testing and validation, it's a complicated beast.  As with the prior CTE these
    -- stats are calculated by warehouse, not by warehouse and size.
    --
    -- this portion has to include both successful and failed queries as both will
    -- cause warehouses to spin up from a suspended status
    select
        warehouse_id
        , sum(datediff('millisecond', IslandStartDate, IslandEndDate)) AS total_wh_querytime_ms
    from
     (
    select
      warehouse_id,
      IslandId,
      min(start_time) AS IslandStartDate,
      max(end_time) AS IslandEndDate
     from
      (
    select
     *,
     case when Grouping.PreviousEndDate >= start_time then 0 else 1 end as IslandStartInd,
     sum(case when Grouping.PreviousEndDate >= start_time then 0 else 1 end) over (order by Grouping.row_number) as IslandId
    from
    (
    select
      row_number() over (order by warehouse_id, start_time, end_time) as row_number,
      warehouse_id,
      start_time,
      end_time,
      max(end_time) over (partition by warehouse_id order by start_time, end_time ROWS between unbounded preceding and 1 preceding) as PreviousEndDate
    from
      queries
      ) grouping
      ) Islands
     group by
      warehouse_id,
      IslandId
     )  
    GROUP BY warehouse_id
),
query_failures as
(select warehouse_id
    , warehouse_size
    , array_agg(object_construct(
        'user_name', user_name,
        'role_name', role_name,
        'query_type', query_type,
        'failed_query_count', failed_query_count)) as failed_query_object
 from (
       select warehouse_id
         , warehouse_size
         , user_name
         , role_name
         , query_type
         , count(*) as failed_query_count
      from queries
      where execution_status in ('FAIL', 'INCIDENT')
      group by warehouse_id
         , warehouse_size
         , user_name
         , role_name
         , query_type
      ) x
 group by warehouse_id
     , warehouse_size
)
select _2_warehouse_name as warehouse_name,
    _3_warehouse_size as warehouse_size,
    _5_credits_used as credits_used,
    _6_total_query_count as total_query_count,
    _6a_successful_query_count as successful_query_count,
    _6b_failed_query_count as failed_query_count,
    _7_credits_per_query as credits_per_query_total,
    _7a_credits_per_successful_query as credits_per_successful_query,
    object_construct(*) as output_variant
from (
    SELECT -- Overall histogram - look at bytes scanned to assess homogeneity
            object_construct(
                '1:      WH Size:',
                whstats.warehouse_size,
                '2: Credit Count:',
                round(c.credits_used),
                '3:  Query Count:',
                whstats.total_query_count,
                '4:       < 1 GB:',
                repeat(
                    '+',
                    (q.count_below_one_gb / whstats.total_query_count) * 40
                ),
                '5:    1 - 20 GB:',
               repeat(
                    '+',
                    (
                        q.count_btw_one_and_twenty_gb / whstats.total_query_count
                    ) * 40
                ),
                '6:   20 - 50 GB:',
                repeat(
                    '+',
                    (
                        q.count_btw_twenty_and_fifty_gb / whstats.total_query_count
                    ) * 40
                ),
                '7:  50 - 100 GB:',
                repeat(
                    '+',
                    (
                        q.count_btw_fifty_and_one_hundred_gb / whstats.total_query_count
                    ) * 40
                ),
                '8: 100 - 250 GB:',
                repeat(
                    '+',
                    (
                        q.count_btw_one_hundred_and_twofifty_gb / whstats.total_query_count
                    ) * 40
                ),
                '9:     > 250 GB:',
                repeat(
                    '+',
                    (q.count_over_twofifty_gb / whstats.total_query_count) * 40
                )
            ) as _1_all_queries_bytes_scanned_histogram -- Warehouse-level information 
,
            whstats.warehouse_name as _2_warehouse_name,
            q.warehouse_size as _3_warehouse_size,
            object_construct(
                'total_time_executing_queries_sec',
                whqt.total_wh_querytime_ms / 1000,
                'total_time_warehouse_running_sec',
                whrt.total_wh_runtime_ms / 1000,
                
                'warehouse_utilization_pct',
                round(whqt.total_wh_querytime_ms / whrt.total_wh_runtime_ms, 5) * 100) as _4_overall_warehouse_utilization_pct,
            round(c.credits_used) as _5_credits_used,
            whstats.total_query_count as _6_total_query_count,
            whstats.successful_query_count as _6a_successful_query_count,
            whstats.failed_query_count as _6b_failed_query_count,
            round(round(c.credits_used) / whstats.total_query_count, 3) as _7_credits_per_query,
            round(round(c.credits_used) / whstats.successful_query_count, 3) as _7a_credits_per_successful_query,
            -- P95 query statistics - filter outliers and look only at queries within two standard deviations of the overall average bytes_scanned
            pq.p95_query_count as _8_p95_query_count,
            object_construct(
                'avg_bytes_large',
                CASE
                    WHEN pq.avg_large >= POWER(2, 40) THEN to_char(ROUND(pq.avg_large / POWER(2, 40), 1)) || ' TB'
                    WHEN pq.avg_large >= POWER(2, 30) THEN to_char(ROUND(pq.avg_large / POWER(2, 30), 1)) || ' GB'
                    WHEN pq.avg_large >= POWER(2, 20) THEN to_char(ROUND(pq.avg_large / POWER(2, 20), 1)) || ' MB'
                    WHEN pq.avg_large >= POWER(2, 10) THEN to_char(ROUND(pq.avg_large / POWER(2, 10), 1)) || ' K'
                    ELSE to_char(pq.avg_large)
                end,
                'avg_large_exe_time_sec',
                ROUND(pq.avg_large_exe_time / 1000, 2),
                'avg_all_exe_time_sec',
                ROUND(pq.avg_execution_time / 1000, 2),
                'avg_partitions_scanned',
                ROUND(pq.avg_partitions_scanned, 2),
                'avg_pruning_pct',
                round(pq.avg_pruning_pct, 4) * 100,
                'pct_queries_with_spillage',
                pq.count_queries_with_spillage / case
                    when pq.p95_query_count = 0 then 1
                    else pq.p95_query_count
                end,
                'avg_spillage_local',
                pq.avg_spillage_local,
                'avg_spillage_remote',
                pq.avg_spillage_remote,
                'avg_bytes_sent_over_network',
                pq.avg_bytes_sent_over_network,
                'pct_queries_with_queueing',
                pq.count_queries_with_queuing / case
                    when pq.p95_query_count = 0 then 1
                    else pq.p95_query_count
                end * 100
            ) as _9_p95_query_statistics,
           qf.failed_query_object as _9_query_failure_info
        FROM whstats whstats
            join all_queries q
                on whstats.warehouse_id = q.warehouse_id
                and whstats.warehouse_size = q.warehouse_size
            join credits c
                on whstats.warehouse_id = c.warehouse_id
            join p95_queries pq
                on whstats.warehouse_id = pq.warehouse_id
                and whstats.warehouse_size = pq.warehouse_size
            left join wh_run_time whrt
                on whstats.warehouse_id = whrt.warehouse_id
            join wh_query_time whqt
                on whstats.warehouse_id = whqt.warehouse_id
            left join query_failures qf
                on whstats.warehouse_id = qf.warehouse_id
                and whstats.warehouse_size = qf.warehouse_size
    ) x
order by _5_credits_used desc, 1, 2
;


-- ================================================================================
-- QUERY: warehouse_utilisation_score_01.sql
-- ================================================================================

with wh_kpis as
        (
            select
                job.warehouse_name
                ,avg(job.stats:stats.serverCount/job.stats:stats.warehouseSize) as avg_pct_warehouse_used
                ,max(job.stats:stats.warehouseSize) as warehouse_size
                ,percentile_cont(.90) within group (order by (job.stats:stats.serverCount/job.stats:stats.warehouseSize)) as p90_wh_used
                ,percentile_cont(.50) within group (order by (job.stats:stats.serverCount/job.stats:stats.warehouseSize)) as p50_wh_used
                ,count(distinct job.uuid) as job_count
                ,sum(dur_queued_load)/1000 as total_queue_time
                ,sum(total_duration)/1000 as total_duration
                ,avg(dur_queued_load/total_duration) as avg_pct_total_queued
                ,avg(dur_xp_executing)/1000 as avg_xp_duration
                ,avg(total_duration)/1000 as avg_total_dur
                ,avg(job.stats:stats.ioRemoteTempWriteBytes)/power(1024, 3) as remote_temp_space_usage
                ,SUM(job.stats:stats.ioLocalTempWriteBytes)/power(1024, 3) as local_temp_space_usage --S/B AVG
                ,sum(case when job.stats:stats.ioRemoteTempWriteBytes is not null then 1 else 0 end) as num_jobs_remote_spilling
                ,sum(case when job.stats:stats.ioRemoteTempWriteBytes is not null then 1 else 0 end) / job_count as pct_jobs_spilled_remote
            from SNOWHOUSE_IMPORT.@ACCOUNT_DEPLOYMENT@.JOB_ETL_V job
                where warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND job.account_id = '@ACCOUNT_ID@'
                  and (
                       warehouse_name not in   ('NULL','COMPUTE_SERVICE_WH') and
                       warehouse_name not like 'COMPUTE_SERVICE_WH_CLUSTERING%' and 
                       warehouse_name not like 'COMPUTE_SERVICE_WH_USER%' and
                       warehouse_name not like 'COMPUTE_SERVICE_WH_MATERIALIZED_VIEW%' and
                       warehouse_name not like 'COMPUTE_SERVICE_WH_SNOWFLAKEDB_UPGRADE_POOL%' and
                       warehouse_name not like 'COMPUTE_SERVICE_WH_BUNDLE_UPGRADE_POOL%' and 
                       warehouse_name not like 'COMPUTE_SERVICE_WH_BUNDLE_INSTANCE_UPGRADE_POOL%' and 
                       warehouse_name not like 'COMPUTE_SERVICE_WH_SEARCH_OPTIMIZATION%' and 
                       warehouse_name not like 'COMPUTE_SERVICE_WH_FASTER_DML%'
                      )
                  and job.latest_cluster_number is not null
                  and job.created_on > dateadd(days, -30, current_date())
            group by 1
            order by 1
        ),  
        cat_scores as (
            select
                 warehouse_name
                 , p90_wh_used
                 , case when p90_wh_used < 1 then 100::number end as p90_wh_used_score --if 90 percent of the jobs dont use the full warehouse, start with 100 
                 , avg_pct_warehouse_used
                 , (100 - (avg_pct_warehouse_used*100))::number  as avg_wh_unused_score --add one point for each avg pct below 100 used
                 , avg_xp_duration
                 , iff(avg_xp_duration < 10,((60 - avg_xp_duration)*2)::number, (60 - avg_xp_duration)::number) as xp_dur_score --subtract a point for each second over 60, add a point for each second below 60
                 , pct_jobs_spilled_remote
                 , -(pct_jobs_spilled_remote*100*2)::number as pct_jobs_spilling_score -- substract 2* % of jobs which spill to remote disk
                 , 100 - (avg_xp_duration/avg_total_dur*100) avg_pct_WH_Idle
            from wh_kpis   
        ),
        total_score as (
            select 
                warehouse_name
                , sum(score) as score
                , round(p90_wh_used*100,2) "% of WH used for 90th %ile"
                , round(avg_pct_warehouse_used*100,2) "Avg % of WH used"
                , round(avg_xp_duration,2) "Avg XP duration (mins)"
                , round(avg_pct_WH_Idle,2) "Avg % WH idle time in jobs"
                , round(pct_jobs_spilled_remote*100,2) "% of jobs spilled to remote"
            from  cat_scores
                  unpivot(score for scores in (p90_wh_used_score, avg_wh_unused_score, xp_dur_score, pct_jobs_spilling_score))
            group by 1, 3, 4, 5, 6, 7
            )
        select 
               wh.name warehouse_name,
               size,
               score,
               case
                  when score <= -300 then 'Prioritise this for review (Too small!)'
                  when score < -100 then 'Please review (Too small)'
                  when score < 0    then 'No action for now'
                  when score = 0    then 'Perfect'
                  when score < 100  then 'No action for now'
                  when score < 300  then 'Please review (Too big)'
                  when score >= 300  then 'Prioritise this for review (Too big!)'
                end as "Action plan",
                "% of WH used for 90th %ile",
                "Avg % of WH used",
                "Avg XP duration (mins)",
                "Avg % WH idle time in jobs",
                "% of jobs spilled to remote"
        from 
        SNOWHOUSE_IMPORT.@ACCOUNT_DEPLOYMENT@.warehouse_etl_v wh
        left join total_score s on wh.name = s.warehouse_name
where wh.account_id = '@ACCOUNT_ID@'
and deleted_on is null
and temp_id = 0
        order by 1

