-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;


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