-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;


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