-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;


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