WITH
before_cte as(
    SELECT
     dummy
     FROM {{ ref('before__processrunstreamfinishingpoint') }}
     WHERE 1=2
),
src_run_strm_etl_d_tbl as(
    SELECT TO_Char(ETL_D,'YYYY-MM-DD') as ETL_D 
    FROM {{ cvar("stg_ctl_db") }}.{{ cvar("ctl_schema") }}.RUN_STRM_ETL_D 
    WHERE RS_M='{{ cvar("run_stream") }}'
)

SELECT 
    ETL_D
FROM src_run_strm_etl_d_tbl