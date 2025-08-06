with RUN_STRM_ETL_D as(
    SELECT 
        RS_M,
        ETL_D
    FROM {{ cvar('stg_ctl_db') }}.{{ cvar("ctl_schema") }}.RUN_STRM_ETL_D
),
src_run_strm_etl_d_tbl as(
    SELECT 
        to_char(ETL_D,'YYYY-MM-DD') as ETL_D
    FROM RUN_STRM_ETL_D 
    WHERE RS_M='{{ cvar("run_stream") }}'
)

SELECT
    ETL_D
FROM src_run_strm_etl_d_tbl