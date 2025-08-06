WITH lkp_step_occr_hash as(
    SELECT
        STEP_OCCR_ID,
        RUN_STRM_OCCR_ID,
        RUN_STRM_C,
        STEP_C,
        STEP_STUS_C,
        STEP_OCCR_ISRT_ROW_CNT,
        STEP_OCCR_UPDT_ROW_CNT,
        STEP_OCCR_FAIL_ROW_CNT,
        STEP_OCCR_RSRT_VALU,
        STEP_OCCR_STRT_S,
        STEP_OCCR_END_S,
        RECD_CRAT_S,
        STEP_SQNO 
    FROM {{ ref('lkp_step_occr_hash__processrunstreamstepoccrbeginandend') }}
),
src_run_strm_etl_d_tbl as(
    SELECT 
        ETL_D
    FROM {{ ref('src_run_strm_etl_d_tbl__processrunstreamstepoccrbeginandend') }}
),
xfm_step_status as(
    SELECT
        a.ETL_D,
        b.STEP_OCCR_ID,
        b.RUN_STRM_OCCR_ID,
        b.RUN_STRM_C,
        b.STEP_C,
        b.STEP_STUS_C,
        b.STEP_OCCR_ISRT_ROW_CNT,
        b.STEP_OCCR_UPDT_ROW_CNT,
        b.STEP_OCCR_FAIL_ROW_CNT,
        b.STEP_OCCR_RSRT_VALU,
        b.STEP_OCCR_STRT_S,
        b.STEP_OCCR_END_S,
        b.RECD_CRAT_S,
        b.STEP_SQNO
    FROM src_run_strm_etl_d_tbl__processrunstreamstepoccrbeginandend a
    left join lkp_step_occr_hash__processrunstreamstepoccrbeginandend b
    ON b.STEP_OCCR_ID = '{{ cvar("step_name") }}'||'{{ cvar("etl_process_dt") }}'
)

SELECT
    ETL_D,
    STEP_OCCR_ID,
    RUN_STRM_OCCR_ID,
    RUN_STRM_C,
    STEP_C,
    STEP_STUS_C,
    STEP_OCCR_ISRT_ROW_CNT,
    STEP_OCCR_UPDT_ROW_CNT,
    STEP_OCCR_FAIL_ROW_CNT,
    STEP_OCCR_RSRT_VALU,
    STEP_OCCR_STRT_S,
    STEP_OCCR_END_S,
    RECD_CRAT_S,
    STEP_SQNO
FROM xfm_step_status
