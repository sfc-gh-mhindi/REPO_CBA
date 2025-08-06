WITH STEP_OCCR as(
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
FROM {{ cvar("stg_ctl_db") }}.{{ cvar("ctl_schema") }}.STEP_OCCR
)
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
    TO_CHAR(STEP_OCCR_STRT_S, 'YYYY-MM-DD HH24:MI:SS') as STEP_OCCR_STRT_S,
    TO_CHAR(STEP_OCCR_END_S, 'YYYY-MM-DD HH24:MI:SS') as STEP_OCCR_END_S,
    TO_CHAR(RECD_CRAT_S, 'YYYY-MM-DD HH24:MI:SS') as RECD_CRAT_S,
    STEP_SQNO 
FROM STEP_OCCR 
WHERE STEP_OCCR_ID = '{{ cvar("step_name") }}'||'{{ cvar("etl_process_dt") }}'
AND RUN_STRM_C = '{{ cvar("run_stream") }}'
AND RUN_STRM_OCCR_ID = '{{ cvar("run_stream") }}'||'{{ cvar("etl_process_dt") }}'
AND STEP_C = '{{ cvar("step_phase") }}'