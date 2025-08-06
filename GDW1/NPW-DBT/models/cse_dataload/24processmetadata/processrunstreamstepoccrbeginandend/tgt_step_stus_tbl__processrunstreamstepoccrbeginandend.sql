{{
    config(
        post_hook=[
            "
            MERGE INTO "~ cvar("stg_ctl_db")~ "."~ cvar("ctl_schema") ~".STEP_OCCR AS target
            USING {{ this }} AS source
            ON target.STEP_OCCR_ID = source.STEP_OCCR_ID AND target.STEP_C = source.STEP_C
            WHEN MATCHED AND source.Ins_Upd_flag = 'U' THEN
                UPDATE SET 
                    target.RUN_STRM_OCCR_ID = source.RUN_STRM_OCCR_ID,
                    target.RUN_STRM_C = source.RUN_STRM_C,
                    target.STEP_STUS_C = source.STEP_STUS_C,
                    target.STEP_OCCR_ISRT_ROW_CNT = source.STEP_OCCR_ISRT_ROW_CNT,
                    target.STEP_OCCR_UPDT_ROW_CNT = source.STEP_OCCR_UPDT_ROW_CNT,
                    target.STEP_OCCR_FAIL_ROW_CNT = source.STEP_OCCR_FAIL_ROW_CNT,
                    target.STEP_OCCR_RSRT_VALU = source.STEP_OCCR_RSRT_VALU,
                    target.STEP_OCCR_STRT_S = source.STEP_OCCR_STRT_S,
                    target.STEP_OCCR_END_S = source.STEP_OCCR_END_S,
                    target.RECD_CRAT_S = source.RECD_CRAT_S,
                    target.STEP_SQNO = source.STEP_SQNO
            WHEN NOT MATCHED AND source.Ins_Upd_flag = 'I' THEN
                INSERT (
                    STEP_OCCR_ID, RUN_STRM_OCCR_ID, RUN_STRM_C, STEP_C, STEP_STUS_C,
                    STEP_OCCR_ISRT_ROW_CNT, STEP_OCCR_UPDT_ROW_CNT, STEP_OCCR_FAIL_ROW_CNT,
                    STEP_OCCR_RSRT_VALU, STEP_OCCR_STRT_S, STEP_OCCR_END_S, RECD_CRAT_S, STEP_SQNO
                )
                VALUES (
                    source.STEP_OCCR_ID, source.RUN_STRM_OCCR_ID, source.RUN_STRM_C, source.STEP_C, source.STEP_STUS_C,
                    source.STEP_OCCR_ISRT_ROW_CNT, source.STEP_OCCR_UPDT_ROW_CNT, source.STEP_OCCR_FAIL_ROW_CNT,
                    source.STEP_OCCR_RSRT_VALU, source.STEP_OCCR_STRT_S, source.STEP_OCCR_END_S, source.RECD_CRAT_S, source.STEP_SQNO
                )
            "
        ]
    )
}}

WITH run_strm_etl_d AS (
    SELECT 
        RS_M,
        ETL_D
    FROM {{ cvar("stg_ctl_db") }}.{{ cvar("ctl_schema") }}.RUN_STRM_ETL_D
    WHERE RS_M='{{ cvar("run_stream") }}'
),
insert_flow AS (
    SELECT
        '{{ cvar("step_name") }}'||'{{ cvar("etl_process_dt") }}' as STEP_OCCR_ID,
        '{{ cvar("run_stream") }}'||'{{ cvar("etl_process_dt") }}' as RUN_STRM_OCCR_ID,
        '{{ cvar("run_stream") }}' as RUN_STRM_C,
        '{{ cvar("step_phase") }}' as STEP_C,
        'R' as STEP_STUS_C,
        NULL as STEP_OCCR_ISRT_ROW_CNT,
        NULL as STEP_OCCR_UPDT_ROW_CNT,
        NULL as STEP_OCCR_FAIL_ROW_CNT,
        NULL as STEP_OCCR_RSRT_VALU,
        CURRENT_TIMESTAMP AS STEP_OCCR_STRT_S,
        NULL as STEP_OCCR_END_S,
        CURRENT_TIMESTAMP AS RECD_CRAT_S,
        0 AS STEP_SQNO
    FROM run_strm_etl_d
),
update_flow AS (
    SELECT
        STEP_OCCR_ID,
        RUN_STRM_OCCR_ID,
        RUN_STRM_C,
        STEP_C,
        CASE 
            WHEN TRIM(STEP_STUS_C) = 'C' THEN 'R' 
            ELSE 'C' 
        END AS STEP_STUS_C,
        STEP_OCCR_ISRT_ROW_CNT,
        STEP_OCCR_UPDT_ROW_CNT,
        STEP_OCCR_FAIL_ROW_CNT,
        STEP_OCCR_RSRT_VALU,
        CASE 
            WHEN TRIM(STEP_STUS_C) = 'C' THEN CURRENT_TIMESTAMP 
            ELSE STEP_OCCR_STRT_S 
        END AS STEP_OCCR_STRT_S,
        CASE 
            WHEN TRIM(STEP_STUS_C) = 'C' THEN NULL 
            ELSE CURRENT_TIMESTAMP 
        END AS STEP_OCCR_END_S,
        CASE 
            WHEN TRIM(STEP_STUS_C) = 'C' THEN CURRENT_TIMESTAMP 
            ELSE RECD_CRAT_S 
        END AS RECD_CRAT_S,
        0 AS STEP_SQNO
    FROM {{ ref('xfm_step_status__processrunstreamstepoccrbeginandend') }}
    WHERE STEP_OCCR_ID = '{{ cvar("step_name") }}{{ cvar("etl_process_dt") }}'
      AND STEP_C = '{{ cvar("step_phase") }}'
)

SELECT
    COALESCE(b.STEP_OCCR_ID,a.STEP_OCCR_ID) as STEP_OCCR_ID,
    COALESCE(b.RUN_STRM_OCCR_ID,a.RUN_STRM_OCCR_ID) as RUN_STRM_OCCR_ID,
    COALESCE(b.RUN_STRM_C,a.RUN_STRM_C) as RUN_STRM_C,
    COALESCE(b.STEP_C,a.STEP_C) as STEP_C,
    COALESCE(b.STEP_STUS_C,a.STEP_STUS_C) as STEP_STUS_C,
    COALESCE(b.STEP_OCCR_ISRT_ROW_CNT,a.STEP_OCCR_ISRT_ROW_CNT) as STEP_OCCR_ISRT_ROW_CNT,
    COALESCE(b.STEP_OCCR_UPDT_ROW_CNT,a.STEP_OCCR_UPDT_ROW_CNT) as STEP_OCCR_UPDT_ROW_CNT,
    COALESCE(b.STEP_OCCR_FAIL_ROW_CNT,a.STEP_OCCR_FAIL_ROW_CNT) as STEP_OCCR_FAIL_ROW_CNT,
    COALESCE(b.STEP_OCCR_RSRT_VALU,a.STEP_OCCR_RSRT_VALU) as STEP_OCCR_RSRT_VALU,
    COALESCE(b.STEP_OCCR_STRT_S,a.STEP_OCCR_STRT_S) as STEP_OCCR_STRT_S,
    COALESCE(b.STEP_OCCR_END_S,a.STEP_OCCR_END_S) as STEP_OCCR_END_S,
    COALESCE(b.RECD_CRAT_S,a.RECD_CRAT_S) as RECD_CRAT_S,
    COALESCE(b.STEP_SQNO,a.STEP_SQNO) as STEP_SQNO,
    case when b.STEP_OCCR_ID is null then 'I' Else 'U' end as Ins_Upd_flag
FROM insert_flow a
left join update_flow b
ON a.STEP_OCCR_ID = b.STEP_OCCR_ID
and a.STEP_C = b.STEP_C