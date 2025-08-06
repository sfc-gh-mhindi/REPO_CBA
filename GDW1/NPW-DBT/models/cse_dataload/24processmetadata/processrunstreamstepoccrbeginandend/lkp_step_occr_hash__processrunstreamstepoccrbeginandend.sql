{{
    config(
        post_hook=[
            "INSERT OVERWRITE INTO "~ cvar("files_schema")~ "."~ cvar("base_dir")~ "__lookupset__step_occr_"~ cvar("run_stream")~ "_hash SELECT * FROM {{ this}}",
        ]
    )
}}

WITH lkp_step_occr as(
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
FROM {{ ref('lkp_step_occr__processrunstreamstepoccrbeginandend') }}
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
    STEP_OCCR_STRT_S,
    STEP_OCCR_END_S,
    RECD_CRAT_S,
    STEP_SQNO    
FROM lkp_step_occr