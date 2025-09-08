{{
    config(
        pre_hook=[
            'CREATE OR REPLACE TRANSIENT TABLE '~cvar("intermediate_db")~'.'~ cvar("files_schema")~ '.'~ cvar("base_dir")~ '__lookupset__step_occr_'~ cvar("run_stream")~ '_hash (STEP_OCCR_ID VARCHAR(255), RUN_STRM_OCCR_ID VARCHAR(255), RUN_STRM_C VARCHAR(255), STEP_C VARCHAR(255), STEP_STUS_C VARCHAR(50), STEP_OCCR_ISRT_ROW_CNT NUMBER(38,0), STEP_OCCR_UPDT_ROW_CNT NUMBER(38,0), STEP_OCCR_FAIL_ROW_CNT NUMBER(38,0), STEP_OCCR_RSRT_VALU VARCHAR(255), STEP_OCCR_STRT_S TIMESTAMP_LTZ(9), STEP_OCCR_END_S TIMESTAMP_LTZ(9), RECD_CRAT_S TIMESTAMP_LTZ(9), STEP_SQNO NUMBER(38,0));'
        ]
    )
}}


with cte as(
    select 1 as dummy
)

SELECT
    dummy
FROM cte