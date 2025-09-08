{{
    config(
        pre_hook=[
            'create or replace table '~cvar("intermediate_db")~'.'~ cvar("files_schema")~ '.'~ cvar("base_dir")~ '__lookupset__run_stream_name_check_' ~ cvar("run_stream") ~ '_hash (RUN_STRM_COUNT INT);',
            'create or replace table '~cvar("intermediate_db")~'.'~ cvar("files_schema") ~ '.'~ cvar("base_dir")~ '__temp__'~ cvar("run_stream")~ '_processmd02_err (ERR_COL varchar);',
            'create or replace table '~ cvar('intermediate_db') ~ '.'~ cvar("files_schema")~ '.' ~ cvar("base_dir") ~ '__temp__' ~ cvar("run_stream")~ '_runstreamdoesnotexist_err (RUN_STRM_COUNT INT);',
        ]
    )
}} 

WITH cte as(
    select 1 as dummy
)

SELECT
    dummy
FROM cte