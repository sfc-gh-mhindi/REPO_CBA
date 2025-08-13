{{
    config(
        pre_hook = [
            'CREATE OR REPLACE TRANSIENT TABLE '~ cvar("intermediate_db")~'.'~ cvar("files_schema")~ '.'~ cvar("base_dir")~ '__lookupset__finish_rs_etl_d_'~ cvar("run_stream") ~ '_hash (ETL_D VARCHAR(10));'
        ]
    )
}}

WITH cte AS (
    SELECT 1 as dummy
)

SELECT
    dummy
FROM cte