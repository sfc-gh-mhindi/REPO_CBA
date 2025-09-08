{{
    config(
        pre_hook=[
            'CREATE OR REPLACE TRANSIENT TABLE ' ~ cvar("intermediate_db") ~ '.' ~ cvar("files_schema") ~ '.'~ cvar("base_dir")~ '__lookupset__MAP_CSE_PACK_PDCT_PL_PL_PACK_CAT_ID__FS (PL_PACKAGE_CAT_ID VARCHAR(16777216),PDCT_N VARCHAR(16777216));'
        ]
    )
}}

WITH cte AS (
    SELECT 1 AS dummy
)

SELECT
    dummy
FROM cte