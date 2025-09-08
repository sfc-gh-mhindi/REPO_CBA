{{
    config(
       pre_hook = [
    'CREATE OR REPLACE TRANSIENT TABLE ' ~ cvar("intermediate_db") ~ '.' ~ cvar("files_schema") ~ '.' ~ cvar("base_dir") ~ '__TEMP__UTIL_PROS_ISAC__TXT1 (NUM_LOAD_ERR NUMBER(38,0) NOT NULL);'
]
    )
}}

WITH cte as(
Select 1 as dummy
)

SELECT
dummy  
FROM cte