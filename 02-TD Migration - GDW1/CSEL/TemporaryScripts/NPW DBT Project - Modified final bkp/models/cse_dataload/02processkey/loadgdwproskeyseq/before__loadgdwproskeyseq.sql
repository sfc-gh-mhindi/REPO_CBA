{{
    config(
        pre_hook=[
            'CREATE OR REPLACE TRANSIENT TABLE '~ cvar("intermediate_db")~ '.' ~ cvar("files_schema") ~ '.PROCESSKEYHASH__HSH (CONV_M VARCHAR(255) NOT NULL, PROS_KEY_I NUMBER(10,0) NOT NULL);',
            'CREATE OR REPLACE TRANSIENT TABLE '~cvar("intermediate_db")~'.'~ cvar("files_schema") ~ '.' ~ cvar("base_dir") ~ '__TEMP__UTIL_PROS_ISAC__TXT(CONV_M VARCHAR(255) NOT NULL, PROS_KEY_I NUMBER(10,0) NOT NULL);'
        ]
    )
}}

WITH cte as(
    SELECT 1 as dummy
)

SELECT
    dummy
FROM cte