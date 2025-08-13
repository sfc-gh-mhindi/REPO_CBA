{{
    config(
        pre_hook=[
            'CREATE OR REPLACE TRANSIENT TABLE '~cvar("intermediate_db")~'.'~ cvar("datasets_schema") ~ '.'~ cvar("base_dir") ~ '__dataset__'~ cvar("run_stream") ~'_PREMAP__DS (PL_APP_ID VARCHAR(12), NOMINATED_BRANCH_ID VARCHAR(12), PL_PACKAGE_CAT_ID VARCHAR(12), ORIG_ETL_D VARCHAR(10));',
            'CREATE OR REPLACE TRANSIENT TABLE '~cvar("intermediate_db")~'.'~ cvar("datasets_schema") ~ '.'~ cvar("base_dir") ~ '__dataset__'~ cvar("run_stream") ~'_PlApp_Nulls_Rejects__DS (PL_APP_ID VARCHAR(12), NOMINATED_BRANCH_ID VARCHAR(12), PL_PACKAGE_CAT_ID VARCHAR(12), ETL_D VARCHAR(10), ORIG_ETL_D VARCHAR(10), EROR_C VARCHAR(10));'
        ]
    )
}}


WITH cte as(
select 
1 as dummy
)

SELECT
dummy
FROM cte