{{ config(materialized='view', tags=['LdMAP_CSE_ENV_CHILD_PATY_RELLkp']) }}

WITH 
,
SrcMAP_CSE_ENV_CHILD_PATY_REL_Tera AS (SELECT FA_CHLD_STAT_CAT_ID,REL_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_ENV_CHLD_PATY_REL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_ENV_CHILD_PATY_REL_Tera