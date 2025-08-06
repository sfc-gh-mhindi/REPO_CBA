{{ config(materialized='view', tags=['LdMAP_CSE_ENV_PATY_RELLkp']) }}

WITH 
,
SrcMAP_CSE_ENV_PATY_REL_Tera AS (SELECT CLNT_RELN_TYPE_ID,CLNT_POSN,REL_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_ENV_PATY_REL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_ENV_PATY_REL_Tera