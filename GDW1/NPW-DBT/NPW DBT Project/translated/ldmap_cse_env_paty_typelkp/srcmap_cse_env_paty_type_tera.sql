{{ config(materialized='view', tags=['LdMAP_CSE_ENV_PATY_TYPELkp']) }}

WITH 
,
SrcMAP_CSE_ENV_PATY_TYPE_Tera AS (SELECT FA_ENTY_CAT_ID,PATY_TYPE_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_ENV_PATY_TYPE WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_ENV_PATY_TYPE_Tera