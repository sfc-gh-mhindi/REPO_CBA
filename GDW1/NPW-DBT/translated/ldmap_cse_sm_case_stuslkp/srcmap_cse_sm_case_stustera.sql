{{ config(materialized='view', tags=['LdMAP_CSE_SM_CASE_STUSLkp']) }}

WITH 
,
SrcMAP_CSE_SM_CASE_STUSTera AS (SELECT MAP_CSE_SM_CASE_STUS.SM_STAT_CAT_ID,
MAP_CSE_SM_CASE_STUS.STUS_C 
FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_SM_CASE_STUS as MAP_CSE_SM_CASE_STUS 
WHERE MAP_CSE_SM_CASE_STUS.efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and MAP_CSE_SM_CASE_STUS.expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_SM_CASE_STUSTera