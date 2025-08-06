{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH 
,
MAP_CSE_SM_CASE_STUS AS (SELECT SM_STAT_CAT_ID,STUS_C  FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_SM_CASE_STUS  WHERE  CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd'))


SELECT * FROM MAP_CSE_SM_CASE_STUS