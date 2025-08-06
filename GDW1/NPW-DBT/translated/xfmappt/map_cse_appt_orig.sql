{{ config(materialized='view', tags=['XfmAppt']) }}

WITH 
,
MAP_CSE_APPT_ORIG AS (SELECT CHNL_CAT_ID, APPT_ORIG_C FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_ORIG  WHERE  CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd'))


SELECT * FROM MAP_CSE_APPT_ORIG