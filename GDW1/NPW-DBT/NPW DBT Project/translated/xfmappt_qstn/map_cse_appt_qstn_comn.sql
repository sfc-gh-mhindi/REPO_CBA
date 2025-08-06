{{ config(materialized='view', tags=['XfmAppt_Qstn']) }}

WITH 
,
MAP_CSE_APPT_QSTN_COMN AS (SELECT QA_QUESTION_ID, QSTN_C  FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_QSTN_COMN  WHERE  CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd') )


SELECT * FROM MAP_CSE_APPT_QSTN_COMN