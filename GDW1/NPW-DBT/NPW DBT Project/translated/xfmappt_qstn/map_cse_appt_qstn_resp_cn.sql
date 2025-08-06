{{ config(materialized='view', tags=['XfmAppt_Qstn']) }}

WITH 
,
MAP_CSE_APPT_QSTN_RESP_CN AS (SELECT QA_ANSWER_ID, RESP_C  FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_QSTN_RESP_CN  WHERE  CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd'))


SELECT * FROM MAP_CSE_APPT_QSTN_RESP_CN