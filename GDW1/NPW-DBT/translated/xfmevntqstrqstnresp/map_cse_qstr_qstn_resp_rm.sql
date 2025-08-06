{{ config(materialized='view', tags=['XfmEvntQstrQstnResp']) }}

WITH 
,
MAP_CSE_QSTR_QSTN_RESP_RM AS (SELECT RATING,RESP_C FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_QSTR_QSTN_RESP_RM WHERE   CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd'))


SELECT * FROM MAP_CSE_QSTR_QSTN_RESP_RM