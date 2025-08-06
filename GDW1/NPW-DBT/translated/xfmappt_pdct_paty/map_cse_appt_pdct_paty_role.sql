{{ config(materialized='view', tags=['XfmAppt_Pdct_Paty']) }}

WITH 
,
MAP_CSE_APPT_PDCT_PATY_ROLE AS (SELECT ROLE_CAT_ID,PATY_ROLE_C  FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_PDCT_PATY_ROLE  WHERE  CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd'))


SELECT * FROM MAP_CSE_APPT_PDCT_PATY_ROLE