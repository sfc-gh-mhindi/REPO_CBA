{{ config(materialized='view', tags=['XfmAppt_Pdct_Paty']) }}

WITH 
,
Copy_of_MAP_CSE_APPT_QLFY AS (SELECT SBTY_CODE,APPT_QLFY_C  FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_QLFY  WHERE  CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd'))


SELECT * FROM Copy_of_MAP_CSE_APPT_QLFY