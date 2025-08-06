{{ config(materialized='view', tags=['XfmAppt_Pdct']) }}

WITH 
,
MAP_CSE_APPT_PDCT AS (SELECT PO_OVERDRAFT_CAT_ID, APPT_PDCT_CATG_C, APPT_PDCT_DURT_C FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_PDCT  WHERE  CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd'))


SELECT * FROM MAP_CSE_APPT_PDCT