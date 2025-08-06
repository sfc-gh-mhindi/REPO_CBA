{{ config(materialized='view', tags=['XfmAppt_Pdct_Purp']) }}

WITH 
,
MAP_CSE_APPT_PDCT_PURP_PO AS (SELECT PL_PROD_PURP_CAT_ID, PURP_TYPE_C  FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_PDCT_PURP_PO  WHERE  CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd');  )


SELECT * FROM MAP_CSE_APPT_PDCT_PURP_PO