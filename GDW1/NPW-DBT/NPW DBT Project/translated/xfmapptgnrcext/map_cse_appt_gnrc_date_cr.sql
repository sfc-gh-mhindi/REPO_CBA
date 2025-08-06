{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH 
,
MAP_CSE_APPT_GNRC_DATE_CR AS (SELECT Promise_Type,change_cat_id,CHNG_REAS_TYPE_C,EXPY_D FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_GNRC_DATE_CR WHERE CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd');)


SELECT * FROM MAP_CSE_APPT_GNRC_DATE_CR