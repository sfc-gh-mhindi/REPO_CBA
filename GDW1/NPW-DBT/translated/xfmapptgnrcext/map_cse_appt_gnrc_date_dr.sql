{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH 
,
MAP_CSE_APPT_GNRC_DATE_DR AS (SELECT DATE_ROLE_C,Promise_Type FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_GNRC_DATE_DR WHERE CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd');)


SELECT * FROM MAP_CSE_APPT_GNRC_DATE_DR