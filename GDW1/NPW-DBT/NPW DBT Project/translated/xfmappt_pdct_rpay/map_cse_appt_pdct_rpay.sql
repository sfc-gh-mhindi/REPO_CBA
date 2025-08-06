{{ config(materialized='view', tags=['XfmAppt_Pdct_Rpay']) }}

WITH 
,
MAP_CSE_APPT_PDCT_RPAY AS (SELECT cast(PO_REPAYMENT_SOURCE_CAT_ID as varchar(4)),RPAY_SRCE_C FROM {{ var('pGDW_TECH_DB') }}.MAP_CSE_APPT_PDCT_RPAY WHERE  CAST('{{ var('pRUN_STRM_PROS_D') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd'))


SELECT * FROM MAP_CSE_APPT_PDCT_RPAY