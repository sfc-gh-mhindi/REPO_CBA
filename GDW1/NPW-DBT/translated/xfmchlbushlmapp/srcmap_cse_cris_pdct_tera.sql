{{ config(materialized='view', tags=['XfmChlBusHlmApp']) }}

WITH 
,
SrcMAP_CSE_CRIS_PDCT_Tera AS (SELECT CRIS_PDCT_ID,ACCT_QLFY_C,SRCE_SYST_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_CRIS_PDCT WHERE CAST('{{ var('ETL_PROCESS_DT') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd');)


SELECT * FROM SrcMAP_CSE_CRIS_PDCT_Tera