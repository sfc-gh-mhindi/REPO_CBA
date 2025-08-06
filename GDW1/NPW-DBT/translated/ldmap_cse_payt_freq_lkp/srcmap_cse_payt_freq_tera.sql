{{ config(materialized='view', tags=['LdMAP_CSE_PAYT_FREQ_Lkp']) }}

WITH 
,
SrcMAP_CSE_PAYT_FREQ_Tera AS (SELECT HL_RPAY_PERD_CAT_ID,PAYT_FREQ_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_PAYT_FREQ WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_PAYT_FREQ_Tera