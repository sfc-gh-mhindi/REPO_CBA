{{ config(materialized='view', tags=['LdMAP_CSE_OVRD_FEE_FRQ_CL_Lkp']) }}

WITH 
,
SrcMAP_CSE_OVRD_FEE_FRQ_CLTera AS (SELECT OVRD_FEE_PCT_FREQ,FREQ_IN_MTHS,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_OVRD_FEE_FRQ_CL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_OVRD_FEE_FRQ_CLTera