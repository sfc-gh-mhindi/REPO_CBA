{{ config(materialized='view', tags=['LdMAP_CSE_FEE_CAPLLkp']) }}

WITH 
,
SrcMAP_CSE_FEE_CAPL_FTera AS (SELECT PL_CAPL_FEE_CAT_ID,FEE_CAPL_F,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_FEE_CAPL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_FEE_CAPL_FTera