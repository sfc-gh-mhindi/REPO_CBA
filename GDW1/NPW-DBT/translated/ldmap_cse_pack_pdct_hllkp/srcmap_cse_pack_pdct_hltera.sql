{{ config(materialized='view', tags=['LdMAP_CSE_PACK_PDCT_HLLkp']) }}

WITH 
,
SrcMAP_CSE_PACK_PDCT_HLTera AS (SELECT HL_PACK_CAT_ID,PDCT_N,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_PACK_PDCT_HL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_PACK_PDCT_HLTera