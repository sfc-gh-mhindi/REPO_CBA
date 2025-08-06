{{ config(materialized='view', tags=['LdMAP_CSE_PACK_PDCT_PLLkp']) }}

WITH 
,
SrcMAP_CSE_PACK_PDCT_PLTera AS (SELECT MAP_CSE_PACK_PDCT_PL.PL_PACK_CAT_ID,
MAP_CSE_PACK_PDCT_PL.PDCT_N,
MAP_CSE_PACK_PDCT_PL.EFFT_D,
MAP_CSE_PACK_PDCT_PL.EXPY_D 
FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_PACK_PDCT_PL as MAP_CSE_PACK_PDCT_PL
WHERE MAP_CSE_PACK_PDCT_PL.efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' 
and MAP_CSE_PACK_PDCT_PL.expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}')


SELECT * FROM SrcMAP_CSE_PACK_PDCT_PLTera