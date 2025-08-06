{{ config(materialized='view', tags=['LdMAP_CSE_CMS_PDCTLkp']) }}

WITH 
,
SrcMAP_CMS_PDCTTera AS (SELECT MAP_CMS_PDCT.CRIS_PDCT_CAT_ID,
MAP_CMS_PDCT.CRIS_PDCT_C,
MAP_CMS_PDCT.CRIS_DESC,
MAP_CMS_PDCT.ACCT_I_PRFX,
MAP_CMS_PDCT.EFFT_D,
MAP_CMS_PDCT.EXPY_D
FROM {{ var('GDW_ACCT_VW') }}.MAP_CMS_PDCT as MAP_CMS_PDCT
WHERE MAP_CMS_PDCT.efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' 
and MAP_CMS_PDCT.expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}')


SELECT * FROM SrcMAP_CMS_PDCTTera