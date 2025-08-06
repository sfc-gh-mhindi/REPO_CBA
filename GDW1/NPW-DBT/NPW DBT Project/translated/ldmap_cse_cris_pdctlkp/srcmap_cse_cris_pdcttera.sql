{{ config(materialized='view', tags=['LdMAP_CSE_CRIS_PDCTLkp']) }}

WITH 
,
SrcMAP_CSE_CRIS_PDCTTera AS (SELECT CRIS_PDCT_ID,ACCT_QLFY_C,SRCE_SYST_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_CRIS_PDCT WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_CRIS_PDCTTera