{{ config(materialized='view', tags=['LdMAP_CSE_ADRS_TYPELkp']) }}

WITH 
,
SrcMAP_CSE_ADRS_TYPETera AS (SELECT ADRS_TYPE_ID,PYAD_TYPE_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_ADRS_TYPE WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_ADRS_TYPETera