{{ config(materialized='view', tags=['LdMAP_CSE_STATELkp']) }}

WITH 
,
SrcMAP_CSE_STATETera AS (SELECT STATE_ID,STAT_X,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_STATE WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_STATETera