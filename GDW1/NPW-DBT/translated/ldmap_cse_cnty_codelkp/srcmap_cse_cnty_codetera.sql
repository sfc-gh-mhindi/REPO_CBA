{{ config(materialized='view', tags=['LdMAP_CSE_CNTY_CODELkp']) }}

WITH 
,
SrcMAP_CSE_CNTY_CODETera AS (SELECT DOCU_COLL_CNTY_ID,ISO_CNTY_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_CNTY WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_CNTY_CODETera