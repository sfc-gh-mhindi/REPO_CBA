{{ config(materialized='view', tags=['LdMAP_CSE_FNDD_METHLkp']) }}

WITH 
,
SrcMAP_CSE_FNDD_METHTera AS (SELECT FNDD_METH_CAT_ID,FNDD_INSS_METH_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_FNDD_METH WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_FNDD_METHTera