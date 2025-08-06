{{ config(materialized='view', tags=['LdMAP_CSE_PL_ACQR_TYPELkp']) }}

WITH 
,
SrcMAP_CSE_PL_ACQR_TYPETera AS (SELECT PL_CMPN_CAT_ID,ACQR_TYPE_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_PL_ACQR_TYPE WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_PL_ACQR_TYPETera