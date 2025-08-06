{{ config(materialized='view', tags=['LdMAP_CSE_LOAN_FNDD_METHLkp']) }}

WITH 
,
SrcMAP_CSE_LOAN_FNDD_METHTera AS (SELECT PL_TARG_CAT_ID,LOAN_FNDD_METH_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_LOAN_FNDD_METH WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_LOAN_FNDD_METHTera