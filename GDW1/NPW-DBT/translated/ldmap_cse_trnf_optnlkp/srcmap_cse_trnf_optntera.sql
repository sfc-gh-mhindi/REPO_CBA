{{ config(materialized='view', tags=['LdMAP_CSE_TRNF_OPTNLkp']) }}

WITH 
,
SrcMAP_CSE_TRNF_OPTNTera AS (SELECT BAL_XFER_OPTN_CAT_ID,TRNF_OPTN_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_TRNF_OPTN WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_TRNF_OPTNTera