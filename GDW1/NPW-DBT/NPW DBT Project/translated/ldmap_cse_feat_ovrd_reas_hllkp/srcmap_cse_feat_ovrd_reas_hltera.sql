{{ config(materialized='view', tags=['LdMAP_CSE_FEAT_OVRD_REAS_HLLkp']) }}

WITH 
,
SrcMAP_CSE_FEAT_OVRD_REAS_HLTera AS (SELECT HL_PROD_INT_MARG_CAT_ID,OVRD_REAS_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_FEAT_OVRD_REAS_HL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_FEAT_OVRD_REAS_HLTera