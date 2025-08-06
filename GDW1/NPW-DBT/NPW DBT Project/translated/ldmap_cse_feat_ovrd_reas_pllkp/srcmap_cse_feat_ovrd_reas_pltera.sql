{{ config(materialized='view', tags=['LdMAP_CSE_FEAT_OVRD_REAS_PLLkp']) }}

WITH 
,
SrcMAP_CSE_FEAT_OVRD_REAS_PLTera AS (SELECT MARG_REAS_CAT_ID,OVRD_REAS_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_FEAT_OVRD_REAS_PL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_FEAT_OVRD_REAS_PLTera