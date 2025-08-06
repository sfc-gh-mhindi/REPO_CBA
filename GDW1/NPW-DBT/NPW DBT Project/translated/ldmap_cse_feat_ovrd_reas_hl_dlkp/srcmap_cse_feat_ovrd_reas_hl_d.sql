{{ config(materialized='view', tags=['LdMAP_CSE_FEAT_OVRD_REAS_HL_DLkp']) }}

WITH 
,
SrcMAP_CSE_FEAT_OVRD_REAS_HL_D AS (SELECT HL_FEE_DISCOUNT_CAT_ID,OVRD_REAS_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_FEAT_OVRD_REAS_HL_D WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_FEAT_OVRD_REAS_HL_D