{{ config(materialized='view', tags=['XfmHL_APPFrmExt']) }}

WITH 
,
MAP_CSE_APPT_PDCT_FEAT AS (SELECT  PEXA_FLAG, FEAT_VALU_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_PDCT_FEAT  WHERE 

CAST('{{ var('ETL_PROCESS_DT') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd');)


SELECT * FROM MAP_CSE_APPT_PDCT_FEAT