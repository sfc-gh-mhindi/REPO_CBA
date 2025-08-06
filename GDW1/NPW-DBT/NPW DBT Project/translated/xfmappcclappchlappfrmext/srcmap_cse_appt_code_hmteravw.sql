{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH 
,
SrcMAP_CSE_APPT_CODE_HMTeraVw AS (select   HLM_APPT_TYPE_CATG_I as HLM_APP_TYPE_CAT_ID, APPT_C  FROM    {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_CODE_HM    where

 efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_CODE_HMTeraVw