{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH 
,
SrcMAP_CSE_ORIG_SRCE_SYST_HMTeraVw AS (select   HL_BUSN_CHNL_CAT_I,ORIG_APPT_SRCE_SYST_C FROM    {{ var('GDW_ACCT_VW') }}.MAP_CSE_ORIG_APPT_SRCE_HM    where

 EFFT_D (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and EXPY_D (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_ORIG_SRCE_SYST_HMTeraVw