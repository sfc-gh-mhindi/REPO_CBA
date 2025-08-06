{{ config(materialized='view', tags=['LdMAP_CSE_APPT_CMPELkp']) }}

WITH 
,
SrcMAP_CSE_APPT_CMPETera AS (SELECT BAL_XFER_INSN_CAT_ID,CMPE_I,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_CMPE WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_CMPETera