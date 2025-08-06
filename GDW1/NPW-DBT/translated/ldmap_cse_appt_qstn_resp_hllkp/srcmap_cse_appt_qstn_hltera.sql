{{ config(materialized='view', tags=['LdMAP_CSE_APPT_QSTN_RESP_HLLkp']) }}

WITH 
,
SrcMAP_CSE_APPT_QSTN_HLTera AS (SELECT QA_ANSWER_ID,RESP_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_QSTN_RESP_HL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_QSTN_HLTera