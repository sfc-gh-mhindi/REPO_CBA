{{ config(materialized='view', tags=['LdMAP_CSE_APPT_QSTNLkp']) }}

WITH 
,
SrcMAP_CSE_APPT_QSTNTera AS (SELECT QSTN_ID,ROW_SECU_F,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_QSTN WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_QSTNTera