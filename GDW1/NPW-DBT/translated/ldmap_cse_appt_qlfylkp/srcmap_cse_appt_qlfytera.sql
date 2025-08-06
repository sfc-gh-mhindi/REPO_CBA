{{ config(materialized='view', tags=['LdMAP_CSE_APPT_QLFYLkp']) }}

WITH 
,
SrcMAP_CSE_APPT_QLFYTera AS (SELECT SBTY_CODE,APPT_QLFY_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_QLFY WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_APPT_QLFYTera