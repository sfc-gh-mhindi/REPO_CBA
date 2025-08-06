{{ config(materialized='view', tags=['LdMAP_CSE_ORIG_SRCE_SYS_CLkp']) }}

WITH 
,
SrcMAP_CSE_ORIG_SRCE_SYS_CTera AS (SELECT orig_srce_syst_i,ORIG_APPT_SRCE_C,efft_d,expy_d FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_ORIG_APPT_SRCE WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_ORIG_SRCE_SYS_CTera