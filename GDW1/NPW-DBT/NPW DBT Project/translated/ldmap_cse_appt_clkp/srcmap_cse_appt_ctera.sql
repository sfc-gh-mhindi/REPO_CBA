{{ config(materialized='view', tags=['LdMAP_CSE_APPT_CLkp']) }}

WITH 
,
SrcMAP_CSE_APPT_CTera AS (SELECT CCL_APP_CAT_ID,APPT_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_C WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_CTera