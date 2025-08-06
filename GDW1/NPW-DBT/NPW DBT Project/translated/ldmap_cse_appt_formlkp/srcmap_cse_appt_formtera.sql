{{ config(materialized='view', tags=['LdMAP_CSE_APPT_FORMLkp']) }}

WITH 
,
SrcMAP_CSE_APPT_FORMTera AS (SELECT CCL_FORM_CAT_ID,APPT_FORM_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_FORM WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_FORMTera