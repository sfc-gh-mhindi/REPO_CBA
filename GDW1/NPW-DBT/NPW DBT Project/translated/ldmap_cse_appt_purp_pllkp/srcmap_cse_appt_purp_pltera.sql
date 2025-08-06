{{ config(materialized='view', tags=['LdMAP_CSE_APPT_PURP_PLLkp']) }}

WITH 
,
SrcMAP_CSE_APPT_PURP_PLTera AS (SELECT PL_PROD_PURP_CAT_ID,PURP_TYPE_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_PURP_PL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_PURP_PLTera