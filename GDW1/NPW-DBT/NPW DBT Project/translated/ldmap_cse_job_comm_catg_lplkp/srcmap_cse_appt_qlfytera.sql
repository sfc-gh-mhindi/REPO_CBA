{{ config(materialized='view', tags=['LdMAP_CSE_JOB_COMM_CATG_LPLkp']) }}

WITH 
,
SrcMAP_CSE_APPT_QLFYTera AS (SELECT CLP_JOB_FAMILY_CAT_ID,JOB_COMM_CATG_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_JOB_COMM_CATG WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_QLFYTera