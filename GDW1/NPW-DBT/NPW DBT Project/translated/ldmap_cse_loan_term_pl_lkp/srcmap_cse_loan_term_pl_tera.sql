{{ config(materialized='view', tags=['LdMAP_CSE_LOAN_TERM_PL_Lkp']) }}

WITH 
,
SrcMAP_CSE_LOAN_TERM_PL_Tera AS (SELECT PL_PROD_TERM_CAT_ID,ACTL_VALU_Q,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_LOAN_TERM_PL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_LOAN_TERM_PL_Tera