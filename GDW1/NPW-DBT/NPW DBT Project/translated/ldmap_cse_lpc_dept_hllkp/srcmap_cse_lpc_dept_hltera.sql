{{ config(materialized='view', tags=['LdMAP_CSE_LPC_DEPT_HLLkp']) }}

WITH 
,
SrcMAP_CSE_LPC_DEPT_HLTera AS (SELECT LPC_OFFICE,DEPT_I,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_LPC_DEPT_HL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_LPC_DEPT_HLTera