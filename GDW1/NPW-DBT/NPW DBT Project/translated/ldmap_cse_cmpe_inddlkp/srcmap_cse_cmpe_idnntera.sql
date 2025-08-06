{{ config(materialized='view', tags=['LdMAP_CSE_CMPE_INDDLkp']) }}

WITH 
,
SrcMAP_CSE_CMPE_IDNNTera AS (SELECT INSN_ID,CMPE_I,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_CMPE_IDNN WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_CMPE_IDNNTera