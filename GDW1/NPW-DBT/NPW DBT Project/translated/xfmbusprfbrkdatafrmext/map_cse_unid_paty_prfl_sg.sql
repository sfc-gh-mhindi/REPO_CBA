{{ config(materialized='view', tags=['XfmBusPrfBrkDataFrmExt']) }}

WITH 
,
MAP_CSE_UNID_PATY_PRFL_SG AS (SELECT SUB_GRDE,SUB_GRDE_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_UNID_PATY_PRFL_SG WHERE CAST('{{ var('ETL_PROCESS_DT') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd');)


SELECT * FROM MAP_CSE_UNID_PATY_PRFL_SG