{{ config(materialized='view', tags=['LdMAP_CSE_UNID_PATY_CATG_Lkp']) }}

WITH 
,
SrcMAP_CSE_UNID_PATY_CATG_Tera AS (SELECT TP_BROK_GRUP_CAT_ID,UNID_PATY_CATG_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_UNID_PATY_CATG_PL WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_UNID_PATY_CATG_Tera