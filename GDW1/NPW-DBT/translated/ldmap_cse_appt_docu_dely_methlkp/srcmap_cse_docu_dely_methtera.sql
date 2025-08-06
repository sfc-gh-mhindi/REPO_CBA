{{ config(materialized='view', tags=['LdMAP_CSE_APPT_DOCU_DELY_METHLkp']) }}

WITH 
,
SrcMAP_CSE_DOCU_DELY_METHTera AS (SELECT DOCU_COLL_CAT_ID,DOCU_DELY_METH_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_DOCU_METH WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMAP_CSE_DOCU_DELY_METHTera