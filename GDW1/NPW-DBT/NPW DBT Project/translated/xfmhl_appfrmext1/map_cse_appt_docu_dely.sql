{{ config(materialized='view', tags=['XfmHL_APPFrmExt1']) }}

WITH 
,
MAP_CSE_APPT_DOCU_DELY AS (SELECT  Trim(EXEC_DOCU_RECV_TYPE) as EXEC_DOCU_RECV_TYPE, DOCU_DELY_RECV_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_DOCU_DELY  WHERE 

CAST('{{ var('ETL_PROCESS_DT') }}' as date format 'yyyymmdd') BETWEEN EFFT_D (date, format 'yyyymmdd') AND EXPY_D (date, format 'yyyymmdd');)


SELECT * FROM MAP_CSE_APPT_DOCU_DELY