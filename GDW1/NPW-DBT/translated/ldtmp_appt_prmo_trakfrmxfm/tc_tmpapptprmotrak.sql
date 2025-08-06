{{ config(materialized='incremental', alias='__var_dbt_pgdw_stag_db__.__var_dbt_pctmp_table_name__', incremental_strategy='append', tags=['LdTMP_APPT_PRMO_TRAKFrmXfm']) }}

SELECT
	APPT_PDCT_I
	TRAK_I
	MOD_DATE
	RECD_IND 
FROM {{ ref('fn_CombInsDelRecds') }}