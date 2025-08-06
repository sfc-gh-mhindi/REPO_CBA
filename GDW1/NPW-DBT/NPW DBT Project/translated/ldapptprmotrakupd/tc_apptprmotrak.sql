{{ config(materialized='incremental', alias='__var_dbt_pgdw_load_db__.__var_dbt_pctable_name__', incremental_strategy='append', tags=['LdApptPrmoTrakUpd']) }}

SELECT
	APPT_PDCT_I
	TRAK_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('sq_ApptPrmoTrakUpd') }}