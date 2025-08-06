{{ config(materialized='incremental', alias='__var_dbt_pgdw_load_db__.__var_dbt_pctable_name__', incremental_strategy='append', tags=['LdApptPrmoTrakIns']) }}

SELECT
	APPT_PDCT_I
	TRAK_I
	TRAK_IDNN_TYPE_C
	PRVD_D
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('sq_ApptPrmoTrakIns') }}