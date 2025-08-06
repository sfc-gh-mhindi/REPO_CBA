{{ config(materialized='incremental', alias='appt_docu', incremental_strategy='append', tags=['LdApptDocuIns']) }}

SELECT
	APPT_I
	DOCU_C
	SRCE_SYST_C
	DOCU_VERS_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('Cpy') }}