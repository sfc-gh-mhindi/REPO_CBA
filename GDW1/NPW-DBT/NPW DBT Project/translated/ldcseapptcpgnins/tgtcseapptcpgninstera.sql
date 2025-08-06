{{ config(materialized='incremental', alias='cse_appt_cpgn', incremental_strategy='append', tags=['LdCseApptCpgnIns']) }}

SELECT
	APPT_I
	CSE_CPGN_CODE_X
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('TgtCseApptCpgnInsertDS') }}