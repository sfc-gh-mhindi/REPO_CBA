{{ config(materialized='incremental', alias='appt_pdct_cpgn', incremental_strategy='append', tags=['LdApptPdctCpgnIns']) }}

SELECT
	APPT_PDCT_I
	CPGN_TYPE_C
	CPGN_I
	REL_C
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('TgtApptPdctCpgnInsertDS') }}