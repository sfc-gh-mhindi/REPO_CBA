{{ config(materialized='incremental', alias='appt_paty', incremental_strategy='append', tags=['LdApptPatyIns']) }}

SELECT
	APPT_I
	PATY_I
	EFFT_D
	EXPY_D
	REL_C
	REL_REAS_C
	REL_STUS_C
	REL_LEVL_C
	SRCE_SYST_C
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptInsertDS') }}