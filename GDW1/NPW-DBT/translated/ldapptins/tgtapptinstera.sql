{{ config(materialized='incremental', alias='appt', incremental_strategy='append', tags=['LdApptIns']) }}

SELECT
	APPT_I
	APPT_C
	APPT_FORM_C
	APPT_QLFY_C
	STUS_TRAK_I
	APPT_ORIG_C
	APPT_N
	SRCE_SYST_C
	SRCE_SYST_APPT_I
	APPT_CRAT_D
	RATE_SEEK_F
	PROS_KEY_EFFT_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptInsertDS') }}