{{ config(materialized='incremental', alias='appt_pdct_purp', incremental_strategy='append', tags=['LdApptPdctPurp_Ins']) }}

SELECT
	APPT_PDCT_I
	EFFT_D
	SRCE_SYST_APPT_PDCT_PURP_I
	PURP_TYPE_C
	PURP_CLAS_C
	SRCE_SYST_C
	PURP_A
	CNCY_C
	MAIN_PURP_F
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptPdctPurpInsertDS') }}