{{ config(materialized='incremental', alias='appt_pdct_paty', incremental_strategy='append', tags=['LdApptPdctPatyIns']) }}

SELECT
	APPT_PDCT_I
	PATY_I
	PATY_ROLE_C
	EFFT_D
	SRCE_SYST_C
	SRCE_SYST_APPT_PDCT_PATY_I
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptPdctPatyInsertDS') }}