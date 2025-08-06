{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='append', tags=['LdApptPdctAcct_Ins']) }}

SELECT
	APPT_PDCT_I
	ACCT_I
	REL_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptInsertDS') }}