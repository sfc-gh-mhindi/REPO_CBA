{{ config(materialized='incremental', alias='acct_appt_pdct', incremental_strategy='append', tags=['LdAcctApptPdct_Ins']) }}

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