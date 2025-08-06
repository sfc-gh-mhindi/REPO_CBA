{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptPdctAcctUpd']) }}

SELECT
	APPT_PDCT_I
	ACCT_I
	REL_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptAcctUpdateDS') }}