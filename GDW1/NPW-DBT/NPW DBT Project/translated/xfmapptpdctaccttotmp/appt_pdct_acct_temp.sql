{{ config(materialized='incremental', alias='appt_pdct_acct_temp', incremental_strategy='append', tags=['XfmApptPdctAcctToTmp']) }}

SELECT
	APPT_PDCT_I
	ACCOUNT_NUMBER
	REPAYMENT_ACCOUNT_NUMBER
	REL_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	ERR_FLG 
FROM {{ ref('Funnel') }}