{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='append', tags=['LdApptPdctRpayIns']) }}

SELECT
	APPT_PDCT_I
	RPAY_TYPE_C
	EFFT_D
	PAYT_FREQ_C
	STRT_RPAY_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptPdctRpayInsertDS') }}