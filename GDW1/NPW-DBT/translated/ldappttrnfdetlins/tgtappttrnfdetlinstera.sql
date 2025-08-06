{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='append', tags=['LdApptTrnfDetlIns']) }}

SELECT
	APPT_I
	APPT_TRNF_I
	EFFT_D
	TRNF_OPTN_C
	TRNF_A
	CNCY_C
	CMPE_I
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptTrnfDetlInsertDS') }}