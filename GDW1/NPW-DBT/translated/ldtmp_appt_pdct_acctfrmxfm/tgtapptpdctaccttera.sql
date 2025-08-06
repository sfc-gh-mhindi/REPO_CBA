{{ config(materialized='incremental', alias='tmp_appt_pdct_acct', incremental_strategy='append', tags=['LdTMP_APPT_PDCT_ACCTFrmXfm']) }}

SELECT
	APPT_PDCT_I
	ACCT_I
	REL_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('SrcApptPdctAcctDS') }}