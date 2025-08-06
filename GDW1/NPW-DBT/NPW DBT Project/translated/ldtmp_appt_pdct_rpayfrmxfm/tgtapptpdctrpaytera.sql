{{ config(materialized='incremental', alias='tmp_appt_pdct_rpay', incremental_strategy='append', tags=['LdTMP_APPT_PDCT_RPAYFrmXfm']) }}

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
	RUN_STRM 
FROM {{ ref('SrcApptPdctRpayDS') }}