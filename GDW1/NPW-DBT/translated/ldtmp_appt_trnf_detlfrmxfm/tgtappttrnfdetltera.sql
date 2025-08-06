{{ config(materialized='incremental', alias='tmp_appt_trnf_detl', incremental_strategy='append', tags=['LdTMP_APPT_TRNF_DETLFrmXfm']) }}

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
	RUN_STRM 
FROM {{ ref('SrcApptTrnfDetlDS') }}