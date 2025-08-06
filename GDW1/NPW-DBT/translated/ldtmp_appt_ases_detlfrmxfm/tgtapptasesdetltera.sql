{{ config(materialized='incremental', alias='tmp_appt_ases_detl', incremental_strategy='append', tags=['LdTMP_APPT_ASES_DETLFrmXfm']) }}

SELECT
	APPT_I
	AMT_TYPE_C
	EFFT_D
	EXPY_D
	CNCY_C
	APPT_ASES_A
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('SrcApptAsesDetlDS') }}