{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__cpl__bus__app__prod__appt__pdct__rpay', incremental_strategy='insert_overwrite', tags=['XfmPL_APP_PRODFrmExt']) }}

SELECT
	APPT_PDCT_I,
	RPAY_TYPE_C,
	EFFT_D,
	PAYT_FREQ_C,
	STRT_RPAY_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutApptPdctRpayDS') }}