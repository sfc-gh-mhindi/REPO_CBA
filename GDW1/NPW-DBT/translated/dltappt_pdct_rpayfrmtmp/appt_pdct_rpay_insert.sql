{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_appt__pdct__rpay__i__cse__com__cpo__bus__ncpr__clnt__20110124', incremental_strategy='insert_overwrite', tags=['DltAppt_Pdct_RpayFrmTMP']) }}

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
	SRCE_SYST_C,
	RPAY_SRCE_C,
	RPAY_SRCE_OTHR_X 
FROM {{ ref('RmvdupFrmTrans') }}