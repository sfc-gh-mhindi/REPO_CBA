{{ config(materialized='incremental', alias='_cba__app_csel4_dev_outbound_cse__com__bus__ccl__chl__com__app__appt__orig__20130605__u', incremental_strategy='insert_overwrite', tags=['DltAPPT_ORIGFrmTMP_APPR_ORIG']) }}

SELECT
	APPT_I,
	APPT_ORIG_CATG_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('Trans__ln_expy_update') }}